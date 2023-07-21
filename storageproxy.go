package main

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	log "github.com/EntropyPool/entropy-logger"
	httpdaemon "github.com/NpoolRD/http-daemon"
	"github.com/NpoolSpacemesh/spacemesh-storage-proxy/db"
	"github.com/NpoolSpacemesh/spacemesh-storage-proxy/task"
	"github.com/NpoolSpacemesh/spacemesh-storage-proxy/types"
	"github.com/NpoolSpacemesh/spacemesh-storage-server/api"
	apitypes "github.com/NpoolSpacemesh/spacemesh-storage-server/types"
	"github.com/boltdb/bolt"
)

type StorageProxyConfig struct {
	DBPath         string   `json:"db_path"`
	LocalPlot      bool     `json:"localplot"`
	LocalHost      string   `json:"host"`
	Port           int      `json:"port"`
	FileServerPort int      `json:"file_server_port"`
	StorageHosts   []string `json:"storage_hosts"`
	PlotPaths      []string `json:"plot_paths"`
}

type StorageProxy struct {
	config       StorageProxyConfig
	curHostIndex int
	mutex        sync.Mutex
}

var (
	_md5 [md5.Size]byte
)

// watcherCfgFile 监测配置文件变更
func (p *StorageProxy) watcherCfgFile(cfgFile string) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	for range tick.C {
		// backup old file
		backup(cfgFile+".old", p.config)
		go func() {
			var err error
			defer func() {
				if err != nil {
					// will auto watch
					restore(cfgFile, cfgFile+".old")
				}
			}()

			cfg := StorageProxyConfig{}
			buf, err := ioutil.ReadFile(cfgFile)
			if err != nil {
				log.Errorf(log.Fields{}, "cannot read config file %v: %v", cfgFile, err)
				return
			}

			m5 := md5.Sum(buf)
			if m5 == _md5 {
				return
			}
			_md5 = m5
			err = json.Unmarshal(buf, &cfg)
			if err != nil {
				log.Errorf(log.Fields{}, "cannot parse config file %v: %v", cfgFile, err)
				return
			}

			if cfg.LocalPlot {
				cfg.LocalHost = "127.0.0.1"
			}
			rand.Seed(time.Now().UnixNano())
			p.mutex.Lock()
			p.config = cfg
			p.curHostIndex = rand.Intn(len(cfg.StorageHosts))
			p.mutex.Unlock()
			log.Infof(log.Fields{}, "config file %v", p.config.StorageHosts)
		}()
	}
}

// backup 备份
func backup(dst string, cfg StorageProxyConfig) error {
	_b, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(dst, _b, 0644)
}

// restore 恢复
func restore(dst, src string) error {
	_b, err := ioutil.ReadFile(src)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(dst, _b, 0644)
}

func NewStorageProxy(cfgFile string) *StorageProxy {
	proxy := &StorageProxy{}
	buf, err := ioutil.ReadFile(cfgFile)
	if err != nil {
		log.Errorf(log.Fields{}, "cannot read config file %v: %v", cfgFile, err)
		return nil
	}

	err = json.Unmarshal(buf, &proxy.config)
	if err != nil {
		log.Errorf(log.Fields{}, "cannot parse config file %v: %v", cfgFile, err)
		return nil
	}

	if proxy.config.LocalPlot {
		proxy.config.LocalHost = "127.0.0.1"
	}
	rand.Seed(time.Now().UnixNano())
	proxy.curHostIndex = rand.Intn(len(proxy.config.StorageHosts))

	// 监听文件变更
	go proxy.watcherCfgFile(cfgFile)

	return proxy
}

func (p *StorageProxy) serveFile() {
	http.Handle(task.PlotFileHandle, http.StripPrefix(task.PlotFileHandle, http.FileServer(http.Dir("/"))))
	for {
		log.Infof(log.Fields{}, "start file server at %v", p.config.FileServerPort)
		err := http.ListenAndServe(fmt.Sprintf(":%v", p.config.FileServerPort), nil)
		if err != nil {
			log.Errorf(log.Fields{}, "fail to listen plot file server %v: %v", p.config.FileServerPort, err)
		}
	}
}

func (p *StorageProxy) Run() error {
	httpdaemon.RegisterRouter(httpdaemon.HttpRouter{
		Location: types.NewPlotAPI,
		Handler:  p.NewPlotRequest,
		Method:   "POST",
	})
	httpdaemon.RegisterRouter(httpdaemon.HttpRouter{
		Location: types.FinishPlotAPI,
		Handler:  p.FinishPlotRequest,
		Method:   "POST",
	})
	httpdaemon.RegisterRouter(httpdaemon.HttpRouter{
		Location: types.FailPlotAPI,
		Handler:  p.FailPlotRequest,
		Method:   "POST",
	})

	httpdaemon.Run(p.config.Port)
	go p.serveFile()
	go p.indexer()

	return nil
}

func (p *StorageProxy) postPlotFile(file string) error {
	var err error

	for retries := 0; retries < len(p.config.StorageHosts); retries++ {
		p.mutex.Lock()
		selectedHostIndex := p.curHostIndex
		p.curHostIndex = (p.curHostIndex + 1) % len(p.config.StorageHosts)
		p.mutex.Unlock()
		host := p.config.StorageHosts[selectedHostIndex]

		if strings.HasPrefix(file, "/") {
			file = strings.Replace(file, "/", "", 1)
		}

		if p.config.LocalPlot {
			host = p.config.LocalHost
		}

		plotUrl := fmt.Sprintf("http://%v:%v%v/%v", p.config.LocalHost, p.config.FileServerPort, task.PlotFilePrefix, file)
		finishUrl := fmt.Sprintf("http://%v:%v%v", p.config.LocalHost, p.config.Port, types.FinishPlotAPI)
		failUrl := fmt.Sprintf("http://%v:%v%v", p.config.LocalHost, p.config.Port, types.FailPlotAPI)

		log.Infof(log.Fields{}, "try to serve file %v -> %v", plotUrl, host)
		_, err = api.UploadPlot(host, "18080", apitypes.UploadPlotInput{
			PlotURL:   plotUrl,
			FinishURL: finishUrl,
			FailURL:   failUrl,
		})
		if err != nil {
			log.Errorf(log.Fields{}, "fail to notify new plot -> %v", host)
			continue
		}

		break
	}

	return err
}

func (p *StorageProxy) indexPath(_path string) error {
	keys := []string{}
	err := filepath.Walk(_path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info == nil {
			log.Infof(log.Fields{}, "%v do not have valid info", path)
			return nil
		}
		if path == _path {
			return nil
		}
		if info.IsDir() {
			log.Infof(log.Fields{}, "index %v in %v", path, _path)
			keys = append(keys, path)
		}
		return nil
	})
	if err != nil {
		log.Errorf(log.Fields{}, "fail to notify new plot %v: %v", _path, err)
		return err
	}

	for _, key := range keys {
		if err := p.indexKey(key); err != nil {
			log.Errorf(log.Fields{}, "fail to index %v/%v: %v", _path, key, err)
		}
	}

	return nil
}

func (p *StorageProxy) indexKey(_path string) error {
	const progressFile = "progress.json"

	type progress struct {
		FileIndex int  `json:"file_index"`
		Completed bool `json:"complete"`
	}
	pg := progress{}
	b, err := ioutil.ReadFile(filepath.Join(_path, progressFile))
	if err == nil {
		if err := json.Unmarshal(b, &pg); err != nil {
			return err
		}
	}

	host := ""
	if !p.config.LocalPlot {
		err = filepath.Walk(_path, func(path string, info os.FileInfo, err error) error {
			if !strings.HasSuffix(path, ".bin") && !strings.HasSuffix(path, ".json") {
				return nil
			}
			if err != nil {
				return nil
			}
			if info == nil {
				log.Infof(log.Fields{}, "%v do not have valid info", path)
				return nil
			}
			if info.IsDir() {
				return nil
			}
			if info.Size() == 0 {
				return nil
			}

			file := path
			if strings.HasPrefix(path, "/") {
				file = strings.Replace(path, "/", "", 1)
			}
			plotUrl := fmt.Sprintf("http://%v:%v%v/%v", p.config.LocalHost, p.config.FileServerPort, task.PlotFilePrefix, file)

			bdb, err := db.BoltClient()
			if err != nil {
				log.Infof(log.Fields{}, "get bolt db client error %v", path)
				return nil
			}
			if err := bdb.Update(func(tx *bolt.Tx) error {
				bk := tx.Bucket(db.DefaultBucket)
				r := bk.Get([]byte(plotUrl))
				if r == nil {
					return nil
				}
				meta := task.Meta{}
				if err := json.Unmarshal(r, &meta); err != nil {
					return err
				}
				host = meta.Host
				return nil
			}); err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			log.Errorf(log.Fields{}, "fail to get host %v: %v", _path, err)
			return err
		}
	}

	plotUrls := []string{}
	err = filepath.Walk(_path, func(path string, info os.FileInfo, err error) error {
		if !strings.HasSuffix(path, ".bin") && !strings.HasSuffix(path, ".json") {
			return nil
		}
		if strings.Contains(path, progressFile) {
			return nil
		}
		if err != nil {
			return nil
		}
		if info == nil {
			log.Infof(log.Fields{}, "%v do not have valid info", path)
			return nil
		}
		if info.IsDir() {
			return nil
		}
		if info.Size() == 0 {
			return nil
		}

		file := path
		if strings.HasPrefix(path, "/") {
			file = strings.Replace(path, "/", "", 1)
		}

		if host == "" {
			p.mutex.Lock()
			selectedHostIndex := p.curHostIndex
			p.curHostIndex = (p.curHostIndex + 1) % len(p.config.StorageHosts)
			p.mutex.Unlock()
			host = p.config.StorageHosts[selectedHostIndex]

			if p.config.LocalPlot {
				host = p.config.LocalHost
			}
		}

		log.Infof(log.Fields{}, "index %v in %v to %v", path, _path, host)

		plotUrl := fmt.Sprintf("http://%v:%v%v/%v", p.config.LocalHost, p.config.FileServerPort, task.PlotFilePrefix, file)
		finishUrl := fmt.Sprintf("http://%v:%v%v", p.config.LocalHost, p.config.Port, types.FinishPlotAPI)
		failUrl := fmt.Sprintf("http://%v:%v%v", p.config.LocalHost, p.config.Port, types.FailPlotAPI)
		plotUrls = append(plotUrls, plotUrl)

		// 入库
		// 更新数据库的数据的状态
		bdb, err := db.BoltClient()
		if err != nil {
			log.Infof(log.Fields{}, "get bolt db client error %v", path)
			return nil
		}
		if err := bdb.Update(func(tx *bolt.Tx) error {
			bk := tx.Bucket(db.DefaultBucket)
			if r := bk.Get([]byte(plotUrl)); r != nil {
				return fmt.Errorf("spacemesh plot file url: %s already added", plotUrl)
			}
			meta := task.Meta{
				Status:    task.TaskTodo,
				Host:      host,
				PlotURL:   plotUrl,
				FinishURL: finishUrl,
				FailURL:   failUrl,
			}
			ms, err := json.Marshal(meta)
			if err != nil {
				return err
			}
			return bk.Put([]byte(plotUrl), ms)
		}); err != nil {
			log.Errorf(log.Fields{}, "%v fail to bolt database %v", plotUrl, err)
			return nil
		}

		return nil
	})
	if err != nil {
		log.Errorf(log.Fields{}, "fail to notify new plot %v: %v", _path, err)
		return err
	}

	if !pg.Completed {
		return nil
	}

	log.Infof(log.Fields{}, "path %v plot completed, check its status", _path)
	keyDone := true
	bdb, err := db.BoltClient()
	if err != nil {
		return err
	}

	for _, plotUrl := range plotUrls {
		if err := bdb.View(func(tx *bolt.Tx) error {
			bk := tx.Bucket(db.DefaultBucket)
			r := bk.Get([]byte(plotUrl))
			if r == nil {
				return fmt.Errorf("invalid ploturl %v", plotUrl)
			}
			meta := task.Meta{}
			if err := json.Unmarshal(r, &meta); err != nil {
				return err
			}
			if meta.Status != task.TaskDone {
				log.Infof(log.Fields{}, "%v plot completed but still fetching %v", _path, plotUrl)
				keyDone = false
			}
			return nil
		}); err != nil {
			return err
		}
		if !keyDone {
			return nil
		}
	}

	log.Infof(log.Fields{}, "path %v transfer done, try to remove it", _path)
	os.RemoveAll(_path)
	for _, plotUrl := range plotUrls {
		if err := bdb.Update(func(tx *bolt.Tx) error {
			bk := tx.Bucket(db.DefaultBucket)
			if err := bk.Delete([]byte(plotUrl)); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
	}

	return nil
}

func (p *StorageProxy) indexer() {
	ticker := time.NewTicker(1 * time.Minute)
	for range ticker.C {
		for _, _path := range p.config.PlotPaths {
			if err := p.indexPath(_path); err != nil {
				log.Errorf(log.Fields{}, "fail to index %v: %v", _path, err)
			}
		}
	}
}

func (p *StorageProxy) NewPlotRequest(w http.ResponseWriter, req *http.Request) (interface{}, string, int) {
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Errorf(log.Fields{}, "fail to read body %v: %v", req.URL, err)
		return nil, err.Error(), -1
	}

	input := types.NewPlotInput{}
	err = json.Unmarshal(b, &input)
	if err != nil {
		log.Errorf(log.Fields{}, "fail to parse new plot %v: %v", input.PlotDir, err)
		return nil, err.Error(), -2
	}

	_, err = os.Stat(input.PlotDir)
	if err != nil {
		log.Errorf(log.Fields{}, "fail to stat new plot %v: %v", input.PlotDir, err)
		return nil, err.Error(), -3
	}

	processed := false
	err = filepath.Walk(input.PlotDir, func(path string, info os.FileInfo, err error) error {
		if !strings.HasSuffix(path, ".plot") {
			return nil
		}
		if info == nil {
			log.Infof(log.Fields{}, "%v do not have valid info", path)
			return nil
		}
		if err != nil {
			return nil
		}
		if info.IsDir() {
			return nil
		}
		if info.Size() == 0 {
			return nil
		}
		processed = true

		var (
			file, host string
		)
		p.mutex.Lock()
		selectedHostIndex := p.curHostIndex
		p.curHostIndex = (p.curHostIndex + 1) % len(p.config.StorageHosts)
		p.mutex.Unlock()
		host = p.config.StorageHosts[selectedHostIndex]

		if strings.HasPrefix(path, "/") {
			file = strings.Replace(path, "/", "", 1)
		}

		if p.config.LocalPlot {
			host = p.config.LocalHost
		}

		plotUrl := fmt.Sprintf("http://%v:%v%v/%v", p.config.LocalHost, p.config.FileServerPort, task.PlotFilePrefix, file)
		finishUrl := fmt.Sprintf("http://%v:%v%v", p.config.LocalHost, p.config.Port, types.FinishPlotAPI)
		failUrl := fmt.Sprintf("http://%v:%v%v", p.config.LocalHost, p.config.Port, types.FailPlotAPI)

		// 入库
		// 更新数据库的数据的状态
		bdb, err := db.BoltClient()
		if err != nil {
			log.Infof(log.Fields{}, "get bolt db client error %v", path)
			return nil
		}
		if err := bdb.Update(func(tx *bolt.Tx) error {
			bk := tx.Bucket(db.DefaultBucket)
			if r := bk.Get([]byte(plotUrl)); r != nil {
				return fmt.Errorf("spacemesh plot file url: %s already added", plotUrl)
			}
			meta := task.Meta{
				Status:    task.TaskTodo,
				Host:      host,
				PlotURL:   plotUrl,
				FinishURL: finishUrl,
				FailURL:   failUrl,
			}
			ms, err := json.Marshal(meta)
			if err != nil {
				return err
			}
			return bk.Put([]byte(plotUrl), ms)
		}); err != nil {
			log.Errorf(log.Fields{}, "%v fail to bolt database %v", plotUrl, err)
			return nil
		}

		for {
			_, err := os.Stat(path)
			if err == nil {
				log.Infof(log.Fields{}, "Waiting for %v finish", path)
				time.Sleep(10 * time.Second)
				continue
			}
			if os.IsNotExist(err) {
				log.Infof(log.Fields{}, "%v finished", path)
				break
			}
			log.Errorf(log.Fields{}, "CANNOT determine %v's stat", path)
			break
		}

		return nil
	})
	if err != nil {
		log.Errorf(log.Fields{}, "fail to notify new plot %v: %v", input.PlotDir, err)
		return nil, err.Error(), -4
	}

	if !processed {
		log.Errorf(log.Fields{}, "cannot find suitable plot file in %v", input.PlotDir)
		return nil, "cannot find suitable plot file", -5
	}

	return nil, "", 0
}

func (p *StorageProxy) FinishPlotRequest(w http.ResponseWriter, req *http.Request) (interface{}, string, int) {
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Errorf(log.Fields{}, "fail to read body of %v", req.URL)
		return nil, err.Error(), -1
	}

	input := types.FinishPlotInput{}
	err = json.Unmarshal(b, &input)
	if err != nil {
		log.Errorf(log.Fields{}, "fail to parse body of %v: %v", req.URL, err)
		return nil, err.Error(), -2
	}
	// 更新数据库的数据的状态
	bdb, err := db.BoltClient()
	if err != nil {
		return nil, err.Error(), -3
	}
	if err := bdb.Update(func(tx *bolt.Tx) error {
		bk := tx.Bucket(db.DefaultBucket)
		r := bk.Get([]byte(input.PlotFile))
		if r == nil {
			return fmt.Errorf("spacemesh plot file %v not found", input.PlotFile)
		}
		meta := task.Meta{}
		if err := json.Unmarshal(r, &meta); err != nil {
			return err
		}
		meta.Status = task.TaskFinish
		ms, err := json.Marshal(meta)
		if err != nil {
			return err
		}
		return bk.Put([]byte(input.PlotFile), ms)
	}); err != nil {
		return nil, err.Error(), -4
	}

	return nil, "", 0
}

func (p *StorageProxy) FailPlotRequest(w http.ResponseWriter, req *http.Request) (interface{}, string, int) {
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return nil, err.Error(), -1
	}

	input := types.FailPlotInput{}
	err = json.Unmarshal(b, &input)
	if err != nil {
		return nil, err.Error(), -2
	}

	p.mutex.Lock()
	selectedHostIndex := p.curHostIndex
	p.curHostIndex = (p.curHostIndex + 1) % len(p.config.StorageHosts)
	p.mutex.Unlock()
	host := p.config.StorageHosts[selectedHostIndex]

	_u, err := url.Parse(input.PlotFile)
	if err != nil {
		return nil, err.Error(), -3
	}

	if p.config.LocalPlot {
		host = p.config.LocalHost
	}

	plotUrl := fmt.Sprintf("http://%v:%v%v", p.config.LocalHost, p.config.FileServerPort, _u.Path)
	finishUrl := fmt.Sprintf("http://%v:%v%v", p.config.LocalHost, p.config.Port, types.FinishPlotAPI)
	failUrl := fmt.Sprintf("http://%v:%v%v", p.config.LocalHost, p.config.Port, types.FailPlotAPI)

	// 更新数据库的数据的状态
	bdb, err := db.BoltClient()
	if err != nil {
		return nil, err.Error(), -4
	}
	if err := bdb.Update(func(tx *bolt.Tx) error {
		bk := tx.Bucket(db.DefaultBucket)
		r := bk.Get([]byte(input.PlotFile))
		if r == nil {
			return fmt.Errorf("spacemesh plot file %v not find", input.PlotFile)
		}

		// 删除原有的
		if err := bk.Delete([]byte(input.PlotFile)); err != nil {
			return err
		}

		// 重新选择别的存储节点
		meta := task.Meta{
			Status:    task.TaskTodo,
			Host:      host,
			PlotURL:   plotUrl,
			FailURL:   failUrl,
			FinishURL: finishUrl,
		}
		ms, err := json.Marshal(meta)
		if err != nil {
			return err
		}
		return bk.Put([]byte(plotUrl), ms)
	}); err != nil {
		return nil, err.Error(), -5
	}

	return nil, "", 0
}
