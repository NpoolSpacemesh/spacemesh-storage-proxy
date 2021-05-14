package main

import (
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
	"github.com/NpoolChia/chia-storage-proxy/db"
	"github.com/NpoolChia/chia-storage-proxy/task"
	"github.com/NpoolChia/chia-storage-proxy/types"
	"github.com/NpoolChia/chia-storage-server/chiaapi"
	apitypes "github.com/NpoolChia/chia-storage-server/types"
	httpdaemon "github.com/NpoolRD/http-daemon"
	"github.com/boltdb/bolt"
	"github.com/fsnotify/fsnotify"
)

type StorageProxyConfig struct {
	DBPath         string   `json:"db_path"`
	LocalPlot      bool     `json:"localplot"`
	LocalHost      string   `json:"host"`
	Port           int      `json:"port"`
	FileServerPort int      `json:"file_server_port"`
	StorageHosts   []string `json:"storage_hosts"`
}

type StorageProxy struct {
	config       StorageProxyConfig
	curHostIndex int
	mutex        sync.Mutex
}

// watcherCfgFile 监测配置文件变更
func (p *StorageProxy) watcherCfgFile(cfgFile string) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatalf(log.Fields{}, "watch config file %v", err)
	}
	defer watcher.Close()
	err = watcher.Add(cfgFile)
	if err != nil {
		log.Fatalf(log.Fields{}, "add watch config file path %v", err)
	}
	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				continue
			}
			// 1. 文件修改 2. 文件被删除，放入新文件
			if event.Op&fsnotify.Write == fsnotify.Write ||
				event.Op&fsnotify.Chmod == fsnotify.Chmod {
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
				}()
			}
		case err, ok := <-watcher.Errors:
			if !ok {
				continue
			}
			log.Errorf(log.Fields{}, "watch config file %v", err)
		}
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
		_, err = chiaapi.UploadChiaPlot(host, "18080", apitypes.UploadPlotInput{
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
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(path, ".plot") {
			log.Infof(log.Fields{}, "%v | %v is not regular plot file", input.PlotDir, path)
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
				return fmt.Errorf("chia plot file url: %s already added", plotUrl)
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
		if r != nil {
			return fmt.Errorf("chia plot file url: %s already added", input.PlotFile)
		}
		meta := task.Meta{}
		if err := json.Unmarshal(r, &meta); err != nil {
			return nil
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
			return fmt.Errorf("chia plot file url: %s already added", input.PlotFile)
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
