package main

import (
	"encoding/json"
	log "github.com/EntropyPool/entropy-logger"
	"github.com/NpoolChia/chia-storage-proxy/types"
	"github.com/NpoolRD/http-daemon"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type StorageProxyConfig struct {
	Port         int      `json:"port"`
	StorageHosts []string `json:"storage_hosts"`
}

type StorageProxy struct {
	config       StorageProxyConfig
	mutex        sync.Mutex
	postingHosts map[string]int
}

func NewStorageProxy(cfgFile string) *StorageProxy {
	proxy := &StorageProxy{
		postingHosts: map[string]int{},
	}

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

	return proxy
}

func (p *StorageProxy) Run() error {
	httpdaemon.RegisterRouter(httpdaemon.HttpRouter{
		Location: types.NewPlotAPI,
		Handler:  p.NewPlotRequest,
		Method:   "POST",
	})

	httpdaemon.Run(p.config.Port)

	return nil
}

func (p *StorageProxy) postPlotFile(file string) error {
	minPosting := 100
	postHost := ""

	p.mutex.Lock()
	for _, host := range p.config.StorageHosts {
		postCount, ok := p.postingHosts[host]
		if !ok {
			p.postingHosts[host] = 0
			postHost = host
			break
		}

		if postCount < minPosting {
			minPosting = postCount
			postHost = host
		}
	}
	p.postingHosts[postHost] += 1
	p.mutex.Unlock()

	log.Infof(log.Fields{}, "try to post file %v -> %v [%v]", file, postHost, minPosting)
	// TODO: post plot file to host

	p.mutex.Lock()
	p.postingHosts[postHost] -= 1
	if p.postingHosts[postHost] == 0 {
		delete(p.postingHosts, postHost)
	}
	p.mutex.Unlock()

	return nil
}

func (p *StorageProxy) NewPlotRequest(w http.ResponseWriter, req *http.Request) (interface{}, string, int) {
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return nil, err.Error(), -1
	}

	input := types.NewPlotInput{}
	err = json.Unmarshal(b, &input)
	if err != nil {
		return nil, err.Error(), -2
	}

	_, err = os.Stat(input.PlotDir)
	if err != nil {
		return nil, err.Error(), -3
	}

	err = filepath.Walk(input.PlotDir, func(path string, info os.FileInfo, err error) error {
		if !strings.HasSuffix(path, ".plot") {
			log.Infof(log.Fields{}, "%v | %v is not regular plot file", input.PlotDir, path)
			return nil
		}
		return p.postPlotFile(path)
	})
	if err != nil {
		return nil, err.Error(), -4
	}

	os.RemoveAll(input.PlotDir)

	return nil, "", 0
}
