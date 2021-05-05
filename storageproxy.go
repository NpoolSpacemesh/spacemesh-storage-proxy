package main

import (
	"encoding/json"
	"fmt"
	log "github.com/EntropyPool/entropy-logger"
	"github.com/NpoolChia/chia-storage-proxy/types"
	"github.com/NpoolRD/http-daemon"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

type StorageProxyConfig struct {
	LocalHost      string   `json:"host"`
	Port           int      `json:"port"`
	FileServerPort int      `json:"file_server_port"`
	StorageHosts   []string `json:"storage_hosts"`
}

type StorageProxy struct {
	config StorageProxyConfig
}

const PlotFilePrefix = "/plotfile"
const PlotFileHandle = PlotFilePrefix + "/"

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

	return proxy
}

func (p *StorageProxy) serveFile() {
	http.Handle(PlotFileHandle, http.StripPrefix(PlotFileHandle, http.FileServer(http.Dir("/mnt"))))
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
	selectedHostIndex := rand.Intn(len(p.config.StorageHosts))
	host := p.config.StorageHosts[selectedHostIndex]

	url := fmt.Sprintf("http://%v:%v/%v", p.config.LocalHost, p.config.FileServerPort, strings.Replace(file, "/mnt", PlotFilePrefix, -1))
	log.Infof(log.Fields{}, "try to serve file %v -> %v [%v]", url, host)

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

	return nil, "", 0
}

func (p *StorageProxy) FinishPlotRequest(w http.ResponseWriter, req *http.Request) (interface{}, string, int) {
	b, err := ioutil.ReadAll(req.Body)
	if err != nil {
		return nil, err.Error(), -1
	}

	input := types.FinishPlotInput{}
	err = json.Unmarshal(b, &input)
	if err != nil {
		return nil, err.Error(), -2
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

	return nil, "", 0
}
