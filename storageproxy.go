package main

import (
	"encoding/json"
	"fmt"
	log "github.com/EntropyPool/entropy-logger"
	"github.com/NpoolChia/chia-storage-proxy/types"
	"github.com/NpoolChia/chia-storage-server/chiaapi"
	apitypes "github.com/NpoolChia/chia-storage-server/types"
	"github.com/NpoolRD/http-daemon"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

type StorageProxyConfig struct {
	LocalPlot      bool     `json:"localplot"`
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

	if proxy.config.LocalPlot {
		proxy.config.LocalHost = "127.0.0.1"
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
	var err error

	for retries := 0; retries < 5; retries++ {
		selectedHostIndex := rand.Intn(len(p.config.StorageHosts))
		host := p.config.StorageHosts[selectedHostIndex]

		if strings.HasPrefix(file, "/") {
			file = strings.Replace(file, "/", "", 1)
		}

		if p.config.LocalPlot {
			host = p.config.LocalHost
		}

		plotUrl := fmt.Sprintf("http://%v:%v%v/%v", p.config.LocalHost, p.config.FileServerPort, PlotFilePrefix, file)
		finishUrl := fmt.Sprintf("http://%v:%v%v", p.config.LocalHost, p.config.Port, types.FinishPlotAPI)
		failUrl := fmt.Sprintf("http://%v:%v%v", p.config.LocalHost, p.config.Port, types.FailPlotAPI)

		log.Infof(log.Fields{}, "try to serve file %v -> %v", plotUrl, host)
		_, err = chiaapi.UploadChiaPlot(fmt.Sprintf("%v:18080", host), apitypes.UploadPlotInput{
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
		if !strings.HasSuffix(path, ".plot") {
			log.Infof(log.Fields{}, "%v | %v is not regular plot file", input.PlotDir, path)
			return nil
		}
		processed = true
		return p.postPlotFile(path)
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
		return nil, err.Error(), -1
	}

	input := types.FinishPlotInput{}
	err = json.Unmarshal(b, &input)
	if err != nil {
		return nil, err.Error(), -2
	}

	files := strings.Split(input.PlotFile, PlotFilePrefix)
	if len(files) < 2 {
		return nil, "invalid file description", -3
	}

	log.Infof(log.Fields{}, "remove finish plot file %v", filepath.Dir(files[1]))
	os.RemoveAll(filepath.Dir(files[1]))

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

	files := strings.Split(input.PlotFile, PlotFilePrefix)
	if len(files) < 2 {
		return nil, "invalid file description", -3
	}

	newInput := types.NewPlotInput{
		PlotDir: filepath.Dir(files[1]),
	}

	log.Infof(log.Fields{}, "retry fail plot file %v", newInput.PlotDir)
	resp, err := httpdaemon.R().
		SetHeader("Content-Type", "application/json").
		SetBody(newInput).
		Post(fmt.Sprintf("http://%v:%v%v", p.config.LocalHost, p.config.Port, types.NewPlotAPI))
	if err != nil {
		log.Errorf(log.Fields{}, "fail to retry fail plot %v: %v", input.PlotFile, err)
		return nil, "fail to retry fail plot", -4
	}
	if resp.StatusCode() != 200 {
		log.Errorf(log.Fields{}, "fail to retry fail plot %v: %v", input.PlotFile, err)
		return nil, "fail to retry fail plot", -5
	}

	apiResp, err := httpdaemon.ParseResponse(resp)
	if err != nil {
		log.Errorf(log.Fields{}, "fail to parse fail retry response: %v", err)
		return nil, err.Error(), -6
	}

	if apiResp.Code != 0 {
		log.Errorf(log.Fields{}, "fail to retry fail plot: %v", apiResp.Msg)
		return nil, apiResp.Msg, -7
	}

	return nil, "", 0
}
