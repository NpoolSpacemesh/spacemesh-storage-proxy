package main

import (
	"os"

	log "github.com/EntropyPool/entropy-logger"
	"github.com/urfave/cli/v2"
	"golang.org/x/xerrors"
)

func main() {
	app := &cli.App{
		Name:                 "chia-storage-proxy",
		Usage:                "Storage proxy for chia plotter",
		Version:              "0.1.0",
		EnableBashCompletion: true,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "config",
				Value: "/etc/chia-storage-proxy.conf",
			},
		},
		Action: func(cctx *cli.Context) error {
			cfgFile := cctx.String("config")

			proxy := NewStorageProxy(cfgFile)
			if proxy == nil {
				return xerrors.Errorf("cannot create storage proxy with %v", cfgFile)
			}

			err := proxy.Run()
			if err != nil {
				return xerrors.Errorf("cannot run storage proxy with %v: %v", cfgFile, err)
			}

			ch := make(chan int)
			<-ch

			return nil
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatalf(log.Fields{}, "fail to run %v: %v", app.Name, err)
	}
}
