package main

import (
	// "flag"
	// "os"
	"log"
	"net/http"

	"github.com/envsh/libp2px/p2put"

)

func main() {
	cfg := p2put.ParseConfig()
	cfg.Dht = false
	_ = cfg

	// cfg.Fset.Parse(os.Args[1:])
	// cfg.KeyFile = *keyFile
	// cfg.ListenPort = *port

	go p2put.MainLibp2p(cfg)
	p2put.InstallRestHandler("/p2pin", nil)
	err := http.ListenAndServe(":4004", nil)
	if err != nil {
		log.Println(err)
	}
}
