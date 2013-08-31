package main

import (
	"log"
	"fmt"
	"net/http"
	_ "net/http/pprof"
)

func (self *AlmazServer) StartHttpface(bindAddress string) {
    log.Printf("Http interface available at %s", bindAddress)
    http.HandleFunc("/", self.http_main)
    http.ListenAndServe(bindAddress, nil)
}

func (self *AlmazServer) http_main(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Metrics count: %d\n", self.storage.MetricCount())
}

