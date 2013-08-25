package main

import (
	"flag"
)

var (
	bindAddress = flag.String("address", ":7701", "address to listen on")
)

func main() {
	flag.Parse()
	server := NewAlmazServer()
	server.Start(*bindAddress)
}
