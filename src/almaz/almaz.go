package main

import (
	"flag"
)

var (
	bindAddress = flag.String("address", ":7701", "address to listen on")
	runAudits = flag.Bool("audit", false, "run audits periodically")
)

func main() {
	flag.Parse()
	server := NewAlmazServer()
	if *runAudits {
		go server.AuditLoop()
	}
	server.Start(*bindAddress)
}
