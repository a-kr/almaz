package main

import (
	"flag"
)

var (
	bindAddress = flag.String("address", ":7701", "address to listen on")
	runAudits = flag.Bool("audit", false, "run audits periodically")
	debug = flag.Bool("debug", false, "print additional info")
	storageDuration = flag.Int("duration-in-hours", 24, "store metrics for last N hours")
	storagePrecision = flag.Int("precision-in-seconds", 60, "store metrics with precision of N seconds")
	acceptanceRegex = flag.String("regex", "", "accept only metrics which match regular expression")
)

func main() {
	flag.Parse()
	server := NewAlmazServer()
	server.storage.SetStorageParams(*storageDuration, *storagePrecision)
	if *acceptanceRegex != "" {
		server.AddAcceptanceRegex(*acceptanceRegex)
	}
	if *runAudits {
		go server.AuditLoop()
	}
	server.StartGraphite(*bindAddress)
}
