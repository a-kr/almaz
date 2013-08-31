package main

import (
	"flag"
)

var (
	bindAddress = flag.String("address", ":7701", "address to listen on for metrics (Carbon-compatible protocol)")
	httpAddress = flag.String("http-address", ":7702", "address to recieve queries (http)")
	runAudits = flag.Bool("audit", false, "run audits periodically")
	persist = flag.Bool("persist", false, "persist to disk (load at startup, save on SIGTERM/SIGINT) (see --persist-path)")
	persistPath = flag.String("persist-path", "almaz.dat", "path to storage file")
	persistInterval = flag.Int("bgsave", 0, "save to disk every N seconds (0 --- do not save). Must have --persist specified.")
	debug = flag.Bool("debug", false, "print additional info")
	storageDuration = flag.Int("duration-in-hours", 24, "store metrics for last N hours")
	storagePrecision = flag.Int("precision-in-seconds", 60, "store metrics with precision of N seconds")
	acceptanceRegex = flag.String("regex", "", "accept only metrics which match regular expression")
)

func main() {
	flag.Parse()
	server := NewAlmazServer(*persistPath)
	server.storage.SetStorageParams(*storageDuration, *storagePrecision)
	if *acceptanceRegex != "" {
		server.AddAcceptanceRegex(*acceptanceRegex)
	}
	if *persist {
		server.LoadFromDisk()
	}
	if *runAudits {
		go server.AuditLoop()
	}
	go server.StartGraphite(*bindAddress)
	go server.StartHttpface(*httpAddress)
	server.WaitForTermination(*persist, *persistInterval)
}
