package main

import (
	"log"
	"time"
	"fmt"
	"net/http"
	_ "net/http/pprof"
)

func (self *AlmazServer) StartHttpface(bindAddress string) {
    log.Printf("Http interface available at %s", bindAddress)
    http.HandleFunc("/", self.http_main)
    http.HandleFunc("/list/all/", self.http_list_all)
    http.HandleFunc("/list/group/", self.http_list_group)
    http.ListenAndServe(bindAddress, nil)
}

func (self *AlmazServer) http_main(w http.ResponseWriter, r *http.Request) {
	self.RLock()
	defer self.RUnlock()
	fmt.Fprintf(w, "Metrics count: %d\n", self.storage.MetricCount())
}

func (self *AlmazServer) http_list_all(w http.ResponseWriter, r *http.Request) {
	self.RLock()
	defer self.RUnlock()

	periods := []int64{60, 15*60, 60*60, 4*60*60, 24*60*60}
	now := time.Now().Unix()

	for k := range self.storage.metrics {
		fmt.Fprintf(w, "%s", k)
		counts_per_period := self.storage.metrics[k].GetSumsPerPeriodUntilNow(periods, now)
		for _, el := range counts_per_period {
			fmt.Fprintf(w, "\t%f", el)
		}
		fmt.Fprintf(w, "\n")
	}
}

func (self *AlmazServer) http_list_group(w http.ResponseWriter, r *http.Request) {
	self.RLock()
	defer self.RUnlock()

	periods := []int64{60, 15*60, 60*60, 4*60*60, 24*60*60}
	groups := []string{
		"stats_counts.adv.shows.*.164.*",
		"stats_counts.adv.shows.*.165.*",
		"stats_counts.adv.shows.*.166.*"}
	now := time.Now().Unix()

	var results [][]float64
	results = self.storage.SumByPeriodGroupingQuery(groups, periods, now)
	for k := range groups {
		fmt.Fprintf(w, "%s", groups[k])
		for _, el := range results[k] {
			fmt.Fprintf(w, "\t%f", el)
		}
		fmt.Fprintf(w, "\n")
	}
}
