package main

import (
	"log"
	"time"
	"bufio"
	"strings"
	"strconv"
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

	now := time.Now().Unix()

	defer r.Body.Close()
	scanner := bufio.NewScanner(r.Body)
	ok := scanner.Scan()
	if !ok {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "400 Bad Request\r\n")
		fmt.Fprintf(w, "Use POST method and specify period durations in seconds ")
		fmt.Fprintf(w, "on the first line of request data.\n")
		return
	}
	periods_str := strings.Split(scanner.Text(), " ")

	periods := make([]int64, len(periods_str))
	for i := range periods_str {
		period, err := strconv.ParseInt(periods_str[i], 10, 64)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, "400 Bad Request\r\n")
			fmt.Fprintf(w, "%s", err)
			return
		}
		periods[i] = period
	}

	groups := make([]string, 0)
	for scanner.Scan() {
		groups = append(groups, scanner.Text())
	}

	var results [][]float64
	results = self.storage.SumByPeriodGroupingQuery(groups, periods, now, true)
	for k := range groups {
		fmt.Fprintf(w, "%s", groups[k])
		for _, el := range results[k] {
			fmt.Fprintf(w, "\t%f", el)
		}
		fmt.Fprintf(w, "\n")
	}
}
