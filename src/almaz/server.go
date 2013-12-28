package main

import (
	"os"
	"log"
	"net"
	"time"
	"sync"
	"os/signal"
	"syscall"
	"bufio"
	"strings"
	"strconv"
	"regexp"
	"utils"
)

type AlmazServer struct {
	sync.RWMutex
	acceptance_regexen []*regexp.Regexp
	storage *Storage
	persist_path string
}

func NewAlmazServer(persist_path string) *AlmazServer {
	s := new(AlmazServer)
	s.acceptance_regexen = make([]*regexp.Regexp, 0)
	s.storage = NewStorage()
	s.persist_path = persist_path
	return s
}

func (self *AlmazServer) AddAcceptanceRegex(re string) {
	rx := regexp.MustCompile(re)
	log.Printf("storing only metrics that match %s", re)
	self.acceptance_regexen = append(self.acceptance_regexen, rx)
}

func (self *AlmazServer) StartGraphite(bindAddress string) {
	listener, err := net.Listen("tcp", bindAddress)
	if err != nil {
		log.Fatalf("failed to listen: %s", err)
	}
	log.Printf("listening on %s", bindAddress)
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf(err.Error())
			continue
		}
		go self.handleGraphiteConnection(conn)
	}
}

func (self *AlmazServer) handleGraphiteConnection(conn net.Conn) {
	defer conn.Close()
	self.RLock()
	defer self.RUnlock()
	t1 := time.Now()

	var fwd_conn net.Conn = nil
	var err error

	if *fwdAddress != "" {
		fwd_conn, err = net.Dial("tcp", *fwdAddress)
		if err != nil {
			//log.Printf("forward conn error: %s", err)
		} else {
			defer fwd_conn.Close()
		}
	}

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		trimmedString := scanner.Text()
		parts := strings.Split(trimmedString, " ")
		if len(parts) == 3 {
			metric := parts[0]
			value, err1 := strconv.ParseFloat(parts[1], 32)
			ts, err2 := strconv.ParseInt(parts[2], 10, 64)
			if err1 != nil || err2 != nil {
				log.Printf("parse error: %s %s", err1, err2)
				continue
			}

			accepted := false
			if len(self.acceptance_regexen) == 0 {
				accepted = true
			} else {
				for _, rx := range(self.acceptance_regexen) {
					if rx.MatchString(metric) {
						accepted = true
						break
					}
				}
			}
			if accepted {
				self.storage.StoreMetric(metric, value, ts)
			}
		}
		if fwd_conn != nil {
			fwd_conn.Write([]byte(trimmedString + "\n"))
		}
	}
	t2 := time.Now()
	dt := t2.Sub(t1)
	if *debug {
		log.Printf("Processed metrics batch in %s; storing %d metrics now",
			dt.String(), self.storage.MetricCount())
	}
}


func (self *AlmazServer) AuditLoop() {
	for {
		time.Sleep(30 * time.Second)
		log.Printf("Audit: metric number = %d", self.storage.MetricCount())
	}
}

func (self *AlmazServer) LoadFromDisk() {
	self.Lock()
	defer self.Unlock()
	log.Printf("Restoring from disk...")
	t1 := time.Now()
	err := self.storage.LoadFromFile(self.persist_path)
	if err != nil {
		log.Printf("Error while loading from disk: %s", err)
	} else {
		t2 := time.Now()
		dt := t2.Sub(t1)
		log.Printf("Done loading (%s)", dt)
	}
}

func (self *AlmazServer) SaveToDisk() {
	self.Lock()
	defer self.Unlock()
	log.Printf("Saving to disk...")
	t1 := time.Now()
	err := self.storage.SaveToFile(self.persist_path)
	if err != nil {
		log.Printf("Error while saving to disk: %s", err)
	} else {
		t2 := time.Now()
		dt := t2.Sub(t1)
		log.Printf("Done saving (%s)", dt)
	}
}

func (self *AlmazServer) ForkAndSaveToDisk() {
	if utils.DoubleFork() > 0 {
		return
	}
	self.SaveToDisk()
	os.Exit(0)
}

func (self *AlmazServer) BgsaveLoop(interval_seconds int) {
	for {
		time.Sleep(time.Duration(interval_seconds) * time.Second)
		self.SaveToDisk()
	}
}

func (self *AlmazServer) WaitForTermination(persist_on_exit bool, bgsave_interval int) {
	var bgsave_int_duration = time.Duration(bgsave_interval) * time.Second
	if !persist_on_exit || bgsave_int_duration <= 0 {
		bgsave_int_duration = time.Duration(60) * time.Second
	}

	impeding_death := make(chan os.Signal, 1)
	signal.Notify(impeding_death, syscall.SIGINT, syscall.SIGTERM)

	bgsave_ticker := time.NewTicker(bgsave_int_duration)

	for {
		select {
			case <-bgsave_ticker.C:
				if persist_on_exit && bgsave_interval > 0 {
					self.ForkAndSaveToDisk()
				}
			case s := <-impeding_death:
				log.Printf("Got signal:", s)
				if persist_on_exit {
					self.SaveToDisk()
				}
				return
		}
	}
}
