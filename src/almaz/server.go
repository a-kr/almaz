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

func (self *AlmazServer) BgsaveLoop(interval_seconds int) {
	for {
		time.Sleep(time.Duration(interval_seconds) * time.Second)
		self.SaveToDisk()
	}
}

func (self *AlmazServer) WaitForTermination(persist_on_exit bool) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)

	s := <-c
	log.Printf("Got signal:", s)
	if persist_on_exit {
		self.SaveToDisk()
	}
}
