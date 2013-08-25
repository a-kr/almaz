package main

import (
	"log"
	"net"
	"time"
	"bufio"
	"strings"
	"strconv"
	"regexp"
)

type AlmazServer struct {
	acceptance_regexen []*regexp.Regexp
	storage *Storage
}

func NewAlmazServer() *AlmazServer {
	s := new(AlmazServer)
	s.acceptance_regexen = make([]*regexp.Regexp, 0)
	s.storage = NewStorage()
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
