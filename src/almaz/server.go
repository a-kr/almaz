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
	"encoding/json"
	"github.com/gorilla/websocket"
)

type AlmazServer struct {
	sync.RWMutex
	acceptance_regexen []*regexp.Regexp
	storage *Storage
	persist_path string
	subscribers []*StreamSubscriber
}

type StreamSubscriber struct {
	conn *websocket.Conn
}

func NewAlmazServer(persist_path string) *AlmazServer {
	s := new(AlmazServer)
	s.acceptance_regexen = make([]*regexp.Regexp, 0)
	s.storage = NewStorage()
	s.persist_path = persist_path
	s.subscribers = make([]*StreamSubscriber, 0)
	return s
}

func NewStreamSubscriber(conn *websocket.Conn) *StreamSubscriber {
	s := &StreamSubscriber{}
	s.conn = conn
	return s
}

func (self *AlmazServer) AddSubscriber(sub *StreamSubscriber) {
	self.Lock()
	defer self.Unlock()
	self.subscribers = append(self.subscribers, sub)
}

func (self *AlmazServer) RemoveSubscriber(removed_sub *StreamSubscriber) {
	self.Lock()
	defer self.Unlock()
	subs := make([]*StreamSubscriber, 0, len(self.subscribers))
	for _, sub := range(self.subscribers) {
		if sub != removed_sub {
			subs = append(subs, sub)
		}
	}
	self.subscribers = subs
}

func (self *AlmazServer) GetSubscribers() []*StreamSubscriber {
	self.RLock()
	defer self.RUnlock()
	subs := make([]*StreamSubscriber, 0, len(self.subscribers))
	for _, sub := range(self.subscribers) {
		subs = append(subs, sub)
	}
	return subs
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

type MetricUpdate struct {
	Metric string `json:"metric"`
	Value int `json:"value"`
	TotalValue int `json:"total_value"`
}

func NewMetricUpdate(metric string, value float64, total int) *MetricUpdate {
	upd := &MetricUpdate{}
	upd.Metric = metric
	upd.Value = int(value)
	upd.TotalValue = total
	return upd
}

func (self *AlmazServer) handleGraphiteConnection(conn net.Conn) {
	defer conn.Close()
	self.RLock()
	defer self.RUnlock()
	t1 := time.Now()

	var fwd_conn net.Conn = nil
	var err error

	metric_updates := make([]*MetricUpdate, 0)

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
			if accepted && value > 0 {
				total := self.storage.StoreMetric(metric, value, ts)
				upd := NewMetricUpdate(metric, value, int(total))
				metric_updates = append(metric_updates, upd)
			}
		}
		if fwd_conn != nil {
			fwd_conn.Write([]byte(trimmedString + "\n"))
		}
	}
	t2 := time.Now()
	dt := t2.Sub(t1)
	go self.PushUpstream(metric_updates)
	if *debug {
		log.Printf("Processed metrics batch in %s; storing %d metrics now",
			dt.String(), self.storage.MetricCount())
	}
}

func (self *AlmazServer) PushUpstream(metric_updates []*MetricUpdate) {
	subscribers := self.GetSubscribers()
	json_bytes, err := json.Marshal(metric_updates)
	if err != nil {
		log.Printf("json encode error: %s", err)
		return
	}
	for _, sub := range(subscribers) {
		if sub.conn == nil {
			continue
		}
		err = sub.conn.WriteMessage(websocket.TextMessage, json_bytes)
		if err != nil {
			log.Printf("WriteMessage error: %s", err)
			sub.conn = nil
			self.RemoveSubscriber(sub)
		}
	}
}

func (self *AlmazServer) AuditLoop() {
	for {
		time.Sleep(33 * time.Second)
		self.PruneOld()
		log.Printf("Audit: metric number = %d", self.storage.MetricCount())
	}
}

func (self *AlmazServer) PruneOld() {
	self.Lock()
	defer self.Unlock()
	ts := time.Now().Unix()

	to_remove := make([]string, 0)

	for name, metric := range(self.storage.metrics) {
		if metric.Age() < ts {
			to_remove = append(to_remove, name)
		}
	}

	for _, name := range(to_remove) {
		self.storage.RemoveMetric(name)
	}
	if len(to_remove) > 0 {
		log.Printf("%d old metrics pruned", len(to_remove))
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
