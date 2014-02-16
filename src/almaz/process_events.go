package main

import (
	"strings"
	"sync"
	"time"
)

const (
	KeepFor = time.Duration(3*24) * time.Hour
)

type Event struct {
	Command  string    `json:"c"`
	Host     string    `json:"h"`
	Time     time.Time `json:"at"`
	TimeUnix int64     `json:"ts"`
	RunKey   string    `json:"k"`
	Event    string    `json:"e"`
	Outcome  string    `json:"o"`
	Duration *float64  `json:"d"`
}

func (self *Event) FullRunKey() string {
	return self.Host + self.RunKey + self.Command
}

type EventDurationLogger struct {
	sync.RWMutex
	Events []*Event
}

func NewEventDurationLogger() *EventDurationLogger {
	logger := &EventDurationLogger{}
	logger.Events = make([]*Event, 0)
	return logger
}

func (self *EventDurationLogger) AddEvent(event *Event) {
	self.Lock()
	defer self.Unlock()

	now := time.Now()
	event.Time = now
	event.Duration = nil
	event.TimeUnix = now.Unix()
	prehistoric_limit := time.Now().Add(-KeepFor)
	slice_start := 0
	for slice_start < len(self.Events) && self.Events[slice_start].Time.Before(prehistoric_limit) {
		slice_start++
	}
	self.Events = append(self.Events[slice_start:], event)
}

func (self *EventDurationLogger) ScanEvents(host_prefix string, command_substring string) []*Event {
	self.RLock()
	defer self.RUnlock()

	result := make([]*Event, 0, len(self.Events))

	start_times := make(map[string]time.Time)

	for _, event := range self.Events {
		if host_prefix != "" && !strings.HasPrefix(event.Command, host_prefix) {
			continue
		}
		if command_substring != "" && !strings.Contains(event.Command, command_substring) {
			continue
		}
		k := event.FullRunKey()
		if event.Event == "start" {
			start_times[k] = event.Time
		} else if event.Event == "finish" {
			start_t, ok := start_times[k]
			if ok {
				event.Duration = new(float64)
				*event.Duration = event.Time.Sub(start_t).Seconds()
			}
		}
		result = append(result, event)
	}
	return result
}
