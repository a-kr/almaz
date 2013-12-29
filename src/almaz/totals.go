package main

import (
	"sync"
)

type Totals struct {
	sync.RWMutex
	totals map[string]int
}

func NewTotals() *Totals {
	t := &Totals{}
	t.totals = make(map[string]int)
	return t
}

func (self *Totals) Get(key string) int {
	self.RLock()
	defer self.RUnlock()
	v, ok := self.totals[key]
	if !ok {
		v = 0
	}
	return v
}

func (self *Totals) Increment(key string, delta int) {
	self.Lock()
	defer self.Unlock()
	v, ok := self.totals[key]
	if !ok {
		v = 0
	}
	self.totals[key] = v + delta
}

func (self *Totals) Set(key string, value int) {
	self.Lock()
	defer self.Unlock()
	self.totals[key] = value
}

func (self *Totals) RemoveMetric(key string) {
	self.Lock()
	defer self.Unlock()
	delete(self.totals, key)
}
