package main

import (
	"sync"
	"strings"
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
	key = self.NormalizeKey(key)
	v, ok := self.totals[key]
	if !ok {
		v = 0
	}
	return v
}

func (self *Totals) NormalizeKey(key string) string {
	parts := strings.Split(key, ".")
	key = parts[len(parts) - 1]
	return key
}

func (self *Totals) Increment(key string, delta int) {
	self.Lock()
	defer self.Unlock()
	key = self.NormalizeKey(key)
	v, ok := self.totals[key]
	if !ok {
		v = 0
	}
	self.totals[key] = v + delta
}

func (self *Totals) Set(key string, value int) {
	self.Lock()
	defer self.Unlock()
	key = self.NormalizeKey(key)
	self.totals[key] = value
}
