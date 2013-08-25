package main

import (
	 "log"
)

const (
	DEFAULT_DURATION = 24 * 60 * 60
	DEFAULT_DT = 60
)

type Storage struct {
	metrics map[string]*Metric
}

type Metric struct {
	array []float32
	dt int
	duration int
	latest_i int
	latest_ts_k int64 // == timestamp / dt
}

func NewStorage() *Storage {
	s := new(Storage)
	s.metrics = make(map[string]*Metric)
	return s
}

func NewMetric(duration int, dt int, starting_ts int64) *Metric {
	m := new(Metric)
	m.latest_i = 0
	m.duration = duration
	m.dt = dt
	m.array = make([]float32, duration / dt)
	m.latest_ts_k = starting_ts / int64(dt)
	return m
}

func (self *Storage) StoreMetric(metric_name string, value float64, ts int64) {
	metric, ok := self.metrics[metric_name]
	if !ok {
		metric = NewMetric(DEFAULT_DURATION, DEFAULT_DT, ts)
		self.metrics[metric_name] = metric
	}
	metric.Store(float32(value), ts)
}

func (self *Metric) Store(value float32, ts int64) {
	dt_64 := int64(self.dt)
	ts_k := ts / dt_64
	/*log.Printf("(%f, %d) ts_k %d, latest_ts_k %d", value, ts, ts_k, self.latest_ts_k)*/
	for self.latest_ts_k < ts_k {
		self.latest_i = (self.latest_i + 1) % len(self.array)
		self.array[self.latest_i] = 0.0
		self.latest_ts_k += 1
	}
	self.array[self.latest_i] += value
}

func (self *Metric) GetValueAt(ts int64) float64 {
	ts_k := ts / int64(self.dt)
	if ts_k <= self.latest_ts_k - int64(len(self.array)) || ts_k > self.latest_ts_k {
		return 0.0
	}
	d_ts_k := self.latest_ts_k - ts_k
	i := (self.latest_i - int(d_ts_k))
	if i < 0 {
		i += len(self.array)
	}
	log.Printf("ts %v, ts_k %v, self.latest_ts_k %v --> i %v", ts, ts_k, self.latest_ts_k, i)
	return float64(self.array[i])
}
