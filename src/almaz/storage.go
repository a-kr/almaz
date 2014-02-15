package main

import (
	"bytes"
	"encoding/gob"
	"log"
	"math"
	"os"
	"strings"
	"sync"
)

const (
	DEFAULT_DURATION = 24 * 60 * 60
	DEFAULT_DT       = 60
)

type Storage struct {
	metrics  map[string]*Metric
	duration int
	dt       int
}

type Metric struct {
	sync.RWMutex
	array       []float32
	dt          int
	duration    int
	latest_i    int
	latest_ts_k int64 // == timestamp / dt
	splitName   []string
	total       float32
}

type StoredMetric struct {
	Array       []float32
	Dt          int
	Duration    int
	Latest_i    int
	Latest_ts_k int64
	Total float32
}

func NewStorage() *Storage {
	s := new(Storage)
	s.duration = DEFAULT_DURATION
	s.dt = DEFAULT_DT
	s.metrics = make(map[string]*Metric)
	return s
}

func NewMetric(duration, dt int, starting_ts int64, name string) *Metric {
	m := new(Metric)
	m.splitName = strings.Split(name, ".")
	m.latest_i = 0
	m.duration = duration
	m.dt = dt
	m.array = make([]float32, duration/dt)
	m.latest_ts_k = starting_ts / int64(dt)
	m.total = 0
	return m
}


func (self *Storage) StoreMetric(metric_name string, value float64, ts int64) float64 {
	metric, ok := self.metrics[metric_name]
	if !ok {
		metric = NewMetric(self.duration, self.dt, ts, metric_name)
		metric.array[0] += float32(value)
		self.metrics[metric_name] = metric
		return value
	}
	r := metric.Store(float32(value), ts)
	return float64(r)
}

func (self *Storage) SetTotal(metric_name string, total float64) {
	metric, ok := self.metrics[metric_name]
	if !ok {
		return
		// for now
	}
	metric.SetTotal(float32(total))
}

func (self *Storage) RemoveMetric(metric_name string) {
	delete(self.metrics, metric_name)
}

func (self *Storage) MetricCount() int {
	return len(self.metrics)
}

func (self *Storage) SetStorageParams(duration_hours int, precision_seconds int) {
	if duration_hours <= 0 {
		log.Fatal("duration must be greater than zero")
	}
	if precision_seconds <= 0 {
		log.Fatal("precision must be greater than zero")
	}
	self.duration = duration_hours * 60 * 60
	self.dt = precision_seconds
}

func matchesPattern(s []string, pattern []string) bool {
	if len(s) != len(pattern) {
		return false
	}
	for i := range s {
		if pattern[i] != "*" && s[i] != pattern[i] {
			return false
		}
	}
	return true
}

func (self *Storage) SumByPeriodGroupingQuery(metric_group_patterns []string, periods []int64, now int64, interpolate bool) [][]float64 {
	sums := make([][]float64, len(metric_group_patterns))
	split_patterns := make([][]string, len(metric_group_patterns))
	for i := range metric_group_patterns {
		sums[i] = make([]float64, len(periods))
		split_patterns[i] = strings.Split(metric_group_patterns[i], ".")
	}

	for _, m := range self.metrics {
		for i := range split_patterns {
			if matchesPattern(m.splitName, split_patterns[i]) {
				this_metric_sum := m.GetSumsPerPeriodUntilNowWithInterpolation(periods, now, interpolate)
				for j := range periods {
					sums[i][j] += this_metric_sum[j]
				}
				break // assume metric can match only one pattern
			}
		}
	}
	return sums
}

func (self *Storage) SaveToFile(filename string) error {
	temppath := filename + ".tmp"
	tempfile, err := os.Create(temppath)
	if err != nil {
		return err
	}

	enc := gob.NewEncoder(tempfile)
	err = enc.Encode(self.metrics)
	if err != nil {
		tempfile.Close()
		os.Remove(temppath)
		return err
	}
	tempfile.Close()
	err = os.Rename(temppath, filename)
	if err != nil {
		os.Remove(temppath)
		return err
	}
	return nil
}

func (self *Storage) LoadFromFile(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()
	dec := gob.NewDecoder(file)
	err = dec.Decode(&self.metrics)
	if err != nil {
		return err
	}
	return nil
}

func (self *Metric) Store(value float32, ts int64) float32 {
	self.Lock()
	defer self.Unlock()
	dt_64 := int64(self.dt)
	ts_k := ts / dt_64
	self.total += value
	/*log.Printf("(%f, %d) ts_k %d, latest_ts_k %d", value, ts, ts_k, self.latest_ts_k)*/
	if self.latest_ts_k > ts_k {
		// amend value in the past
		i := int64(self.latest_i) - (self.latest_ts_k - ts_k)
		if i < 0 {
			i += int64(len(self.array))
		}
		if i < 0 {
			// falls outside the storage period
			return self.total
		}
		return self.total
	}
	if ts_k > self.latest_ts_k+int64(len(self.array)) {
		// jump into the future, might as well erase the entire array and start over
		self.latest_ts_k = ts_k
		self.latest_i = 0
		for i := range self.array {
			self.array[i] = 0.0
		}
		self.array[0] = value
		return self.total
	}
	for self.latest_ts_k < ts_k {
		self.latest_i = (self.latest_i + 1) % len(self.array)
		self.array[self.latest_i] = 0.0
		self.latest_ts_k += 1
	}
	self.array[self.latest_i] += value
	return self.total
}

func (self *Metric) SetTotal(value float32) {
	self.Lock()
	defer self.Unlock()
	self.total = value
}

func (self *Metric) GetValueAt(ts int64) float64 {
	self.RLock()
	defer self.RUnlock()
	ts_k := ts / int64(self.dt)
	if ts_k <= self.latest_ts_k-int64(len(self.array)) || ts_k > self.latest_ts_k {
		return 0.0
	}
	d_ts_k := self.latest_ts_k - ts_k
	i := (self.latest_i - int(d_ts_k))
	if i < 0 {
		i += len(self.array)
	}
	/*log.Printf("ts %v, ts_k %v, self.latest_ts_k %v --> i %v", ts, ts_k, self.latest_ts_k, i)*/
	return float64(self.array[i])
}

func (self *Metric) GetSumBetween(ts1 int64, ts2 int64) float64 {
	self.RLock()
	defer self.RUnlock()
	ts1_k := ts1 / int64(self.dt)
	ts2_k := ts2 / int64(self.dt)
	if ts2_k <= self.latest_ts_k-int64(len(self.array)) || ts1_k > self.latest_ts_k {
		return 0.0
	}

	if ts1_k <= self.latest_ts_k-int64(len(self.array)) {
		ts1_k = self.latest_ts_k - int64(len(self.array)) + 1
	}
	if ts2_k > self.latest_ts_k {
		ts2_k = self.latest_ts_k
	}

	d_ts1_k := self.latest_ts_k - ts1_k
	i := (self.latest_i - int(d_ts1_k))
	if i < 0 {
		i += len(self.array)
	}

	sum := 0.0
	for ts1_k <= ts2_k {
		sum += float64(self.array[i])
		i = (i + 1) % len(self.array)
		ts1_k += 1
	}
	return sum
}

func (self *Metric) GetSumForLastNSeconds(seconds int64, now_ts int64) float64 {
	ts2 := now_ts + int64(self.dt)
	ts1 := now_ts - seconds
	return self.GetSumBetween(ts1, ts2)
}

func (self *Metric) GetSumsPerPeriodUntilNow(periods []int64, now int64) []float64 {
	return self.GetSumsPerPeriodUntilNowWithInterpolation(periods, now, false)
}

func (self *Metric) GetSumsPerPeriodUntilNowWithInterpolation(periods []int64, now int64, interpolate bool) []float64 {
	self.RLock()
	defer self.RUnlock()
	dt_64 := int64(self.dt)
	now_k := now / dt_64
	period_starts_k := make([]int64, len(periods))
	period_sums := make([]float64, len(periods))
	min_k := now_k
	k_intr := float64(now-now_k*dt_64) / float64(self.dt)
	if now == now_k*dt_64 {
		k_intr = 1.0
	}

	for i := range periods {
		period_start_ts := now - periods[i]
		period_starts_k[i] = int64(math.Ceil(float64(period_start_ts) / float64(dt_64)))
		if period_starts_k[i] < min_k {
			min_k = period_starts_k[i]
		}
		period_sums[i] = 0.0
		if interpolate {
			additional_piece := (1 - k_intr) * self.GetValueAt(period_start_ts)
			period_sums[i] += additional_piece
		}
	}

	if now_k <= self.latest_ts_k-int64(len(self.array)) || min_k > self.latest_ts_k {
		return period_sums
	}

	if min_k <= self.latest_ts_k-int64(len(self.array)) {
		min_k = self.latest_ts_k - int64(len(self.array)) + 1
	}

	d_min_k := self.latest_ts_k - min_k
	i := (self.latest_i - int(d_min_k))
	if i < 0 {
		i += len(self.array)
	}

	for min_k <= now_k {
		var current_val float64
		if min_k <= self.latest_ts_k {
			current_val = float64(self.array[i])
		} else {
			current_val = 0.0
		}
		for j := range periods {
			if period_starts_k[j] <= min_k {
				period_sums[j] += current_val
			}
		}
		i = (i + 1) % len(self.array)
		min_k += 1
	}
	return period_sums
}

func (self *Metric) GobEncode() ([]byte, error) {
	var sm StoredMetric
	var buf bytes.Buffer
	sm.Array = self.array
	sm.Dt = self.dt
	sm.Duration = self.duration
	sm.Latest_i = self.latest_i
	sm.Latest_ts_k = self.latest_ts_k
	sm.Total = self.total
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(&sm)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (self *Metric) GobDecode(_bytes []byte) error {
	var sm StoredMetric
	buf := bytes.NewBuffer(_bytes)
	dec := gob.NewDecoder(buf)
	err := dec.Decode(&sm)
	if err != nil {
		return err
	}
	self.array = sm.Array
	self.dt = sm.Dt
	self.duration = sm.Duration
	self.latest_i = sm.Latest_i
	self.latest_ts_k = sm.Latest_ts_k
	self.total = sm.Total
	return nil
}

func (self *Metric) Age() int64 {
	return self.latest_ts_k * int64(self.dt)
}
