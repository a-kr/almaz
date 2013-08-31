package main

import (
	"os"
	"math"
	"bytes"
	"io/ioutil"
	"encoding/gob"
	"sync"
	"log"
)

const (
	DEFAULT_DURATION = 24 * 60 * 60
	DEFAULT_DT = 60
)

type Storage struct {
	metrics map[string]*Metric
	duration int
	dt int
}

type Metric struct {
	sync.RWMutex
	array []float32
	dt int
	duration int
	latest_i int
	latest_ts_k int64 // == timestamp / dt
}

type StoredMetric struct {
	Array []float32
	Dt int
	Duration int
	Latest_i int
	Latest_ts_k int64
}

func NewStorage() *Storage {
	s := new(Storage)
	s.duration = DEFAULT_DURATION
	s.dt = DEFAULT_DT
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
		metric = NewMetric(self.duration, self.dt, ts)
		self.metrics[metric_name] = metric
	}
	metric.Store(float32(value), ts)
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

func (self *Storage) SaveToFile(filename string) error {
	tempfile, err := ioutil.TempFile("", "")
	if err != nil {
		return err
	}
	temppath := tempfile.Name()

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

func (self *Metric) Store(value float32, ts int64) {
	self.Lock()
	defer self.Unlock()
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
	self.RLock()
	defer self.RUnlock()
	ts_k := ts / int64(self.dt)
	if ts_k <= self.latest_ts_k - int64(len(self.array)) || ts_k > self.latest_ts_k {
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
	if ts2_k <= self.latest_ts_k - int64(len(self.array)) || ts1_k > self.latest_ts_k {
		return 0.0
	}

	if ts1_k <= self.latest_ts_k - int64(len(self.array)) {
		ts1_k = self.latest_ts_k - int64(len(self.array)) + 1
	}
	if ts2_k > self.latest_ts_k {
		ts2_k = self.latest_ts_k + 1
	}

	d_ts1_k := self.latest_ts_k - ts1_k
	i := (self.latest_i - int(d_ts1_k))
	if i < 0 {
		i += len(self.array)
	}

	sum := 0.0
	for ts1_k < ts2_k {
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
	self.RLock()
	defer self.RUnlock()
	dt_64 := int64(self.dt)
	now_k := now / dt_64
	period_starts_k := make([]int64, len(periods))
	period_sums := make([]float64, len(periods))
	min_k := now_k

	for i := range periods {
		period_starts_k[i] = int64(math.Ceil((float64(now) - float64(periods[i])) / float64(dt_64)))
		if period_starts_k[i] < min_k {
			min_k = period_starts_k[i]
		}
		period_sums[i] = 0.0
	}

	if now_k <= self.latest_ts_k - int64(len(self.array)) || min_k > self.latest_ts_k {
		log.Printf("min_k %d, latest_ts_k %d, exiting", min_k, self.latest_ts_k)
		return period_sums
	}

	if min_k <= self.latest_ts_k - int64(len(self.array)) {
		min_k = self.latest_ts_k - int64(len(self.array)) + 1
	}
	if now_k > self.latest_ts_k {
		now_k = self.latest_ts_k + 1
	}

	d_min_k := self.latest_ts_k - min_k
	i := (self.latest_i - int(d_min_k))
	if i < 0 {
		i += len(self.array)
	}

	for min_k < now_k {
		/*log.Printf("Now at %d s, index %d", min_k * dt_64, i)*/
		for j := range periods {
			if period_starts_k[j] <= min_k {
				/*log.Printf(" - period %d..now gets +%f increment", periods[j], self.array[i])*/
				period_sums[j] += float64(self.array[i])
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
	return nil
}
