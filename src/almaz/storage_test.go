package main

import (
	"testing"
	"fmt"
	"runtime"
)

func AssertEqual(t *testing.T, a, b interface{}) {
	if fmt.Sprintf("%v", a) != fmt.Sprintf("%v", b) {
		_, file, line, _ := runtime.Caller(1)
		t.Errorf("%s:%v:\n\t\t%v != %v", file, line, a, b)
	}
}

func Test_Addition(t *testing.T) {
	m := NewMetric(60, 10, 1)
	m.Store(2, 1) // bucket 0
	m.Store(2, 1) // bucket 0
	m.Store(1, 4) // bucket 0
	m.Store(1, 9) // bucket 0
	m.Store(1, 11) // bucket 1
	m.Store(88, 24) // bucket 2
	m.Store(11, 28) // bucket 2
	AssertEqual(t, m.array[0], 2+2+1+1)
	AssertEqual(t, m.array[1], 1)
	AssertEqual(t, m.array[2], 88+11)
}

func Test_Circularity(t *testing.T) {
	m := NewMetric(60, 10, 1)
	m.Store(1, 1) // bucket 0
	m.Store(12, 12) // bucket 1
	m.Store(38, 38) // bucket 3
	m.Store(55, 55) // bucket 5
	AssertEqual(t, m.array[0], 1)
	AssertEqual(t, m.array[1], 12)
	AssertEqual(t, m.array[2], 0)
	AssertEqual(t, m.array[3], 38)
	AssertEqual(t, m.array[4], 0)
	AssertEqual(t, m.array[5], 55)
	AssertEqual(t, m.latest_i, 5)
	m.Store(64, 64) // bucket 0
	m.Store(88, 88) // bucket 2
	AssertEqual(t, m.array[0], 64)
	AssertEqual(t, m.array[1], 0)
	AssertEqual(t, m.array[2], 88)
	AssertEqual(t, m.array[3], 38)
	AssertEqual(t, m.array[4], 0)
	AssertEqual(t, m.array[5], 55)
	AssertEqual(t, m.latest_i, 2)

	AssertEqual(t, m.GetValueAt(1), 0)
	AssertEqual(t, m.GetValueAt(14), 0)
	AssertEqual(t, m.GetValueAt(29), 0)
	AssertEqual(t, m.GetValueAt(32), 38)
	AssertEqual(t, m.GetValueAt(46), 0)
	AssertEqual(t, m.GetValueAt(55), 55)
	AssertEqual(t, m.GetValueAt(60), 64)
	AssertEqual(t, m.GetValueAt(73), 0)
	AssertEqual(t, m.GetValueAt(81), 88)
	AssertEqual(t, m.GetValueAt(96), 0)
	AssertEqual(t, m.GetValueAt(105), 0)

	AssertEqual(t, m.GetSumBetween(1, 19), 0)
	AssertEqual(t, m.GetSumBetween(1, 29), 0)
	AssertEqual(t, m.GetSumBetween(1, 35), 0) // excl. right bounds
	AssertEqual(t, m.GetSumBetween(1, 45), 38)
	AssertEqual(t, m.GetSumBetween(1, 49), 38)
	AssertEqual(t, m.GetSumBetween(1, 59), 38)
	AssertEqual(t, m.GetSumBetween(1, 64), 38+55)
	AssertEqual(t, m.GetSumBetween(39, 64), 38+55)
	AssertEqual(t, m.GetSumBetween(41, 69), 55)
	AssertEqual(t, m.GetSumBetween(41, 77), 55 + 64)
	AssertEqual(t, m.GetSumBetween(41, 83), 55 + 64)
	AssertEqual(t, m.GetSumBetween(41, 92), 55 + 64 + 88)
	AssertEqual(t, m.GetSumBetween(41, 102), 55 + 64 + 88)
	AssertEqual(t, m.GetSumBetween(41, 183), 55 + 64 + 88)
	AssertEqual(t, m.GetSumBetween(0, 183), 38 + 55 + 64 + 88)
	AssertEqual(t, m.GetSumBetween(92, 183), 0)

	AssertEqual(t, m.GetSumForLastNSeconds(10, 100), 0)
	AssertEqual(t, m.GetSumForLastNSeconds(20, 100), 88)
}

func Test_PeriodSums(t *testing.T) {
	m := NewMetric(60, 10, 1)
	m.Store(1, 1) // bucket 0
	m.Store(12, 12) // bucket 1
	m.Store(38, 38) // bucket 3
	m.Store(55, 55) // bucket 5
	m.Store(64, 64) // bucket 0
	m.Store(88, 88) // bucket 2

	s := m.GetSumsPerPeriodUntilNow([]int64{10, 20, 30, 40, 60, 100}, 94)
	AssertEqual(t, len(s), 6)
	AssertEqual(t, s[0], 0) // sum @90..99 s
	AssertEqual(t, s[1], 88) // sum @80..99 s
	AssertEqual(t, s[2], 88) // sum @70..99 s
	AssertEqual(t, s[3], 64 + 88) // sum @60..99 s
	AssertEqual(t, s[4], 55 + 64 + 88) // sum @40..99 s
	AssertEqual(t, s[5], 1 + 12 - 1 - 12 + 38 + 55 + 64 + 88) // sum @-10..99 s but only 30..99 are stored

	s = m.GetSumsPerPeriodUntilNow([]int64{10, 20, 30, 40, 60, 100}, 174)
	AssertEqual(t, len(s), 6)
	AssertEqual(t, s[0], 0)
	AssertEqual(t, s[1], 0)
	AssertEqual(t, s[2], 0)
	AssertEqual(t, s[3], 0)
	AssertEqual(t, s[4], 0)
	AssertEqual(t, s[5], 88) // sum @80..179

	s = m.GetSumsPerPeriodUntilNow([]int64{10, 20}, 22)
	AssertEqual(t, len(s), 2)
	AssertEqual(t, s[0], 0) // no data before 30 seconds
	AssertEqual(t, s[1], 0)
}
