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
}
