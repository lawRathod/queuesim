package core

import (
	"math"
	"strconv"
)

// SimTime represents absolute virtual simulation time in abstract ticks.
//
// Semantics:
//   - SimTime is always non-negative at API boundaries.
//   - Arithmetic is saturating (clamped), never wrapping.
type SimTime int64

// NewSimTime constructs a SimTime from a raw integer, clamping negatives to zero.
func NewSimTime(v int64) SimTime {
	if v <= 0 {
		return 0
	}
	return SimTime(v)
}

// Int64 returns the raw tick value.
func (t SimTime) Int64() int64 {
	if t < 0 {
		return 0
	}
	return int64(t)
}

// Add returns t + delta with saturating bounds [0, MaxInt64].
func (t SimTime) Add(delta int64) SimTime {
	if delta == math.MinInt64 {
		return 0
	}
	if delta < 0 {
		base := t.Int64()
		abs := -delta
		if abs >= base {
			return 0
		}
		return SimTime(base - abs)
	}
	base := t.Int64()
	if delta > math.MaxInt64-base {
		return SimTime(math.MaxInt64)
	}
	return SimTime(base + delta)
}

// Sub returns t - delta with saturating bounds [0, MaxInt64].
func (t SimTime) Sub(delta int64) SimTime {
	if delta == math.MinInt64 {
		return SimTime(math.MaxInt64)
	}
	if delta < 0 {
		base := t.Int64()
		abs := -delta
		if abs > math.MaxInt64-base {
			return SimTime(math.MaxInt64)
		}
		return SimTime(base + abs)
	}
	base := t.Int64()
	if delta >= base {
		return 0
	}
	return SimTime(base - delta)
}

// String returns a human-readable tick-based representation.
func (t SimTime) String() string {
	return "t=" + strconv.FormatInt(t.Int64(), 10)
}
