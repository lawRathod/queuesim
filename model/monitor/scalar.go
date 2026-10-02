package monitor

// counter records the latest value of a monotonically growing total (e.g.
// throughput / completed count). The registered `at` closure must be a pure
// read of the underlying component.
type counter struct {
	name     string
	labelSet string
	at       func() uint64
	value    uint64
}

// gauge records a series of instantaneous samples (e.g. queue depth). For
// each sample the gauge tracks the last, max, running sum and count —
// enough to report last/max/mean deterministically — and appends to a
// series for depth-sequence inspection.
type gauge struct {
	name     string
	labelSet string
	at       func() int64
	last     int64
	max      int64
	count    uint64
	sum      uint64
	series   []int64
}

// histogramMetric wraps a value-batch sampler that feeds an internal
// Histogram. The registered `at` closure returns the batch of integer
// samples accumulated since the previous sample; the monitor folds them
// into the histogram.
type histogramMetric struct {
	name     string
	labelSet string
	at       func() []int64
	h        *Histogram
}
