package monitor

import "testing"

// TestHistogramPercentiles pins the pure-integer log-scale percentile math.
func TestHistogramPercentiles(t *testing.T) {
	h := NewHistogram()
	// 1,2,3,4,5 -> buckets: 1->[1],2,3->[2],4,5->[4] (bucket idx 1,2,2,3,3)
	for v := int64(1); v <= 5; v++ {
		h.Add(v)
	}
	if h.Count() != 5 {
		t.Fatalf("Count=%d want 5", h.Count())
	}
	if h.MaxMs() != 5 {
		t.Fatalf("MaxMs=%d want 5", h.MaxMs())
	}
	if h.MeanMs() != 3 {
		t.Fatalf("MeanMs=%d want 3", h.MeanMs())
	}
	// 100th percentile = max bucket representative (lower bound of the
	// bucket holding the max), here bucket 3 covers [4,7] → 4.
	if got := h.PercentileMs(100, 100); got != 4 {
		t.Fatalf("p100=%d want 4", got)
	}
	// Empty hist.
	if NewHistogram().PercentileMs(50, 100) != 0 {
		t.Fatal("empty percentile should be 0")
	}
	// Negative clamp.
	h2 := NewHistogram()
	h2.Add(-5)
	if h2.PercentileMs(100, 100) != 0 {
		t.Fatalf("clamped negative should bucket to 0, got %d", h2.PercentileMs(100, 100))
	}
}

// TestHistogramDeterminism asserts the same insertion sequence yields a
// byte-identical digest across reconstructions.
func TestHistogramDeterminism(t *testing.T) {
	add := func() *Histogram {
		h := NewHistogram()
		for _, v := range []int64{0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144} {
			h.Add(v)
		}
		return h
	}
	a, b := add(), add()
	if a.Digest() != b.Digest() {
		t.Fatal("histogram digest not deterministic")
	}
	if a.MeanMs() != b.MeanMs() || a.MaxMs() != b.MaxMs() {
		t.Fatal("histogram stats differ across identical insertions")
	}
}
