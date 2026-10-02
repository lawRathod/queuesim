package monitor

import "testing"

// TestHistogramStdDev pins the second-moment stddev against a hand-computed
// value: [2,4,4,4,5,5,7,9] has mean 5 and population variance 4, so the
// integer stddev is 2.
func TestHistogramStdDev(t *testing.T) {
	h := NewHistogram()
	for _, v := range []int64{2, 4, 4, 4, 5, 5, 7, 9} {
		h.Add(v)
	}
	if got := h.StdDevMs(); got != 2 {
		t.Fatalf("StdDevMs = %d, want 2", got)
	}
	// Degenerate counts.
	if NewHistogram().StdDevMs() != 0 {
		t.Fatal("empty histogram stddev should be 0")
	}
	h1 := NewHistogram()
	h1.Add(100)
	if h1.StdDevMs() != 0 {
		t.Fatalf("single-sample stddev = %d, want 0", h1.StdDevMs())
	}
}

// TestHistogramP999Gate pins the Count gate: a p999 measured from fewer than
// P999MinCount samples is not reportable (ok=false, value 0) because it
// collapses onto the maximum; at the gate it becomes reportable.
func TestHistogramP999Gate(t *testing.T) {
	h := NewHistogram()
	for i := 0; i < int(P999MinCount)-1; i++ {
		h.Add(5)
	}
	if v, ok := h.P999Ms(); ok || v != 0 {
		t.Fatalf("p999 under gate = (%d,%v), want (0,false)", v, ok)
	}
	h.Add(5) // now exactly P999MinCount samples
	if h.Count() != P999MinCount {
		t.Fatalf("count = %d, want %d", h.Count(), P999MinCount)
	}
	if v, ok := h.P999Ms(); !ok || v != 4 {
		t.Fatalf("p999 at gate = (%d,%v), want (4,true) (bucket [4,7] lower bound)", v, ok)
	}
}

// TestHistogramP999BucketLowerBound pins the honest resolution: p999 returns
// the bucket LOWER bound, so a tail sample above the p999 bucket does not
// move the reading, and at the gate boundary the reading equals the max
// bucket's lower bound.
func TestHistogramP999BucketLowerBound(t *testing.T) {
	h := NewHistogram()
	for i := 0; i < int(P999MinCount)-1; i++ {
		h.Add(5) // bucket 3 -> lower bound 4
	}
	h.Add(1000) // one outlier: bucket 10 -> lower bound 512
	if got := h.MaxMs(); got != 1000 {
		t.Fatalf("MaxMs = %d, want 1000", got)
	}
	if v, ok := h.P999Ms(); !ok || v != 4 {
		t.Fatalf("p999 = (%d,%v), want (4,true): the p999 index lands in bucket 3, not the max bucket", v, ok)
	}
	// Exactly-P999MinCount identical samples degenerate to the max bucket's
	// lower bound (documented limitation).
	h2 := NewHistogram()
	for i := 0; i < int(P999MinCount); i++ {
		h2.Add(100)
	}
	if v, ok := h2.P999Ms(); !ok || v != 64 {
		t.Fatalf("p999 of uniform 100 = (%d,%v), want (64,true) (max bucket [64,127] lower bound)", v, ok)
	}
	if v, ok := h2.P999Ms(); !ok || v != h2.PercentileMs(999, 1000) {
		t.Fatal("P999Ms must equal PercentileMs(999,1000) above the gate")
	}
}

// TestHistogramSumSqOverflowGuard pins the saturating guard: a sample whose
// square does not fit uint64 saturates sumSq at MaxUint64 and never wraps to
// a bogus small value.
func TestHistogramSumSqOverflowGuard(t *testing.T) {
	h := NewHistogram()
	h.Add(1 << 33) // (2^33)^2 = 2^66 > MaxUint64
	if h.sumSq != ^uint64(0) {
		t.Fatalf("sumSq = %d, want saturated %d", h.sumSq, ^uint64(0))
	}
	h.Add(1 << 33) // saturation is sticky
	if h.sumSq != ^uint64(0) {
		t.Fatalf("sumSq wrapped after saturation: %d", h.sumSq)
	}
	if h.StdDevMs() < 0 {
		t.Fatalf("StdDevMs negative after saturation: %d", h.StdDevMs())
	}
	// A bounded accumulation that crosses MaxUint64 also saturates.
	h2 := NewHistogram()
	for i := 0; i < 4; i++ {
		h2.Add(1 << 31) // (2^31)^2 = 2^62; four = 2^64 > MaxUint64
	}
	if h2.sumSq != ^uint64(0) {
		t.Fatalf("sumSq = %d, want saturated %d", h2.sumSq, ^uint64(0))
	}
}

// TestMonitorHistogramTailPlumbing is the ADR-116 end-to-end gate: the tail
// statistics must flow through Summary (not just live on Histogram), and the
// Count gate must suppress a p999 below P999MinCount.
func TestMonitorHistogramTailPlumbing(t *testing.T) {
	big := make([]int64, P999MinCount)
	for i := range big {
		big[i] = 5
	}
	m := New()
	m.AddHistogram("tail", func() []int64 { return big })
	m.Sample()
	h, ok := m.Summary().Histogram("tail")
	if !ok {
		t.Fatal("missing histogram tail in summary")
	}
	if h.Count != P999MinCount {
		t.Fatalf("count = %d, want %d", h.Count, P999MinCount)
	}
	if h.P999Ms != 4 {
		t.Fatalf("summary P999Ms = %d, want 4 (bucket [4,7] lower bound)", h.P999Ms)
	}
	if h.StdDevMs != 0 {
		t.Fatalf("uniform-sample summary StdDevMs = %d, want 0", h.StdDevMs)
	}

	// A hand-computed non-degenerate stddev through the plumbing, and a
	// below-gate histogram whose P999Ms is suppressed to 0.
	m2 := New()
	spread := []int64{2, 4, 4, 4, 5, 5, 7, 9}
	m2.AddHistogram("spread", func() []int64 { return spread })
	m2.Sample()
	h2, ok := m2.Summary().Histogram("spread")
	if !ok {
		t.Fatal("missing histogram spread in summary")
	}
	if h2.StdDevMs != 2 {
		t.Fatalf("summary StdDevMs = %d, want 2", h2.StdDevMs)
	}
	if h2.P999Ms != 0 {
		t.Fatalf("summary P999Ms below gate = %d, want 0", h2.P999Ms)
	}
}
