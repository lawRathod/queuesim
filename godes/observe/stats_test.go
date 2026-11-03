package observe

import (
	"math"
	"testing"
)

func TestStatsBasic(t *testing.T) {
	s := NewStats()
	for _, v := range []float64{2, 4, 4, 4, 5, 5, 7, 9} {
		s.Push(v)
	}
	if s.N() != 8 {
		t.Fatalf("N=%d want 8", s.N())
	}
	if math.Abs(s.Mean()-5) > 1e-9 {
		t.Fatalf("Mean=%v want 5", s.Mean())
	}
	if math.Abs(s.StdDev()-2.13809) > 1e-4 { // classic sample-stddev example
		t.Fatalf("StdDev=%v want ~2.13809", s.StdDev())
	}
	if s.Min() != 2 || s.Max() != 9 {
		t.Fatalf("Min=%v Max=%v want 2/9", s.Min(), s.Max())
	}
}

func TestStatsEmpty(t *testing.T) {
	s := NewStats()
	if s.N() != 0 || s.Mean() != 0 || s.StdDev() != 0 {
		t.Fatal("empty stats should be zero")
	}
	lo, hi := s.ConfidenceInterval(0.05)
	if lo != 0 || hi != 0 {
		t.Fatal("empty CI should be zero")
	}
	if s.Min() != math.Inf(1) || s.Max() != math.Inf(-1) {
		t.Fatal("empty min/max should be +/-Inf")
	}
}

func TestStatsSingleValue(t *testing.T) {
	s := NewStats()
	s.Push(3.5)
	lo, hi := s.ConfidenceInterval(0.05)
	if lo != 3.5 || hi != 3.5 {
		t.Fatalf("single-value CI = [%v,%v], want degenerate [3.5,3.5]", lo, hi)
	}
}

func TestStatsCI95Known(t *testing.T) {
	// n=8, mean=5, sd≈2.138: t_{0.975,7}=2.365 → half-width ≈ 1.787.
	s := NewStats()
	for _, v := range []float64{2, 4, 4, 4, 5, 5, 7, 9} {
		s.Push(v)
	}
	lo, hi := s.ConfidenceInterval(0.05)
	wantHalf := 2.365 * 2.13809 / math.Sqrt(8)
	if math.Abs((hi-lo)/2-wantHalf) > 1e-3 {
		t.Fatalf("half-width=%v want ~%v", (hi-lo)/2, wantHalf)
	}
	if math.Abs(lo+s.Mean()-s.Mean()-lo) > 1e-9 { // sanity
		t.Fatal("lo sanity")
	}
	if !(lo < 5 && 5 < hi) {
		t.Fatalf("CI [%v,%v] should contain mean 5", lo, hi)
	}
}

func TestStatsCILargeN(t *testing.T) {
	// Large n: CI should approach mean ± 1.96*sd/sqrt(n).
	s := NewStats()
	for i := 0; i < 10000; i++ {
		s.Push(float64(i % 100)) // mean ~49.5, sd ~28.87
	}
	lo, hi := s.ConfidenceInterval(0.05)
	half := (hi - lo) / 2
	want := 1.959964 * s.StdDev() / math.Sqrt(float64(s.N()))
	if math.Abs(half-want) > 1e-6 {
		t.Fatalf("half-width=%v want %v", half, want)
	}
}

func TestStatsPercentile(t *testing.T) {
	s := NewStats(StatsConfig{KeepValues: true})
	for i := 1; i <= 100; i++ {
		s.Push(float64(i))
	}
	if got := s.Percentile(50); got != 50.5 {
		t.Fatalf("p50=%v want 50.5", got)
	}
	if got := s.Percentile(0); got != 1 {
		t.Fatalf("p0=%v want 1", got)
	}
	if got := s.Percentile(100); got != 100 {
		t.Fatalf("p100=%v want 100", got)
	}
	if got := s.Percentile(25); math.Abs(got-25.75) > 1e-9 {
		t.Fatalf("p25=%v want 25.75", got)
	}
}

func TestStatsPercentileRequiresKeepValues(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic without KeepValues")
		}
	}()
	s := NewStats()
	s.Push(1)
	s.Percentile(50)
}

func TestNormalQuantile(t *testing.T) {
	tests := []struct {
		p    float64
		want float64
	}{
		{0.975, 1.959964},
		{0.95, 1.644854},
		{0.5, 0},
		{0.995, 2.575829},
	}
	for _, tt := range tests {
		got := normalQuantile(tt.p)
		if math.Abs(got-tt.want) > 1e-5 {
			t.Fatalf("normalQuantile(%v)=%v want ~%v", tt.p, got, tt.want)
		}
	}
}

func TestTCritical(t *testing.T) {
	tests := []struct {
		df   float64
		want float64
	}{
		{1, 12.706},
		{7, 2.365},
		{30, 2.042},
		{120, 1.980},
		{500, 1.959964}, // normal fallback
	}
	for _, tt := range tests {
		got := tCritical(tt.df, 0.05)
		if math.Abs(got-tt.want) > 1e-3 {
			t.Fatalf("tCritical(%v)=%v want ~%v", tt.df, got, tt.want)
		}
	}
}
