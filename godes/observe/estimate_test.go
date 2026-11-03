package observe

import (
	"math"
	"testing"
)

func TestBatchMeansKnown(t *testing.T) {
	// 4 batches of 2: means 1.5, 3.5, 5.5, 7.5 → grand 4.5,
	// sample variance = ((−3)²+(−1)²+1²+3²)/3 = 20/3, std_err = sqrt(var/4).
	samples := []float64{1, 2, 3, 4, 5, 6, 7, 8}
	est, ok := BatchMeans(samples, 4, 0.95)
	if !ok {
		t.Fatal("BatchMeans ok=false on a valid input")
	}
	if est.Batches != 4 || est.BatchSize != 2 {
		t.Fatalf("Batches/BatchSize = %d/%d, want 4/2", est.Batches, est.BatchSize)
	}
	if math.Abs(est.Mean-4.5) > 1e-12 {
		t.Fatalf("Mean = %v, want 4.5", est.Mean)
	}
	wantSE := math.Sqrt((20.0 / 3) / 4)
	if math.Abs(est.StdErr-wantSE) > 1e-12 {
		t.Fatalf("StdErr = %v, want %v", est.StdErr, wantSE)
	}
	z := normalQuantile(0.975)
	if math.Abs(est.CILow-(4.5-z*wantSE)) > 1e-9 || math.Abs(est.CIHigh-(4.5+z*wantSE)) > 1e-9 {
		t.Fatalf("CI = [%v,%v], want mean ± z·se", est.CILow, est.CIHigh)
	}
}

// TestBatchMeansConstantSeries pins the zero-variance case: a constant series
// has zero batch-mean variance, so the interval width is exactly 0 (never NaN).
func TestBatchMeansConstantSeries(t *testing.T) {
	samples := make([]float64, 20)
	for i := range samples {
		samples[i] = 3.25
	}
	est, ok := BatchMeans(samples, 4, 0.95)
	if !ok {
		t.Fatal("BatchMeans ok=false on a constant series")
	}
	if est.Mean != 3.25 || est.StdErr != 0 || est.CILow != 3.25 || est.CIHigh != 3.25 {
		t.Fatalf("constant series estimate = %+v, want mean 3.25 with zero-width CI", est)
	}
	if math.IsNaN(est.CILow) || math.IsNaN(est.CIHigh) {
		t.Fatal("constant series produced a NaN interval")
	}
}

func TestBatchMeansRejects(t *testing.T) {
	samples := []float64{1, 2, 3, 4, 5, 6}
	cases := []struct {
		name       string
		samples    []float64
		batches    int
		confidence float64
	}{
		{"nil", nil, 4, 0.95},
		{"empty", []float64{}, 4, 0.95},
		{"batches<2", samples, 1, 0.95},
		{"batches=0", samples, 0, 0.95},
		{"batches>len", samples, 8, 0.95},
		{"confidence=0", samples, 3, 0},
		{"confidence<0", samples, 3, -0.5},
		{"confidence=1", samples, 3, 1},
		{"confidence=NaN", samples, 3, math.NaN()},
	}
	for _, c := range cases {
		if _, ok := BatchMeans(c.samples, c.batches, c.confidence); ok {
			t.Fatalf("%s: BatchMeans ok=true, want false", c.name)
		}
	}
}

// TestBatchMeansFloorSplit pins the floor split: a remainder is discarded so
// every batch is the same size.
func TestBatchMeansFloorSplit(t *testing.T) {
	est, ok := BatchMeans([]float64{1, 2, 3, 4, 5, 6, 7}, 3, 0.95)
	if !ok {
		t.Fatal("ok=false")
	}
	if est.BatchSize != 2 || est.Batches != 3 {
		t.Fatalf("Batches/BatchSize = %d/%d, want 3/2 (the 7th sample is discarded)", est.Batches, est.BatchSize)
	}
}

func TestWilsonIntervalKnown(t *testing.T) {
	cases := []struct {
		name              string
		successes, trials uint64
		wantLo, wantHi    float64
	}{
		{"0/10", 0, 10, 0.0, 0.2775},
		{"10/10", 10, 10, 0.7225, 1.0},
		{"50/100", 50, 100, 0.4038, 0.5962},
	}
	for _, c := range cases {
		lo, hi, ok := WilsonInterval(c.successes, c.trials, 0.95)
		if !ok {
			t.Fatalf("%s: ok=false", c.name)
		}
		if math.Abs(lo-c.wantLo) > 1e-3 || math.Abs(hi-c.wantHi) > 1e-3 {
			t.Fatalf("%s: interval = [%v,%v], want [%v,%v]", c.name, lo, hi, c.wantLo, c.wantHi)
		}
	}
}

// TestWilsonIntervalAllSuccessAllFailure pins the boundary inputs: the
// interval stays inside [0,1] and is non-degenerate (the Wilson interval does
// not collapse to a point, unlike the Wald interval).
func TestWilsonIntervalAllSuccessAllFailure(t *testing.T) {
	lo, hi, ok := WilsonInterval(100, 100, 0.95)
	if !ok {
		t.Fatal("all-success: ok=false")
	}
	if hi != 1 || lo <= 0 || lo >= 1 {
		t.Fatalf("all-success interval = [%v,%v], want hi=1 and 0<lo<1", lo, hi)
	}
	lo, hi, ok = WilsonInterval(0, 100, 0.95)
	if !ok {
		t.Fatal("all-failure: ok=false")
	}
	if lo != 0 || hi <= 0 || hi >= 1 {
		t.Fatalf("all-failure interval = [%v,%v], want lo=0 and 0<hi<1", lo, hi)
	}
}

func TestWilsonIntervalRejects(t *testing.T) {
	cases := []struct {
		name              string
		successes, trials uint64
		confidence        float64
	}{
		{"trials=0", 0, 0, 0.95},
		{"successes>trials", 5, 3, 0.95},
		{"confidence=0", 1, 2, 0},
		{"confidence<0", 1, 2, -0.25},
		{"confidence=1", 1, 2, 1},
		{"confidence=NaN", 1, 2, math.NaN()},
	}
	for _, c := range cases {
		if _, _, ok := WilsonInterval(c.successes, c.trials, c.confidence); ok {
			t.Fatalf("%s: ok=true, want false", c.name)
		}
	}
}
