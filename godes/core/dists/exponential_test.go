package dists

import (
	"math"
	"testing"
)

func TestExponentialNew(t *testing.T) {
	d := NewExponential(10.0, 42)
	if d == nil {
		t.Fatal("expected non-nil distribution")
	}
	if d.Rate() != 10.0 {
		t.Fatalf("expected rate 10.0, got %f", d.Rate())
	}
}

func TestExponentialDeterministic(t *testing.T) {
	d1 := NewExponential(10.0, 42)
	d2 := NewExponential(10.0, 42)
	for i := 0; i < 10; i++ {
		v1 := d1.Sample()
		v2 := d2.Sample()
		if v1 != v2 {
			t.Fatalf("determinism violation at draw %d: %f vs %f", i, v1, v2)
		}
	}
}

func TestExponentialDifferentSeeds(t *testing.T) {
	d1 := NewExponential(10.0, 42)
	d2 := NewExponential(10.0, 99)
	v1 := d1.Sample()
	v2 := d2.Sample()
	if v1 == v2 {
		t.Logf("WARNING: same first draw from different seeds: %f", v1)
	}
}

func TestExponentialSamplePositive(t *testing.T) {
	d := NewExponential(10.0, 42)
	for i := 0; i < 100; i++ {
		v := d.Sample()
		if v < 0 || math.IsInf(v, 0) || math.IsNaN(v) {
			t.Fatalf("invalid sample at draw %d: %f", i, v)
		}
	}
}

func TestExponentialNewPanicsOnZeroRate(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for rate=0")
		}
	}()
	NewExponential(0, 42)
}

func TestExponentialNewPanicsOnNegativeRate(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for negative rate")
		}
	}()
	NewExponential(-1.0, 42)
}

func TestExponentialZeroValuePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on zero value Sample()")
		}
	}()
	var d ExponentialDistribution
	d.Sample()
}

func TestExponentialWithProvider(t *testing.T) {
	mock := &mockProvider{}
	site := DrawSite{Tag: "arrival", SiteID: 0}
	d := NewExponential(10.0, 42).WithProvider(mock, site)

	val := d.Sample()
	if val != 42.0 {
		t.Fatalf("expected delegated value 42.0, got %f", val)
	}
	if len(mock.calls) != 1 {
		t.Fatalf("expected 1 provider call, got %d", len(mock.calls))
	}
	if mock.calls[0].rate != 10.0 {
		t.Fatalf("expected rate 10.0, got %f", mock.calls[0].rate)
	}
	if mock.calls[0].site.SiteID != 0 {
		t.Fatalf("expected SiteID 0, got %d", mock.calls[0].site.SiteID)
	}
}

func TestExponentialWithProviderSkipsInternalRNG(t *testing.T) {
	mock := &mockProvider{}
	site := DrawSite{Tag: "test", SiteID: 1}
	d := NewExponential(5.0, 42).WithProvider(mock, site)

	d.Sample()
	d.Sample()
	if len(mock.calls) != 2 {
		t.Fatalf("expected 2 provider calls, got %d", len(mock.calls))
	}
}

func TestExponentialDifferentRatesDiverge(t *testing.T) {
	d1 := NewExponential(1.0, 42)
	d2 := NewExponential(10.0, 42)
	allSame := true
	for i := 0; i < 10; i++ {
		if d1.Sample() != d2.Sample() {
			allSame = false
			break
		}
	}
	if allSame {
		t.Fatal("different rates with same seed produced identical streams")
	}
}
