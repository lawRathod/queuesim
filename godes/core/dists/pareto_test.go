package dists

import (
	"math"
	"testing"
)

func TestParetoDistribution(t *testing.T) {
	// xm=1, alpha=2, u=0.5: x = 1 / (0.5)^(1/2) = sqrt(2).
	d := NewPareto(1, 2, seqSource(0.5))
	want := math.Sqrt(2)
	if math.Abs(d.Sample()-want) > 1e-9 {
		t.Fatalf("Sample() = %v, want %v", d.Sample(), want)
	}
}

func TestParetoMean(t *testing.T) {
	// alpha>1: xm*alpha/(alpha-1)
	d := NewPareto(2, 3, fixedSource(0.5))
	if math.Abs(d.Mean()-3) > 1e-9 { // 2*3/2 = 3
		t.Fatalf("Mean() = %v, want 3", d.Mean())
	}
	// alpha<=1: infinite mean
	d2 := NewPareto(2, 1, fixedSource(0.5))
	if !math.IsInf(d2.Mean(), 1) {
		t.Fatalf("Mean() for alpha=1 should be +Inf, got %v", d2.Mean())
	}
}

func TestParetoPositive(t *testing.T) {
	d := NewPareto(1, 1.5, fixedSource(0.3))
	for i := 0; i < 100; i++ {
		if v := d.Sample(); v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) {
			t.Fatalf("non-positive/non-finite sample %v", v)
		}
	}
}

func TestParetoDeterministic(t *testing.T) {
	d1 := NewPareto(1, 2, seqSource(0.3, 0.8))
	d2 := NewPareto(1, 2, seqSource(0.3, 0.8))
	if d1.Sample() != d2.Sample() || d1.Sample() != d2.Sample() {
		t.Fatal("determinism violation")
	}
}

func TestParetoPanicsOnInvalid(t *testing.T) {
	tests := []struct {
		name string
		xm   float64
		al   float64
	}{
		{name: "zero scale", xm: 0, al: 2},
		{name: "negative scale", xm: -1, al: 2},
		{name: "zero shape", xm: 1, al: 0},
		{name: "negative shape", xm: 1, al: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected panic")
				}
			}()
			NewPareto(tt.xm, tt.al, fixedSource(0.5))
		})
	}
}
