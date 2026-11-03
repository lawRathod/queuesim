package dists

import (
	"math"
	"testing"
)

func TestTriangularDistribution(t *testing.T) {
	tests := []struct {
		name string
		lo   float64
		hi   float64
		mode float64
		seq  []float64
		want float64
	}{
		// Symmetric [0,2] mode 1: u<0.5 branch -> lo + sqrt(u*(2)*(1))
		{name: "below mode", lo: 0, hi: 2, mode: 1, seq: []float64{0.25}, want: math.Sqrt(0.25 * 2)},
		// u=0.5 exactly: below branch (u<fc=0.5) -> lo+sqrt(0.5*2*1)=1
		{name: "at mode", lo: 0, hi: 2, mode: 1, seq: []float64{0.5}, want: 1},
		// u=0.75 above mode -> hi - sqrt((0.25)*2*1) = 2 - sqrt(0.5)
		{name: "above mode", lo: 0, hi: 2, mode: 1, seq: []float64{0.75}, want: 2 - math.Sqrt(0.5)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewTriangular(tt.lo, tt.hi, tt.mode, seqSource(tt.seq...))
			got := d.Sample()
			if math.Abs(got-tt.want) > 1e-9 {
				t.Fatalf("Sample() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTriangularMean(t *testing.T) {
	d := NewTriangular(0, 2, 1, fixedSource(0.5))
	// (lo+mode+hi)/3 = 1
	if math.Abs(d.Mean()-1) > 1e-9 {
		t.Fatalf("Mean() = %v, want 1", d.Mean())
	}
}

func TestTriangularDeterministic(t *testing.T) {
	d1 := NewTriangular(0, 1, 0.5, seqSource(0.2, 0.8))
	d2 := NewTriangular(0, 1, 0.5, seqSource(0.2, 0.8))
	if d1.Sample() != d2.Sample() || d1.Sample() != d2.Sample() {
		t.Fatal("determinism violation")
	}
}

func TestTriangularPanicsOnInvalid(t *testing.T) {
	tests := []struct {
		name string
		lo   float64
		hi   float64
		mode float64
	}{
		{name: "inverted", lo: 2, hi: 1, mode: 1.5},
		{name: "equal", lo: 1, hi: 1, mode: 1},
		{name: "mode below", lo: 0, hi: 2, mode: -1},
		{name: "mode above", lo: 0, hi: 2, mode: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected panic")
				}
			}()
			NewTriangular(tt.lo, tt.hi, tt.mode, fixedSource(0.5))
		})
	}
}
