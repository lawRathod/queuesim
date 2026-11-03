package dists

import (
	"math"
	"testing"
)

func TestEmpiricalDistribution(t *testing.T) {
	d := NewEmpirical([]float64{10, 20, 30}, []float64{1, 2, 1}, seqSource(0.1, 0.5, 0.9))
	// CDF: 0.25, 0.75, 1.0
	if got := d.Sample(); got != 10 {
		t.Fatalf("draw 0.1 -> %v, want 10", got)
	}
	if got := d.Sample(); got != 20 {
		t.Fatalf("draw 0.5 -> %v, want 20", got)
	}
	if got := d.Sample(); got != 30 {
		t.Fatalf("draw 0.9 -> %v, want 30", got)
	}
}

func TestEmpiricalFallbackLastValue(t *testing.T) {
	// A draw in the final bin returns the last value.
	d := NewEmpirical([]float64{1, 2}, []float64{1, 1}, seqSource(0.999999))
	if got := d.Sample(); got != 2 {
		t.Fatalf("got %v, want 2", got)
	}
	// The iterative CDF search must never miss the last value when u is
	// at (or beyond) the normalized 1.0 boundary of the final bin.
	d2 := NewEmpirical([]float64{1, 2}, []float64{1, 1}, seqSource(1.0))
	if got := d2.Sample(); got != 2 {
		t.Fatalf("boundary draw got %v, want 2 (last-value fallback)", got)
	}
}

func TestEmpiricalMean(t *testing.T) {
	d := NewEmpirical([]float64{0, 10}, []float64{3, 1}, fixedSource(0.5))
	if math.Abs(d.Mean()-2.5) > 1e-9 {
		t.Fatalf("Mean() = %v, want 2.5", d.Mean())
	}
}

func TestEmpiricalCopies(t *testing.T) {
	d := NewEmpirical([]float64{1, 2}, []float64{1, 1}, fixedSource(0.5))
	v := d.Values()
	w := d.Weights()
	v[0] = 99
	w[0] = 99
	// Mutating the copies must not affect the distribution.
	if got := d.Sample(); got != 1 && got != 2 {
		t.Fatalf("distribution mutated by caller, got %v", got)
	}
}

func TestEmpiricalDeterministic(t *testing.T) {
	d1 := NewEmpirical([]float64{1, 2, 3}, []float64{1, 1, 1}, seqSource(0.2, 0.8))
	d2 := NewEmpirical([]float64{1, 2, 3}, []float64{1, 1, 1}, seqSource(0.2, 0.8))
	if d1.Sample() != d2.Sample() || d1.Sample() != d2.Sample() {
		t.Fatal("determinism violation")
	}
}

func TestEmpiricalPanicsOnInvalid(t *testing.T) {
	tests := []struct {
		name   string
		values []float64
		weight []float64
	}{
		{name: "empty", values: nil, weight: nil},
		{name: "mismatch", values: []float64{1, 2}, weight: []float64{1}},
		{name: "negative weight", values: []float64{1}, weight: []float64{-1}},
		{name: "zero total", values: []float64{1}, weight: []float64{0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected panic")
				}
			}()
			NewEmpirical(tt.values, tt.weight, fixedSource(0.5))
		})
	}
}

func TestEmpiricalNilSourcePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for nil Source")
		}
	}()
	NewEmpirical([]float64{1}, []float64{1}, nil)
}
