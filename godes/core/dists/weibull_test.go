package dists

import (
	"math"
	"testing"
)

func TestWeibullDistribution(t *testing.T) {
	// k=2, lambda=1, u=0.5: x = 1 * (-ln(0.5))^(1/2) = sqrt(ln 2).
	d := NewWeibull(2, 1, seqSource(0.5))
	want := math.Sqrt(math.Log(2))
	if math.Abs(d.Sample()-want) > 1e-9 {
		t.Fatalf("Sample() = %v, want %v", d.Sample(), want)
	}
}

func TestWeibullScale(t *testing.T) {
	d1 := NewWeibull(2, 1, seqSource(0.4))
	d2 := NewWeibull(2, 3, seqSource(0.4))
	if math.Abs(d2.Sample()/d1.Sample()-3) > 1e-9 {
		t.Fatal("scale not multiplicative")
	}
}

func TestWeibullShapeScaleAccessors(t *testing.T) {
	d := NewWeibull(3, 4, fixedSource(0.5))
	if math.Abs(d.Shape()-3) > 1e-9 || math.Abs(d.Scale()-4) > 1e-9 {
		t.Fatal("accessors wrong")
	}
}

func TestWeibullMean(t *testing.T) {
	// lambda * Gamma(1 + 1/k)
	d := NewWeibull(2, 1, fixedSource(0.5))
	g, _ := math.Lgamma(1.5)
	want := math.Exp(g) // Gamma(1.5) = sqrt(pi)/2
	if math.Abs(d.Mean()-want) > 1e-9 {
		t.Fatalf("Mean() = %v, want %v", d.Mean(), want)
	}
}

func TestWeibullDeterministic(t *testing.T) {
	d1 := NewWeibull(2, 1, seqSource(0.3, 0.9))
	d2 := NewWeibull(2, 1, seqSource(0.3, 0.9))
	if d1.Sample() != d2.Sample() || d1.Sample() != d2.Sample() {
		t.Fatal("determinism violation")
	}
}

func TestWeibullPanicsOnInvalid(t *testing.T) {
	tests := []struct {
		name string
		k    float64
		lam  float64
	}{
		{name: "zero shape", k: 0, lam: 1},
		{name: "negative shape", k: -1, lam: 1},
		{name: "zero scale", k: 1, lam: 0},
		{name: "negative scale", k: 1, lam: -2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected panic")
				}
			}()
			NewWeibull(tt.k, tt.lam, fixedSource(0.5))
		})
	}
}
