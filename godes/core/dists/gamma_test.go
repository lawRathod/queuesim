package dists

import (
	"math"
	"testing"
)

func TestGammaDistribution(t *testing.T) {
	// Gamma(1, 2) == Exponential(rate=1/2): inverse CDF -2*ln(1-u).
	d := NewGamma(1, 2, seqSource(0.5))
	want := 2 * math.Log(2)
	if math.Abs(d.Sample()-want) > 1e-9 {
		t.Fatalf("Gamma(1,2) Sample() = %v, want %v", d.Sample(), want)
	}
}

func TestGammaMean(t *testing.T) {
	d := NewGamma(3, 2, fixedSource(0.5))
	if math.Abs(d.Mean()-6) > 1e-9 {
		t.Fatalf("Mean() = %v, want 6", d.Mean())
	}
}

func TestGammaShapeScaleAccessors(t *testing.T) {
	d := NewGamma(2, 5, fixedSource(0.5))
	if math.Abs(d.Shape()-2) > 1e-9 || math.Abs(d.Scale()-5) > 1e-9 {
		t.Fatal("accessors wrong")
	}
}

func TestGammaFinitePositive(t *testing.T) {
	// k<1 boost path and k>=1 Marsaglia–Tsang path. Use a PRNG source so
	// the rejection sampler gets varying draws (a constant source hangs).
	for _, k := range []float64{0.5, 1, 2.5, 10} {
		d := NewGamma(k, 1, prngSource(uint64(k*100)+1))
		for i := 0; i < 50; i++ {
			v := d.Sample()
			if v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) {
				t.Fatalf("k=%v: non-positive/non-finite sample %v", k, v)
			}
		}
	}
}

func TestGammaDeterministic(t *testing.T) {
	// Determinism on a PRNG-backed source (rejection loop draws vary but
	// identically across two instances).
	d1 := NewGamma(2.5, 1, prngSource(7))
	d2 := NewGamma(2.5, 1, prngSource(7))
	for i := 0; i < 5; i++ {
		if d1.Sample() != d2.Sample() {
			t.Fatalf("determinism violation at draw %d", i)
		}
	}
}

func TestGammaPanicsOnInvalid(t *testing.T) {
	tests := []struct {
		name string
		k    float64
		th   float64
	}{
		{name: "zero shape", k: 0, th: 1},
		{name: "negative shape", k: -1, th: 1},
		{name: "zero scale", k: 1, th: 0},
		{name: "negative scale", k: 1, th: -2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected panic")
				}
			}()
			NewGamma(tt.k, tt.th, fixedSource(0.5))
		})
	}
}
