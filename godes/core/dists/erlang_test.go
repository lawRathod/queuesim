package dists

import (
	"math"
	"testing"
)

func TestErlangDistribution(t *testing.T) {
	// Erlang(2, rate=1) = Gamma(2, 1): Marsaglia–Tsang k=2 path.
	// Use a PRNG source for the rejection sampler.
	d := NewErlang(2, 1, prngSource(3))
	if math.Abs(d.Mean()-2) > 1e-9 {
		t.Fatalf("Mean() = %v, want 2", d.Mean())
	}
	if d.Shape() != 2 || math.Abs(d.Rate()-1) > 1e-9 {
		t.Fatal("accessors wrong")
	}
	for i := 0; i < 50; i++ {
		if v := d.Sample(); v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) {
			t.Fatalf("non-positive/non-finite sample %v", v)
		}
	}
}

func TestErlangDeterministic(t *testing.T) {
	d1 := NewErlang(3, 2, prngSource(11))
	d2 := NewErlang(3, 2, prngSource(11))
	for i := 0; i < 5; i++ {
		if d1.Sample() != d2.Sample() {
			t.Fatalf("determinism violation at draw %d", i)
		}
	}
}

func TestErlangPanicsOnInvalid(t *testing.T) {
	tests := []struct {
		name string
		k    int
		rate float64
	}{
		{name: "zero shape", k: 0, rate: 1},
		{name: "negative shape", k: -2, rate: 1},
		{name: "zero rate", k: 1, rate: 0},
		{name: "negative rate", k: 1, rate: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected panic")
				}
			}()
			NewErlang(tt.k, tt.rate, fixedSource(0.5))
		})
	}
}

func TestDeterministicDistribution(t *testing.T) {
	d := NewDeterministic(42.5)
	if d.Sample() != 42.5 || d.Sample() != 42.5 {
		t.Fatal("deterministic dist must always return the fixed value")
	}
	if d.Value() != 42.5 {
		t.Fatalf("Value() = %v, want 42.5", d.Value())
	}
	// Consumes zero draws by construction — no source needed.
	if d.Sample() != 42.5 {
		t.Fatal("deterministic dist must not need a source")
	}
}
