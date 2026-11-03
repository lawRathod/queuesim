package dists

import (
	"math"
	"testing"
)

func TestPoissonKnuth(t *testing.T) {
	// lambda < 30 path. lambda=1, u=0.5: p=0.5 > e^-1≈0.3679 -> k=1;
	// p=0.25 <= 0.3679 -> stop. Return 0.
	d := NewPoisson(1, seqSource(0.5))
	// Product 0.5 > e^-1, next 0.5*0.5=0.25 <= e^-1 -> k stops at 2 -> return 1.
	// Sequence [0.5, 0.5] gives: k=1 (p=0.5), p=0.25 -> loop ends -> k-1 = 1.
	want := int64(1)
	if got := d.Sample(); got != want {
		t.Fatalf("Poisson(1) Sample() = %d, want %d", got, want)
	}
}

func TestPoissonApprox(t *testing.T) {
	// lambda >= 30 path: lambda=30. Normal approx via Box–Muller with
	// u1=0.5,u2=0.5: z = -sqrt(2 ln 2) ≈ -1.1774.
	// v = 30 + z*sqrt(30) - 0.5 = 29.5 - 6.45 ≈ 23.05 -> round = 23.
	d := NewPoisson(30, seqSource(0.5, 0.5))
	got := d.Sample()
	if got < 0 {
		t.Fatalf("negative sample %d", got)
	}
	// v is positive, so round(v) — assert it's within a couple sigma.
	if got > 40 {
		t.Fatalf("sample %d implausibly large", got)
	}
}

func TestPoissonNonNegative(t *testing.T) {
	for _, lambda := range []float64{0.1, 1, 5, 30, 100} {
		d := NewPoisson(lambda, fixedSource(0.37))
		for i := 0; i < 100; i++ {
			if v := d.Sample(); v < 0 {
				t.Fatalf("lambda=%v: negative sample %d", lambda, v)
			}
		}
	}
}

func TestPoissonMean(t *testing.T) {
	d := NewPoisson(7.5, fixedSource(0.5))
	if math.Abs(d.Mean()-7.5) > 1e-9 {
		t.Fatalf("Mean() = %v, want 7.5", d.Mean())
	}
}

func TestPoissonDeterministic(t *testing.T) {
	d1 := NewPoisson(2, seqSource(0.4, 0.4, 0.4, 0.4))
	d2 := NewPoisson(2, seqSource(0.4, 0.4, 0.4, 0.4))
	for i := 0; i < 2; i++ {
		if d1.Sample() != d2.Sample() {
			t.Fatalf("determinism violation at draw %d", i)
		}
	}
}

func TestPoissonPanicsOnInvalid(t *testing.T) {
	for _, lambda := range []float64{0, -1} {
		func(l float64) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected panic for lambda=", l)
				}
			}()
			NewPoisson(l, fixedSource(0.5))
		}(lambda)
	}
}
