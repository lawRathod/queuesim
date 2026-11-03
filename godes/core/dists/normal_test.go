package dists

import (
	"math"
	"testing"
)

func TestNormalDistribution(t *testing.T) {
	// Box–Muller with u1=0.5, u2=0.5: sqrt(-2 ln 0.5)*cos(pi) = -sqrt(2 ln 2) ≈ -1.1774.
	d := NewNormal(0, 1, seqSource(0.5, 0.5))
	got := d.Sample()
	want := -math.Sqrt(2 * math.Log(2))
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("Sample() = %v, want %v", got, want)
	}
}

func TestNormalShiftAndScale(t *testing.T) {
	// mu=5, sigma=2: same shape, shifted/scaled by the deterministic z.
	d1 := NewNormal(0, 1, seqSource(0.5, 0.5))
	z := d1.Sample()
	d2 := NewNormal(5, 2, seqSource(0.5, 0.5))
	if math.Abs(d2.Sample()-(5+2*z)) > 1e-9 {
		t.Fatalf("shift/scale incorrect")
	}
}

func TestNormalMoments(t *testing.T) {
	d := NewNormal(2.5, 0.8, fixedSource(0.5))
	if math.Abs(d.Mean()-2.5) > 1e-9 {
		t.Fatalf("Mean() = %v, want 2.5", d.Mean())
	}
	if math.Abs(d.StdDev()-0.8) > 1e-9 {
		t.Fatalf("StdDev() = %v, want 0.8", d.StdDev())
	}
}

func TestNormalFinite(t *testing.T) {
	d := NewNormal(0, 1, fixedSource(0.3))
	for i := 0; i < 100; i++ {
		v := d.Sample()
		if math.IsInf(v, 0) || math.IsNaN(v) {
			t.Fatalf("non-finite sample %v", v)
		}
	}
}

func TestNormalDeterministic(t *testing.T) {
	d1 := NewNormal(0, 1, seqSource(0.2, 0.7, 0.1))
	d2 := NewNormal(0, 1, seqSource(0.2, 0.7, 0.1))
	for i := 0; i < 3; i++ {
		if d1.Sample() != d2.Sample() {
			t.Fatalf("determinism violation at draw %d", i)
		}
	}
}

func TestNormalPanicsOnInvalidSigma(t *testing.T) {
	for _, sigma := range []float64{0, -1} {
		func(s float64) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected panic for sigma=", s)
				}
			}()
			NewNormal(0, s, fixedSource(0.5))
		}(sigma)
	}
}

func TestNormalNilSourcePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for nil Source")
		}
	}()
	NewNormal(0, 1, nil)
}
