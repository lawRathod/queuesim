package dists

import (
	"math"
	"testing"
)

func TestLognormalDistribution(t *testing.T) {
	// ln(X) = z, so X = exp(z). Reuse the normal pin: u1=0.5,u2=0.5 -> z = -sqrt(2 ln 2).
	d := NewLognormal(0, 1, seqSource(0.5, 0.5))
	got := d.Sample()
	want := math.Exp(-math.Sqrt(2 * math.Log(2)))
	if math.Abs(got-want) > 1e-9 {
		t.Fatalf("Sample() = %v, want %v", got, want)
	}
}

func TestLognormalPositive(t *testing.T) {
	d := NewLognormal(0, 1, fixedSource(0.3))
	for i := 0; i < 100; i++ {
		if v := d.Sample(); v <= 0 || math.IsNaN(v) || math.IsInf(v, 0) {
			t.Fatalf("non-positive/non-finite sample %v", v)
		}
	}
}

func TestLognormalMeanMedian(t *testing.T) {
	d := NewLognormal(1, 2, fixedSource(0.5))
	if math.Abs(d.Mean()-math.Exp(1+2)) > 1e-9 {
		t.Fatalf("Mean() = %v, want %v", d.Mean(), math.Exp(1+2))
	}
	if math.Abs(d.Median()-math.Exp(1)) > 1e-9 {
		t.Fatalf("Median() = %v, want %v", d.Median(), math.Exp(1))
	}
}

func TestLognormalDeterministic(t *testing.T) {
	d1 := NewLognormal(0, 1, seqSource(0.2, 0.6))
	d2 := NewLognormal(0, 1, seqSource(0.2, 0.6))
	if d1.Sample() != d2.Sample() {
		t.Fatal("determinism violation")
	}
}

func TestLognormalPanicsOnInvalidSigma(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for sigma<=0")
		}
	}()
	NewLognormal(0, 0, fixedSource(0.5))
}
