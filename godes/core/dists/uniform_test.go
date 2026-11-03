package dists

import (
	"math"
	"testing"
)

// seqSource returns a Source over a fixed deterministic sequence, used to
// pin exact outputs.
func seqSource(seq ...float64) Source {
	i := 0
	return FuncSource(func() float64 {
		if i >= len(seq) {
			return 0.5 // default beyond the pinned prefix
		}
		v := seq[i]
		i++
		return v
	})
}

// fixedSource always returns the same value.
func fixedSource(v float64) Source { return FuncSource(func() float64 { return v }) }

// prngSource returns a deterministic PRNG-backed source. Used primarily
// for rejection-sampling distributions (gamma/erlang/Marsaglia–Tsang)
// whose acceptance loop needs varying draws and would hang on a constant
// source.
func prngSource(seed uint64) Source {
	r := newTestPCG(seed)
	return FuncSource(r.Float64)
}

func TestUniformDistribution(t *testing.T) {
	tests := []struct {
		name string
		lo   float64
		hi   float64
		seq  []float64
		want float64
	}{
		{name: "zero draw", lo: 0, hi: 1, seq: []float64{0}, want: 0},
		{name: "mid draw", lo: 0, hi: 1, seq: []float64{0.5}, want: 0.5},
		{name: "offset range", lo: 10, hi: 20, seq: []float64{0.5}, want: 15},
		{name: "upper draw", lo: 0, hi: 1, seq: []float64{0.999}, want: 0.999},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewUniform(tt.lo, tt.hi, seqSource(tt.seq...))
			got := d.Sample()
			if math.Abs(got-tt.want) > 1e-9 {
				t.Fatalf("Sample() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUniformPanicsOnInvalidRange(t *testing.T) {
	for _, tc := range []struct {
		name string
		lo   float64
		hi   float64
	}{
		{name: "equal bounds", lo: 5, hi: 5},
		{name: "inverted bounds", lo: 5, hi: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected panic")
				}
			}()
			NewUniform(tc.lo, tc.hi, fixedSource(0.5))
		})
	}
}

func TestUniformNilSourcePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for nil Source")
		}
	}()
	NewUniform(0, 1, nil)
}

func TestUniformDeterministic(t *testing.T) {
	d1 := NewUniform(0, 1, seqSource(0.25, 0.5, 0.75))
	d2 := NewUniform(0, 1, seqSource(0.25, 0.5, 0.75))
	for i := 0; i < 3; i++ {
		if d1.Sample() != d2.Sample() {
			t.Fatalf("determinism violation at draw %d", i)
		}
	}
}

func TestIntegerDistribution(t *testing.T) {
	tests := []struct {
		name string
		min  int64
		max  int64
		seq  []float64
		want int64
	}{
		{name: "zero draw", min: 0, max: 9, seq: []float64{0}, want: 0},
		{name: "half draw", min: 0, max: 9, seq: []float64{0.5}, want: 5},
		{name: "near upper", min: 0, max: 9, seq: []float64{0.99}, want: 9},
		{name: "offset", min: 100, max: 103, seq: []float64{0.5}, want: 102},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := NewInteger(tt.min, tt.max, seqSource(tt.seq...))
			got := d.Sample()
			if got != tt.want {
				t.Fatalf("Sample() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestIntegerInRange(t *testing.T) {
	d := NewInteger(-5, 5, fixedSource(0.42))
	for i := 0; i < 100; i++ {
		v := d.Sample()
		if v < -5 || v > 5 {
			t.Fatalf("sample %d out of range", v)
		}
	}
}

func TestIntegerMean(t *testing.T) {
	d := NewInteger(0, 10, fixedSource(0.5))
	if math.Abs(d.Mean()-5) > 1e-9 {
		t.Fatalf("Mean() = %v, want 5", d.Mean())
	}
}

func TestIntegerPanicsOnInvertedRange(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic")
		}
	}()
	NewInteger(5, 3, fixedSource(0.5))
}
