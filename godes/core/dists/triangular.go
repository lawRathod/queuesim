package dists

import "math"

// TriangularDistribution samples a triangular distribution over [lo, hi)
// with mode m. Construct via NewTriangular. Each Sample consumes one draw.
type TriangularDistribution struct {
	lo, hi, mode float64
	src          Source
}

// NewTriangular creates a triangular distribution with lower bound lo,
// upper bound hi, and mode. Panics if lo >= hi, mode outside [lo,hi], or
// src is nil.
func NewTriangular(lo, hi, mode float64, src Source) *TriangularDistribution {
	if lo >= hi {
		panic("dists: Triangular requires lo < hi")
	}
	if mode < lo || mode > hi {
		panic("dists: Triangular requires mode in [lo, hi]")
	}
	if src == nil {
		panic("dists: Triangular requires a non-nil Source")
	}
	return &TriangularDistribution{lo: lo, hi: hi, mode: mode, src: src}
}

// Sample draws one value from the triangular distribution via inverse CDF.
func (d *TriangularDistribution) Sample() float64 {
	u := d.src.Float64()
	fc := (d.mode - d.lo) / (d.hi - d.lo)
	if u < fc {
		return d.lo + math.Sqrt(u*(d.hi-d.lo)*(d.mode-d.lo))
	}
	return d.hi - math.Sqrt((1-u)*(d.hi-d.lo)*(d.hi-d.mode))
}

// Mean returns the expected value (lo+mode+hi)/3.
func (d *TriangularDistribution) Mean() float64 {
	return (d.lo + d.mode + d.hi) / 3
}
