package dists

import "math"

// PoissonDistribution samples a Poisson distribution with mean lambda.
// Uses the Knuth algorithm for lambda < 30 and a normal approximation
// with continuity correction for lambda >= 30 (a common, fast,
// deterministic approximation). Construct via NewPoisson.
type PoissonDistribution struct {
	lambda float64
	src    Source
}

// NewPoisson creates a Poisson distribution with mean lambda > 0.
// Panics if lambda <= 0 or src is nil.
func NewPoisson(lambda float64, src Source) *PoissonDistribution {
	if lambda <= 0 {
		panic("dists: Poisson requires lambda > 0")
	}
	if src == nil {
		panic("dists: Poisson requires a non-nil Source")
	}
	return &PoissonDistribution{lambda: lambda, src: src}
}

// Sample draws one non-negative integer from Poisson(lambda).
func (d *PoissonDistribution) Sample() int64 {
	if d.lambda < 30 {
		return knuthPoisson(d.lambda, d.src)
	}
	return approxPoisson(d.lambda, d.src)
}

// Mean returns the mean lambda.
func (d *PoissonDistribution) Mean() float64 { return d.lambda }

// knuthPoisson samples via the Knuth algorithm (multiplying uniforms
// until the product drops below e^-lambda).
func knuthPoisson(lambda float64, src Source) int64 {
	l := math.Exp(-lambda)
	var k int64
	p := 1.0
	for p > l {
		k++
		p *= unitGuard(src.Float64())
	}
	return k - 1
}

// approxPoisson samples via a normal approximation with continuity
// correction: max(0, round(N(lambda, lambda) - 0.5)).
func approxPoisson(lambda float64, src Source) int64 {
	// Box–Muller normal draw.
	u1 := unitGuard(src.Float64())
	u2 := src.Float64()
	z := math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
	v := lambda + z*math.Sqrt(lambda) - 0.5
	if v < 0 {
		return 0
	}
	return int64(math.Round(v))
}
