package dists

import "math"

// LognormalDistribution samples a lognormal distribution: ln(X) ~
// N(mu, sigma^2). Construct via NewLognormal. Each Sample consumes two
// source draws (via the underlying normal).
type LognormalDistribution struct {
	mu    float64
	sigma float64
	norm  *NormalDistribution
}

// NewLognormal creates a lognormal distribution where ln(X) ~ N(mu,
// sigma^2). Panics if sigma <= 0 or src is nil.
func NewLognormal(mu, sigma float64, src Source) *LognormalDistribution {
	if sigma <= 0 {
		panic("dists: Lognormal requires sigma > 0")
	}
	if src == nil {
		panic("dists: Lognormal requires a non-nil Source")
	}
	return &LognormalDistribution{
		mu:    mu,
		sigma: sigma,
		norm:  NewNormal(mu, sigma, src),
	}
}

// Sample draws one value from the lognormal distribution (always > 0).
func (d *LognormalDistribution) Sample() float64 {
	return math.Exp(d.norm.Sample())
}

// Mean returns the expected value exp(mu + sigma^2/2).
func (d *LognormalDistribution) Mean() float64 {
	return math.Exp(d.mu + d.sigma*d.sigma/2)
}

// Median returns exp(mu).
func (d *LognormalDistribution) Median() float64 { return math.Exp(d.mu) }
