package dists

import "math"

// NormalDistribution samples a normal distribution with the Box–Muller
// transform, consuming exactly two source draws per Sample. The zero value
// is not usable; construct via NewNormal.
type NormalDistribution struct {
	mu    float64
	sigma float64
	src   Source
}

// NewNormal creates a normal distribution with mean mu and standard
// deviation sigma. Panics if sigma <= 0 or src is nil.
func NewNormal(mu, sigma float64, src Source) *NormalDistribution {
	if sigma <= 0 {
		panic("dists: Normal requires sigma > 0")
	}
	if src == nil {
		panic("dists: Normal requires a non-nil Source")
	}
	return &NormalDistribution{mu: mu, sigma: sigma, src: src}
}

// Sample draws one value from N(mu, sigma^2) via Box–Muller.
func (d *NormalDistribution) Sample() float64 {
	u1 := unitGuard(d.src.Float64())
	u2 := d.src.Float64()
	return d.mu + d.sigma*math.Sqrt(-2*math.Log(u1))*math.Cos(2*math.Pi*u2)
}

// Mean returns the mean mu.
func (d *NormalDistribution) Mean() float64 { return d.mu }

// StdDev returns the standard deviation sigma.
func (d *NormalDistribution) StdDev() float64 { return d.sigma }
