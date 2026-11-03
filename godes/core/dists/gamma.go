package dists

import "math"

// GammaDistribution samples a gamma distribution with shape k > 0 and
// scale theta > 0 (mean k*theta). Uses Marsaglia–Tsang for k >= 1 and
// the k<1 boost of add 1 for k in (0,1). Construct via NewGamma.
type GammaDistribution struct {
	k, theta float64
	src      Source
}

// NewGamma creates a gamma distribution with shape k > 0 and scale theta
// > 0. Panics on invalid parameters or nil src.
func NewGamma(k, theta float64, src Source) *GammaDistribution {
	if k <= 0 || theta <= 0 {
		panic("dists: Gamma requires k > 0 and theta > 0")
	}
	if src == nil {
		panic("dists: Gamma requires a non-nil Source")
	}
	return &GammaDistribution{k: k, theta: theta, src: src}
}

// Sample draws one value from Gamma(k, theta). Special cases: k == 1 is
// the exponential distribution (exact, no rejection); k < 1 draws
// Gamma(k+1, theta) and multiplies by U^(1/k) (the standard boost).
func (d *GammaDistribution) Sample() float64 {
	k := d.k
	if k == 1 {
		// Exponential(scale=theta): inverse CDF -theta*ln(1-u).
		u := unitGuard(d.src.Float64())
		return d.theta * -math.Log(1-u)
	}
	if k < 1 {
		x := marsagliaTsang(k+1, d.src)
		u := unitGuard(d.src.Float64())
		return d.theta * x * math.Pow(u, 1/d.k)
	}
	return d.theta * marsagliaTsang(k, d.src)
}

// Shape returns the shape parameter k.
func (d *GammaDistribution) Shape() float64 { return d.k }

// Scale returns the scale parameter theta.
func (d *GammaDistribution) Scale() float64 { return d.theta }

// Mean returns the expected value k*theta.
func (d *GammaDistribution) Mean() float64 { return d.k * d.theta }

// marsagliaTsang samples Gamma(k, 1) for k >= 1.
func marsagliaTsang(k float64, src Source) float64 {
	d := k - 1.0/3
	c := 1 / math.Sqrt(9*d)
	for {
		u := src.Float64()
		// Normal draw via Box–Muller (consumes two draws).
		v := 0.0
		for v == 0 {
			u1 := unitGuard(src.Float64())
			u2 := src.Float64()
			v = math.Sqrt(-2*math.Log(u1)) * math.Cos(2*math.Pi*u2)
		}
		x := v * v
		if v >= -1/c && math.Log(u) < 0.5*x+d*(1-v+math.Log(v)) {
			return d * v
		}
	}
}
