package dists

import "math"

// WeibullDistribution samples a Weibull distribution with shape k and
// scale lambda. Construct via NewWeibull. Each Sample consumes one draw.
type WeibullDistribution struct {
	k, lambda float64
	src       Source
}

// NewWeibull creates a Weibull distribution with shape k > 0 and scale
// lambda > 0. Panics on invalid parameters or nil src.
func NewWeibull(k, lambda float64, src Source) *WeibullDistribution {
	if k <= 0 || lambda <= 0 {
		panic("dists: Weibull requires k > 0 and lambda > 0")
	}
	if src == nil {
		panic("dists: Weibull requires a non-nil Source")
	}
	return &WeibullDistribution{k: k, lambda: lambda, src: src}
}

// Sample draws one value from the Weibull distribution via inverse CDF.
func (d *WeibullDistribution) Sample() float64 {
	u := unitGuard(d.src.Float64())
	return d.lambda * math.Pow(-math.Log(1-u), 1/d.k)
}

// Shape returns the shape parameter k.
func (d *WeibullDistribution) Shape() float64 { return d.k }

// Scale returns the scale parameter lambda.
func (d *WeibullDistribution) Scale() float64 { return d.lambda }

// Mean returns the expected value lambda * Gamma(1 + 1/k).
func (d *WeibullDistribution) Mean() float64 {
	return d.lambda * gammaLnExp(1+1/d.k)
}
