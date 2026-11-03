package dists

import "math"

// ParetoDistribution samples a Pareto distribution with scale xm > 0 and
// shape alpha > 0. Construct via NewPareto. Each Sample consumes one draw.
type ParetoDistribution struct {
	xm, alpha float64
	src       Source
}

// NewPareto creates a Pareto distribution with scale xm > 0 and shape
// alpha > 0. Panics on invalid parameters or nil src.
func NewPareto(xm, alpha float64, src Source) *ParetoDistribution {
	if xm <= 0 || alpha <= 0 {
		panic("dists: Pareto requires xm > 0 and alpha > 0")
	}
	if src == nil {
		panic("dists: Pareto requires a non-nil Source")
	}
	return &ParetoDistribution{xm: xm, alpha: alpha, src: src}
}

// Sample draws one value from the Pareto distribution via inverse CDF.
func (d *ParetoDistribution) Sample() float64 {
	u := unitGuard(d.src.Float64())
	return d.xm / math.Pow(1-u, 1/d.alpha)
}

// Mean returns the expected value (xm*alpha/(alpha-1)) when alpha > 1;
// +Inf otherwise (infinite mean for alpha <= 1).
func (d *ParetoDistribution) Mean() float64 {
	if d.alpha <= 1 {
		return math.Inf(1)
	}
	return d.xm * d.alpha / (d.alpha - 1)
}
