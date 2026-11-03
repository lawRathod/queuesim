package dists

// ErlangDistribution samples an Erlang distribution — Gamma with integer
// shape k — via the sum of k independent exponential(rate) draws. This
// is the standard, exact construction (no approximation). Construct via
// NewErlang. Each Sample consumes k source draws.
type ErlangDistribution struct {
	k     int
	rate  float64
	gamma *GammaDistribution
}

// NewErlang creates an Erlang distribution with shape k >= 1 and rate
// lambda > 0 (mean k/lambda). Panics on invalid parameters or nil src.
func NewErlang(k int, rate float64, src Source) *ErlangDistribution {
	if k < 1 {
		panic("dists: Erlang requires k >= 1")
	}
	if rate <= 0 {
		panic("dists: Erlang requires rate > 0")
	}
	if src == nil {
		panic("dists: Erlang requires a non-nil Source")
	}
	return &ErlangDistribution{
		k:     k,
		rate:  rate,
		gamma: NewGamma(float64(k), 1/rate, src),
	}
}

// Sample draws one value from Erlang(k, rate) — Gamma(k, 1/rate) with
// integer shape.
func (d *ErlangDistribution) Sample() float64 {
	return d.gamma.Sample()
}

// Shape returns the shape k.
func (d *ErlangDistribution) Shape() int { return d.k }

// Rate returns the rate lambda.
func (d *ErlangDistribution) Rate() float64 { return d.rate }

// Mean returns the expected value k/rate.
func (d *ErlangDistribution) Mean() float64 { return float64(d.k) / d.rate }
