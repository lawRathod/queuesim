package dists

// Source draws uniform random values in [0, 1) from a deterministic random
// source. Any implementation of core.RNG satisfies Source (both expose
// Float64()), as does FuncSource over a plain closure.
//
// Each distribution in this package is built on Source so that determinism,
// seeding, and (via a DrawSite-aware source) tracing/replay all flow from
// the caller's choice of source — the distribution itself performs no wall
// clock access and holds no internal state beyond its parameters.
type Source interface {
	// Float64 returns a uniform draw in [0, 1).
	Float64() float64
}

// FuncSource adapts a plain func() float64 into a Source. Handy for passing
// a closure that clamps, routes through a DrawSite, or delegates to an
// existing RNG such as dists.RandomFromFunc's Float64 field.
type FuncSource func() float64

// Float64 implements Source.
func (f FuncSource) Float64() float64 { return f() }

// unitGuard clamps a uniform draw away from the endpoints that break
// log-based inverse-CDF transforms: a draw of exactly 0 would yield +Inf
// for -ln(1-u), and a draw of 1 would yield +Inf for -ln(u). The clamp is
// tiny (1e-12) and deterministic.
func unitGuard(u float64) float64 {
	if u <= 0 {
		return 1e-12
	}
	if u >= 1 {
		return 1 - 1e-12
	}
	return u
}
