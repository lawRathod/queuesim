package dists

import "math"

// gammaLnExp returns exp(lgamma(x)) = Gamma(x) for x > 0, computed via
// math.Lgamma for numerical stability. Falls back to 1 for x <= 0 (not
// expected at call sites, which always pass positive arguments).
func gammaLnExp(x float64) float64 {
	if x <= 0 {
		return 1
	}
	v, _ := math.Lgamma(x)
	return math.Exp(v)
}
