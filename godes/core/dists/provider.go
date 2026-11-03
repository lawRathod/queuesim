package dists

import (
	"hash/fnv"
	"math"
)

// DrawSite is a stable identifier for a random sampling location.
// It enables trace recording and replay to associate each draw with
// its origin in the model.
//
// The zero value has Tag="" and SiteID=0. It is valid but should be
// reserved for unlabeled sampling sites.
type DrawSite struct {
	// Tag is a human-readable label, e.g. "arrival", "service", "retry_backoff".
	Tag string
	// SiteID is a stable hash of the site identity, derived via FNV-1a.
	// Use NewDrawSite to compute this from a tag.
	SiteID uint64
}

// NewDrawSite creates a DrawSite with a SiteID derived from tag via
// FNV-1a hashing. This matches the substream derivation pattern in
// GoDES/core/rng.go and ensures consistent site identification across
// runs.
func NewDrawSite(tag string) DrawSite {
	h := fnv.New64a()
	_, _ = h.Write([]byte(tag))
	return DrawSite{Tag: tag, SiteID: h.Sum64()}
}

// RandomProvider is an injectable sampling facade.
// Distributions delegate to a provider when one is attached, enabling
// tracing, replay, importance sampling, and splitting without modifying
// the distribution code.
type RandomProvider interface {
	// SampleExponential draws an exponential random variable with the
	// given rate (events per second). Returns the sampled value in seconds.
	//
	// rate must be > 0. If rate <= 0, behavior is undefined: implementations
	// may return 0, +Inf, NaN, or panic.
	SampleExponential(site DrawSite, rate float64) float64
}

// RandomFromFunc adapts a function returning a uniform float64 in [0,1)
// into a RandomProvider. The function is called once per SampleExponential;
// the exponential sample is computed via the inverse-CDF method
// (-ln(1-u)/rate) so the resulting draw has rate (events per second).
//
// This is the bridge used by callers that already have a core.RNG (e.g.,
// core.Simulation.Substream) and want to drive a distribution through
// it without depending on the core package from dists.
type RandomFromFunc struct {
	// Float64 must return a uniform value in [0, 1). It is called
	// once per SampleExponential invocation.
	Float64 func() float64
}

// SampleExponential implements RandomProvider.
func (r *RandomFromFunc) SampleExponential(_ DrawSite, rate float64) float64 {
	u := r.Float64()
	if u <= 0 {
		u = 1e-9
	}
	return -math.Log(1.0-u) / rate
}

// Compile-time check: RandomFromFunc implements RandomProvider.
var _ RandomProvider = (*RandomFromFunc)(nil)
