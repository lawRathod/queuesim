package dists

import (
	"encoding/binary"
	"hash/fnv"
	"math"
	mathrand "math/rand/v2"
)

// ExponentialDistribution samples from an exponential distribution.
//
// When a RandomProvider is attached via WithProvider, all sampling is delegated
// to the provider (enabling tracing/replay). When no provider is attached,
// sampling uses an internal PCG-based RNG seeded deterministically.
//
// The zero value is not usable; construct instances via NewExponential.
type ExponentialDistribution struct {
	rate     float64
	provider RandomProvider
	site     DrawSite
	rng      *mathrand.Rand // nil when provider is set
}

// NewExponential creates an ExponentialDistribution with the given rate
// (events per second, must be > 0) and a seed for deterministic substream
// derivation. Panics if rate <= 0.
func NewExponential(rate float64, seed uint64) *ExponentialDistribution {
	if rate <= 0 {
		panic("dists: rate must be positive")
	}
	distSeed := deriveDistSeed(seed, rate)
	return &ExponentialDistribution{
		rate: rate,
		rng:  mathrand.New(mathrand.NewPCG(distSeed, distSeed^0x9E3779B97F4A7C15)),
	}
}

// WithProvider attaches a RandomProvider for tracing/replay.
// When set, all sampling is delegated to the provider.
func (d *ExponentialDistribution) WithProvider(p RandomProvider, site DrawSite) *ExponentialDistribution {
	d.provider = p
	d.site = site
	d.rng = nil
	return d
}

// Sample draws one value from the distribution. Returns seconds as float64.
// Panics if the distribution was not initialized via NewExponential (rng is nil
// and no provider is attached).
func (d *ExponentialDistribution) Sample() float64 {
	if d.provider != nil {
		return d.provider.SampleExponential(d.site, d.rate)
	}
	if d.rng == nil {
		panic("dists: ExponentialDistribution not initialized, use NewExponential")
	}
	u := d.rng.Float64()
	return -math.Log(1.0-u) / d.rate
}

// Rate returns the rate parameter (events per second).
func (d *ExponentialDistribution) Rate() float64 {
	return d.rate
}

// deriveDistSeed produces a deterministic substream seed using FNV-1a.
// Matches the pattern in core/rng.go.
func deriveDistSeed(base uint64, rate float64) uint64 {
	h := fnv.New64a()
	buf := [8]byte{}
	binary.LittleEndian.PutUint64(buf[:], base)
	_, _ = h.Write(buf[:])
	binary.LittleEndian.PutUint64(buf[:], math.Float64bits(rate))
	_, _ = h.Write(buf[:])
	return h.Sum64()
}
