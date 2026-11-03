package dists

import mathrand "math/rand/v2"

// newTestPCG returns a *rand.Rand seeded deterministically on a PCG64,
// matching the engine's stdlib choice so tests are stable across runs.
func newTestPCG(seed uint64) *mathrand.Rand {
	return mathrand.New(mathrand.NewPCG(seed, seed^0x9E3779B97F4A7C15))
}
