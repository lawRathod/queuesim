package core

import (
	"encoding/binary"
	"hash/fnv"
	"math/rand/v2"
	"sync"
)

// RNG defines deterministic random primitives used by the engine.
type RNG interface {
	Uint64() uint64
	Int64N(n int64) int64
	Float64() float64
}

type stdlibRNG struct {
	r *rand.Rand
}

func newStdlibRNG(seed uint64) *stdlibRNG {
	return &stdlibRNG{r: rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15))}
}

func (s *stdlibRNG) Uint64() uint64 {
	return s.r.Uint64()
}

func (s *stdlibRNG) Int64N(n int64) int64 {
	if n <= 0 {
		return 0
	}
	return s.r.Int64N(n)
}

func (s *stdlibRNG) Float64() float64 {
	return s.r.Float64()
}

// lockedRNG serializes draws through the simulation mutex — race-free.
// Draw ORDER remains caller-dependent (fixed in Layer 3, ADR-058); this
// layer only removes the data race.
type lockedRNG struct {
	mu    *sync.Mutex
	inner RNG
}

func (l *lockedRNG) Uint64() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.Uint64()
}

func (l *lockedRNG) Int64N(n int64) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.Int64N(n)
}

func (l *lockedRNG) Float64() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.inner.Float64()
}

func deriveSubstreamSeed(root uint64, name string) uint64 {
	h := fnv.New64a()
	buf := [8]byte{}
	binary.LittleEndian.PutUint64(buf[:], root)
	_, _ = h.Write(buf[:])
	_, _ = h.Write([]byte(name))
	return h.Sum64()
}

// DeriveSubstreamSeed returns the deterministic seed of the named substream
// under root. It is the exported form of the engine's own derivation, so a
// caller outside the engine (a bench that draws without a Simulation)
// addresses the SAME stream family rather than re-inventing one.
//
// ADR-089: a substream name is a dotted path, e.g. "raft.soak.faults", so two
// draw sites cannot collide by sharing a bare name.
//
// Determinism contract: a pure function of (root, name). It draws no random
// value and reads no clock, so the same pair always yields the same seed.
func DeriveSubstreamSeed(root uint64, name string) uint64 {
	return deriveSubstreamSeed(root, name)
}

// NewSubstreamRNG returns the deterministic RNG for the named substream of
// root — the same stream Simulation.Substream(name) hands out for a simulation
// seeded with root. It is for callers that need a named substream but have no
// Simulation to ask (the raft bench draws from the run seed at setup, before
// any engine exists).
//
// Determinism contract: the same (root, name) returns a source producing the
// same sequence; distinct names produce independent sequences. The returned
// RNG is not safe for concurrent use; each caller owns its stream.
func NewSubstreamRNG(root uint64, name string) RNG {
	return newStdlibRNG(deriveSubstreamSeed(root, name))
}
