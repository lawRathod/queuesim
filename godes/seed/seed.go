package seed

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"sync"
)

// Env is the environment variable carrying the one run seed. It is the name
// the ADR-119 runtime overlay already reads, so a second name would be a second
// number needing a derivation rule (ADR-122).
const Env = "DSTSEED"

// DefaultRunSeed is the single documented unseeded default: what "no seed
// given" means on every leg, instead of a locally-remembered convention. It
// equals the literal every recorded scenario config already carries, so
// normalizing the unseeded path through it moves no artifact.
const DefaultRunSeed int64 = 42

// ErrDeclaredMismatch reports a global run seed that disagrees with a
// pre-registered protocol seed (ADR-122 role (b)). Callers test it with
// errors.Is.
var ErrDeclaredMismatch = errors.New("seed: DSTSEED disagrees with a pre-registered protocol seed")

// loader is the process-wide, once-read environment result. It is a named
// struct rather than three package vars so the once and its payload cannot
// drift apart.
var loader struct {
	once sync.Once
	seed int64
	set  bool
}

// Global returns the run seed supplied by the DSTSEED environment variable and
// whether one was supplied at all. The environment is read once for the whole
// process: the runtime overlay read its copy in schedinit, before main, so a
// later read that followed a changed environment would disagree with it.
//
// ok is false for an unset variable, an empty value and "0" alike, because
// dstSetSeed(0) leaves the runtime seam off. A malformed or out-of-range value
// panics, matching the overlay's own throw.
func Global() (int64, bool) {
	loader.once.Do(func() {
		loader.seed, loader.set = parse(os.Getenv(Env))
	})
	return loader.seed, loader.set
}

// Default returns explicit when it is non-zero, else DefaultRunSeed. It is the
// one place the unseeded path is normalized, used by the scenario config
// loaders in place of their local "if Seed == 0 { Seed = 42 }" convention.
//
// It does NOT consult the environment: a config loader fills in the default its
// artifact would carry with no global seed given. Resolve applies the override.
func Default(explicit int64) int64 {
	if explicit == 0 {
		return DefaultRunSeed
	}
	return explicit
}

// Resolve returns the effective run seed for a leg whose config declares
// explicit: the global (DSTSEED) seed when one is set and non-zero, else
// Default(explicit).
//
// Determinism contract: Resolve is a pure function of the once-read environment
// and its argument. It consumes no RNG draw, reads no clock, and iterates no
// map. Its result selects which member of an existing deterministic stream
// family the run uses — it never changes the number or order of events, so a
// run is reproducible from (mode, resolved seed). It panics only when DSTSEED
// itself is malformed (via Global).
func Resolve(explicit int64) int64 {
	if g, ok := Global(); ok {
		return g
	}
	return Default(explicit)
}

// GuardDeclared returns an error wrapping ErrDeclaredMismatch when a global run
// seed is set and differs from declared, a pre-registered protocol seed that
// must never be resolved (ADR-122 role (b)); it returns nil when no global seed
// is set or when the two agree.
//
// what names the declared seed set in the error, e.g. "discovery_sweep
// CalSeed". A declared seed of 0 is treated as "no declared seed" and passes:
// 0 is unset at the runtime layer, and a cell whose seed has not been assigned
// yet is not a declared protocol seed.
//
// The guard is deliberately disagreement-only rather than a refusal whenever
// DSTSEED is set: the CAL anchors legitimately agree with DSTSEED=42, and
// scripts/experiment.sh sets DSTSEED on every compare-arm run.
func GuardDeclared(declared int64, what string) error {
	if declared == 0 {
		return nil
	}
	g, ok := Global()
	if !ok || g == declared {
		return nil
	}
	return fmt.Errorf("%w: %s=%d but %s is the pre-registered protocol seed %d; unset %s to run the registered sweep, or re-pre-register the seed set with %d before running it (ADR-122 role (b))",
		ErrDeclaredMismatch, Env, g, what, declared, Env, g)
}

// parse reads a DSTSEED literal. It returns ok=false for "" and "0" (both
// unset at the runtime layer) and panics on any non-decimal-digit value or on a
// value that does not fit an int64.
//
// The runtime's own dstSeedFromEnv accepts the same decimal grammar and throws
// on malformed input; it wraps silently on overflow, and this parser panics
// instead because a silently wrapped seed is a seed the run does not know it
// used.
func parse(s string) (int64, bool) {
	if s == "" {
		return 0, false
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		panic(fmt.Sprintf("seed: %s must be a decimal integer, got %q", Env, s))
	}
	if v == 0 {
		return 0, false
	}
	if v > math.MaxInt64 {
		panic(fmt.Sprintf("seed: %s=%s exceeds the maximum run seed %d", Env, s, int64(math.MaxInt64)))
	}
	return int64(v), true
}
