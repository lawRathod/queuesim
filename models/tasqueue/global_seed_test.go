package tasqueue

import (
	"fmt"
	"testing"

	"github.com/lawRathod/queuesim/godes/seed"
	"github.com/lawRathod/queuesim/godes/simtest"
)

// globalSeedLegConfig is the tasqueue model-leg config the global-seed tests
// run: the full stochastic+DAG gate config (failP draw, drain-time drop,
// groups, OnError spawns, ETA timer) with the declared seed under test.
func globalSeedLegConfig(declared int64) Config {
	cfg := gateConfig()
	cfg.Seed = declared
	return cfg
}

// globalSeedLegRun runs the leg for a config declaring `declared` and returns
// the digest plus the rendered stats.
func globalSeedLegRun(t *testing.T, declared int64) (Result, string) {
	t.Helper()
	top, err := NewTopology(globalSeedLegConfig(declared))
	if err != nil {
		t.Fatalf("NewTopology(declared=%d): %v", declared, err)
	}
	res := top.Run()
	return res, fmt.Sprintf("%+v", res.Stats)
}

// TestModelLegIsSeedSensitive is the non-vacuity precondition for
// TestGlobalRunSeedReachesModelLeg, mirroring the models/temporal sibling: with
// NO global seed the two declared seeds that test compares must produce
// different traces.
func TestModelLegIsSeedSensitive(t *testing.T) {
	if g, ok := seed.Global(); ok {
		t.Skipf("DSTSEED=%d is set: every run in this process resolves to it, so two declared seeds cannot differ — run this test with DSTSEED unset", g)
	}
	a, statsA := globalSeedLegRun(t, seed.DefaultRunSeed)
	b, statsB := globalSeedLegRun(t, 7)
	if a.Digest == b.Digest && statsA == statsB {
		t.Fatal("declared seeds 42 and 7 produce identical digests and stats — this config cannot observe the run seed, so TestGlobalRunSeedReachesModelLeg would be vacuous")
	}
}

// TestGlobalRunSeedReachesModelLeg is gate (iii) for the tasqueue model leg:
// with a global seed set, a config declaring any other seed runs as if it
// declared the global one. Env-driven by construction — the environment is read
// once per process (seed.Global).
func TestGlobalRunSeedReachesModelLeg(t *testing.T) {
	g, ok := seed.Global()
	if !ok {
		t.Skip("DSTSEED unset — no global run seed to reach this leg")
	}
	if g == seed.DefaultRunSeed {
		t.Skipf("DSTSEED=%d equals the config default — the override is unobservable in this process", g)
	}
	overridden, statsOverridden := globalSeedLegRun(t, seed.DefaultRunSeed)
	declared, statsDeclared := globalSeedLegRun(t, g)
	if overridden.Digest != declared.Digest || statsOverridden != statsDeclared {
		t.Fatalf("declared %d did not run as the global seed %d:\ndigest overridden=%s declared=%s\nstats overridden=%s\ndeclared        =%s",
			seed.DefaultRunSeed, g, overridden.Digest, declared.Digest, statsOverridden, statsDeclared)
	}
	unset, statsUnset := globalSeedLegRun(t, 0)
	if unset.Digest != declared.Digest || statsUnset != statsDeclared {
		t.Fatalf("declared 0 (unset) did not resolve to the global seed %d:\ndigest unset=%s declared=%s", g, unset.Digest, declared.Digest)
	}
	// Determinism under the override: the same process + env must reproduce the
	// trace (N=5, the house bar).
	simtest.AssertDeterministic(t, 5, g, func(int64) string {
		top, err := NewTopology(globalSeedLegConfig(seed.DefaultRunSeed))
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run().Digest
	})
	t.Logf("tasqueue leg: declared 42/0 both run as DSTSEED=%d (digest %s)", g, declared.Digest)
}
