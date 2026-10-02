package temporal

import (
	"fmt"
	"testing"

	"github.com/law/GoDES/faultsched"
	"github.com/law/GoDES/seed"
	"github.com/law/GoDES/simtest"
)

// globalSeedLegConfig is the model-leg config both global-seed tests run: a
// non-nil FaultSchedule (so the runtime's fault-schedule seed, which is read
// from rt.cfg.Seed in fault_target.go, is exercised too), the stochastic
// add_drop/poll_jitter draws, and enough arrivals that the trace is
// seed-sensitive.
func globalSeedLegConfig(declared int64, sched *faultsched.Schedule) Config {
	cfg := DefaultConfig()
	cfg.Seed = declared
	cfg.BurstCount = 50
	cfg.SimEndMs = 20000
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0
	cfg.FaultSchedule = sched
	return cfg
}

// globalSeedLegWindows is the fault window set for the global-seed tests.
func globalSeedLegWindows() []faultsched.Window {
	return []faultsched.Window{
		{Kind: faultsched.KindAddDrop, StartMs: 1, EndMs: 10001, Rate: 0.3},
		{Kind: faultsched.KindPollJitter, StartMs: 1, EndMs: 10001, Rate: 0.5},
	}
}

// globalSeedLegRun runs the model leg for a config declaring `declared` and
// returns the digest plus the rendered stats.
func globalSeedLegRun(t *testing.T, declared int64, sched *faultsched.Schedule) (Result, string) {
	t.Helper()
	top, err := NewTopology(globalSeedLegConfig(declared, sched))
	if err != nil {
		t.Fatalf("NewTopology(declared=%d): %v", declared, err)
	}
	res := top.Run()
	return res, fmt.Sprintf("%+v", res.Stats)
}

// TestModelLegIsSeedSensitive is the non-vacuity precondition for
// TestGlobalRunSeedReachesModelLeg: in a process with NO global seed, the two
// declared seeds that test compares must produce different traces. Without
// this, "declared 42 == declared DSTSEED" could hold because the config ignores
// its seed entirely.
func TestModelLegIsSeedSensitive(t *testing.T) {
	if g, ok := seed.Global(); ok {
		t.Skipf("DSTSEED=%d is set: every run in this process resolves to it, so two declared seeds cannot differ — run this test with DSTSEED unset", g)
	}
	sched := mustSchedule(t, seed.DefaultRunSeed, globalSeedLegWindows())
	a, statsA := globalSeedLegRun(t, seed.DefaultRunSeed, sched)
	b, statsB := globalSeedLegRun(t, 7, sched)
	if a.Digest == b.Digest && statsA == statsB {
		t.Fatal("declared seeds 42 and 7 produce identical digests and stats — this config cannot observe the run seed, so TestGlobalRunSeedReachesModelLeg would be vacuous")
	}
}

// TestGlobalRunSeedReachesModelLeg is gate (iii) for the model leg: with a
// global seed set, a config that declares any other seed runs as if it declared
// the global one — including the fault schedule, which the runtime seeds from
// rt.cfg.Seed. The test is env-driven by construction: the environment is read
// once per process (seed.Global), so the only honest way to drive the override
// is to run the process with DSTSEED set.
func TestGlobalRunSeedReachesModelLeg(t *testing.T) {
	g, ok := seed.Global()
	if !ok {
		t.Skip("DSTSEED unset — no global run seed to reach this leg")
	}
	if g == seed.DefaultRunSeed {
		t.Skipf("DSTSEED=%d equals the config default — the override is unobservable in this process", g)
	}
	sched := mustSchedule(t, g, globalSeedLegWindows())
	overridden, statsOverridden := globalSeedLegRun(t, seed.DefaultRunSeed, sched)
	declared, statsDeclared := globalSeedLegRun(t, g, sched)
	if overridden.Digest != declared.Digest || statsOverridden != statsDeclared {
		t.Fatalf("declared %d did not run as the global seed %d:\ndigest overridden=%s declared=%s\nstats overridden=%s\ndeclared        =%s",
			seed.DefaultRunSeed, g, overridden.Digest, declared.Digest, statsOverridden, statsDeclared)
	}
	// An unset (0) declared seed must resolve to the global seed too, not to
	// the config default.
	unset, statsUnset := globalSeedLegRun(t, 0, sched)
	if unset.Digest != declared.Digest || statsUnset != statsDeclared {
		t.Fatalf("declared 0 (unset) did not resolve to the global seed %d:\ndigest unset=%s declared=%s", g, unset.Digest, declared.Digest)
	}
	// Determinism under the override: the same process + env must reproduce the
	// trace (N=5, the house bar) — the global seed selects one member of an
	// existing deterministic stream family, never a fresh stream.
	simtest.AssertDeterministic(t, 5, g, func(int64) string {
		top, err := NewTopology(globalSeedLegConfig(seed.DefaultRunSeed, sched))
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run().Digest
	})
	t.Logf("model leg: declared 42/0 both run as DSTSEED=%d (digest %s)", g, declared.Digest)
}
