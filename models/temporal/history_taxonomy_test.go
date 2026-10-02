package temporal

import (
	"testing"

	"github.com/law/GoDES/core"
	"github.com/lawRathod/queuesim/models/temporal/components/history"
)

// Wave-two history-taxonomy pins: complete() splits activity outcomes into
// retryable execution failures (fault-window fail, heartbeat timeout — the
// CanRetry path) and terminal schedule-to-close expiry (Failed++, no
// CanRetry/Backoff consult). The S2C clock anchors at the FIRST schedule
// (the BornMs lineage, preserved across retries) and the retryable path
// carries a now+backoff expiration gate (model.ExceedsDeadline against the
// S2C deadline when set).

// TestHistoryS2CExceededIsTerminalNoRetry pins the S2C-terminal split: every
// execution (50ms service) outlives the 30ms schedule-to-close cap, so all
// workflows fail terminally with ZERO retries — the S2C path never consults
// CanRetry/Backoff.
func TestHistoryS2CExceededIsTerminalNoRetry(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 10
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0
	cfg.Vector = nil
	cfg.ActivityMs = 50
	cfg.ScheduleToCloseMs = 30 // every execution outlives this via service time alone

	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	if res.Stats.Failed != 10 {
		t.Fatalf("Failed = %d, want 10 (S2C expiry is terminal)", res.Stats.Failed)
	}
	if res.Stats.Retries != 0 {
		t.Fatalf("Retries = %d, want 0 (S2C path never consults CanRetry/Backoff)", res.Stats.Retries)
	}
	if res.Stats.Completed != 0 {
		t.Fatalf("Completed = %d, want 0", res.Stats.Completed)
	}
}

// TestHistoryRetryDeadlineGateTerminal pins the now+backoff expiration gate:
// the first attempt fails inside the fault window while S2C-viable, but the
// 1000ms backoff would fire past the 100ms S2C deadline — so the retry never
// re-enters and the workflow fails terminally with ZERO retries scheduled.
func TestHistoryRetryDeadlineGateTerminal(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 10
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.FaultStartMs = 1
	cfg.FaultEndMs = 0 // open-ended failures
	cfg.RetryInitialMs = 1000
	cfg.RetryCoeff = 1.0
	cfg.ScheduleToCloseMs = 100 // first execution viable; backoff fires past it

	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	if res.Stats.Failed != 10 {
		t.Fatalf("Failed = %d, want 10 (over-deadline retry goes terminal)", res.Stats.Failed)
	}
	if res.Stats.Retries != 0 {
		t.Fatalf("Retries = %d, want 0 (over-deadline retry never re-enters)", res.Stats.Retries)
	}
	if res.Stats.Completed != 0 {
		t.Fatalf("Completed = %d, want 0", res.Stats.Completed)
	}
}

// TestHistoryNoS2CAllowsRetries is the deadline-gate control: the same
// open-ended fault window with NO S2C cap retries freely — workflows never
// fail terminally under an unlimited policy and retries flow.
func TestHistoryNoS2CAllowsRetries(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 10
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.FaultStartMs = 1
	cfg.FaultEndMs = 0 // open-ended failures
	cfg.RetryInitialMs = 100
	cfg.RetryCoeff = 1.0
	cfg.ScheduleToCloseMs = 0 // no deadline: the gate stays open

	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	if res.Stats.Retries == 0 {
		t.Fatal("Retries = 0, want > 0 (no S2C deadline: retryable failures must re-enter)")
	}
	if res.Stats.Failed != 0 {
		t.Fatalf("Failed = %d, want 0 (unlimited policy, no deadline: nothing terminal)", res.Stats.Failed)
	}
	if res.Stats.Completed != 0 {
		t.Fatalf("Completed = %d, want 0 (open-ended failures never complete)", res.Stats.Completed)
	}
}

// TestHistoryHeartbeatTimeoutIsRetryable pins the heartbeat service-time
// bound: an execution longer than HeartbeatTimeoutMs fails as a RETRYABLE
// activity failure (no cadence modeling — a pure bound check). With a cap of
// 3 attempts each workflow burns 3 executions and 2 retries, then fails.
func TestHistoryHeartbeatTimeoutIsRetryable(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 10
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0
	cfg.Vector = nil
	cfg.ActivityMs = 50
	cfg.HeartbeatTimeoutMs = 10
	cfg.RetryMaxAttempts = 3

	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	if res.Stats.Failed != 10 {
		t.Fatalf("Failed = %d, want 10 (heartbeat failures exhaust the cap)", res.Stats.Failed)
	}
	if res.Stats.Completed != 0 {
		t.Fatalf("Completed = %d, want 0", res.Stats.Completed)
	}
	if res.Stats.Retries != 20 {
		t.Fatalf("Retries = %d, want 20 (2 retries x 10 workflows — heartbeat fail is retryable)", res.Stats.Retries)
	}
	if res.Stats.AttemptsExecuted != 30 {
		t.Fatalf("AttemptsExecuted = %d, want 30 (3 attempts x 10 workflows)", res.Stats.AttemptsExecuted)
	}
}

// historyTaxonomyRuntime builds the minimal runtime complete() needs for the
// unit-level anchor pins: sim + one shard + the validated config. The
// success path (fanout/persist/sticky) is never reached by these pins.
func historyTaxonomyRuntime(t *testing.T, cfg Config) *runtime {
	t.Helper()
	cfg.BurstCount = 1
	cfg.PollersWF = 1
	cfg.PollersAct = 1
	rt, err := newRuntime(cfg)
	if err != nil {
		t.Fatalf("newRuntime: %v", err)
	}
	rt.sim = newSim(t)
	rt.shards = append(rt.shards, history.NewHistoryShard(rt.sim, 0, 0, 0, 0, 0, 0, 0, 0))
	return rt
}

// TestHistoryRetryPreservesBornMs pins the BornMs lineage: a retryable
// failure re-enters the timer queue with its FIRST-schedule birth intact
// (not reset to the retry-fire time), so the S2C age keeps counting across
// attempts.
func TestHistoryRetryPerAttemptBirth(t *testing.T) {
	cfg := DefaultConfig()
	cfg.FaultStartMs = 1
	cfg.FaultEndMs = 0 // open-ended failures: the attempt fails retryably
	cfg.RetryInitialMs = 100
	cfg.RetryCoeff = 1.0
	rt := historyTaxonomyRuntime(t, cfg)

	fired := false
	var got Task
	rt.sim.ScheduleAt(core.NewSimTime(5000), func(*core.Simulation) {
		rt.complete(Task{ID: 7, Attempt: 1, Class: TaskActivity, BornMs: 100})
	})
	rt.sim.Run()
	rt.shards[0].Tick(core.NewSimTime(5100), nil, func(tk Task) {
		fired = true
		got = tk
	})
	if !fired {
		t.Fatal("retry timer never fired (100ms backoff from t=5000 must fire at t=5100)")
	}
	// Per-attempt S2C: each retry is a NEW ActivityTask with a fresh
	// schedule-to-close window (service/history/workflow/retry.go), so the
	// retry's born time is the completion time (t=5000), NOT the original
	// first-schedule (100). Preserving the first-schedule lineage here made
	// the matching backlog-age gate latch (isBacklogNegligible) on old tasks.
	if got.BornMs != 5000 {
		t.Fatalf("retry BornMs = %d, want 5000 (per-attempt birth at completion time)", got.BornMs)
	}
	if got.Attempt != 2 {
		t.Fatalf("retry Attempt = %d, want 2", got.Attempt)
	}
}

// TestHistoryS2CAnchorUsesFirstSchedule pins the S2C anchor: the expiry age
// is now − BornMs (first schedule), not now − latest execution. An attempt
// completing at t=5000 with BornMs=0 and a 100ms cap is terminal even with
// no fault-window failure and an unlimited retry policy.
func TestHistoryS2CAnchorUsesFirstSchedule(t *testing.T) {
	cfg := DefaultConfig()
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0 // no execution failure: only the S2C anchor can fail this
	cfg.ScheduleToCloseMs = 100
	rt := historyTaxonomyRuntime(t, cfg)

	rt.sim.ScheduleAt(core.NewSimTime(5000), func(*core.Simulation) {
		rt.complete(Task{ID: 7, Attempt: 1, Class: TaskActivity, BornMs: 0})
	})
	rt.sim.Run()
	if rt.stats.Failed != 1 {
		t.Fatalf("Failed = %d, want 1 (S2C anchored at first schedule is terminal)", rt.stats.Failed)
	}
	if rt.stats.Retries != 0 {
		t.Fatalf("Retries = %d, want 0 (S2C path never retries)", rt.stats.Retries)
	}
	if rt.shards[0].LenTimer() != 0 {
		t.Fatalf("LenTimer = %d, want 0 (no retry timer on the S2C path)", rt.shards[0].LenTimer())
	}
}
