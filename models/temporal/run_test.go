package temporal

import (
	"testing"

	"github.com/lawRathod/queuesim/model"
)

// Run-level mechanism tests: the model faithfully reproduces the real
// system's mechanisms (not parity numbers — those gates are retired).
// Internal state is reachable via the Topology shim's runtime for
// mechanism assertions.

// TestCalmRunNoQueuing: with no faults, the transfer processors drip one
// task per tick and every add sync-matches a parked poller within the
// 200ms window — the backlog NEVER forms (the property v1 could never
// exhibit). 50 workflows complete, zero spooling, zero rejects.
func TestCalmRunNoQueuing(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 50
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0
	cfg.Vector = nil

	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()

	if res.Stats.Completed != 50 {
		t.Fatalf("completed = %d, want 50", res.Stats.Completed)
	}
	if res.Stats.RejectedAdds != 0 {
		t.Fatalf("rejected = %d, want 0 (no capacity pressure)", res.Stats.RejectedAdds)
	}
	if aps := res.Stats.AttemptsPerStart(); aps > 1.05 {
		t.Fatalf("calm aps = %.3f, want ≈ 1.0 (sync-match)", aps)
	}
	// The calm no-queuing property: every add (150 = 50 wf + 50 act + 50
	// wf-complete) sync-matches; nothing spools and the observed depth
	// never exceeds 0.
	if res.Stats.Spooled != 0 {
		t.Fatalf("spooled = %d, want 0 (calm must not spool)", res.Stats.Spooled)
	}
	if res.Stats.SyncMatched != 150 {
		t.Fatalf("sync-matched = %d, want 150 (all adds sync-match under calm load)", res.Stats.SyncMatched)
	}
	if res.Stats.MaxQueue != 0 {
		t.Fatalf("max queue = %d, want 0 (calm backlog never forms)", res.Stats.MaxQueue)
	}
	if res.Stats.Failed != 0 {
		t.Fatalf("failed = %d, want 0", res.Stats.Failed)
	}
}

// TestDrainTransitionSemantics pins the DrainMs contract: the signal is the
// last observed non-empty → empty transition of the matching backlog after
// the run started. A storm whose retry backlog persists across tick
// boundaries and drains AFTER the fault window records a real post-window
// time; a storm with open-ended failures and unlimited retries never drains
// and reports 0 (the never-drained sentinel).
func TestDrainTransitionSemantics(t *testing.T) {
	// Recovering storm: a 1ms retry backoff makes the retry add rate exceed
	// the dispatch capacity during the fault window, so the backlog builds
	// across tick boundaries; when the window ends the backlog drains —
	// the transition must land AFTER FaultEndMs.
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 50
	cfg.SimEndMs = 60000
	cfg.FaultStartMs = 1
	cfg.FaultEndMs = 10001
	cfg.ActivityMs = 5
	cfg.RetryInitialMs = 1 // retries arrive faster than the pollers dispatch
	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	if res.Stats.MaxQueue == 0 {
		t.Fatal("recovering storm formed no backlog (test config invalid)")
	}
	if res.Stats.Completed == 0 {
		t.Fatal("recovering storm completed nothing (test config invalid)")
	}
	if res.DrainMs <= cfg.FaultEndMs {
		t.Fatalf("DrainMs = %d, want > FaultEndMs=%d (backlog must drain after the fault window)", res.DrainMs, cfg.FaultEndMs)
	}

	// Never-draining storm: open-ended failures + unlimited retries keep
	// the backlog non-empty for the whole horizon.
	cfg2 := DefaultConfig()
	cfg2.Seed = 1
	cfg2.BurstCount = 20
	cfg2.SimEndMs = 60000
	cfg2.FaultStartMs = 1
	cfg2.FaultEndMs = 0
	cfg2.RetryMaxAttempts = 0
	cfg2.RetryInitialMs = 1
	cfg2.ActivityMs = 5
	top2, err := NewTopology(cfg2)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res2 := top2.Run()
	if res2.Stats.MaxQueue == 0 {
		t.Fatal("never-draining storm formed no backlog (test config invalid)")
	}
	if res2.Stats.Completed != 0 {
		t.Fatalf("completed = %d, want 0 (open-ended failures must never complete)", res2.Stats.Completed)
	}
	if res2.DrainMs != 0 {
		t.Fatalf("DrainMs = %d, want 0 (never-draining storm)", res2.DrainMs)
	}
}

// TestStormRunAmplification: with an activity-failure window, attempts
// exceed starts (retry amplification) — the signature the thesis tracks.
func TestStormRunAmplification(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 50
	cfg.SimEndMs = 60000
	cfg.FaultStartMs = 1
	cfg.FaultEndMs = 10001
	cfg.MatchingCapacity = 1000

	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()

	if res.Stats.AttemptsPerStart() <= 1.0 {
		t.Fatalf("aps = %.2f, want > 1.0 (retry amplification under storm)", res.Stats.AttemptsPerStart())
	}
	if res.Stats.Retries == 0 {
		t.Fatal("retries = 0, want > 0")
	}
	if res.Stats.Spooled == 0 {
		t.Fatalf("spooled = %d, want > 0 (storm should overflow the sync-match path)", res.Stats.Spooled)
	}
}

// TestLifecycleEndToEnd: one workflow completes exactly once through the
// full path (wf → activity → wf-complete), with exact mechanism counts:
// 3 add attempts (1 wf + 1 act + 1 wf-complete), all sync-matched under
// calm load.
func TestLifecycleEndToEnd(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 2
	cfg.BurstCount = 1
	cfg.SimEndMs = 5000
	cfg.MatchingCapacity = 100
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0

	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()

	if res.Stats.Completed != 1 {
		t.Fatalf("completed = %d, want 1", res.Stats.Completed)
	}
	if res.Stats.Failed != 0 {
		t.Fatalf("failed = %d, want 0", res.Stats.Failed)
	}
	if res.Stats.AttemptedAdds != 3 {
		t.Fatalf("attempted adds = %d, want 3 (1 wf + 1 act + 1 wf-complete)", res.Stats.AttemptedAdds)
	}
	if res.Stats.SyncMatched != 3 {
		t.Fatalf("sync-matched = %d, want 3 (all three tasks sync-match under calm load)", res.Stats.SyncMatched)
	}
}

// TestStickyEnabledCompletes: with StickyEnabled, wf-complete tasks route
// through the per-worker sticky buffers (Stick succeeds — the default
// cache of 0 means "forever"), the sticky drain hands them to parked wf
// pollers (sticky-first), and every workflow completes exactly once. This
// pins that the knob does not hang the lifecycle.
func TestStickyEnabledCompletes(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 4
	cfg.BurstCount = 20
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.StickyEnabled = true // StickyCacheMs = 0 (default) → cache never expires
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0

	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	if res.Stats.Completed != 20 {
		t.Fatalf("completed = %d, want 20 (sticky lifecycle must complete)", res.Stats.Completed)
	}
	if res.Stats.Failed != 0 {
		t.Fatalf("failed = %d, want 0", res.Stats.Failed)
	}
	if top.rt.sticky.Len() != 0 {
		t.Fatalf("sticky backlog = %d, want 0 (all sticky tasks drained)", top.rt.sticky.Len())
	}
}

// TestStickyFallbackDelays: with a short sticky cache, workflows whose
// worker's cache expired fall back to the normal matching-wf path (the
// real fallback when the sticky worker is unavailable).
func TestStickyFallbackDelays(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 5
	cfg.BurstCount = 20
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.StickyEnabled = true
	cfg.StickyCacheMs = 1 // 1ms residency: the cache expires almost immediately
	cfg.StickyFallbackMs = 5
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0

	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	if res.Stats.Completed != 20 {
		t.Fatalf("completed = %d, want 20 (fallback must complete the lifecycle)", res.Stats.Completed)
	}
	if top.rt.sticky.Len() != 0 {
		t.Fatalf("sticky backlog = %d, want 0 (fallback path must not strand tasks)", top.rt.sticky.Len())
	}
}

// TestZeroDrawsWhenVectorOff: a topology with no vector and no arrival
// rate creates no stochastic substreams (zero-draw contract).
func TestZeroDrawsWhenVectorOff(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 3
	cfg.BurstCount = 5
	cfg.SimEndMs = 1000
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0
	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	if top.rt.arrivalRNG != nil {
		t.Fatal("arrival substream created while ArrivalRate = 0")
	}
	top.Run()
}

// TestVectorActivityFailure: the activity_failure vector drives attempts
// beyond starts (the search-language kind works).
func TestVectorActivityFailure(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 50
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	v := model.DecisionVector{Name: "activity_failure-activity-1-10000-0", Kind: model.KindActivityFailure, Target: "activity", StartMs: 1, DurationMs: 10000, Rate: 0}
	cfg.Vector = &v

	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	if res.Stats.AttemptsPerStart() <= 1.0 {
		t.Fatalf("aps = %.2f, want > 1.0 (activity_failure vector)", res.Stats.AttemptsPerStart())
	}
}

// --- Fault-surface acceptance (decision-vector kinds) -------------------

// TestAddDropSpoolDrop: the add_drop vector drives spool-time drops on the
// matching queues (transport loss at the boundary).
func TestAddDropSpoolDrop(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 50
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.PollersWF = 1
	cfg.PollersAct = 1
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0
	v := model.DecisionVector{Name: "add_drop-matching-1-0-0.3", Kind: model.KindAddDrop, Target: "matching", StartMs: 1, DurationMs: 0, Rate: 0.3}
	cfg.Vector = &v

	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	// With 1 worker, most tasks spool → the spool-time drop fires.
	if res.Stats.Dropped == 0 {
		t.Fatal("dropped = 0, want > 0 (add_drop must drop spooled tasks)")
	}
	// Drops are consumed (transport-level loss), but must not kill all workflows.
	if res.Stats.Completed == 0 {
		t.Fatal("completed = 0, want > 0 (drops must not kill all workflows)")
	}
}

// TestAddDropDoesNotConsumeRetryBudget is the mechanism-fidelity regression
// for the add_drop transport-drop bug (Shared Fault Schedule follow-up):
// a dropped AddActivityTask RPC returns a clean Unavailable error to history,
// whose queues framework NACKs and RESUBMITS on its own queue-level timer —
// the activity attempt counter is NOT touched (temporal/queues/executable.go
// Nack → rescheduler; the attempt only increments when the activity actually
// STARTS and fails, service/history/.../complete). The model used to treat
// the drop like an activity failure: matching.go's add-boundary drop path
// did tk.Attempt++ before rescheduling, so with a FINITE retry cap
// (RetryMaxAttempts=3) a task dropped at the add boundary N times consumed
// its retry budget and terminally failed (Failed++) without ever starting.
//
// Real semantics: the attempt must stay UNCHANGED across drops — a task
// dropped 100 times still runs as attempt 1 when it finally gets through
// (and can still fail + retry as attempt 1 under the cap). The task re-enters
// via the shard rescheduler (RescheduleAt — the queues-framework resubmit
// loop), just without the attempt increment.
func TestAddDropDoesNotConsumeRetryBudget(t *testing.T) {
	run := func() Result {
		cfg := DefaultConfig()
		cfg.Seed = 1
		cfg.BurstCount = 50
		cfg.SimEndMs = 60000
		cfg.MatchingCapacity = 1000
		cfg.PollersWF = 1
		cfg.PollersAct = 1
		cfg.ActivityMs = 20 // an attempt that starts takes 20ms — the drops are the ONLY pressure
		cfg.RetryInitialMs = 1
		cfg.RetryCoeff = 1.0
		cfg.RetryMaxAttempts = 3 // finite cap: drop-induced increments would exhaust it
		cfg.FaultStartMs = 0
		cfg.FaultEndMs = 0
		v := model.DecisionVector{Name: "add_drop-matching-1-0-0.9", Kind: model.KindAddDrop, Target: "matching", StartMs: 1, DurationMs: 0, Rate: 0.9}
		cfg.Vector = &v

		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		res := top.Run()
		if res.Stats.Dropped == 0 {
			t.Fatal("dropped = 0, want > 0 (test config must exercise the drop path)")
		}
		return res
	}

	res := run()
	// A drop never terminates a workflow: a task that is dropped 100 times
	// still starts as attempt 1. With drops not consuming budget, only
	// genuine attempt exhaustion can fail a workflow — and there is no
	// failure source here (FaultStartMs=0/FaultEndMs=0 and the vector is
	// add_drop; the config-window caveat in faults/activity_failure.go has
	// a zero window to apply). A Failed workflow under the buggy behavior
	// is the drop path terminally failing a task whose budget the drops ate
	// (the pre-fix run of THIS config failed 47/50 workflows).
	if res.Stats.Failed != 0 {
		t.Fatalf("failed = %d, want 0 (drops must not consume the retry budget: a task dropped at the add boundary still runs on its original attempt)", res.Stats.Failed)
	}
	// No activity-failure window → no legitimate retry source. Under the
	// bug, every drop that pushed a task to attempt 2/3 and was dropped
	// again tallied Retries++; under the fix the drop path never retries.
	if res.Stats.Retries != 0 {
		t.Fatalf("retries = %d, want 0 (no activity-failure window; any retry here would be a drop-induced attempt increment)", res.Stats.Retries)
	}
	// Throughput is NOT the regression's assertion (a 0.9 per-re-add drop
	// storm with a 1-poller fleet legitimately drains slowly — each
	// resubmit waits a 1s reschedule round and ~90% are dropped again, and
	// completed workflows' wf-complete events can still be in the drop
	// queue at horizon exhaustion), but the fixed mechanism must let
	// workflows COMPLETE through the drops rather than terminally failing
	// them. The pre-fix bug terminated them (Failed 47, Completed 3); the
	// fix must complete a substantial fraction within the horizon.
	if res.Stats.Completed < 10 {
		t.Fatalf("completed = %d, want >= 10 (drops must delay, not kill: workflows complete when their re-add finally gets through)", res.Stats.Completed)
	}
}

// TestWfDropSpoolDropWfOnly: wf_drop drops only wf-class tasks (matching-wf
// spool), leaving activity tasks untouched.
func TestWfDropSpoolDropWfOnly(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 50
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0
	v := model.DecisionVector{Name: "wf_drop-matching-wf-0-0-0.7", Kind: model.KindWfDrop, Target: "matching-wf", StartMs: 0, DurationMs: 0, Rate: 0.7}
	cfg.Vector = &v

	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	top.Run()
	if top.rt.wfQ.SpoolDropRate() == 0 {
		t.Fatal("wf queue spool drop = 0, want 0.7 (wf_drop targets matching-wf)")
	}
	if top.rt.actQ.SpoolDropRate() != 0 {
		t.Fatalf("act queue spool drop = %v, want 0 (wf_drop must not touch matching-act)", top.rt.actQ.SpoolDropRate())
	}
}

// TestS2STimeoutMaxWait: the s2s_timeout vector sets the activity queue's
// residency deadline (schedule-to-start), causing expiry when workers are
// scarce.
func TestS2STimeoutMaxWait(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 50
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.PollersWF = 1
	cfg.PollersAct = 1
	cfg.ActivityMs = 50 // worker busy → backlog waits > 30ms deadline
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0
	v := model.DecisionVector{Name: "s2s_timeout-matching-0-0-30", Kind: model.KindS2STimeout, Target: "matching", StartMs: 0, DurationMs: 0, Rate: 30}
	cfg.Vector = &v

	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	if res.Stats.Expired == 0 {
		t.Fatal("expired = 0, want > 0 (s2s_timeout must expire backlogged activities)")
	}
}

// TestPollerMinForcesCount: the poller_min vector pins the poller count
// down during its window (worker starvation).
func TestPollerMinForcesCount(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 50
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0
	v := model.DecisionVector{Name: "poller_min-matching-0-0-1", Kind: model.KindPollerMin, Target: "matching", StartMs: 0, DurationMs: 0, Rate: 1}
	cfg.Vector = &v

	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	top.Run()
	if top.rt.pollerForced != 1 {
		t.Fatalf("pollerForced = %d, want 1 (open-ended poller_min window)", top.rt.pollerForced)
	}
}

// TestZeroRateNeutrality: a vector whose window NEVER ACTIVATES (a drop
// kind with rate 0 and a start in the far future) behaves identically to
// no vector — bit-exact stats (the neutrality contract).
func TestZeroRateNeutrality(t *testing.T) {
	run := func(v *model.DecisionVector) TopologyStats {
		cfg := DefaultConfig()
		cfg.Seed = 5
		cfg.BurstCount = 20
		cfg.SimEndMs = 20000
		cfg.MatchingCapacity = 1000
		cfg.FaultStartMs = 0
		cfg.FaultEndMs = 0
		cfg.Vector = v
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run().Stats
	}
	base := run(nil)
	zeroVec := model.DecisionVector{Name: "zero", Kind: model.KindAddDrop, Target: "matching", StartMs: 999999, DurationMs: 1000, Rate: 0}
	withVec := run(&zeroVec)
	if base != withVec {
		t.Fatalf("zero-rate vector not neutral:\nbase=%+v\nvec =%+v", base, withVec)
	}
}

// TestTransferDelayDispatchDelay: the transfer_delay vector adds dispatch
// latency on the matching queues (poll-path transport latency).
func TestTransferDelayDispatchDelay(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 50
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0
	v := model.DecisionVector{Name: "transfer_delay-transfer-1-0-100", Kind: model.KindTransferDelay, Target: "transfer", StartMs: 1, DurationMs: 0, Rate: 100}
	cfg.Vector = &v

	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	top.Run()
	if top.rt.wfQ.DispatchDelay() != 100 || top.rt.actQ.DispatchDelay() != 100 {
		t.Fatalf("dispatch delay = %d/%d, want 100/100", top.rt.wfQ.DispatchDelay(), top.rt.actQ.DispatchDelay())
	}
}

// TestPersistenceStallSlows: persistence_stall multiplies the serialized
// persistence latency, stretching the shard's write cursor (throughput
// degrades even though sync-match keeps aps = 1.0).
func TestPersistenceStallSlows(t *testing.T) {
	lastWrite := func(mult float64) int64 {
		cfg := DefaultConfig()
		cfg.Seed = 1
		cfg.BurstCount = 50
		cfg.SimEndMs = 60000
		cfg.MatchingCapacity = 1000
		cfg.PersistMs = 1 // small base persist latency
		cfg.FaultStartMs = 0
		cfg.FaultEndMs = 0
		cfg.PollersWF = 4
		cfg.PollersAct = 4
		if mult > 0 {
			v := model.DecisionVector{Name: "persistence_stall", Kind: model.KindPersistenceStall, Target: "history", StartMs: 1, DurationMs: 0, Rate: mult}
			cfg.Vector = &v
		}
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		_ = top.Run()
		return top.rt.shards[0].LastWriteMs()
	}
	calm := lastWrite(0)
	stalled := lastWrite(20)
	if stalled <= calm {
		t.Fatalf("persistence stall did not stretch the write cursor: calm %d stalled %d", calm, stalled)
	}
}

// TestPollJitterEffect: poll_jitter makes pollers re-poll frequently
// (early long-poll expiry). The fault is wired (a substream is created)
// and the model still runs deterministically.
func TestPollJitterEffect(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 2
	cfg.BurstCount = 50
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0
	v := model.DecisionVector{Name: "poll_jitter", Kind: model.KindPollJitter, Target: "matching", StartMs: 1, DurationMs: 0, Rate: 0.9}
	cfg.Vector = &v
	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	if top.rt.jitterRNG != nil {
		t.Fatal("jitter substream created before first draw")
	}
	res := top.Run()
	if top.rt.jitterRNG == nil {
		t.Fatal("poll_jitter did not create its substream (fault not applied)")
	}
	if res.Stats.Completed != 50 {
		t.Fatalf("completed = %d, want 50 (jitter must not break workflows)", res.Stats.Completed)
	}
}

// TestS2CShortenTimeouts: s2c_shorten forces activities to timeout under a
// tight deadline, driving retries.
func TestS2CShortenTimeouts(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 50
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.ActivityMs = 20 // activities take 20ms
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0
	v := model.DecisionVector{Name: "s2c_shorten", Kind: model.KindS2CShorten, Target: "activity", StartMs: 1, DurationMs: 0, Rate: 5}
	cfg.Vector = &v
	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	// Source-faithful (temporal/service/history/workflow/retry.go isRetryable):
	// SCHEDULE_TO_CLOSE is NOT retryable — a 5ms S2C deadline against 20ms
	// activities makes each execution exceed the deadline and terminate the
	// workflow. No retry (the deadline bounds the whole attempt chain).
	if res.Stats.Retries != 0 {
		t.Fatalf("s2c_shorten produced %d retries, want 0 (S2C is terminal, retry.go isRetryable)", res.Stats.Retries)
	}
	if res.Stats.Failed == 0 {
		t.Fatal("s2c_shorten produced no failures (5ms S2C deadline vs 20ms activities, want terminal fail)")
	}
}

// TestBacklogSurgeSpike: backlog_surge injects a secondary burst that
// spikes the matching backlog.
func TestBacklogSurgeSpike(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 3
	cfg.BurstCount = 5
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.PollersWF = 1
	cfg.PollersAct = 1
	cfg.ActivityMs = 5
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0
	v := model.DecisionVector{Name: "backlog_surge", Kind: model.KindBacklogSurge, Target: "frontend", StartMs: 2000, DurationMs: 0, Rate: 100}
	cfg.Vector = &v
	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	if res.Stats.Started != 105 {
		t.Fatalf("started = %d, want 105 (5 + 100 surge)", res.Stats.Started)
	}
}
