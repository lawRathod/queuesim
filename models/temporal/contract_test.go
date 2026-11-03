package temporal

import (
	"testing"

	"github.com/lawRathod/queuesim/godes/core"
	"github.com/lawRathod/queuesim/model"
	"github.com/lawRathod/queuesim/models/temporal/components/history"
	"github.com/lawRathod/queuesim/models/temporal/components/matcher"
)

// matcherQueue builds a bare MatchQueue for mechanism-unit tests.
func matcherQueue(sim *core.Simulation, name string) *matcher.MatchQueue {
	return matcher.NewMatchQueue(sim, name, 0, 0, 1, 0, 0)
}

// Mechanism-contract tests: the model's constants and behaviors must match
// the REAL Temporal server, which is the source of truth (ADR-078). Each
// pin carries the submodule file:line reference; the numbers are read from
// ../temporal (the `go.temporal.io/server` submodule at the repo root).
//
// Verified against the submodule 2026-08-26:
//
//   - MatchingSyncMatchWaitDuration = 200ms
//     temporal/common/dynamicconfig/constants.go:1128-1131
//   - taskRescheduleInitialInterval = 1s / coefficient 1.1 / max 3min
//     temporal/common/util.go:67-69
//   - SCHEDULE_TO_START is NOT retryable (only START_TO_CLOSE and HEARTBEAT
//     are) → an S2S timeout fails the workflow
//     temporal/service/history/workflow/retry.go:115-133
//   - MatchingLongPollExpirationInterval = 1min
//     temporal/common/dynamicconfig/constants.go:1122-1125
//   - MatchingPollerScalingBacklogAgeScaleUp = 200ms
//     temporal/common/dynamicconfig/constants.go:1354-1357
//   - elastic scale-up: backlog age > PollerScalingBacklogAgeScaleUp → +1
//     temporal/service/matching/physical_task_queue_manager.go:876-884
//
// Verified against the submodule 2026-09-08 (wave two — history queue
// processor defaults + sticky reference horizon):
//
//   - TransferTaskBatchSize = 100
//     temporal/common/dynamicconfig/constants.go:1867-1871
//   - TransferProcessorSchedulerWorkerCount = 512
//     temporal/common/dynamicconfig/constants.go:1882-1886
//   - TimerTaskBatchSize = 100
//     temporal/common/dynamicconfig/constants.go:1790-1794
//   - TimerProcessorSchedulerWorkerCount = 512
//     temporal/common/dynamicconfig/constants.go:1795-1799
//   - TimerProcessorMaxTimeShift = 1s
//     temporal/common/dynamicconfig/constants.go:1845-1849
//   - DefaultWorkflowTaskTimeout = 10s (the sticky-S2S reference horizon)
//     temporal/common/primitives/constants.go:16 (wired as the namespace
//     default at temporal/common/dynamicconfig/constants.go:2363-2366)
//   - persistence latency claims NO server default (explicit non-pin — no
//     such constant exists; PersistMs stays 0)
//   - activity-retry backoff ceiling: the model's legacy 60s
//     (model.RetryController zero-value) is a calibration, not a source
//     pin — RetryMaximumIntervalMs 0 selects it neutrally

// Server-default constants (ms) — the file:line refs above are the pins.
const (
	serverSyncMatchWindowMs        = 200
	serverReschedInitialMs         = 1000
	serverReschedBackoffCoeff      = 1.1
	serverReschedMaxMs             = 180000
	serverLongPollMs               = 60000
	serverPollerScaleUpBacklogMs   = 200
	serverScheduleToStartRetryable = false
	serverTransferBatch            = 100
	serverTransferWorkers          = 512
	serverTimerBatch               = 100
	serverTimerWorkers             = 512
	serverTimerMaxShiftMs          = 1000
	serverReschedulerBackoffMs     = 1000
	// serverStickyS2SReferenceMs is the sticky schedule-to-start reference
	// horizon — DefaultWorkflowTaskTimeout
	// (temporal/common/primitives/constants.go:16). The model default
	// stays 0 (disabled/neutral); the pin documents the horizon a set
	// StickyS2STimeoutMs is measured against.
	serverStickyS2SReferenceMs = 10000
	// serverRetryLegacyMaxMs is the model's legacy activity-retry backoff
	// ceiling (model.RetryController zero-value) — a CALIBRATION, not a
	// source pin: no such server constant is claimed.
	serverRetryLegacyMaxMs = 60000
	// serverBacklogNegligibleAgeMs is the isBacklogNegligible gate threshold
	// — the real server disables foreground sync-match + pauses forwarding
	// when getBacklogAge() >= this (~5s, task_queue.go getBacklogAge /
	// isBacklogNegligible, dynamicconfig/constants.go:1277-1279).
	serverBacklogNegligibleAgeMs = 5000
)

// TestSyncMatchWindowMatchesSubmodule pins DefaultConfig's sync-match
// window to MatchingSyncMatchWaitDuration
// (temporal/common/dynamicconfig/constants.go:1128-1131).
func TestSyncMatchWindowMatchesSubmodule(t *testing.T) {
	if got := DefaultConfig().SyncMatchWindowMs; got != serverSyncMatchWindowMs {
		t.Fatalf("SyncMatchWindowMs = %d, want %d (MatchingSyncMatchWaitDuration)", got, serverSyncMatchWindowMs)
	}
}

// TestBacklogGateMatchesSubmodule pins DefaultConfig's backlog-age gate
// (Mechanism A) to the real isBacklogNegligible threshold — the server
// disables foreground sync-match + pauses forwarding when the backlog age
// reaches ~5s (task_queue.go isBacklogNegligible / getBacklogAge,
// matcher.go:109-116; dynamicconfig/constants.go:1277-1279). The default
// lives in DefaultConfig (like the SyncMatchWindowMs default), not in
// newRuntime normalization.
func TestBacklogGateMatchesSubmodule(t *testing.T) {
	if got := DefaultConfig().BacklogGateMs; got != serverBacklogNegligibleAgeMs {
		t.Fatalf("BacklogGateMs = %d, want %d (isBacklogNegligible 5s gate)", got, serverBacklogNegligibleAgeMs)
	}
}

// TestChildDrainFractionDefault pins DefaultConfig's child write-partition
// local drain capacity (Mechanism B's throughput side, the structural drain
// that replaced the ForwardDeadRateFactor proxy). The default is the fraction
// of DispatchRate at which a forwarding-dead child drains its own backlog —
// calibrated to reproduce the real server's window-0 successful-add drop
// (model c2@0 ~7.28 aps vs bubble mean 7.33). 0.75 is NOT a source pin (there
// is no such server constant); it is the structural drain-capacity
// calibration, documented as residual.
func TestChildDrainFractionDefault(t *testing.T) {
	if got := DefaultConfig().ChildDrainFraction; got != 0.75 {
		t.Fatalf("ChildDrainFraction = %v, want 0.75 (window-0 child drain calibration)", got)
	}
}

// TestLongPollMatchesSubmodule pins newRuntime's long-poll default to
// MatchingLongPollExpirationInterval
// (temporal/common/dynamicconfig/constants.go:1122-1125).
func TestLongPollMatchesSubmodule(t *testing.T) {
	if got := DefaultConfig().LongPollMs; got != serverLongPollMs {
		t.Fatalf("LongPollMs = %d, want %d (MatchingLongPollExpirationInterval)", got, serverLongPollMs)
	}
	rt, err := newRuntime(Config{BurstCount: 1, PollersWF: 1, PollersAct: 1})
	if err != nil {
		t.Fatal(err)
	}
	if rt.cfg.LongPollMs != serverLongPollMs {
		t.Fatalf("newRuntime LongPollMs = %d, want %d", rt.cfg.LongPollMs, serverLongPollMs)
	}
}

// TestRescheduleBackoffCurveMatchesSubmodule pins the queues-framework
// reschedule policy — 1s initial, 1.1× coefficient, 3min cap —
// (temporal/common/util.go:67-69). The model's rescheduleBackoffMs is the
// same exponential policy used for dropped/rejected adds.
func TestRescheduleBackoffCurveMatchesSubmodule(t *testing.T) {
	// Attempts are 1-based; the backoff is measured BEFORE the given
	// attempt (1000 * 1.1^(attempt-1)), matching the server policy
	// construction at common/util.go:224-226
	// (backoff.NewExponentialRetryPolicy(initial).WithBackoffCoefficient(c).
	//  WithMaximumInterval(max)).
	cases := []struct {
		attempt int32
		want    int64
	}{
		{1, 1000},                 // initial
		{2, 1100},                 // 1000 * 1.1
		{3, 1210},                 // 1000 * 1.1^2
		{4, 1331},                 // 1000 * 1.1^3
		{5, 1464},                 // 1000 * 1.1^4 = 1464.1 → round
		{6, 1611},                 // 1000 * 1.1^5 = 1610.51 → round
		{56, serverReschedMaxMs},  // 1000 * 1.1^55 ≈ 189059 ≥ 180000 → cap
		{100, serverReschedMaxMs}, // hard cap
	}
	for _, tc := range cases {
		if got := rescheduleBackoffMs(tc.attempt); got != tc.want {
			t.Fatalf("rescheduleBackoffMs(%d) = %d, want %d (common/util.go:67-69)", tc.attempt, got, tc.want)
		}
	}
}

// TestActivityS2SExpiryIsTerminal pins the ACTIVITY schedule-to-start
// timeout semantics: the real server's activity retry policy does NOT retry
// S2S timeouts (only START_TO_CLOSE and HEARTBEAT are retryable —
// temporal/service/history/workflow/retry.go:124-133), so an activity that
// waits out its S2S deadline fails the workflow without a retry. NARROWED
// to activity tasks: this pin makes no claim about workflow-task S2S
// expiry (sticky/wf tasks reschedule via fallback — see
// TestStickyS2SHeadExpiryReschedules). The model enforces the activity
// case at dispatch: an expired activity task is terminal (Failed++).
func TestActivityS2SExpiryIsTerminal(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 50
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.PollersWF = 1
	cfg.PollersAct = 1
	cfg.ActivityMs = 50 // poller busy: backlogged activities exceed the S2S deadline
	cfg.ActivityS2STimeoutMs = 30
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0

	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	if res.Stats.Expired == 0 && res.Stats.Failed == 0 {
		t.Fatal("S2S timeout produced no expiry signal (Expired/Failed = 0)")
	}
	if res.Stats.Retries != 0 {
		t.Fatalf("S2S expiry retried (%d retries): SCHEDULE_TO_START is TERMINAL (isRetryable=false, retry.go:115-133)", res.Stats.Retries)
	}
	if res.Stats.Failed == 0 {
		t.Fatalf("S2S expiry did not fail any workflow (Failed=0, Expired=%d)", res.Stats.Expired)
	}
}

// TestElasticScaleThresholdMatchesSubmodule pins the elastic-matching
// scale-up threshold to MatchingPollerScalingBacklogAgeScaleUp
// (temporal/common/dynamicconfig/constants.go:1354-1357): when elastic
// scaling is enabled (PollerScaleMax > configured fleet) and the backlog
// age exceeds 200ms, the active poller count grows by one
// (physical_task_queue_manager.go:876-884).
func TestElasticScaleThresholdMatchesSubmodule(t *testing.T) {
	rt, err := newRuntime(Config{
		BurstCount: 1, PollersWF: 1, PollersAct: 1,
		PollerScaleMax: 4, // enable scaling
	})
	if err != nil {
		t.Fatal(err)
	}
	if rt.cfg.PollerScaleUpBacklogMs != serverPollerScaleUpBacklogMs {
		t.Fatalf("PollerScaleUpBacklogMs = %d, want %d (matching.pollerScalingMinimumBacklog)", rt.cfg.PollerScaleUpBacklogMs, serverPollerScaleUpBacklogMs)
	}

	// Behavior: a backlog persisting past the threshold scales the act
	// poller target up by one (makePollerScalingDecisionImpl semantics).
	sim := newSim(t)
	rt.sim = sim
	rt.wfQ = matcherQueue(sim, "matching-wf")
	rt.actQ = matcherQueue(sim, "matching-act")
	rt.actQ.Add(mkTask(1), sim.Now())
	rt.pollerTargetAct = 1
	rt.scaleElastic(1) // backlog age starts now
	rt.scaleElastic(1)
	if rt.pollerTargetAct != 1 {
		t.Fatalf("target scaled before the threshold: %d", rt.pollerTargetAct)
	}
	rt.scaleElastic(serverPollerScaleUpBacklogMs + 1) // age ≥ 200ms
	if rt.pollerTargetAct != 2 {
		t.Fatalf("target = %d, want 2 (backlog age > 200ms → +1 poller)", rt.pollerTargetAct)
	}
}

// TestSyncMatchOnlyUnderCalmKeepsApsOne is the headline mechanism property:
// under calm load every activity task sync-matches (a poller is parked
// within the 200ms window), so retry amplification stays ≈ 1.0 — the
// property v1 could never exhibit. Also covered in run_test.go at the
// run level.
func TestSyncMatchOnlyUnderCalmKeepsApsOne(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 50
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0

	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	aps := res.Stats.AttemptsPerStart()
	if aps > 1.05 {
		t.Fatalf("calm aps = %.3f, want ≈ 1.0 (sync-match under calm load)", aps)
	}
	if res.Stats.SyncMatched == 0 {
		t.Fatal("sync-matched = 0, want > 0")
	}
	if res.Stats.Completed != uint64(cfg.BurstCount) {
		t.Fatalf("completed = %d, want %d", res.Stats.Completed, cfg.BurstCount)
	}
}

// TestTransferBatchCalibrationDefault pins DefaultConfig's effective
// transfer-queue reader batch. CALIBRATION OVER PIN: the server default is
// 100 (TransferTaskBatchSize,
// temporal/common/dynamicconfig/constants.go:1867-1871), but the serial
// 4-poller model cannot compose the server's 512 parallel transfer workers
// (applying 100 makes calm spool and S2S>0), so the effective default is
// the calibrated 1. The server reference stays documented on the field.
func TestTransferBatchCalibrationDefault(t *testing.T) {
	if got := DefaultConfig().TransferBatch; got != 1 {
		t.Fatalf("TransferBatch = %d, want 1 (calibration default; server reference %d documented but not applied)", got, serverTransferBatch)
	}
}

// TestTimerBatchMatchesSubmodule pins DefaultConfig's timer-queue reader
// batch size to TimerTaskBatchSize
// (temporal/common/dynamicconfig/constants.go:1790-1794).
func TestTimerBatchMatchesSubmodule(t *testing.T) {
	if got := DefaultConfig().TimerBatch; got != serverTimerBatch {
		t.Fatalf("TimerBatch = %d, want %d (TimerTaskBatchSize)", got, serverTimerBatch)
	}
}

// TestTransferWorkersMatchesSubmodule pins DefaultConfig's transfer-queue
// processor worker count to TransferProcessorSchedulerWorkerCount
// (temporal/common/dynamicconfig/constants.go:1882-1886).
func TestTransferWorkersMatchesSubmodule(t *testing.T) {
	if got := DefaultConfig().TransferWorkers; got != serverTransferWorkers {
		t.Fatalf("TransferWorkers = %d, want %d (TransferProcessorSchedulerWorkerCount)", got, serverTransferWorkers)
	}
}

// TestTimerWorkersMatchesSubmodule pins DefaultConfig's timer-queue
// processor worker count to TimerProcessorSchedulerWorkerCount
// (temporal/common/dynamicconfig/constants.go:1795-1799).
func TestTimerWorkersMatchesSubmodule(t *testing.T) {
	if got := DefaultConfig().TimerWorkers; got != serverTimerWorkers {
		t.Fatalf("TimerWorkers = %d, want %d (TimerProcessorSchedulerWorkerCount)", got, serverTimerWorkers)
	}
}

// TestTimerMaxShiftMatchesSubmodule pins DefaultConfig's timer time-shift
// cap to TimerProcessorMaxTimeShift
// (temporal/common/dynamicconfig/constants.go:1845-1849).
func TestTimerMaxShiftMatchesSubmodule(t *testing.T) {
	if got := DefaultConfig().TimerMaxShiftMs; got != serverTimerMaxShiftMs {
		t.Fatalf("TimerMaxShiftMs = %d, want %d (TimerProcessorMaxTimeShift 1s)", got, serverTimerMaxShiftMs)
	}
}

// TestReschedulerBackoffDefaultMatchesSubmodule pins DefaultConfig's
// rescheduler backoff to the queues-framework reschedule initial interval
// (taskRescheduleInitialInterval, temporal/common/util.go:67) — the same
// 1s the rescheduleBackoffMs curve starts from.
func TestReschedulerBackoffDefaultMatchesSubmodule(t *testing.T) {
	if got := DefaultConfig().ReschedulerBackoffMs; got != serverReschedulerBackoffMs {
		t.Fatalf("ReschedulerBackoffMs = %d, want %d (taskRescheduleInitialInterval 1s)", got, serverReschedulerBackoffMs)
	}
}

// TestShardTransferBatchDrainsPerTick pins the wave-two batch behaviorally:
// a shard built with the DefaultConfig batch drains exactly TransferBatch
// transfers per tick (100, then the 50-task remainder) — the batch-100
// processing bound the server's queue readers enforce.
func TestShardTransferBatchDrainsPerTick(t *testing.T) {
	sim := newSim(t)
	cfg := DefaultConfig()
	// Exercise the batching mechanism explicitly at the server reference
	// value (independent of the model's calibrated default of 1).
	cfg.TransferBatch = serverTransferBatch
	sh := history.NewHistoryShard(sim, 0, cfg.PersistMs,
		int64(cfg.TransferBatch), int64(cfg.TransferWorkers),
		int64(cfg.TimerBatch), int64(cfg.TimerWorkers),
		cfg.TimerMaxShiftMs, cfg.ReschedulerBackoffMs)
	const total = 150
	for i := 0; i < total; i++ {
		sh.EnqueueTransfer(mkTask(uint64(i + 1)))
	}
	var drained int
	onTransfer := func(Task) { drained++ }
	sh.Tick(sim.Now(), onTransfer, nil)
	if drained != serverTransferBatch {
		t.Fatalf("first tick drained = %d, want %d (TransferBatch)", drained, serverTransferBatch)
	}
	if got := sh.LenTransfer(); got != total-serverTransferBatch {
		t.Fatalf("remaining = %d, want %d", got, total-serverTransferBatch)
	}
	sh.Tick(sim.Now(), onTransfer, nil)
	if drained != total {
		t.Fatalf("two ticks drained = %d, want %d", drained, total)
	}
	if got := sh.LenTransfer(); got != 0 {
		t.Fatalf("remaining = %d, want 0", got)
	}
}

// TestStickyS2SHeadExpiryReschedules pins the WORKFLOW-task S2S semantic:
// a sticky task that waits out the sticky schedule-to-start horizon does
// NOT fail — it falls back to the normal queue (a reschedule). The head
// task stuck at BornMs 0 expires exactly at the 10s reference horizon
// (DefaultWorkflowTaskTimeout,
// temporal/common/primitives/constants.go:16), reporting the configured
// fallback delay for the caller's re-add; a younger head does not expire.
func TestStickyS2SHeadExpiryReschedules(t *testing.T) {
	sim := newSim(t)
	s := history.NewStickyQueueWithS2S(sim, 0, 5, serverStickyS2SReferenceMs)
	if ok, _ := s.Stick(0, Task{ID: 7, Attempt: 1, Class: TaskWfComplete, BornMs: 0}, 0); !ok {
		t.Fatal("Stick refused a fresh task")
	}
	if _, _, ok := s.PollExpired(0, serverStickyS2SReferenceMs-1); ok {
		t.Fatal("PollExpired fired before the horizon (age 9999 < 10000)")
	}
	if got := s.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1 (no expiry before the horizon)", got)
	}
	tk, fb, ok := s.PollExpired(0, serverStickyS2SReferenceMs)
	if !ok {
		t.Fatal("PollExpired did not fire at the horizon (age 10000 >= 10000)")
	}
	if tk.ID != 7 || tk.Class != TaskWfComplete {
		t.Fatalf("expired task = %+v, want the stuck wf-complete task (reschedule, not terminal)", tk)
	}
	if fb != 5 {
		t.Fatalf("fallback delay = %d, want 5 (the sticky fallback re-add delay)", fb)
	}
	if got := s.Len(); got != 0 {
		t.Fatalf("Len = %d, want 0 (head consumed by expiry)", got)
	}
	if _, _, ok := s.PollExpired(0, serverStickyS2SReferenceMs); ok {
		t.Fatal("PollExpired fired on an empty buffer")
	}
}

// TestStickyS2SHeadOnlyAndFutureDated pins the two PollExpired edge
// contracts: expiry observes the HEAD only (one task per call — a second
// stuck task waits for the next call), and a future-dated head (negative
// age) never expires.
func TestStickyS2SHeadOnlyAndFutureDated(t *testing.T) {
	sim := newSim(t)
	s := history.NewStickyQueueWithS2S(sim, 0, 5, serverStickyS2SReferenceMs)
	if ok, _ := s.Stick(0, Task{ID: 7, Attempt: 1, Class: TaskWfComplete, BornMs: 0}, 0); !ok {
		t.Fatal("Stick refused the first task")
	}
	if ok, _ := s.Stick(0, Task{ID: 8, Attempt: 1, Class: TaskWfComplete, BornMs: 0}, 0); !ok {
		t.Fatal("Stick refused the second task")
	}
	tk, _, ok := s.PollExpired(0, serverStickyS2SReferenceMs)
	if !ok || tk.ID != 7 {
		t.Fatalf("first PollExpired = %+v, %v, want head task 7 only", tk, ok)
	}
	if got := s.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1 (head-only: task 8 waits)", got)
	}
	if head, ok := s.Peek(0); !ok || head.ID != 8 {
		t.Fatalf("Peek = %+v, %v, want surviving task 8", head, ok)
	}

	// Future-dated head: BornMs ahead of nowMs (negative age) never
	// expires, however far past the horizon the clock reads.
	future := history.NewStickyQueueWithS2S(sim, 0, 5, serverStickyS2SReferenceMs)
	if ok, _ := future.Stick(0, Task{ID: 9, Attempt: 1, Class: TaskWfComplete, BornMs: 50000}, 0); !ok {
		t.Fatal("Stick refused the future-dated task")
	}
	if _, _, ok := future.PollExpired(0, serverStickyS2SReferenceMs); ok {
		t.Fatal("PollExpired fired on a future-dated head (age 10000-50000 < 0)")
	}
	if got := future.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1 (future-dated head intact)", got)
	}
}

// TestStickyS2SDisabledIsNeutral pins the disabled path: a default sticky
// queue (s2sTimeoutMs = 0) never expires — even an ancient head stays
// buffered and pollable, exactly the pre-timeout behavior.
func TestStickyS2SDisabledIsNeutral(t *testing.T) {
	sim := newSim(t)
	s := history.NewStickyQueue(sim, 0, 5)
	if got := s.S2STimeoutMs(); got != 0 {
		t.Fatalf("S2STimeoutMs = %d, want 0 (disabled by default)", got)
	}
	if ok, _ := s.Stick(0, Task{ID: 9, Attempt: 1, Class: TaskWfComplete, BornMs: 0}, 0); !ok {
		t.Fatal("Stick refused a fresh task")
	}
	if _, _, ok := s.PollExpired(0, 1<<62); ok {
		t.Fatal("PollExpired fired while disabled (must be neutral)")
	}
	if got := s.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1 (disabled expiry mutates nothing)", got)
	}
	if tk, ok := s.Peek(0); !ok || tk.ID != 9 {
		t.Fatalf("Peek = %+v, %v, want the intact head task", tk, ok)
	}
}

// TestScheduleToCloseExceedIsTerminal pins the schedule-to-close
// semantic: an activity whose execution outlives ScheduleToCloseMs fails
// the workflow terminally (Failed++, no retry) — the S2C deadline bounds
// the whole attempt chain, so the S2C path never consults CanRetry,
// regardless of retry budget (here unlimited). Expired stays 0 — the
// failure is S2C, not S2S (cf. TestActivityS2SExpiryIsTerminal).
func TestScheduleToCloseExceedIsTerminal(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 3
	cfg.BurstCount = 10
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.ActivityMs = 50 // service time outlasts the S2C cap below
	cfg.ScheduleToCloseMs = 30
	cfg.RetryMaxAttempts = 0 // unlimited: S2C exceed is terminal anyway
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0

	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	if res.Stats.Expired != 0 {
		t.Fatalf("Expired = %d, want 0 (no S2S deadline configured)", res.Stats.Expired)
	}
	if res.Stats.Retries != 0 {
		t.Fatalf("Retries = %d, want 0 (S2C exceed never retries, even with budget)", res.Stats.Retries)
	}
	if res.Stats.Failed == 0 {
		t.Fatal("Failed = 0, want > 0 (S2C exceed fails the workflow)")
	}
	if res.Stats.Completed != 0 {
		t.Fatalf("Completed = %d, want 0 (nothing fits in the S2C cap)", res.Stats.Completed)
	}
}

// TestRetryMaximumIntervalDefaultIsLegacy pins the RetryMaximumIntervalMs
// default to the legacy ceiling: 0 selects the model's 60s
// maxBackoffMs (a CALIBRATION — no server constant claimed), so the
// zero-value controller and an explicit-60000 controller agree on every
// attempt.
func TestRetryMaximumIntervalDefaultIsLegacy(t *testing.T) {
	if got := DefaultConfig().RetryMaximumIntervalMs; got != 0 {
		t.Fatalf("RetryMaximumIntervalMs = %d, want 0 (neutral — legacy 60s ceiling)", got)
	}
	legacy := model.RetryController{InitialIntervalMs: 100, Coefficient: 2.0}
	explicit := model.RetryController{InitialIntervalMs: 100, Coefficient: 2.0, MaximumIntervalMs: serverRetryLegacyMaxMs}
	for attempt := int32(1); attempt <= 25; attempt++ {
		if got, want := legacy.BackoffMs(attempt), explicit.BackoffMs(attempt); got != want {
			t.Fatalf("BackoffMs(%d): legacy = %d, explicit-60000 = %d (must match)", attempt, got, want)
		}
	}
	if got := legacy.BackoffMs(25); got != serverRetryLegacyMaxMs {
		t.Fatalf("legacy BackoffMs(25) = %d, want %d (ceiling)", got, serverRetryLegacyMaxMs)
	}
}

// TestPersistDefaultHasNoSourcePin pins the explicit non-pin: PersistMs
// defaults to 0 (no persistence latency) and the model claims NO server
// default for it — the zero must stay zero until a source is found.
func TestPersistDefaultHasNoSourcePin(t *testing.T) {
	if got := DefaultConfig().PersistMs; got != 0 {
		t.Fatalf("PersistMs = %d, want 0 (no source pin claimed)", got)
	}
}

// TestDeterminismAnchorFixedSeed pins the wave-two anchor invariance: three
// runs of the wave-two defaults under one fixed seed yield bit-identical
// outcome counters (same seed → same trace), and the calm anchor still
// drains (every started workflow completes).
func TestDeterminismAnchorFixedSeed(t *testing.T) {
	run := func() TopologyStats {
		cfg := DefaultConfig()
		cfg.Seed = 7
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run().Stats
	}
	a, b, c := run(), run(), run()
	if a != b || b != c {
		t.Fatalf("fixed-seed runs diverged:\nfirst:  %+v\nsecond: %+v\nthird:  %+v", a, b, c)
	}
	if a.Completed != a.Started {
		t.Fatalf("anchor did not drain: completed = %d, started = %d", a.Completed, a.Started)
	}
}
