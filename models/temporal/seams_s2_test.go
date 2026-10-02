package temporal

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/lawRathod/queuesim/model"
	"github.com/lawRathod/queuesim/model/capture"
)

// S2 append-threshold seam tests (WP2; ADR-106:2; plan
// 2026-09-07-four-grounded-performance-seams-phase1.md S2; ADR-103 — model
// leg PARTIAL: reject-at-cap + escalation-backoff re-add are implemented
// here; saturation claims belong to the C11 G0 gate on the bubble, never to
// this leg).
//
// Source pins (temporal submodule at ffddf6fdc — verified line-for-line):
//   - constants.go:1161-1165  matching.outstandingTaskAppendsThreshold = 250
//     (the per-task-queue append-channel cap the model seam reads)
//   - task_writer.go:103-121  appendTask: full append channel →
//     ResourceExhausted{CAUSE_SYSTEM_OVERLOADED, "Too many outstanding
//     appends to the task queue"} — the reject-at-cap semantic
//   - executable.go:749-774   shouldResubmitOnNack: ResourceExhausted is
//     retryable (resubmit while resourceExhaustedCount <= 1)
//   - executable.go:776-805   backoffDuration: on ResourceExhausted take
//     max(generic policy on attempt, taskResourceExhuastedReschedulePolicy
//     on the shed count)
//   - util.go:82-84           taskResourceExhausted 3s initial × 1.5
//     coefficient, 5min max — the escalation schedule
//
// Neutrality contract (all WP2 tests): cfg.AppendThreshold == 0 leaves the
// pinned digests (seams_test.go: a72689e9d1… / c7b0d97d14…) byte-identical.

// s2StormConfig is the deterministic reject-at-cap scenario: a burst of
// workflows whose activities ALWAYS fail (activity_failure window active
// from t=0) with a flat 100ms unlimited retry — so matching-act is flooded
// by retry re-adds while a SINGLE 300ms activity poller drains it. With the
// append cap engaged the act backlog pins at the cap and over-arriving adds
// shed; the same task is shed repeatedly, so its reschedules escalate
// 3s → 4.5s → 6.75s → … The wf queue stays clear (4 instant wf pollers), so
// only the ACT channel saturates — the C11 mechanism's channel.
func s2StormConfig() Config {
	cfg := DefaultConfig()
	cfg.Seed = 7
	cfg.BurstCount = 20
	cfg.SimEndMs = 30000
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0
	cfg.ArrivalRate = 0
	cfg.ActivityMs = 300
	cfg.PollersAct = 1
	cfg.ActivityS2STimeoutMs = 0
	cfg.RetryInitialMs = 100
	cfg.RetryCoeff = 1.0
	cfg.RetryMaxAttempts = 0 // unlimited — the always-fail storm never stops
	cfg.Vector = &model.DecisionVector{Name: "act_fail", Kind: model.KindActivityFailure, Target: "activity", StartMs: 0, DurationMs: 30000}
	return cfg
}

// TestS2ThresholdZeroOff pins the WP2 zero-default contract (TDD first):
// with AppendThreshold at its zero value the seam is inert — the RNG-storm
// digest stays byte-identical to the recorded draws pin (c7b0d97d14…, the
// WP0 gate) and zero ResourceExhausted sheds are recorded. A threshold so
// large it can never trip (1<<30) is bit-identical to 0 — the seam being
// consulted-but-never-tripped is today's exact behavior.
func TestS2ThresholdZeroOff(t *testing.T) {
	if got := DefaultConfig().AppendThreshold; got != 0 {
		t.Fatalf("DefaultConfig().AppendThreshold = %d, want 0 (zero = seam off)", got)
	}
	run := func(threshold int) (string, TopologyStats, int) {
		cfg := rngConfig()
		cfg.Capture = true
		cfg.AppendThreshold = threshold
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		res := top.Run()
		g := capture.Aggregate(top.Capture().Events())
		return res.Digest, res.Stats, countKind(g, "act_reject_re") + countKind(g, "wf_reject_re")
	}
	zeroDigest, zeroStats, zeroSheds := run(0)
	if zeroSheds != 0 {
		t.Fatalf("AppendThreshold=0 recorded %d ResourceExhausted sheds — seam must be inert at 0", zeroSheds)
	}
	const pinDraws = "c7b0d97d145079f610ce447adb1f2afa2f580031ae7a2f3328f43684d860a5bf"
	if got := fmt.Sprintf("%x", sha256.Sum256([]byte(zeroDigest))); got != pinDraws {
		t.Fatalf("draws digest moved at AppendThreshold=0:\nsha256 = %s\npin     = %s", got, pinDraws)
	}
	// A never-tripped non-zero cap is byte-identical to 0 (off).
	hugeDigest, hugeStats, hugeSheds := run(1 << 30)
	if hugeSheds != 0 {
		t.Fatalf("untrippable threshold recorded %d sheds", hugeSheds)
	}
	if hugeDigest != zeroDigest {
		t.Fatalf("untrippable AppendThreshold perturbed the trace:\n0    %q\n1<<30 %q", zeroDigest, hugeDigest)
	}
	if fmt.Sprintf("%+v", hugeStats) != fmt.Sprintf("%+v", zeroStats) {
		t.Fatalf("stats differ between AppendThreshold 0 and 1<<30:\n0    %+v\n1<<30 %+v", zeroStats, hugeStats)
	}
}

// TestS2NeutralityDigest is the WP2 byte-identity gate (run before/after —
// the digests pinned in seams_test.go must NOT move): with all seam fields
// at their zero default both canonical N5 digests stay byte-identical to
// the recorded pins (zero-draw a72689e9d1…, draws c7b0d97d14…). WP2 wired
// the S2 reject path into every add; this proves the zero path is untouched.
func TestS2NeutralityDigest(t *testing.T) {
	runZeroDraw := func() string {
		cfg := seamZeroConfig()
		cfg.Seed = 7
		cfg.BurstCount = 30
		cfg.SimEndMs = 20000
		cfg.FaultStartMs = 1
		cfg.FaultEndMs = 5001
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run().Digest
	}
	runDraws := func() string {
		cfg := rngConfig()
		cfg.ForwarderMaxOutstandingPolls = 0
		cfg.ForwarderMaxOutstandingTasks = 0
		cfg.AppendThreshold = 0
		cfg.PollerScaleWaitMs = 0
		cfg.PollerDecisionsPerSec = 0
		cfg.PersistenceMaxQPS = 0
		cfg.PersistenceQPSBurstRatio = 0
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run().Digest
	}
	const pinZeroDraw = "a72689e91d1c1ca6feb5b79c30ed2aed52df1f1e88bfc446a1dcb59bdf3ea87e"
	const pinDraws = "c7b0d97d145079f610ce447adb1f2afa2f580031ae7a2f3328f43684d860a5bf"
	if got := fmt.Sprintf("%x", sha256.Sum256([]byte(runZeroDraw()))); got != pinZeroDraw {
		t.Fatalf("zero-draw digest moved with seams at zero:\nsha256 = %s\npin     = %s", got, pinZeroDraw)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256([]byte(runDraws()))); got != pinDraws {
		t.Fatalf("draws digest moved with seams at zero:\nsha256 = %s\npin     = %s", got, pinDraws)
	}
}

// TestS2ResourceExhaustedBackoffPolicy pins the escalation schedule against
// util.go:82-84 + executable.go:776-805: the k-th consecutive shed waits
// max(generic policy on attempt, 3s × 1.5^(k-1), cap 5min) — the server's
// taskResourceExhuastedReschedulePolicy (no 20% jitter; model convention —
// rescheduleBackoffMs omits it too). k=1..4 = 3000/4500/6750/10125 ms; the
// 5th rounds to 15188; k=13 hits the 5min cap; a huge-attempt task keeps
// its generic 3min cap on the first shed (backoffDuration's max()).
func TestS2ResourceExhaustedBackoffPolicy(t *testing.T) {
	cases := []struct {
		k    int
		want int64
	}{
		{1, 3000}, {2, 4500}, {3, 6750}, {4, 10125},
		{5, 15188}, // 3000×1.5^4 = 15187.5 → math.Round
		{12, 259493},
		{13, 300000}, // cap — 5min (taskResourceExhaustedRescheduleMaxInterval)
		{99, 300000},
	}
	for _, c := range cases {
		if got := rescheduleResourceExhaustedMs(1, c.k); got != c.want {
			t.Errorf("rescheduleResourceExhaustedMs(attempt=1, k=%d) = %d, want %d", c.k, got, c.want)
		}
	}
	// attempt 1 → generic policy is 1s, so the RE policy always wins there.
	if got := rescheduleResourceExhaustedMs(1, 1); got != 3000 {
		t.Fatalf("first shed must be the 3s initial, got %d", got)
	}
	// backoffDuration's max(): a task deep in its generic backoff keeps the
	// generic cap on its FIRST RE shed (180s > 3s); the RE cap wins later.
	if got := rescheduleResourceExhaustedMs(100, 1); got != 180000 {
		t.Errorf("attempt 100, k=1: want max(180000, 3000) = 180000, got %d", got)
	}
	if got := rescheduleResourceExhaustedMs(100, 13); got != 300000 {
		t.Errorf("attempt 100, k=13: want max(180000, 300000) = 300000, got %d", got)
	}
}

// TestS2RejectAtCapEscalationBackoff is the WP2 contract test: reject-at-cap
// with escalation-backoff re-add, end to end. Under the deterministic storm
// (cap 10, single 300ms activity poller, always-failing activities with a
// 100ms flat unlimited retry):
//   - matching-act reaches the append cap and sheds over-arriving adds
//     (ResourceExhausted — task_writer.go:103-121), recorded as act_reject_re;
//   - sheds NEVER route to the legacy transfer-queue reject (act_reject stays
//     0) and no transport drops are involved (act_drop 0);
//   - the shed task is rescheduled into history (shard rescheduler) and
//     RE-ENTERS matching later (resched_act records) — not the transfer queue;
//   - the SAME task, shed repeatedly, waits the escalating backoff: gaps
//     between consecutive sheds are 3s×1.5^(k-1)+ε and non-decreasing
//     (3002 / 4502 / 6752 / 10127 observed) — executable.go:776-805 picking
//     the resource-exhausted policy of util.go:82-84.
func TestS2RejectAtCapEscalationBackoff(t *testing.T) {
	cfg := s2StormConfig()
	cfg.AppendThreshold = 10
	cfg.Capture = true
	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	evs := top.Capture().Events()
	g := capture.Aggregate(evs)

	if nShed := countKind(g, "act_reject_re"); nShed == 0 {
		t.Fatalf("expected ResourceExhausted sheds at cap %d (got 0)", cfg.AppendThreshold)
	}
	if countKind(g, "wf_reject_re") != 0 {
		t.Error("wf queue should not saturate in this cell (4 instant wf pollers)")
	}
	if countKind(g, "act_reject") != 0 {
		t.Error("sheds must never take the legacy transfer-queue reject path (act_reject)")
	}
	if countKind(g, "act_drop") != 0 {
		t.Error("the storm has no transport drops — sheds must not be confounded with drops")
	}
	if nResched := countKind(g, "resched_act"); nResched == 0 {
		t.Error("expected shed tasks to re-enter matching via the shard rescheduler (resched_act)")
	}

	// Escalation: group consecutive act_reject_re events per task and find the
	// most-shedded task; its inter-shed gaps must be the growing backoff.
	byTask := map[string][]int64{}
	for _, e := range evs {
		if e.Kind == "act_reject_re" {
			byTask[e.TaskID] = append(byTask[e.TaskID], e.TsMs)
		}
	}
	maxID, maxSheds := "", 0
	for id, ts := range byTask {
		if len(ts) > maxSheds {
			maxID, maxSheds = id, len(ts)
		}
	}
	if maxSheds < 4 {
		t.Fatalf("expected a task shed >= 4 times for escalation evidence, max was %d (%s)", maxSheds, maxID)
	}
	ts := byTask[maxID]
	var prev int64
	for i := 1; i < len(ts); i++ {
		gap := ts[i] - ts[i-1]
		if gap < prev {
			t.Fatalf("task %s: shed gaps must be non-decreasing (escalation); gap[%d]=%d < gap[%d]=%d",
				maxID, i, gap, i-1, prev)
		}
		prev = gap
	}
	gap1 := ts[1] - ts[0]
	if gap1 < 3000 {
		t.Fatalf("task %s: first re-add waited %d ms, want >= 3000 (the 3s initial, util.go:82)", maxID, gap1)
	}
	// By the 4th shed the wait must exceed the initial interval substantially
	// (observed 6752 at k=3): escalation, not a flat 3s.
	gap3 := ts[3] - ts[2]
	if gap3 < 6000 {
		t.Fatalf("task %s: 4th-shed wait = %d ms, want >= 6000 (3s×1.5^3, escalation beyond the flat initial)", maxID, gap3)
	}
	t.Logf("cap=%d sheds=%d most-shedded task=%s gaps=%v completed=%d", cfg.AppendThreshold, countKind(g, "act_reject_re"), maxID, gapsOf(ts), res.Stats.Completed)
}

func gapsOf(ts []int64) []int64 {
	g := make([]int64, 0, len(ts)-1)
	for i := 1; i < len(ts); i++ {
		g = append(g, ts[i]-ts[i-1])
	}
	return g
}
