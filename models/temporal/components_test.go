package temporal

import (
	"testing"

	"github.com/lawRathod/queuesim/godes/core"
	"github.com/lawRathod/queuesim/models/temporal/components/history"
	"github.com/lawRathod/queuesim/models/temporal/components/matcher"
	"github.com/lawRathod/queuesim/models/temporal/components/worker"
)

func newSim(t *testing.T) *core.Simulation {
	t.Helper()
	sim, err := core.NewSimulation(42, core.FIFOPolicy{}) //nolint:forbidigo // test helper — *_test.go allowlisted (ADR-091)
	if err != nil {
		t.Fatalf("NewSimulation: %v", err)
	}
	return sim
}

func mkTask(id uint64) Task {
	return Task{ID: id, Attempt: 1, BornMs: 0}
}

// TestZeroDrawsWhenOff: with the dispatch throttle off (rate <= 0), the
// queue must NOT create a substream (zero-draw contract, ADR-068/078).
func TestZeroDrawsWhenOff(t *testing.T) {
	sim := newSim(t)
	q := matcher.NewMatchQueue(sim, "q", 0, 0, 1, 0, 0)
	q.Add(mkTask(1), sim.Now())
	if q.HasRNG() {
		t.Fatal("throttle substream created while rate is 0 (zero draws when off violated)")
	}
}

// TestWorkerPoolLifecycle pins the pool's execution loop: Start schedules
// the completion at now+serviceMs, in-flight is held while executing, and
// the onDone hook fires on completion (the re-poll loop the matching
// service's repollWf/repollAct rely on).
func TestWorkerPoolLifecycle(t *testing.T) {
	sim := newSim(t)
	q := matcher.NewMatchQueue(sim, "q", 0, 0, 1, 0, 0)
	done := false
	p := worker.NewPool(sim, q, 10, 1)
	p.SetOnDone(func() { done = true })
	if p.InFlight() != 0 {
		t.Fatalf("in-flight = %d, want 0", p.InFlight())
	}
	at := p.Start(mkTask(1), sim.Now())
	if at.Int64() != 10 {
		t.Fatalf("completion at = %d, want 10 (now+serviceMs)", at.Int64())
	}
	if p.InFlight() != 1 {
		t.Fatalf("in-flight = %d, want 1", p.InFlight())
	}
	sim.Run() // completes at t+10
	if p.InFlight() != 0 {
		t.Fatalf("in-flight after run = %d, want 0", p.InFlight())
	}
	if !done {
		t.Fatal("onDone hook not fired")
	}
}

// --- TokenBucket -------------------------------------------------------

func TestTokenBucketUnlimited(t *testing.T) {
	b := matcher.NewTokenBucket(0, 0)
	if !b.Consume(0) {
		t.Fatal("unlimited bucket refused")
	}
	if b.Refused() != 0 {
		t.Fatalf("refused = %d, want 0", b.Refused())
	}
	if b.Consumed() != 1 {
		t.Fatalf("consumed = %d, want 1", b.Consumed())
	}
}

func TestTokenBucketRefill(t *testing.T) {
	b := matcher.NewTokenBucket(1, 1) // 1 token/s, burst 1
	if !b.Consume(0) {
		t.Fatal("initial token refused")
	}
	if b.Consume(1) {
		t.Fatal("no refill after 1ms")
	}
	if b.Refused() != 1 {
		t.Fatalf("refused = %d, want 1", b.Refused())
	}
	if !b.Consume(1000) { // 1s passed → 1 token
		t.Fatal("refill after 1s refused")
	}
	if b.Consume(1999) {
		t.Fatal("granted at 999ms after refill (only 0.999 tokens)")
	}
	if !b.Consume(2000) {
		t.Fatal("refill at exactly 1s later refused")
	}
}

// --- MatchQueue --------------------------------------------------------

// TestSyncMatchFirst: a parked poller receives the task inline at the
// producer's sim instant, without touching the backlog.
func TestSyncMatchFirst(t *testing.T) {
	sim := newSim(t)
	q := matcher.NewMatchQueue(sim, "q", 0, 0, 1, 0, 0)
	var got []uint64
	sim.ScheduleAt(sim.Now(), func(*core.Simulation) {
		q.Park(sim.Now().Add(60000).Int64(), func(tt Task) { got = append(got, tt.ID) })
	})
	sim.ScheduleAt(sim.Now(), func(*core.Simulation) {
		sm, acc := q.Add(mkTask(1), sim.Now())
		if !sm || !acc {
			t.Fatalf("Add(sync) = %v,%v want true,true", sm, acc)
		}
	})
	sim.Run()
	if q.Len() != 0 {
		t.Fatalf("backlog len = %d, want 0 (sync match must not spool)", q.Len())
	}
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("taken = %v, want [1]", got)
	}
}

// TestAddSpoolsOnMiss: no parked poller → task buffers to the backlog.
func TestAddSpoolsOnMiss(t *testing.T) {
	sim := newSim(t)
	q := matcher.NewMatchQueue(sim, "q", 0, 0, 1, 0, 0)
	sm, acc := q.Add(mkTask(1), sim.Now())
	if sm || !acc {
		t.Fatalf("Add(no poller) = %v,%v want false,true", sm, acc)
	}
	if q.Len() != 1 {
		t.Fatalf("backlog len = %d, want 1", q.Len())
	}
}

// TestCapacityReject: bounded backlog rejects at capacity.
func TestCapacityReject(t *testing.T) {
	sim := newSim(t)
	q := matcher.NewMatchQueue(sim, "q", 1, 0, 1, 0, 0)
	// Source-faithful: matching is persistence-backed (unbounded), so a full
	// queue never rejects — the capacity field is retained for API compat
	// only and rejected stays zero. Prove the 2nd add is accepted (spools)
	// and rejected is 0.
	if _, acc := q.Add(mkTask(1), sim.Now()); !acc {
		t.Fatal("first add rejected")
	}
	if _, acc := q.Add(mkTask(2), sim.Now()); !acc {
		t.Fatal("second add rejected (persistence-backed matching has no capacity reject)")
	}
	_, _, rejected, _, _, _ := q.Stats()
	if rejected != 0 {
		t.Fatalf("rejected = %d, want 0 (persistence-backed, no capacity reject)", rejected)
	}
}

// TestExpiryAtPoll: a task that waits out its deadline expires at dispatch
// (three-point expiry). The queue RETURNS the expired task to the caller
// (ok=true) for disposition — the topology treats an S2S/wf-task deadline
// as TERMINAL (the workflow fails; SCHEDULE_TO_START is not retryable,
// service/history/workflow/retry.go:115-133), while dropped tasks re-enter
// via the rescheduler. This test pins the QUEUE contract only.
func TestExpiryAtPoll(t *testing.T) {
	sim := newSim(t)
	q := matcher.NewMatchQueue(sim, "q", 0, 100, 1, 0, 0)
	sim.Advance(10)
	if _, acc := q.Add(mkTask(1), sim.Now()); !acc {
		t.Fatal("add rejected")
	}
	// Park a poller so dispatch can proceed.
	q.Park(sim.Now().Add(100000).Int64(), func(Task) {})
	sim.Advance(200) // past the 100ms deadline
	tk, ok := q.TryDispatch(sim.Now())
	// The expired task is RETURNED (retryable): ok=true, task present.
	if !ok {
		t.Fatalf("TryDispatch = false want the expired task returned (retryable)")
	}
	if tk.ID != 1 {
		t.Fatalf("returned task ID = %d, want 1 (the expired task)", tk.ID)
	}
	if q.ExpiredCount()[ExpireAtPoll] != 1 {
		t.Fatalf("expired[Poll] = %d, want 1", q.ExpiredCount()[ExpireAtPoll])
	}
	if q.Len() != 0 {
		t.Fatalf("backlog len = %d, want 0 (expired removed)", q.Len())
	}
	if q.Parked() != 1 {
		t.Fatalf("parked = %d, want 1 (poller stays parked after expiry)", q.Parked())
	}
}

// TestDispatchThrottle: with a 1-token/s bucket, only one dispatch per
// second succeeds; the rest are refused (rate limiter semantics).
func TestDispatchThrottle(t *testing.T) {
	sim := newSim(t)
	q := matcher.NewMatchQueue(sim, "q", 0, 0, 1, 1, 1) // 1/s, burst 1
	sim.Advance(10)
	for i := 0; i < 3; i++ {
		if _, acc := q.Add(mkTask(uint64(i)), sim.Now()); !acc {
			t.Fatalf("add %d rejected", i)
		}
	}
	// Park a poller so dispatch can proceed.
	var taken int
	sim.ScheduleAt(sim.Now(), func(*core.Simulation) {
		q.Park(sim.Now().Add(100000).Int64(), func(Task) { taken++ })
	})
	// Dispatch at t=10: first succeeds, next refused (no token).
	q.Park(sim.Now().Add(100000).Int64(), func(Task) { taken++ })
	sim.ScheduleAt(sim.Now(), func(*core.Simulation) {
		if _, ok := q.TryDispatch(sim.Now()); !ok {
			t.Fatal("first dispatch refused")
		}
		if _, ok := q.TryDispatch(sim.Now()); ok {
			t.Fatal("second dispatch should be throttled")
		}
	})
	sim.Run()
	if taken != 1 {
		t.Fatalf("taken = %d, want 1", taken)
	}
	_, _, _, _, refused, _ := q.Stats()
	if refused != 1 {
		t.Fatalf("refused = %d, want 1", refused)
	}
}

// TestSyncMatchCancelWake: a sync-matched poller's scheduled long-poll
// wake event is canceled (stale wake must not double-handoff).
func TestSyncMatchCancelWake(t *testing.T) {
	sim := newSim(t)
	q := matcher.NewMatchQueue(sim, "q", 0, 0, 1, 0, 0)
	var taken int
	var p *matcher.ParkedPoller
	sim.ScheduleAt(sim.Now(), func(*core.Simulation) {
		p = q.Park(sim.Now().Add(60000).Int64(), func(Task) { taken++ })
	})
	sim.ScheduleAt(sim.Now(), func(*core.Simulation) {
		q.Add(mkTask(1), sim.Now()) // sync match → consumed
	})
	// Advance far past the poll expiry; the stale wake must be a no-op.
	sim.ScheduleIn(120000, func(*core.Simulation) {
		if taken != 1 {
			t.Fatalf("taken = %d, want 1 (stale wake double-handoff)", taken)
		}
		if q.Parked() != 0 {
			t.Fatalf("parked = %d, want 0", q.Parked())
		}
	})
	_ = p
	sim.Run()
}

// --- HistoryShard ------------------------------------------------------

func TestShardTimerNeverEarly(t *testing.T) {
	sim := newSim(t)
	// timerShiftMs is a read-ahead horizon, NOT an early-fire window: a
	// timer due at t=1000 must not fire at t=900 even with shift 100.
	s := history.NewHistoryShard(sim, 0, 0, 1, 1, 1, 1, 100, 0)
	s.AddTimer(mkTask(1), 1000)
	sim.Advance(900)
	var fired []uint64
	s.Tick(sim.Now(), nil, func(t Task) { fired = append(fired, t.ID) })
	if len(fired) != 0 {
		t.Fatalf("timer fired at t=900 = %d, want 0 (never early)", len(fired))
	}
	if s.LenTimer() != 1 {
		t.Fatalf("timer len = %d, want 1 (still queued)", s.LenTimer())
	}
	// Due at t=1000: fires exactly once.
	sim.Advance(100)
	s.Tick(sim.Now(), nil, func(t Task) { fired = append(fired, t.ID) })
	if len(fired) != 1 {
		t.Fatalf("timer fired at t=1000 = %d, want 1 (due)", len(fired))
	}
	if s.LenTimer() != 0 {
		t.Fatalf("timer len = %d, want 0", s.LenTimer())
	}
}

func TestShardTimerNotBeforeDue(t *testing.T) {
	sim := newSim(t)
	s := history.NewHistoryShard(sim, 0, 0, 1, 1, 1, 1, 100, 0)
	s.AddTimer(mkTask(1), 1000)
	sim.Advance(999) // 1ms before due — shift must not fire it early
	var fired int
	s.Tick(sim.Now(), nil, func(Task) { fired++ })
	if fired != 0 {
		t.Fatalf("timer fired at t=999, want 0 (never early)")
	}
}

func TestShardTransferBatch(t *testing.T) {
	sim := newSim(t)
	s := history.NewHistoryShard(sim, 0, 0, 2, 1, 1, 1, 0, 0)
	for i := 0; i < 5; i++ {
		s.EnqueueTransfer(mkTask(uint64(i)))
	}
	var processed []uint64
	s.Tick(sim.Now(), func(t Task) { processed = append(processed, t.ID) }, nil)
	if len(processed) != 2 {
		t.Fatalf("processed = %d, want 2 (batch 2)", len(processed))
	}
	if s.LenTransfer() != 3 {
		t.Fatalf("transfer len = %d, want 3", s.LenTransfer())
	}
}

// TestShardBatchOnlyWorkersNoEffect pins that the per-tick caps are the
// batch sizes only: workers scale nothing because the model drains
// serially on the single simulation goroutine.
func TestShardBatchOnlyWorkersNoEffect(t *testing.T) {
	sim := newSim(t)
	s := history.NewHistoryShard(sim, 0, 0, 2, 3, 2, 3, 0, 0)
	for i := 0; i < 5; i++ {
		s.EnqueueTransfer(mkTask(uint64(i)))
		s.AddTimer(mkTask(uint64(100+i)), 0) // due immediately
	}
	var processed, fired int
	s.Tick(sim.Now(), func(Task) { processed++ }, func(Task) { fired++ })
	if processed != 2 {
		t.Fatalf("processed = %d, want 2 (transferBatch 2, workers no effect)", processed)
	}
	if fired != 2 {
		t.Fatalf("fired = %d, want 2 (timerBatch 2, workers no effect)", fired)
	}
}

func TestShardRescheduler(t *testing.T) {
	sim := newSim(t)
	s := history.NewHistoryShard(sim, 0, 0, 1, 1, 1, 1, 0, 50)
	s.Reschedule(mkTask(1), 100)
	sim.Advance(49)
	s.Tick(sim.Now(), nil, nil)
	if s.LenResched() != 1 {
		t.Fatalf("resched len = %d, want 1 (not due yet)", s.LenResched())
	}
	sim.Advance(2) // t=51 ≥ retryAt 150? no: 100+50=150; advance 2 → t=51 < 150
	s.Tick(sim.Now(), nil, nil)
	if s.LenResched() != 1 {
		t.Fatalf("resched len = %d at t=51, want 1 (due at 150)", s.LenResched())
	}
	sim.Advance(99) // t=150
	s.Tick(sim.Now(), nil, nil)
	if s.LenResched() != 0 {
		t.Fatalf("resched len = %d, want 0 (due)", s.LenResched())
	}
	if s.LenTransfer() != 1 {
		t.Fatalf("transfer len = %d, want 1 (rescheduled task re-enqueued)", s.LenTransfer())
	}
}

// TestShardIntakeCounters pins the WP3.2 history-boundary intake counters:
// each enqueue method increments its own kind exactly once, and a Tick re-route
// (the legacy resched→transfer fallback and the onResched hook) does NOT
// double-count — intake counts OFFERS, not queue movements.
func TestShardIntakeCounters(t *testing.T) {
	sim := newSim(t)
	s := history.NewHistoryShard(sim, 0, 0, 1, 1, 1, 1, 0, 50)
	for i := 0; i < 3; i++ {
		s.EnqueueTransfer(mkTask(uint64(i + 1)))
	}
	for i := 0; i < 2; i++ {
		s.AddTimer(mkTask(uint64(10+i)), 1000)
	}
	s.Reschedule(mkTask(20), 0)    // due at t=50
	s.RescheduleAt(mkTask(21), 90) // due at t=90
	if got := s.IntakeTransfer(); got != 3 {
		t.Fatalf("IntakeTransfer = %d, want 3", got)
	}
	if got := s.IntakeTimer(); got != 2 {
		t.Fatalf("IntakeTimer = %d, want 2", got)
	}
	if got := s.IntakeResched(); got != 2 {
		t.Fatalf("IntakeResched = %d, want 2", got)
	}

	// Drain some transfers; intake is unchanged (movements are not offers).
	s.Tick(sim.Now(), func(Task) {}, nil)
	if got := s.IntakeTransfer(); got != 3 {
		t.Fatalf("IntakeTransfer after drain = %d, want 3", got)
	}

	// Advance past the due resched and Tick with NO onResched hook: the
	// fallback re-routes the entry into the transfer queue. That must NOT
	// increment IntakeResched again.
	sim.Advance(60) // t=60: resched@50 due, resched@90 not
	s.Tick(sim.Now(), nil, nil)
	if got := s.IntakeResched(); got != 2 {
		t.Fatalf("IntakeResched after fallback re-route = %d, want 2", got)
	}
	if s.LenResched() != 1 {
		t.Fatalf("resched len = %d at t=60, want 1 (only the @90 entry left)", s.LenResched())
	}

	// And with the onResched hook at t=90: still no double-count.
	sim.Advance(30) // t=90
	s.Tick(sim.Now(), nil, nil, func(Task) {})
	if got := s.IntakeResched(); got != 2 {
		t.Fatalf("IntakeResched after onResched route = %d, want 2", got)
	}
}

// TestShardReschedToScheduler pins that a due reschedule routes to the
// onResched hook when provided — not to the transfer queue.
func TestShardReschedToScheduler(t *testing.T) {
	sim := newSim(t)
	s := history.NewHistoryShard(sim, 0, 0, 1, 1, 1, 1, 0, 50)
	s.Reschedule(mkTask(7), 100) // due at t=150
	sim.Advance(150)
	var routed []uint64
	s.Tick(sim.Now(), nil, nil, func(t Task) { routed = append(routed, t.ID) })
	if len(routed) != 1 || routed[0] != 7 {
		t.Fatalf("routed = %v, want [7] (due resched to onResched)", routed)
	}
	if s.LenTransfer() != 0 {
		t.Fatalf("transfer len = %d, want 0 (must not fall back to transfer queue)", s.LenTransfer())
	}
	if s.LenResched() != 0 {
		t.Fatalf("resched len = %d, want 0 (due)", s.LenResched())
	}
}

// --- StickyQueue -------------------------------------------------------

func TestStickyPoll(t *testing.T) {
	sim := newSim(t)
	s := history.NewStickyQueue(sim, 0, 5)
	ok, fb := s.Stick(1, mkTask(1), 0)
	if !ok || fb != 0 {
		t.Fatalf("Stick = %v,%d want true,0", ok, fb)
	}
	got, ok := s.Poll(1)
	if !ok || got.ID != 1 {
		t.Fatalf("Poll = %v,%v want {1},true", got, ok)
	}
}

func TestStickyFallback(t *testing.T) {
	sim := newSim(t)
	s := history.NewStickyQueue(sim, 100, 5)
	// worker 1 active at t=0; cache expires at t=100.
	s.MarkActive(1, 0)
	sim.Advance(200)
	ok, fb := s.Stick(1, mkTask(1), 200)
	if ok {
		t.Fatal("Stick = true, want fallback (cache expired)")
	}
	if fb != 5 {
		t.Fatalf("fallback = %d, want 5", fb)
	}
	if s.Len() != 0 {
		t.Fatalf("len = %d, want 0 (task not stuck)", s.Len())
	}
}
