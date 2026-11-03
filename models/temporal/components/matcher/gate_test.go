package matcher

import (
	"testing"

	"github.com/lawRathod/queuesim/godes/core"
)

// gateSim builds a Simulation for MatchQueue mechanism tests.
func gateSim(t *testing.T) *core.Simulation {
	t.Helper()
	sim, err := core.NewSimulation(42, core.FIFOPolicy{}) //nolint:forbidigo // test helper — see components_test.go
	if err != nil {
		t.Fatalf("NewSimulation: %v", err)
	}
	return sim
}

// gateTask builds a task born at the given sim ms.
func gateTask(id uint64, bornMs int64) Task {
	return Task{ID: id, Attempt: 1, BornMs: bornMs}
}

// TestForwardReliableSyncMatch pins Mechanism B's HEALTHY path: when the
// child partition CAN forward to a root poller (a healthy sync-match
// window), an add with a parked poller sync-matches — the existing
// fast-path behavior, byte-neutral at the healthy default.
func TestForwardReliableSyncMatch(t *testing.T) {
	sim := gateSim(t)
	q := NewMatchQueue(sim, "q", 0, 0, 1, 0, 0) // window 0 = disabled (sync whenever parked)
	q.SetForwardReliable(true)
	var got []uint64
	sim.ScheduleAt(sim.Now(), func(*core.Simulation) {
		q.Park(sim.Now().Add(60000).Int64(), func(tt Task) { got = append(got, tt.ID) })
	})
	sim.ScheduleAt(sim.Now(), func(*core.Simulation) {
		sm, acc := q.Add(gateTask(1, sim.Now().Int64()), sim.Now())
		if !sm || !acc {
			t.Fatalf("Add(forwarding healthy) = %v,%v want true,true (sync)", sm, acc)
		}
	})
	sim.Run()
	if q.Len() != 0 {
		t.Fatalf("backlog len = %d, want 0 (sync must not spool)", q.Len())
	}
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("taken = %v, want [1]", got)
	}
}

// TestForwardDeadForcesSpool pins Mechanism B's driver: when the child
// partition cannot forward (short/no sync-match window kills the root
// offload), an add with a parked poller does NOT sync-match — it spools to
// the LOCAL backlog (everything stays local → backlog accumulates). This is
// the causal mechanism that feeds the backlog-age gate (Mechanism A).
func TestForwardDeadForcesSpool(t *testing.T) {
	sim := gateSim(t)
	q := NewMatchQueue(sim, "q", 0, 0, 1, 0, 0)
	q.SetForwardReliable(false)
	var got []uint64
	sim.ScheduleAt(sim.Now(), func(*core.Simulation) {
		q.Park(sim.Now().Add(60000).Int64(), func(tt Task) { got = append(got, tt.ID) })
	})
	sim.ScheduleAt(sim.Now(), func(*core.Simulation) {
		sm, acc := q.Add(gateTask(1, sim.Now().Int64()), sim.Now())
		if sm || !acc {
			t.Fatalf("Add(forwarding dead) = %v,%v want false,true (spool, stays local)", sm, acc)
		}
	})
	sim.Run()
	if q.Len() != 1 {
		t.Fatalf("backlog len = %d, want 1 (add must not sync-match off the parked poller)", q.Len())
	}
	if len(got) != 0 {
		t.Fatalf("taken = %v, want none (parked poller not served while forwarding is dead)", got)
	}
}

// TestBacklogGateAllowsSyncUnderAge pins Mechanism A's open state: while the
// head-of-backlog age is within the gate, the sync-match fast path stays
// available (a parked poller is served) — the healthy window-200 behavior.
func TestBacklogGateAllowsSyncUnderAge(t *testing.T) {
	sim := gateSim(t)
	q := NewMatchQueue(sim, "q", 0, 0, 1, 0, 0)
	q.SetBacklogGate(1000) // 1s gate
	q.SetForwardReliable(true)
	sim.Advance(10)
	if _, acc := q.Add(gateTask(1, 10), sim.Now()); !acc {
		t.Fatal("first add rejected")
	} // head born at t=10, sits in backlog
	q.Park(sim.Now().Add(60000).Int64(), func(Task) {}) // parked at t=10
	sim.Advance(50)                                     // t=60; head age = 50 <= 1000 → gate OPEN
	sm, acc := q.Add(gateTask(2, 60), sim.Now())
	if !sm || !acc {
		t.Fatalf("Add(gate open) = %v,%v want true,true (sync allowed under age threshold)", sm, acc)
	}
}

// TestBacklogGateDisablesSyncPastAge pins Mechanism A's closed state: once
// the head-of-backlog age EXCEEDS the gate, the sync-match fast path is
// disabled (adds route to spool) even with a parked poller and healthy
// forwarding — the real server's isBacklogNegligible()→false gate
// (matcher.go:109-116, threshold 5s/constants.go:1277-1279).
func TestBacklogGateDisablesSyncPastAge(t *testing.T) {
	sim := gateSim(t)
	q := NewMatchQueue(sim, "q", 0, 0, 1, 0, 0)
	q.SetBacklogGate(100) // 100ms gate
	q.SetForwardReliable(true)
	sim.Advance(10)
	if _, acc := q.Add(gateTask(1, 10), sim.Now()); !acc {
		t.Fatal("first add rejected")
	} // head born at t=10
	q.Park(sim.Now().Add(60000).Int64(), func(Task) {}) // parked at t=10, healthy forward
	sim.Advance(200)                                    // t=210; head age = 200 > 100 → gate CLOSED
	sm, acc := q.Add(gateTask(2, 210), sim.Now())
	if sm || !acc {
		t.Fatalf("Add(gate closed) = %v,%v want false,true (sync disabled by backlog-age gate)", sm, acc)
	}
	if q.Len() != 2 {
		t.Fatalf("backlog len = %d, want 2 (both tasks backlogged)", q.Len())
	}
}

// TestBacklogGateTripsAtExactAge pins the `>=` boundary: a head-of-backlog
// task resident EXACTLY backlogGateMs closes the gate (matching the real
// server's isBacklogNegligible false at getBacklogAge() >= 5s).
func TestBacklogGateTripsAtExactAge(t *testing.T) {
	sim := gateSim(t)
	q := NewMatchQueue(sim, "q", 0, 0, 1, 0, 0)
	q.SetBacklogGate(100)
	q.SetForwardReliable(true)
	sim.Advance(10)
	if _, acc := q.Add(gateTask(1, 10), sim.Now()); !acc {
		t.Fatal("first add rejected")
	}
	q.Park(sim.Now().Add(60000).Int64(), func(Task) {})
	sim.Advance(100) // t=110; head age = exactly 100 == gate → gate CLOSED
	sm, acc := q.Add(gateTask(2, 110), sim.Now())
	if sm || !acc {
		t.Fatalf("Add(gate at exact age) = %v,%v want false,true (gate closes at >= threshold)", sm, acc)
	}
}

// TestBacklogGateDisabledByZero pins that the default gate (0 = off) leaves
// the sync-match fast path usable even with an old backlog head — the
// byte-neutral default (no gate → today's exact behavior).
func TestBacklogGateDisabledByZero(t *testing.T) {
	sim := gateSim(t)
	q := NewMatchQueue(sim, "q", 0, 0, 1, 0, 0) // backlogGateMs 0 (default)
	q.SetForwardReliable(true)
	sim.Advance(10)
	if _, acc := q.Add(gateTask(1, 10), sim.Now()); !acc {
		t.Fatal("first add rejected")
	}
	q.Park(sim.Now().Add(60000).Int64(), func(Task) {})
	sim.Advance(50000) // head age huge, but gate OFF
	sm, acc := q.Add(gateTask(2, sim.Now().Int64()), sim.Now())
	if !sm || !acc {
		t.Fatalf("Add(gate off) = %v,%v want true,true (0 = gate disabled, sync still fires)", sm, acc)
	}
}

// --- Sync-match offer semantics (MatchingSyncMatchWaitDuration = 200ms,
// temporal/common/dynamicconfig/constants.go:1128-1131) ------------------
//
// The window is a RENDEZVOUS bound, not a hold: a plain (non-forwarded)
// add with no parked poller spools IMMEDIATELY — the real Offer returns
// false for a local add that misses the sync match (matcher.go:144-168) and
// the partition manager persists the task at once (SpoolTask,
// task_queue_partition_manager.go:360-387). Only a FORWARDED DB-backlog
// task meeting a poller-less root waits up to the window
// (matcher.go:172-187 offerOrTimeout) — that path is modeled at matching_offer.go
// offerOrTimeout, NOT in the queue Add. The tests below pin which path
// waits and which does not.

// TestOfferDeadlineSpoolsImmediatelyWithoutPoller pins the plain no-poller
// add: with no parked poller and a live window, Add spools AT ONCE into the
// counted backlog — there is deliberately NO queue-level Offer wait — and
// no deferred spool fires later. BornMs is preserved: the task enters the
// backlog untouched.
func TestOfferDeadlineSpoolsImmediatelyWithoutPoller(t *testing.T) {
	sim := gateSim(t)
	q := NewMatchQueue(sim, "q", 0, 0, 1, 0, 0, 200) // window 200
	q.SetForwardReliable(true)
	sim.Advance(10) // now=10
	sm, acc := q.Add(gateTask(1, 10), sim.Now())
	if sm || !acc {
		t.Fatalf("Add(no poller, window live) = %v,%v want false,true (spool, not sync)", sm, acc)
	}
	if q.Len() != 1 {
		t.Fatalf("backlog len = %d, want 1 (spool is immediate, not deferred to the deadline)", q.Len())
	}
	sim.Advance(199) // now=209: before any would-be deadline
	if q.Len() != 1 {
		t.Fatalf("backlog len at t=209 = %d, want 1 (no waiting task materializes)", q.Len())
	}
	sim.Advance(1) // now=210: past the would-be deadline
	if q.Len() != 1 {
		t.Fatalf("backlog len at t=210 = %d, want 1 (no deferred spool event exists)", q.Len())
	}
	_, spooled, _, _, _, _ := q.Stats()
	if spooled != 1 {
		t.Fatalf("spooled = %d, want 1", spooled)
	}
	// The task enters the backlog untouched (BornMs not re-stamped): the
	// gate's head-age (now − BornMs) sees the ORIGINAL birth.
	tk, ok := q.PopHead()
	if !ok {
		t.Fatal("PopHead = empty, want the spooled offer")
	}
	if tk.ID != 1 || tk.BornMs != 10 {
		t.Fatalf("spooled task = {ID %d, BornMs %d}, want {1, 10} (BornMs preserved at spool)", tk.ID, tk.BornMs)
	}
}

// TestOfferDeadlineSameInstantSpoolsFIFO pins the deterministic order of
// concurrent no-poller offers: N adds at the same instant (no parked poller,
// live window) each spool immediately, and the backlog holds them in
// insertion order.
func TestOfferDeadlineSameInstantSpoolsFIFO(t *testing.T) {
	sim := gateSim(t)
	q := NewMatchQueue(sim, "q", 0, 0, 1, 0, 0, 100) // window 100
	q.SetForwardReliable(true)
	sim.Advance(5) // now=5
	for _, id := range []uint64{7, 8, 9} {
		if sm, acc := q.Add(gateTask(id, 5), sim.Now()); sm || !acc {
			t.Fatalf("Add(%d) = %v,%v want false,true (offer waits)", id, sm, acc)
		}
	}
	sim.Advance(100) // now=105, all three deadlines fire
	if q.Len() != 3 {
		t.Fatalf("backlog len = %d, want 3", q.Len())
	}
	for i, want := range []uint64{7, 8, 9} {
		tk, ok := q.PopHead()
		if !ok || tk.ID != want {
			t.Fatalf("spool order [%d] = %v,%d want ID %d (FIFO insertion order)", i, ok, tk.ID, want)
		}
		if tk.BornMs != 5 {
			t.Fatalf("spool order [%d] BornMs = %d, want 5 (create-time preserved)", i, tk.BornMs)
		}
	}
}

// TestOfferDeadlineSyncIgnoresPollerAge pins the removal of the old
// poller-age check: an already-parked poller is always eligible for
// sync-match, no matter how long it has been parked. Under the old check
// (parked-for <= window) this add would have spooled.
func TestOfferDeadlineSyncIgnoresPollerAge(t *testing.T) {
	sim := gateSim(t)
	q := NewMatchQueue(sim, "q", 0, 0, 1, 0, 0, 50) // window 50
	q.SetForwardReliable(true)
	var got []uint64
	q.Park(sim.Now().Add(60000).Int64(), func(tt Task) { got = append(got, tt.ID) }) // parked at t=0
	sim.Advance(200)                                                                 // poller age 200 >> window 50
	sm, acc := q.Add(gateTask(2, sim.Now().Int64()), sim.Now())
	if !sm || !acc {
		t.Fatalf("Add(parked poller, age past window) = %v,%v want true,true (poller age not consulted)", sm, acc)
	}
	if q.Len() != 0 {
		t.Fatalf("backlog len = %d, want 0 (sync must not spool)", q.Len())
	}
	if len(got) != 1 || got[0] != 2 {
		t.Fatalf("taken = %v, want [2]", got)
	}
}

// TestOfferDeadlineGateClosedSpoolsImmediately pins that a closed
// backlog-age gate (isBacklogNegligible → false, matcher.go:109-116)
// bypasses the Offer wait entirely: the add spools at once even with a live
// window and a parked poller, and no deferred spool fires later.
func TestOfferDeadlineGateClosedSpoolsImmediately(t *testing.T) {
	sim := gateSim(t)
	q := NewMatchQueue(sim, "q", 0, 0, 1, 0, 0, 200) // window 200
	q.SetBacklogGate(100)
	q.SetForwardReliable(true)
	if !q.SpoolOnly(gateTask(1, 0)) {
		t.Fatal("SpoolOnly rejected")
	}
	sim.Advance(150) // now=150; head age 150 >= gate 100 → gate CLOSED
	q.Park(sim.Now().Add(60000).Int64(), func(Task) {})
	sm, acc := q.Add(gateTask(2, sim.Now().Int64()), sim.Now())
	if sm || !acc {
		t.Fatalf("Add(gate closed, window live) = %v,%v want false,true (immediate spool, no wait)", sm, acc)
	}
	if q.Len() != 2 {
		t.Fatalf("backlog len = %d, want 2 (spooled at once, not deferred)", q.Len())
	}
	sim.Advance(500) // past any would-be deadline: no second spool may fire
	if q.Len() != 2 {
		t.Fatalf("backlog len after wait = %d, want 2 (no deferred spool while gate closed)", q.Len())
	}
}

// TestNoCapacityRejectKeepsRejectedZero pins the removal of the invented
// capacity reject: matching is persistence-backed (unbounded), so adds past
// the nominal capacity still spool and the rejected counter stays zero.
func TestNoCapacityRejectKeepsRejectedZero(t *testing.T) {
	sim := gateSim(t)
	q := NewMatchQueue(sim, "q", 1, 0, 1, 0, 0) // nominal capacity 1, window 0
	if _, acc := q.Add(gateTask(1, 0), sim.Now()); !acc {
		t.Fatal("first add rejected")
	}
	sm, acc := q.Add(gateTask(2, 0), sim.Now())
	if sm || !acc {
		t.Fatalf("second add over capacity = %v,%v want false,true (never reject)", sm, acc)
	}
	if q.Len() != 2 {
		t.Fatalf("backlog len = %d, want 2 (unbounded spool)", q.Len())
	}
	_, _, rejected, _, _, _ := q.Stats()
	if rejected != 0 {
		t.Fatalf("rejected = %d, want 0 (counter stays zero)", rejected)
	}
	if !q.SpoolOnly(gateTask(3, 0)) {
		t.Fatal("SpoolOnly rejected (must never reject)")
	}
}

// TestNoExpireAtSpoolKeepsPollExpiry pins the removal of the invented
// add-time expiry: a task that already waited out maxWaitMs still spools at
// add (ExpireAtSpool stays zero); the residency deadline fires only at
// dispatch (ExpireAtPoll — kept).
func TestNoExpireAtSpoolKeepsPollExpiry(t *testing.T) {
	sim := gateSim(t)
	q := NewMatchQueue(sim, "q", 0, 100, 1, 0, 0) // maxWait 100, window 0
	sim.Advance(500)                              // now=500
	sm, acc := q.Add(gateTask(1, 0), sim.Now())   // age 500 > maxWait 100
	if sm || !acc {
		t.Fatalf("Add(aged task) = %v,%v want false,true (spool, no add-time expiry)", sm, acc)
	}
	if q.Len() != 1 {
		t.Fatalf("backlog len = %d, want 1 (aged task still spooled)", q.Len())
	}
	if q.ExpiredCount()[ExpireAtSpool] != 0 {
		t.Fatalf("expired[Spool] = %d, want 0 (slot never increments)", q.ExpiredCount()[ExpireAtSpool])
	}
	q.Park(sim.Now().Add(100000).Int64(), func(Task) {})
	tk, ok := q.TryDispatch(sim.Now())
	if !ok || tk.ID != 1 {
		t.Fatalf("TryDispatch = %v,%d want true,1 (expired task returned)", ok, tk.ID)
	}
	if q.ExpiredCount()[ExpireAtPoll] != 1 {
		t.Fatalf("expired[Poll] = %d, want 1 (deadline fires at dispatch)", q.ExpiredCount()[ExpireAtPoll])
	}
}

// TestSyncConsumeThrottledSpools pins sync-Consume-before-match: the
// sync-match fast path takes a dispatch token first; a throttled offer
// (bucket empty) spools instead of sync-matching, leaving the poller parked.
func TestSyncConsumeThrottledSpools(t *testing.T) {
	sim := gateSim(t)
	q := NewMatchQueue(sim, "q", 0, 0, 1, 1, 1) // 1 token/s, burst 1
	q.SetForwardReliable(true)
	var taken int
	q.Park(sim.Now().Add(100000).Int64(), func(Task) { taken++ })
	sm, acc := q.Add(gateTask(1, 0), sim.Now())
	if !sm || !acc {
		t.Fatalf("first Add = %v,%v want true,true (token available → sync)", sm, acc)
	}
	q.Park(sim.Now().Add(100000).Int64(), func(Task) { taken++ })
	sm, acc = q.Add(gateTask(2, 0), sim.Now()) // same ms: no refill → throttled
	if sm || !acc {
		t.Fatalf("throttled Add = %v,%v want false,true (spool, no sync without token)", sm, acc)
	}
	if q.Len() != 1 {
		t.Fatalf("backlog len = %d, want 1 (throttled offer spooled)", q.Len())
	}
	if q.Parked() != 1 {
		t.Fatalf("parked = %d, want 1 (poller unconsumed by throttled offer)", q.Parked())
	}
	if taken != 1 {
		t.Fatalf("taken = %d, want 1", taken)
	}
}

// TestTokenBucketRecycleCappedAtBurst pins Recycle: one returned token
// re-arms a single Consume, and double-recycle does not overfill past burst.
func TestTokenBucketRecycleCappedAtBurst(t *testing.T) {
	b := NewTokenBucket(1, 1)
	// Recycle-on-full: bucket starts at burst; recycling must not overfill.
	b.Recycle()
	if !b.Consume(0) {
		t.Fatal("initial Consume after Recycle-on-full refused")
	}
	if b.Consume(0) {
		t.Fatal("overfill: second Consume after capped Recycle granted")
	}
	if b.Consume(0) {
		t.Fatal("second Consume granted without refill")
	}
	b.Recycle()
	b.Recycle() // capped: still exactly one token
	if !b.Consume(0) {
		t.Fatal("Consume after Recycle refused")
	}
	if b.Consume(0) {
		t.Fatal("overfill: second Consume after capped Recycle granted")
	}
	u := NewTokenBucket(0, 0) // unlimited: Recycle is a safe no-op
	u.Recycle()
	if !u.Consume(0) {
		t.Fatal("unlimited Consume after Recycle refused")
	}
}
