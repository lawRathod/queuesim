package temporal

import (
	"crypto/sha256"
	"fmt"
	"testing"
)

// S1 — Forwarder caps (WP1 of the Four Grounded Performance Seams, Phase 1;
// ADR-106 / plan 2026-09-07-four-grounded-performance-seams-phase1.md).
// Seam home: matching_offer.go (isForwardingAllowed/offerOrTimeout/
// offerForwarded/onTransfer/onTimer).
//
// Server contract (pinned to the temporal submodule at ffddf6fdc):
//
//   - matching.forwarderMaxOutstandingPolls = 1 — "the max number of
//     inflight polls from the forwarder"
//     temporal/common/dynamicconfig/constants.go:1220-1224
//   - matching.forwarderMaxOutstandingTasks = 1 — "the max number of
//     inflight addTask/queryTask from the forwarder"
//     temporal/common/dynamicconfig/constants.go:1225-1229
//   - the forwarder keeps TWO token pools sized from those settings
//     (addReqToken / pollReqToken buffered channels, one token per inflight
//     call; a caller without a token does NOT forward — the item stays
//     queued where it is, backpressure not drop)
//     service/matching/forwarder.go:36-42,84-91,312-325,352-355
//   - the task pool gates the child→root ForwardTask call
//     (matcher.go:146-157 — token acquired, released after the RPC); the
//     root that cannot forward (isForwardingAllowed false = root partition)
//     blocks a forwarded DB-backlog task trying to match a poller until the
//     offer deadline (matcher.go:159-165 offerOrTimeout, matcher.go:172-187
//     block-until-poller-or-deadline); a forwarded task the root cannot
//     sync-match is NOT spooled at the root — it returns to the child,
//     which persists it (errRemoteSyncMatchFailed,
//     service/matching/task_queue_partition_manager.go:376-378,387).
//   - the poll pool gates ForwardPoll (matcher.go:488-498).
//
// Model seam (honest single-seam approximation, documented on
// matching_offer.go): Config.ForwarderMaxOutstandingTasks > 0 bounds the
// forwarded adds the ROOT holds in its poller rendezvous (armed
// offerOrTimeout waits — the only forwarded items in flight across sim
// events); while the pool is exhausted the forward seam refuses (the add is
// absorbed into the CHILD queue: backpressure, never a drop). 0 = off =
// today's behavior, byte-identical (proven by TestS1NeutralityDigest).
// Config.ForwarderMaxOutstandingPolls has no binding site at the offer seam
// (the model parks its SDK pollers directly on the ROOT and never emits a
// child→root ForwardPoll; the real poll-forward structure is the model's
// pollForward, matching_scale.go:157, off by default) — it is read,
// validated (WP0), and pinned here; its live binding is reported for
// Manager arbitration (file-disjoint ownership).

const (
	// serverForwarderMaxOutstandingPolls is the real server default for
	// matching.forwarderMaxOutstandingPolls (constants.go:1220-1224).
	serverForwarderMaxOutstandingPolls = 1
	// serverForwarderMaxOutstandingTasks is the real server default for
	// matching.forwarderMaxOutstandingTasks (constants.go:1225-1229).
	serverForwarderMaxOutstandingTasks = 1
)

// s1FlaggedTask builds a forwarded DB-backlog activity add (Rescheduled +
// side-carred forwarded flag, the state the MustOffer branch of the forward
// seam consumes) at the current sim time.
func s1FlaggedTask(rt *runtime, id uint64) Task {
	tk := Task{ID: id, Attempt: 1, Class: TaskActivity, BornMs: rt.sim.Now().Int64(), Rescheduled: true}
	matchWaveFor(rt).forwarded[matchFwdFlagOf(tk)] = true
	return tk
}

// s1RT builds a materialized runtime for seam-level S1 drives: the one-time
// inflow start is skipped (tickRT), so NO poller is ever parked — the ROOT
// has no parked poller and the MustOffer branch of the forward seam
// (Rescheduled + forwarded + Parked()==0) is reachable by construction.
func s1RT(t *testing.T, tasksCap int) *runtime {
	t.Helper()
	cfg := DefaultConfig()
	cfg.Seed = 42
	cfg.BurstCount = 1
	cfg.Shards = 1
	cfg.MatchingCapacity = 1000
	cfg.Vector = nil
	cfg.ForwarderMaxOutstandingTasks = tasksCap
	return tickRT(t, cfg)
}

// TestS1ForwarderCapServerDefaultsPinned pins the S1 contract to the source:
// the real server sizes its forwarder token pools at 1/1
// (constants.go:1220-1229 — the WP0 config doc already pins the model's
// ZERO default = off in TestSeamConfigFieldsZeroDefault; this test pins the
// server reference the seam is grounded on, and that the model's off
// position stays zero rather than inheriting the server default — a
// non-zero default would violate the neutrality contract by construction).
func TestS1ForwarderCapServerDefaultsPinned(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.ForwarderMaxOutstandingPolls != 0 {
		t.Fatalf("ForwarderMaxOutstandingPolls = %d, want 0 (model off; server default %d is NOT inherited — zero = off = byte-identical, constants.go:1220-1224)", cfg.ForwarderMaxOutstandingPolls, serverForwarderMaxOutstandingPolls)
	}
	if cfg.ForwarderMaxOutstandingTasks != 0 {
		t.Fatalf("ForwarderMaxOutstandingTasks = %d, want 0 (model off; server default %d is NOT inherited — zero = off = byte-identical, constants.go:1225-1229)", cfg.ForwarderMaxOutstandingTasks, serverForwarderMaxOutstandingTasks)
	}
}

// TestS1ForwarderCapBackpressure is the S1 mechanism contract at the forward
// seam: with Config.ForwarderMaxOutstandingTasks = N, at most N forwarded
// adds may occupy the ROOT's poller rendezvous (armed offerOrTimeout
// waits); a further forwarded add is NOT dropped and does NOT start a
// second concurrent wait — the forward seam refuses it and the add is
// absorbed into the CHILD queue (backpressure). When the occupying wait
// resolves the slot frees and the queued add drains to the root. Cap 2
// admits two concurrent waits before refusing.
func TestS1ForwarderCapBackpressure(t *testing.T) {
	// Cap 1: one forwarded add occupies the seam; a second is queued.
	rt := s1RT(t, 1)
	now := rt.sim.Now()
	t1 := s1FlaggedTask(rt, 1)
	t2 := s1FlaggedTask(rt, 2)

	// Add #1: the ROOT path is open (healthy window, empty child gate, pool
	// free) → MustOffer arm: the forwarded add is now in flight at the ROOT.
	rt.addAct(t1, now)
	if got := s1For(rt).outTasks; got != 1 {
		t.Fatalf("in-flight forwarded tasks after #1 = %d, want 1 (MustOffer wait armed)", got)
	}
	if rt.actQ.Len() != 0 || rt.childQ.Len() != 0 {
		t.Fatalf("after #1: actQ=%d childQ=%d, want 0/0 (the add is in the rendezvous wait, nowhere else)", rt.actQ.Len(), rt.childQ.Len())
	}

	// Add #2: the pool is exhausted (outTasks 1 >= cap 1) → the forward seam
	// refuses → the add is absorbed into the CHILD queue — backpressure,
	// NOT a drop and NOT a second concurrent wait.
	rt.addAct(t2, now)
	if rt.childQ.Len() != 1 {
		t.Fatalf("after #2: childQ=%d, want 1 (cap-exhausted forward is queued in the child, not dropped)", rt.childQ.Len())
	}
	if rt.actQ.Len() != 0 {
		t.Fatalf("after #2: actQ=%d, want 0 (no second add reached the ROOT rendezvous)", rt.actQ.Len())
	}
	if got := s1For(rt).outTasks; got != 1 {
		t.Fatalf("in-flight forwarded tasks after #2 = %d, want 1 (cap 1: one wait only)", got)
	}
	if rt.stats.Dropped != 0 {
		t.Fatalf("dropped = %d, want 0 (the cap never drops — backpressure queues)", rt.stats.Dropped)
	}

	// Resolve #1: the sim fires the MustOffer deadline → offerForwarded
	// releases the slot and re-offers #1 to the ROOT (spool: no poller is
	// parked), so the pool is free again.
	rt.sim.Advance(201)
	if got := s1For(rt).outTasks; got != 0 {
		t.Fatalf("in-flight forwarded tasks after resolution = %d, want 0 (slot released on resolution)", got)
	}
	if rt.actQ.Len() != 1 {
		t.Fatalf("after resolution: actQ=%d, want 1 (#1 re-offered to the ROOT at its deadline)", rt.actQ.Len())
	}

	// The queued #2 is not lost: the child drain (the child's own relief)
	// forwards it to the ROOT once the pool has room — the cap queues, and
	// the queue drains.
	rt.drainChildToRoot(rt.sim.Now())
	if rt.childQ.Len() != 0 {
		t.Fatalf("after drain: childQ=%d, want 0 (the backpressured add left the child)", rt.childQ.Len())
	}
	if rt.actQ.Len() != 2 {
		t.Fatalf("after drain: actQ=%d, want 2 (both adds reached the ROOT — queued, never dropped)", rt.actQ.Len())
	}

	// Cap 2: two forwarded adds may occupy the seam concurrently; the third
	// is queued.
	rt2 := s1RT(t, 2)
	now2 := rt2.sim.Now()
	a := s1FlaggedTask(rt2, 1)
	b := s1FlaggedTask(rt2, 2)
	c := s1FlaggedTask(rt2, 3)
	rt2.addAct(a, now2)
	rt2.addAct(b, now2)
	if got := s1For(rt2).outTasks; got != 2 {
		t.Fatalf("cap 2: in-flight after two waits = %d, want 2 (both armed concurrently)", got)
	}
	rt2.addAct(c, now2)
	if rt2.childQ.Len() != 1 {
		t.Fatalf("cap 2: childQ after third add = %d, want 1 (only the third is queued)", rt2.childQ.Len())
	}
	if got := s1For(rt2).outTasks; got != 2 {
		t.Fatalf("cap 2: in-flight after third add = %d, want 2 (unchanged — the refusal never exceeds the cap)", got)
	}
	rt2.sim.Advance(201)
	if got := s1For(rt2).outTasks; got != 0 {
		t.Fatalf("cap 2: in-flight after both resolutions = %d, want 0", got)
	}
}

// TestS1CapZeroOff pins the zero = off neutrality contract at the seam
// itself: when Config.ForwarderMaxOutstandingTasks = 0 the cap gate is
// bypassed entirely — even a stale (corrupted) in-flight count is never
// consulted and never modified, so a forwarded add still takes the ROOT
// path and arms its MustOffer wait exactly as before the seam existed.
func TestS1CapZeroOff(t *testing.T) {
	rt := s1RT(t, 0) // cap 0 = off
	// Simulate a stale occupancy as if a previous lifecycle had counted
	// waits: the off path must neither consult nor touch it.
	s1For(rt).outTasks = 7
	tk := s1FlaggedTask(rt, 1)
	rt.addAct(tk, rt.sim.Now())
	if rt.childQ.Len() != 0 {
		t.Fatalf("cap 0: childQ=%d, want 0 (the cap gate is bypassed — the add took the ROOT path)", rt.childQ.Len())
	}
	if rt.actQ.Len() != 0 {
		t.Fatalf("cap 0: actQ=%d, want 0 (the add is in its MustOffer wait, not spooled)", rt.actQ.Len())
	}
	if got := s1For(rt).outTasks; got != 7 {
		t.Fatalf("cap 0: stale in-flight count = %d, want 7 untouched (off never consults or mutates the pool)", got)
	}
	// And the wait resolves normally (the release is likewise skipped at
	// cap 0 — the count stays exactly as it was).
	rt.sim.Advance(201)
	if got := s1For(rt).outTasks; got != 7 {
		t.Fatalf("cap 0: stale in-flight count after resolution = %d, want 7 (release skipped at cap 0)", got)
	}
	if rt.actQ.Len() != 1 {
		t.Fatalf("cap 0: actQ after resolution = %d, want 1 (the add re-offered normally)", rt.actQ.Len())
	}
}

// runDigest runs cfg and returns (digest, stats string, end ms).
func runDigest(t *testing.T, cfg Config) (string, string, int64) {
	t.Helper()
	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	return res.Digest, fmt.Sprintf("%+v", res.Stats), res.SimEndMs
}

// TestS1CapOnDormantSeamByteIdentical pins the honest single-seam
// approximation end to end: in every REACHABLE full run today the MustOffer
// branch of the forward seam never arms (verified: zero act_mustoffer_wait
// across healthy-window, dead-window and small-capacity storm cells — the
// flag-carrying child drain still spools straight to the root,
// partition.go offerChildToRoot, pending the documented one-line
// offerOrTimeout threading), so the in-flight pool stays empty and the cap
// — tasks or polls, alone or together — leaves the trace byte-identical to
// caps off. This is the neutrality guard for the WP1 additive code; the
// mechanism itself is proven live in TestS1ForwarderCapBackpressure.
func TestS1CapOnDormantSeamByteIdentical(t *testing.T) {
	base := func() Config {
		cfg := DefaultConfig()
		cfg.Seed = 42
		cfg.BurstCount = 60
		cfg.SimEndMs = 20000
		cfg.FaultStartMs = 1
		cfg.FaultEndMs = 11000
		cfg.DispatchRate = 39 // calibrated dispatch ceiling (c2 cell)
		cfg.DispatchBurst = 1
		cfg.MatchingCapacity = 1000
		cfg.ActivityMs = 0
		return cfg
	}
	deadWin := func(c *Config) { c.SyncMatchWindowMs = 0 } // dead-forwarding window → child-absorb cell
	for name, mutate := range map[string]func(*Config){
		"healthy": nil,
		"deadwin": deadWin,
	} {
		caps := map[string]func(*Config){
			"off":     func(*Config) {},
			"tasks-1": func(c *Config) { c.ForwarderMaxOutstandingTasks = 1 },
			"polls-1": func(c *Config) { c.ForwarderMaxOutstandingPolls = 1 },
			"both-1":  func(c *Config) { c.ForwarderMaxOutstandingTasks = 1; c.ForwarderMaxOutstandingPolls = 1 },
			"server-1-1": func(c *Config) {
				c.ForwarderMaxOutstandingTasks = serverForwarderMaxOutstandingTasks
				c.ForwarderMaxOutstandingPolls = serverForwarderMaxOutstandingPolls
			},
		}
		cfg := base()
		if mutate != nil {
			mutate(&cfg)
		}
		var wantDigest, wantStats string
		var wantEnd int64
		for i := 0; i < 2; i++ { // repeat: determinism of the identity
			d, s, e := runDigest(t, cfg)
			if i == 0 {
				wantDigest, wantStats, wantEnd = d, s, e
			} else if d != wantDigest || s != wantStats || e != wantEnd {
				t.Fatalf("%s/off: run %d not deterministic: digest %q vs %q", name, i, d, wantDigest)
			}
		}
		for capName, apply := range caps {
			c := cfg
			apply(&c)
			d, s, e := runDigest(t, c)
			if d != wantDigest || s != wantStats || e != wantEnd {
				t.Fatalf("%s/%s perturbed the run (caps must be inert while the MustOffer seam cannot arm):\n  digest %q\n  want    %q", name, capName, d, wantDigest)
			}
		}
	}
}

// TestS1NeutralityDigest is the WP1 neutrality gate: with both forwarder
// caps at their zero (off) default the canonical N5 digests stay
// byte-identical to the WP0 pins recorded in seams_test.go — the zero-draw
// path sha256 a72689e9d1… and the draws path sha256 c7b0d97d14… (re-captured
// at the 2026-09-09 traceability closeout, commit 8c45016). Any WP that
// wires a seam must not change behavior at zero.
func TestS1NeutralityDigest(t *testing.T) {
	zero := seamZeroConfig() // every seam field EXPLICITLY zero (seams_test.go)
	runZeroDraw := func() string {
		cfg := zero
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
		t.Fatalf("S1 zero-draw digest moved with caps at zero:\nsha256 = %s\npin     = %s", got, pinZeroDraw)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256([]byte(runDraws()))); got != pinDraws {
		t.Fatalf("S1 draws digest moved with caps at zero:\nsha256 = %s\npin     = %s", got, pinDraws)
	}
}
