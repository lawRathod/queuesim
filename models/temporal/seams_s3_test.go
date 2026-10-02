package temporal

import (
	"crypto/sha256"
	"fmt"
	"testing"
)

// S3 seam (WP3, ADR-106 / plan
// 2026-09-07-four-grounded-performance-seams-phase1.md): server-side
// poller-scaling cap on scaleElastic — cfg.PollerScaleWaitMs (wait gate:
// decisions scaled at most every WaitMs) + cfg.PollerDecisionsPerSec
// (decisions/s token bucket). 0 = off = today's behavior. WP0 already
// added the fields + validation (seams_test.go); WP3 wires the behavior in
// matching_scale.go.
//
// Source pins (temporal submodule @ ffddf6fdc):
//
//   - MatchingPollerScalingBacklogAgeScaleUp   = 200ms
//     common/dynamicconfig/constants.go:1354-1357
//   - MatchingPollerScalingWaitTime            = 1s
//     common/dynamicconfig/constants.go:1360-1363
//   - MatchingPollerScalingDecisionsPerSecond  = 10/s
//     common/dynamicconfig/constants.go:1366-1371
//   - makePollerScalingDecisionImpl (the decision mechanics the seam
//     caps: idle ≥ WaitTime → scale-down; backlog age > minimum backlog →
//     +1; decision cadence rate-limited per physical queue)
//     service/matching/physical_task_queue_manager.go:851-898
//
// Divergence (recorded, never silently fixed — ADR-102): the real server
// issues the suggestion only on a MATCHED task and the SDK executes it only
// if its ingestion-growth gate passes (sdk/internal_worker_base.go:733-783,
// 100ms report). Neither is modeled; the model may relieve faster than the
// real server and that divergence is a first-class C7 finding.
//
// The model seam does NOT carry server defaults: the fields stay 0 = off
// (the neutrality contract). A C7 sweep cell opts into the seam by setting
// the server-default values itself (wait 1000ms, decisions 10/s) — pinned
// below as references, mirroring how contract_test.go pins server values.

// S3 server-reference constants (ms / decisions-s) — the pins above.
const (
	serverPollerScaleWaitMs          = 1000 // matching.pollerScalingWaitTime
	serverPollerScaleDecisionsPerSec = 10   // matching.pollerScalingDecisionsPerSecond
)

// s3RT builds a scaling-enabled runtime (PollerScaleMax above the fixed
// fleet) with bare queues, ready for manual scaleElastic drives at caller
// chosen sim times — the TestElasticScaleThresholdMatchesSubmodule pattern.
func s3RT(t *testing.T, cfg Config) *runtime {
	t.Helper()
	cfg.PollerScaleMax = 8 // enable elastic scaling
	if cfg.BurstCount == 0 {
		cfg.BurstCount = 1
	}
	if cfg.PollersWF == 0 {
		cfg.PollersWF = 1
	}
	if cfg.PollersAct == 0 {
		cfg.PollersAct = 1
	}
	rt, err := newRuntime(cfg)
	if err != nil {
		t.Fatalf("newRuntime: %v", err)
	}
	sim := newSim(t)
	rt.sim = sim
	rt.wfQ = matcherQueue(sim, "matching-wf")
	rt.actQ = matcherQueue(sim, "matching-act")
	return rt
}

// s3Backlog puts one task on the act queue so scaleElastic sees a
// non-empty backlog (the queue is never drained in manual drives).
func s3Backlog(rt *runtime) {
	rt.actQ.Add(mkTask(1), rt.sim.Now())
}

// TestS3ServerDefaultsPinned pins the S3 source references: the three
// poller-scaling server defaults (constants.go:1354-1371) and the model's
// zero-default seam fields. The model does NOT adopt the server defaults —
// both S3 fields stay 0 = off (neutrality), and a sweep cell opts in with
// the reference values below; PollerScaleUpBacklogMs (the third knob, the
// 200ms minimum backlog age) already normalizes to the server default when
// scaling is enabled (contract_test.go:226-229).
func TestS3ServerDefaultsPinned(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.PollerScaleWaitMs != 0 {
		t.Fatalf("PollerScaleWaitMs = %d, want 0 (zero default = seam off; server reference %dms at constants.go:1360-1363)", cfg.PollerScaleWaitMs, serverPollerScaleWaitMs)
	}
	if cfg.PollerDecisionsPerSec != 0 {
		t.Fatalf("PollerDecisionsPerSec = %v, want 0 (zero default = seam off; server reference %d/s at constants.go:1366-1371)", cfg.PollerDecisionsPerSec, serverPollerScaleDecisionsPerSec)
	}
	// A runtime built from DefaultConfig keeps the seam off (no hidden
	// normalization) — the byte-neutral off path.
	rt, err := newRuntime(Config{BurstCount: 1, PollersWF: 1, PollersAct: 1, PollerScaleMax: 2})
	if err != nil {
		t.Fatal(err)
	}
	if rt.cfg.PollerScaleWaitMs != 0 || rt.cfg.PollerDecisionsPerSec != 0 {
		t.Fatalf("newRuntime normalized the seam on: wait %d decisions %v (must stay 0 = off)", rt.cfg.PollerScaleWaitMs, rt.cfg.PollerDecisionsPerSec)
	}
}

// TestS3WaitGateAndDecisionsCap is the S3 behavior pin (server default wait
// 1s / decisions 10/s, constants.go:1360-1371): the wait gate allows at
// most ONE scale decision per PollerScaleWaitMs, and the decisions/s token
// bucket caps decisions below the natural cadence. The natural (uncapped)
// model cadence is +1 per PollerScaleUpBacklogMs (200ms) re-arm past the
// backlog age — 5/s — so the server-default wait (1s) dominates and the
// server-default decisions cap (10/s > 5/s) is a non-binding ceiling.
func TestS3WaitGateAndDecisionsCap(t *testing.T) {
	// Scenario A — wait gate at the server default (1000ms), decisions off:
	// the first decision fires at the natural 200ms backlog age (no prior
	// decision to wait on — the gate bounds the interval BETWEEN
	// decisions), every later decision is spaced ≥ 1000ms.
	t.Run("wait-gate-server-default", func(t *testing.T) {
		rt := s3RT(t, Config{PollerScaleWaitMs: serverPollerScaleWaitMs})
		if rt.pollerTargetAct != 1 {
			t.Fatalf("initial target = %d, want 1", rt.pollerTargetAct)
		}
		s3Backlog(rt)
		rt.scaleElastic(1)    // backlog onset (age 0): no decision
		rt.scaleElastic(201)  // age 200ms → first decision
		rt.scaleElastic(500)  // 299ms since the decision → wait gate holds
		rt.scaleElastic(1200) // 999ms since → still held
		if rt.pollerTargetAct != 2 {
			t.Fatalf("target = %d, want 2 (wait gate must hold at 999/1000ms)", rt.pollerTargetAct)
		}
		rt.scaleElastic(1201) // 1000ms since → next decision
		if rt.pollerTargetAct != 3 {
			t.Fatalf("target = %d, want 3 (wait gate re-opens at exactly WaitMs)", rt.pollerTargetAct)
		}
		rt.scaleElastic(2201) // another 1000ms → cadence 1 decision/s
		if rt.pollerTargetAct != 4 {
			t.Fatalf("target = %d, want 4 (steady wait-gate cadence = 1 per %dms)", rt.pollerTargetAct, serverPollerScaleWaitMs)
		}
	})

	// Scenario B — decisions/s cap BELOW the natural cadence (2/s, one token
	// per 500ms): the bucket binds and spaces decisions 500ms apart.
	t.Run("decisions-cap-binds", func(t *testing.T) {
		rt := s3RT(t, Config{PollerDecisionsPerSec: 2})
		s3Backlog(rt)
		rt.scaleElastic(1)   // onset
		rt.scaleElastic(201) // first decision (initial token): natural age trigger
		if rt.pollerTargetAct != 2 {
			t.Fatalf("target = %d, want 2 (first decision at the 200ms backlog age)", rt.pollerTargetAct)
		}
		rt.scaleElastic(401) // age 200ms again (natural trigger) but bucket at 0.4 → cap holds
		rt.scaleElastic(501) // bucket at 0.6 → still held
		if rt.pollerTargetAct != 2 {
			t.Fatalf("target = %d, want 2 (2/s cap must bind below the 200ms natural cadence)", rt.pollerTargetAct)
		}
		rt.scaleElastic(701) // bucket refilled (500ms × 2/s = 1 token) → decision
		if rt.pollerTargetAct != 3 {
			t.Fatalf("target = %d, want 3 (bucket grants at 1 token / 500ms)", rt.pollerTargetAct)
		}
		rt.scaleElastic(1101) // 400ms since the last decision → held
		if rt.pollerTargetAct != 3 {
			t.Fatalf("target = %d, want 3 (still inside the 500ms refill window)", rt.pollerTargetAct)
		}
		rt.scaleElastic(1201) // 500ms → decision
		if rt.pollerTargetAct != 4 {
			t.Fatalf("target = %d, want 4 (steady 2/s decision cadence)", rt.pollerTargetAct)
		}
	})

	// Scenario C — decisions/s cap ABOVE the natural cadence (10/s = 1 token
	// per 100ms, the server default): a non-binding ceiling. The natural
	// 200ms cadence (5/s) passes through unchanged.
	t.Run("decisions-cap-nonbinding", func(t *testing.T) {
		rt := s3RT(t, Config{PollerDecisionsPerSec: serverPollerScaleDecisionsPerSec})
		s3Backlog(rt)
		rt.scaleElastic(1)
		rt.scaleElastic(201) // → 2
		rt.scaleElastic(401) // → 3
		rt.scaleElastic(601) // → 4
		rt.scaleElastic(801) // → 5
		if rt.pollerTargetAct != 5 {
			t.Fatalf("target = %d, want 5 (10/s cap is above the 5/s natural cadence and must not bind)", rt.pollerTargetAct)
		}
	})
}

// TestS3ZeroOff pins the zero default = today's exact behavior: with both
// S3 fields at 0, scaleElastic keeps the pre-seam cadence (+1 per 200ms
// backlog-age re-arm — no wait gate, no decisions cap) and the seam leaves
// ZERO mutable state behind (the gate registry is never created on the off
// path).
func TestS3ZeroOff(t *testing.T) {
	before := len(s3Gates)
	rt := s3RT(t, Config{})
	s3Backlog(rt)
	rt.scaleElastic(1)
	rt.scaleElastic(201) // age 200ms → +1 (no wait gate)
	if rt.pollerTargetAct != 2 {
		t.Fatalf("target = %d, want 2 (first decision at the 200ms backlog age)", rt.pollerTargetAct)
	}
	rt.scaleElastic(401) // +1 per 200ms re-arm — the uncapped cadence
	rt.scaleElastic(601)
	rt.scaleElastic(801)
	if rt.pollerTargetAct != 5 {
		t.Fatalf("target = %d, want 5 (zero-off cadence = +1 per %dms, the pre-seam behavior)", rt.pollerTargetAct, serverPollerScaleUpBacklogMs)
	}
	if got := len(s3Gates); got != before {
		t.Fatalf("s3Gates grew from %d to %d on the zero-off path (seam must create no state at zero)", before, got)
	}
}

// TestS3ElasticRunDeterminism pins the seam-LIVE determinism and effect:
// an elastic run whose act backlog persists past the 200ms scale-up age
// (busy 2-poller fleet at 1s activity service) is bit-identical across N=3
// with the seam off AND with the server-default seam on (wait 1000ms,
// 10 decisions/s), and the seam-on digest DIFFERS from seam-off — the wait
// gate measurably throttles relief in a full run, so the test is never
// vacuous. (Stats equality is the determinism claim; the digest difference
// is the effect claim.)
func TestS3ElasticRunDeterminism(t *testing.T) {
	mk := func(on bool) Config {
		cfg := DefaultConfig()
		cfg.Seed = 7
		cfg.BurstCount = 30
		cfg.SimEndMs = 60000
		cfg.FaultStartMs = 0
		cfg.FaultEndMs = 0
		cfg.MatchingCapacity = 1000
		cfg.PollersWF = 4
		cfg.PollersAct = 2 // starved act fleet → persistent backlog
		cfg.ActivityMs = 1000
		cfg.PollerScaleMax = 8 // enable elastic scaling
		if on {
			cfg.PollerScaleWaitMs = serverPollerScaleWaitMs
			cfg.PollerDecisionsPerSec = serverPollerScaleDecisionsPerSec
		}
		return cfg
	}
	firstDigest := map[bool]string{}
	firstStats := map[bool]string{}
	for _, on := range []bool{false, true} {
		for i := 0; i < 3; i++ {
			res := runTopo(t, mk(on)).Result
			if i == 0 {
				firstDigest[on] = res.Digest
				firstStats[on] = fmt.Sprintf("%+v", res.Stats)
				continue
			}
			if res.Digest != firstDigest[on] {
				t.Fatalf("seam %v run %d: digest mismatch\n%q\n%q", on, i, res.Digest, firstDigest[on])
			}
			if s := fmt.Sprintf("%+v", res.Stats); s != firstStats[on] {
				t.Fatalf("seam %v run %d: stats mismatch\n%s\n%s", on, i, s, firstStats[on])
			}
		}
	}
	if firstDigest[true] == firstDigest[false] {
		t.Fatal("seam-on digest == seam-off digest: the wait gate must throttle a live elastic run")
	}
	t.Logf("seam-off digest %q / seam-on digest %q differ (elastic relief throttled)", firstDigest[false], firstDigest[true])
}

// TestS3NeutralityDigest is the WP3 byte-identity gate (the neutrality
// contract, ADR-106): with both S3 fields at their zero default the two
// canonical N5 digests stay byte-identical to the recorded pins — zero-draw
// sha256 a72689e9d1… and draws sha256 c7b0d97d14… (the WP0 pins from
// seams_test.go, re-pinned here so WP3's own gate runs the digest before
// and after its seam wiring). A WP3 change that moves a digest at zero is a
// neutrality violation by construction.
func TestS3NeutralityDigest(t *testing.T) {
	runZeroDraw := func() string {
		cfg := DefaultConfig()
		cfg.Seed = 7
		cfg.BurstCount = 30
		cfg.SimEndMs = 20000
		cfg.FaultStartMs = 1
		cfg.FaultEndMs = 5001
		cfg.PollerScaleWaitMs = 0
		cfg.PollerDecisionsPerSec = 0
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run().Digest
	}
	runDraws := func() string {
		cfg := rngConfig()
		cfg.PollerScaleWaitMs = 0
		cfg.PollerDecisionsPerSec = 0
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run().Digest
	}

	const pinZeroDraw = "a72689e91d1c1ca6feb5b79c30ed2aed52df1f1e88bfc446a1dcb59bdf3ea87e"
	const pinDraws = "c7b0d97d145079f610ce447adb1f2afa2f580031ae7a2f3328f43684d860a5bf"

	if got := fmt.Sprintf("%x", sha256.Sum256([]byte(runZeroDraw()))); got != pinZeroDraw {
		t.Fatalf("zero-draw digest moved with S3 at zero:\nsha256 = %s\npin     = %s", got, pinZeroDraw)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256([]byte(runDraws()))); got != pinDraws {
		t.Fatalf("draws digest moved with S3 at zero:\nsha256 = %s\npin     = %s", got, pinDraws)
	}
}
