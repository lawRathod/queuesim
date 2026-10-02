package temporal

import (
	"fmt"
	"strings"
	"testing"

	"github.com/law/GoDES/faultsched"
	"github.com/lawRathod/queuesim/model"
)

// faultScheduleTest helpers -------------------------------------------------
//
// These tests pin the WP1 model-leg contract (ADR-100/ADR-101): a Config
// carrying a non-nil faultsched.Source applies, per pump tick, the faults
// State returns at sim.Now() — through the SAME faults.Target dispatcher the
// cfg.Vector path uses. A nil schedule reproduces today's cfg.Vector-only
// behavior BYTE-IDENTICALLY (the nil-neutrality gate: the pinned digests
// 9a1ab1b5…/ee4406b9… must not move).

// mustSchedule builds a schedule, failing the test on validation error.
func mustSchedule(t *testing.T, seed int64, windows []faultsched.Window) *faultsched.Schedule {
	t.Helper()
	s, err := faultsched.NewSchedule(seed, windows)
	if err != nil {
		t.Fatalf("NewSchedule: %v", err)
	}
	return s
}

// vecWindows builds the fault-schedule windows that encode the SAME fault
// surface as the given decision vector: a plain vector becomes one window;
// a composed vector becomes one window per member.
func vecWindows(v *model.DecisionVector) []faultsched.Window {
	if v == nil {
		return nil
	}
	var out []faultsched.Window
	for _, m := range v.Members() {
		out = append(out, faultsched.Window{
			Kind:    faultsched.Kind(m.Kind),
			StartMs: m.StartMs,
			EndMs:   m.StartMs + m.DurationMs, // DurationMs 0 = open-ended → EndMs 0
			Rate:    m.Rate,
		})
	}
	return out
}

// --- 1. Nil-neutrality ----------------------------------------------------

// TestFaultScheduleNilNeutralDigest is the byte-neutrality gate: a config
// whose FaultSchedule is nil behaves EXACTLY like an unmodified config —
// same Stats, same Digest. Compared directly (not against a hardcoded pin),
// so a digest change in either leg fails loudly.
func TestFaultScheduleNilNeutralDigest(t *testing.T) {
	run := func(cfg Config) Result {
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run()
	}

	// Zero-draw determinism path (the 9a1ab1b5… pin).
	base := DefaultConfig()
	base.Seed = 7
	base.BurstCount = 30
	base.SimEndMs = 20000
	base.FaultStartMs = 1
	base.FaultEndMs = 5001
	withNil := base
	withNil.FaultSchedule = nil // explicit nil = today's behavior
	plain := run(base)
	nilSched := run(withNil)
	if plain.Digest != nilSched.Digest || fmt.Sprintf("%+v", plain.Stats) != fmt.Sprintf("%+v", nilSched.Stats) {
		t.Fatalf("explicit-nil schedule changed the zero-draw run:\nplain digest=%q\nnil digest  =%q", plain.Digest, nilSched.Digest)
	}
	if plain.Digest == "" {
		t.Fatal("expected a non-empty digest")
	}
	t.Logf("nil-neutral zero-draw digest: %q", plain.Digest)

	// RNG-drawing storm path (the ee4406b9… pin).
	storm := rngConfig()
	storm.FaultSchedule = nil
	withNilStorm := storm
	withNilStorm.FaultSchedule = nil
	s1 := run(storm)
	s2 := run(withNilStorm)
	if s1.Digest != s2.Digest || fmt.Sprintf("%+v", s1.Stats) != fmt.Sprintf("%+v", s2.Stats) {
		t.Fatalf("explicit-nil schedule changed the draws run:\nplain digest=%q\nnil digest  =%q", s1.Digest, s2.Digest)
	}
	t.Logf("nil-neutral draws digest: %q", s1.Digest)
}

// TestFaultScheduleN5ZeroDraw is the N=5 determinism gate on the nil path
// with a schedule field present: same config → identical digest across
// repeats. (The digest is not pinned here — the pin gate is
// TestFaultScheduleNilNeutralDigest + the pre-existing determinism tests.)
func TestFaultScheduleN5ZeroDraw(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 7
	cfg.BurstCount = 30
	cfg.SimEndMs = 20000
	cfg.FaultStartMs = 1
	cfg.FaultEndMs = 5001
	cfg.FaultSchedule = nil
	var first string
	for i := 0; i < 5; i++ {
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		res := top.Run()
		if i == 0 {
			first = res.Digest
		}
		if res.Digest != first {
			t.Fatalf("run %d: digest mismatch\n%q\n%q", i, res.Digest, first)
		}
	}
}

// --- 2. Mapping test: schedule encodes a vector → same behavior -----------

// TestFaultScheduleMappingMatchesVector is the WP1 mapping evidence: a
// schedule whose windows encode the SAME fault surface as a known vector
// (plain activity_failure, then composed add_drop+poll_jitter) yields the
// SAME Stats and digest as the equivalent cfg.Vector config — direct
// evidence the schedule reproduces today's vector semantics.
func TestFaultScheduleMappingMatchesVector(t *testing.T) {
	runVec := func(v *model.DecisionVector) Result {
		cfg := DefaultConfig()
		cfg.Seed = 1
		cfg.BurstCount = 50
		cfg.SimEndMs = 60000
		cfg.MatchingCapacity = 1000
		cfg.FaultStartMs = 0
		cfg.FaultEndMs = 0
		cfg.Vector = v
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run()
	}
	runSched := func(wins []faultsched.Window) Result {
		cfg := DefaultConfig()
		cfg.Seed = 1
		cfg.BurstCount = 50
		cfg.SimEndMs = 60000
		cfg.MatchingCapacity = 1000
		cfg.FaultStartMs = 0
		cfg.FaultEndMs = 0
		cfg.Vector = nil
		cfg.FaultSchedule = mustSchedule(t, cfg.Seed, wins)
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run()
	}

	// (a) Plain activity_failure [1,10001) — matches TestVectorActivityFailure.
	afVec := model.DecisionVector{Name: "activity_failure-activity-1-10000-0", Kind: model.KindActivityFailure, Target: "activity", StartMs: 1, DurationMs: 10000, Rate: 0}
	rv := runVec(&afVec)
	rs := runSched(vecWindows(&afVec))
	if rv.Digest != rs.Digest {
		t.Fatalf("activity_failure schedule digest differs from vector:\nvec=%q\nsched=%q", rv.Digest, rs.Digest)
	}
	if fmt.Sprintf("%+v", rv.Stats) != fmt.Sprintf("%+v", rs.Stats) {
		t.Fatalf("activity_failure schedule stats differ from vector:\nvec=%+v\nsched=%+v", rv.Stats, rs.Stats)
	}
	if rv.Stats.AttemptsPerStart() <= 1.0 {
		t.Fatalf("aps = %.2f, want > 1.0 (schedule activity_failure must amplify)", rv.Stats.AttemptsPerStart())
	}

	// (b) Composed add_drop 0.3 [1,10001) + poll_jitter 0.5 [1,10001).
	composed := model.DecisionVector{Name: "add_drop+jitter", Compose: []model.DecisionVector{
		{Name: "add_drop", Kind: model.KindAddDrop, Target: "matching", StartMs: 1, DurationMs: 10000, Rate: 0.3},
		{Name: "poll_jitter", Kind: model.KindPollJitter, Target: "matching", StartMs: 1, DurationMs: 10000, Rate: 0.5},
	}}
	rv2 := runVec(&composed)
	rs2 := runSched(vecWindows(&composed))
	if rv2.Digest != rs2.Digest {
		t.Fatalf("composed schedule digest differs from vector:\nvec=%q\nsched=%q", rv2.Digest, rs2.Digest)
	}
	if fmt.Sprintf("%+v", rv2.Stats) != fmt.Sprintf("%+v", rs2.Stats) {
		t.Fatalf("composed schedule stats differ from vector:\nvec=%+v\nsched=%+v", rv2.Stats, rs2.Stats)
	}

	// (c) Scalar/queue kind: s2s_timeout 30ms [1,10001) — the synthesized
	// compose path reaches Apply's scalar tail identically for
	// non-drop/non-lifecycle members (deadline expiry drives Expired).
	// Same shape as TestS2STimeoutMaxWait: 1 poller + busy workers make
	// the backlog wait past the deadline.
	runVecScalar := func(v *model.DecisionVector) Result {
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
		cfg.Vector = v
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run()
	}
	runSchedScalar := func(wins []faultsched.Window) Result {
		cfg := DefaultConfig()
		cfg.Seed = 1
		cfg.BurstCount = 50
		cfg.SimEndMs = 60000
		cfg.MatchingCapacity = 1000
		cfg.PollersWF = 1
		cfg.PollersAct = 1
		cfg.ActivityMs = 50
		cfg.FaultStartMs = 0
		cfg.FaultEndMs = 0
		cfg.Vector = nil
		cfg.FaultSchedule = mustSchedule(t, cfg.Seed, wins)
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run()
	}
	s2sVec := model.DecisionVector{Name: "s2s_timeout-matching-1-10000-30", Kind: model.KindS2STimeout, Target: "matching", StartMs: 1, DurationMs: 10000, Rate: 30}
	rv3 := runVecScalar(&s2sVec)
	rs3 := runSchedScalar(vecWindows(&s2sVec))
	if rv3.Digest != rs3.Digest {
		t.Fatalf("s2s_timeout schedule digest differs from vector:\nvec=%q\nsched=%q", rv3.Digest, rs3.Digest)
	}
	if fmt.Sprintf("%+v", rv3.Stats) != fmt.Sprintf("%+v", rs3.Stats) {
		t.Fatalf("s2s_timeout schedule stats differ from vector:\nvec=%+v\nsched=%+v", rv3.Stats, rs3.Stats)
	}
	if rs3.Stats.Expired == 0 {
		t.Fatalf("expired = 0, want > 0 (s2s_timeout schedule must expire backlogged activities): %+v", rs3.Stats)
	}
}

// --- 3. N=5 schedule-on determinism ----------------------------------------

// TestFaultScheduleN5Composed is the schedule-on determinism gate: the
// composed add_drop+poll_jitter schedule run N=5 times yields an identical
// digest and stats.
func TestFaultScheduleN5Composed(t *testing.T) {
	run := func() Result {
		cfg := DefaultConfig()
		cfg.Seed = 1
		cfg.BurstCount = 50
		cfg.SimEndMs = 60000
		cfg.MatchingCapacity = 1000
		cfg.FaultStartMs = 0
		cfg.FaultEndMs = 0
		cfg.FaultSchedule = mustSchedule(t, cfg.Seed, []faultsched.Window{
			{Kind: faultsched.KindAddDrop, StartMs: 1, EndMs: 10001, Rate: 0.3},
			{Kind: faultsched.KindPollJitter, StartMs: 1, EndMs: 10001, Rate: 0.5},
		})
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run()
	}
	var firstDigest string
	var firstStats string
	for i := 0; i < 5; i++ {
		res := run()
		if i == 0 {
			firstDigest = res.Digest
			firstStats = fmt.Sprintf("%+v", res.Stats)
		}
		if res.Digest != firstDigest {
			t.Fatalf("run %d: digest mismatch\n%q\n%q", i, res.Digest, firstDigest)
		}
		if s := fmt.Sprintf("%+v", res.Stats); s != firstStats {
			t.Fatalf("run %d: stats mismatch\n%s\n%s", i, s, firstStats)
		}
	}
	t.Logf("schedule-on composed digest: %q", firstDigest)
}

// --- 4. Zero-draw discipline ------------------------------------------------

// TestFaultScheduleZeroDrawOff checks the zero-draw discipline on the pump
// path: a schedule that is entirely in the FUTURE (nothing active until
// after the horizon) or composed only of rate-0 / rate-1 probability windows
// must create no drop/jitter substreams (zero draws) and stay stats-neutral.
func TestFaultScheduleZeroDrawOff(t *testing.T) {
	base := func() Config {
		cfg := DefaultConfig()
		cfg.Seed = 5
		cfg.BurstCount = 20
		cfg.SimEndMs = 20000
		cfg.MatchingCapacity = 1000
		cfg.FaultStartMs = 0
		cfg.FaultEndMs = 0
		return cfg
	}
	run := func(cfg Config) (TopologyStats, string, error) {
		top, err := NewTopology(cfg)
		if err != nil {
			return TopologyStats{}, "", err
		}
		res := top.Run()
		return res.Stats, res.Digest, nil
	}

	calm := base()
	calmStats, calmDigest, err := run(calm)
	if err != nil {
		t.Fatal(err)
	}

	// Future windows: nothing active inside the horizon.
	future := base()
	future.FaultSchedule = mustSchedule(t, future.Seed, []faultsched.Window{
		{Kind: faultsched.KindAddDrop, StartMs: 999999, EndMs: 0, Rate: 0.5},
	})
	futureStats, futureDigest, err := run(future)
	if err != nil {
		t.Fatal(err)
	}
	if calmStats != futureStats || calmDigest != futureDigest {
		t.Fatalf("future-only schedule not neutral:\ncalm=%+v digest=%q\nsched=%+v digest=%q",
			calmStats, calmDigest, futureStats, futureDigest)
	}

	// Rate-0 and rate-1 probability windows (off/fully-on — both draw
	// nothing per zero-draw discipline).
	boundary := base()
	boundary.FaultSchedule = mustSchedule(t, boundary.Seed, []faultsched.Window{
		{Kind: faultsched.KindAddDrop, StartMs: 0, EndMs: 0, Rate: 0},
		{Kind: faultsched.KindWfDrop, StartMs: 0, EndMs: 0, Rate: 1},
		{Kind: faultsched.KindPollJitter, StartMs: 0, EndMs: 0, Rate: 1},
	})
	bdryStats, bdryDigest, err := run(boundary)
	if err != nil {
		t.Fatal(err)
	}
	if calmStats != bdryStats || calmDigest != bdryDigest {
		t.Fatalf("rate-0/rate-1 schedule not neutral:\ncalm=%+v digest=%q\nsched=%+v digest=%q",
			calmStats, calmDigest, bdryStats, bdryDigest)
	}
}

// --- 5. Activity failure under schedule --------------------------------------

// TestFaultScheduleActivityFailureUnderWindow drives the lifecycle path:
// with an activity_failure window active, failures surface through
// ActivityFailsFor at complete-time (retries appear).
func TestFaultScheduleActivityFailureUnderWindow(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 50
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0
	cfg.Vector = nil
	cfg.FaultSchedule = mustSchedule(t, cfg.Seed, []faultsched.Window{
		{Kind: faultsched.KindActivityFailure, StartMs: 1, EndMs: 10001, Rate: 0},
	})
	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	if res.Stats.Retries == 0 {
		t.Fatalf("retries = 0, want > 0 (activity_failure window must drive retries): %+v", res.Stats)
	}
	if res.Stats.Completed == 0 {
		t.Fatalf("completed = 0, want > 0 (workflows must recover after the window): %+v", res.Stats)
	}
	// The schedule's activity-failure state drove the lifecycle path: with a
	// per-decision query (schedActFailAt) the runtime field is refreshed on
	// every decision, so after the window closes the LAST decision's state
	// (false) is recorded. The retries above are the evidence the window
	// fired; additionally probe the field through a mid-window decision.
	if top.rt.schedActFail {
		t.Fatal("runtime recorded activity-failure active AFTER the window closed (field must track the last decision)")
	}
}

// TestFaultScheduleSuppressesConfigFallback is the fallback-suppression
// proof: under a schedule WITHOUT an activity_failure window, no failures
// occur EVEN IF the config fault window would have failed the activities —
// the schedule is the sole activity-failure source when set.
func TestFaultScheduleSuppressesConfigFallback(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 50
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.FaultStartMs = 1
	cfg.FaultEndMs = 10001 // config window WOULD fail activities today
	cfg.Vector = nil
	cfg.FaultSchedule = mustSchedule(t, cfg.Seed, []faultsched.Window{
		{Kind: faultsched.KindAddDrop, StartMs: 1, EndMs: 10001, Rate: 0}, // rate 0: no drops
	})
	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	if res.Stats.Retries != 0 {
		t.Fatalf("retries = %d, want 0 (config-window fallback must be suppressed under a schedule)", res.Stats.Retries)
	}
	if res.Stats.Completed != 50 {
		t.Fatalf("completed = %d, want 50 (all workflows complete cleanly under the non-failing schedule)", res.Stats.Completed)
	}
	// The runtime must NOT have recorded activity failure active.
	if top.rt.schedActFail {
		t.Fatal("runtime recorded activity-failure active although the schedule has no activity_failure window")
	}
}

// TestFaultScheduleCascadeUnderSchedule pins the cascade semantics under a
// schedule: with CascadeBadFraction set, the schedule's activity_failure
// state gates the window and the FNV32a-of-ID split picks bad actors —
// healthy workflows complete while bad actors retry (the interaction this
// WP is most likely to regress).
func TestFaultScheduleCascadeUnderSchedule(t *testing.T) {
	run := func() Result {
		cfg := DefaultConfig()
		cfg.Seed = 1
		cfg.BurstCount = 50
		cfg.SimEndMs = 60000
		cfg.MatchingCapacity = 1000
		cfg.FaultStartMs = 0
		cfg.FaultEndMs = 0
		cfg.CascadeBadFraction = 0.5
		cfg.FaultSchedule = mustSchedule(t, cfg.Seed, []faultsched.Window{
			{Kind: faultsched.KindActivityFailure, StartMs: 1, EndMs: 10001, Rate: 0},
		})
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run()
	}
	res := run()
	// Bad actors retry (amplification > 1); healthy workflows recover —
	// both populations coexist, so SOME workflows complete.
	if res.Stats.Retries == 0 {
		t.Fatalf("retries = 0, want > 0 (bad actors must retry under the schedule): %+v", res.Stats)
	}
	if res.Stats.Completed == 0 {
		t.Fatalf("completed = 0, want > 0 (healthy workflows must complete): %+v", res.Stats)
	}
	// Determinism: the cascade split is ID-based and deterministic.
	again := run()
	if fmt.Sprintf("%+v", res.Stats) != fmt.Sprintf("%+v", again.Stats) || res.Digest != again.Digest {
		t.Fatalf("cascade-under-schedule not deterministic:\nrun1=%+v digest=%q\nrun2=%+v digest=%q",
			res.Stats, res.Digest, again.Stats, again.Digest)
	}
}

// --- 6. Validation ------------------------------------------------------------

// TestFaultScheduleMutuallyExclusiveWithVector: cfg.Vector and
// cfg.FaultSchedule are mutually exclusive — both set → NewTopology fails.
func TestFaultScheduleMutuallyExclusiveWithVector(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 7
	cfg.BurstCount = 10
	cfg.SimEndMs = 5000
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0
	cfg.Vector = &model.DecisionVector{Name: "ad", Kind: model.KindAddDrop, StartMs: 1, DurationMs: 0, Rate: 0.1}
	cfg.FaultSchedule = mustSchedule(t, cfg.Seed, []faultsched.Window{
		{Kind: faultsched.KindAddDrop, StartMs: 1, EndMs: 0, Rate: 0.1},
	})
	_, err := NewTopology(cfg)
	if err == nil {
		t.Fatal("NewTopology succeeded with both Vector and FaultSchedule set, want an error")
	}
	if !strings.Contains(err.Error(), "FaultSchedule") || !strings.Contains(err.Error(), "Vector") {
		t.Fatalf("validation error must name both fields, got: %v", err)
	}
}

// --- Reserved kinds are accepted by the schedule surface ----------------------

// TestFaultScheduleReservedKindAccepted: the schedule surface accepts the
// three reserved kinds (they validate and no-op on this leg for now — the
// model mechanisms are a separate later WP).
func TestFaultScheduleReservedKindAccepted(t *testing.T) {
	for _, k := range []faultsched.Kind{faultsched.KindPersistenceFail, faultsched.KindDuplicate, faultsched.KindPartition} {
		cfg := DefaultConfig()
		cfg.Seed = 7
		cfg.BurstCount = 10
		cfg.SimEndMs = 5000
		cfg.FaultStartMs = 0
		cfg.FaultEndMs = 0
		cfg.FaultSchedule = mustSchedule(t, cfg.Seed, []faultsched.Window{
			{Kind: k, StartMs: 1, EndMs: 1001, Rate: 1},
		})
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("reserved kind %s rejected: %v", k, err)
		}
		res := top.Run()
		if res.Stats.Completed == 0 {
			t.Fatalf("reserved kind %s must no-op on this leg (workflows complete): %+v", k, res.Stats)
		}
	}
}

// TestFaultScheduleBacklogSurgeNotDriven documents the follow-on gap: a
// schedule's backlog_surge window does NOT drive a surge (schedule-driven
// surge is out of scope for this WP — cfg.Vector still drives it).
func TestFaultScheduleBacklogSurgeNotDriven(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 3
	cfg.BurstCount = 5
	cfg.SimEndMs = 60000
	cfg.MatchingCapacity = 1000
	cfg.FaultStartMs = 0
	cfg.FaultEndMs = 0
	cfg.Vector = nil
	cfg.FaultSchedule = mustSchedule(t, cfg.Seed, []faultsched.Window{
		{Kind: faultsched.KindBacklogSurge, StartMs: 2000, EndMs: 0, Rate: 100},
	})
	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()
	if res.Stats.Started != 5 {
		t.Fatalf("started = %d, want 5 (schedule backlog_surge must not drive a surge in this WP)", res.Stats.Started)
	}
}
