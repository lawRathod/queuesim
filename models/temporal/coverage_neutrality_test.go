package temporal

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/law/GoDES/faultsched"
)

// This file is the ADR-112 model-leg neutrality gate: the OPTIONAL
// ScheduleDecisionLog recorder must be purely observational — setting it
// leaves the trace and digest byte-identical. It pins the two canonical
// digests (zero-draw a72689e9d1…, draws c7b0d97d14…) with the recorder SET
// as well as nil, and asserts the two arms agree. It also exercises a
// schedule-driven config (where the recorder is actually read) and asserts
// the digest/stats are unmoved while the recorder observed queries.

const (
	pinCoverageZeroDraw = "a72689e91d1c1ca6feb5b79c30ed2aed52df1f1e88bfc446a1dcb59bdf3ea87e"
	pinCoverageDraws    = "c7b0d97d145079f610ce447adb1f2afa2f580031ae7a2f3328f43684d860a5bf"
)

// TestScheduleDecisionLogZeroDefault pins the neutral default: the recorder
// is nil (off) in DefaultConfig.
func TestScheduleDecisionLogZeroDefault(t *testing.T) {
	if got := DefaultConfig().ScheduleDecisionLog; got != nil {
		t.Fatalf("DefaultConfig().ScheduleDecisionLog = %v, want nil (off by default)", got)
	}
}

// TestScheduleDecisionLogNeutralPinned pins the byte-neutrality of the
// recorder on the two canonical digest scenarios: with the recorder SET the
// digest equals the recorded pin AND the nil-arm digest, for N=5.
func TestScheduleDecisionLogNeutralPinned(t *testing.T) {
	// Zero-draw determinism path (TestDeterminismN5 scenario).
	runZeroDraw := func(cov *faultsched.Coverage) string {
		cfg := seamZeroConfig()
		cfg.Seed = 7
		cfg.BurstCount = 30
		cfg.SimEndMs = 20000
		cfg.FaultStartMs = 1
		cfg.FaultEndMs = 5001
		cfg.ScheduleDecisionLog = cov
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run().Digest
	}
	// RNG-drawing storm path (rngConfig scenario).
	runDraws := func(cov *faultsched.Coverage) string {
		cfg := rngConfig()
		cfg.ForwarderMaxOutstandingPolls = 0
		cfg.ForwarderMaxOutstandingTasks = 0
		cfg.AppendThreshold = 0
		cfg.PollerScaleWaitMs = 0
		cfg.PollerDecisionsPerSec = 0
		cfg.PersistenceMaxQPS = 0
		cfg.PersistenceQPSBurstRatio = 0
		cfg.ScheduleDecisionLog = cov
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run().Digest
	}

	for i := 0; i < 5; i++ {
		zeroOff := runZeroDraw(nil)
		zeroOn := runZeroDraw(faultsched.NewCoverage(100, 20000))
		if zeroOn != zeroOff {
			t.Fatalf("zero-draw digest moved with ScheduleDecisionLog set:\non =%s\noff=%s", zeroOn, zeroOff)
		}
		if got := fmt.Sprintf("%x", sha256.Sum256([]byte(zeroOn))); got != pinCoverageZeroDraw {
			t.Fatalf("zero-draw digest moved with ScheduleDecisionLog set:\nsha256 = %s\npin     = %s", got, pinCoverageZeroDraw)
		}

		drawsOff := runDraws(nil)
		drawsOn := runDraws(faultsched.NewCoverage(100, 20000))
		if drawsOn != drawsOff {
			t.Fatalf("draws digest moved with ScheduleDecisionLog set:\non =%s\noff=%s", drawsOn, drawsOff)
		}
		if got := fmt.Sprintf("%x", sha256.Sum256([]byte(drawsOn))); got != pinCoverageDraws {
			t.Fatalf("draws digest moved with ScheduleDecisionLog set:\nsha256 = %s\npin     = %s", got, pinCoverageDraws)
		}
	}
}

// TestScheduleDecisionLogNeutralScheduleOn exercises the recorder where it is
// actually read — a non-nil FaultSchedule — and pins byte-neutrality plus
// the non-vacuous record (queries were observed).
func TestScheduleDecisionLogNeutralScheduleOn(t *testing.T) {
	run := func(cov *faultsched.Coverage) Result {
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
		cfg.ScheduleDecisionLog = cov
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run()
	}

	cov := faultsched.NewCoverage(100, 60000)
	on := run(cov)
	off := run(nil)
	if on.Digest != off.Digest {
		t.Fatalf("schedule-on digest moved with ScheduleDecisionLog set:\non =%s\noff=%s", on.Digest, off.Digest)
	}
	if fmt.Sprintf("%+v", on.Stats) != fmt.Sprintf("%+v", off.Stats) {
		t.Fatalf("schedule-on stats moved with ScheduleDecisionLog set:\non =%+v\noff=%+v", on.Stats, off.Stats)
	}
	if cov.Queries() == 0 {
		t.Fatal("ScheduleDecisionLog recorded no queries under a non-nil FaultSchedule — the instrument is not wired")
	}
	if len(cov.QueriedQuant()) == 0 {
		t.Fatal("ScheduleDecisionLog recorded no quanta under a non-nil FaultSchedule")
	}
	if got := cov.ActiveQuant(faultsched.KindAddDrop); len(got) == 0 {
		t.Fatal("ScheduleDecisionLog recorded no add_drop-active quanta under the add_drop schedule")
	}
	t.Logf("schedule-on neutral digest %s; recorder observed %d queries over %d quanta (%d add_drop-active)",
		on.Digest, cov.Queries(), len(cov.QueriedQuant()), len(cov.ActiveQuant(faultsched.KindAddDrop)))
}
