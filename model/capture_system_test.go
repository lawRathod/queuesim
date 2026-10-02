package model

import (
	"strings"
	"testing"

	"github.com/law/GoDES/core"
)

// TestSystemCaptureSeamDisabled asserts EnableCapture=false leaves the
// recorder nil (byte-neutral default) and never registers anything.
func TestSystemCaptureSeamDisabled(t *testing.T) {
	sys, _, _ := demoSystem(t, SystemConfig{Seed: 7, PumpMs: 1, SimEndMs: 50})
	if err := sys.Build(); err != nil {
		t.Fatal(err)
	}
	if sys.Recorder() != nil {
		t.Fatal("expected nil recorder when capture disabled")
	}
	out := sys.Queue("producer", "out")
	out.Add(Task{ID: 1, Attempt: 1})
	res := sys.Run()
	if res.Digest == "" {
		t.Fatal("expected digest")
	}
	// Capture OFF must not perturb the digest vs a system built before the
	// EnableCapture field existed: default false → same behavior.
}

// TestSystemCaptureSeamEnabled asserts EnableCapture=true creates a
// recorder at Build that a model can record into (via tick hook) and read
// back after Run; digest remains non-empty and the recorder is registered
// nowhere (no monitor metrics, no topology node).
func TestSystemCaptureSeamEnabled(t *testing.T) {
	sys, _, _ := demoSystem(t, SystemConfig{Seed: 7, PumpMs: 1, SimEndMs: 50, EnableCapture: true})
	// Register the recording hook BEFORE Build — exactly how a
	// runtime-mediated model records at its chokepoints (the recorder is
	// created inside Build, before the pump runs).
	sys.AddTickHook(func(now core.SimTime) {
		sys.Recorder().Record("hook", "system", "tick", now.Int64())
	})
	if err := sys.Build(); err != nil {
		t.Fatal(err)
	}
	cap := sys.Recorder()
	if cap == nil {
		t.Fatal("expected a recorder when capture enabled")
	}
	out := sys.Queue("producer", "out")
	out.Add(Task{ID: 1, Attempt: 1})
	res := sys.Run()
	if res.Digest == "" {
		t.Fatal("expected digest with capture enabled")
	}
	evs := cap.Events()
	if len(evs) == 0 {
		t.Fatal("expected recorded events")
	}
	// Seq order must be monotonic in tick order.
	for i := 1; i < len(evs); i++ {
		if evs[i].Seq <= evs[i-1].Seq {
			t.Fatalf("seq not monotonic: %+v", evs)
		}
	}
	if evs[0].From != "hook" || evs[0].To != "system" || evs[0].Kind != "tick" {
		t.Errorf("first event fields wrong: %+v", evs[0])
	}
	// Capture output must not appear in the metrics digest: the recorder
	// is observational only.
	for _, g := range res.Metrics.Gauges {
		if strings.Contains(g.Name, "hook") || strings.Contains(g.Name, "capture") {
			t.Errorf("capture artifact leaked into metrics: %s", g.Name)
		}
	}
}

// TestSystemCaptureSeamDigestUnchanged asserts enabling capture does NOT
// perturb the simulation digest: the same model with EnableCapture true vs
// false yields byte-identical digests (capture is observational and never
// mutates sim or monitor state).
func TestSystemCaptureSeamDigestUnchanged(t *testing.T) {
	runDigest := func(enable bool) string {
		sys, _, _ := demoSystem(t, SystemConfig{Seed: 7, PumpMs: 1, SimEndMs: 50, EnableCapture: enable})
		if enable {
			sys.AddTickHook(func(now core.SimTime) {
				sys.Recorder().Record("hook", "system", "tick", now.Int64())
			})
		}
		if err := sys.Build(); err != nil {
			t.Fatal(err)
		}
		out := sys.Queue("producer", "out")
		for i := 0; i < 5; i++ {
			out.Add(Task{ID: uint64(i), Attempt: 1})
		}
		return sys.Run().Digest
	}
	off, on := runDigest(false), runDigest(true)
	if off == "" {
		t.Fatal("expected non-empty digest")
	}
	if off != on {
		t.Fatalf("capture perturbed the digest:\noff=%q\non =%q", off, on)
	}
}
