package tasqueue

import (
	"strings"
	"testing"

	"github.com/lawRathod/queuesim/model/capture"
)

// TestTasqueueCaptureDisabledNil asserts Config.Capture=false leaves the
// Topology recorder nil — the byte-neutral default.
func TestTasqueueCaptureDisabledNil(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 5
	cfg.SimEndMs = 5000
	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	top.Run()
	if top.Capture() != nil {
		t.Fatal("Capture() should be nil when Config.Capture is false")
	}
}

// TestTasqueueCaptureWiring runs a small capture-on workload and asserts
// the recorded transitions form the canonical chain load → queue →
// dispatch → exec and back (retry) or terminal (complete), using only the
// model's own component names.
func TestTasqueueCaptureWiring(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 7
	cfg.BurstCount = 10
	cfg.SimEndMs = 10000
	cfg.Capture = true
	cfg.FailP = 0.5
	cfg.MaxRetries = 1

	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()

	evs := top.Capture().Events()
	if len(evs) == 0 {
		t.Fatal("expected captured events")
	}
	for i := 1; i < len(evs); i++ {
		if evs[i].Seq != evs[i-1].Seq+1 {
			t.Fatalf("seq gap at %d: %d -> %d", i, evs[i-1].Seq, evs[i].Seq)
		}
	}

	g := capture.Aggregate(evs)
	var foundEnqueue, foundDeliver, foundTake, foundTerminal bool
	for _, e := range g.Edges {
		switch {
		case e.Kind == "enqueue" && e.From == "load" && e.To == "queue":
			foundEnqueue = true
		case e.Kind == "deliver" && e.From == "queue" && e.To == "dispatch":
			foundDeliver = true
		case e.Kind == "take" && e.From == "dispatch" && e.To == "exec":
			foundTake = true
		case e.Kind == "complete" && e.From == "exec" && e.To == "terminal":
			foundTerminal = true
		}
		for _, node := range []string{e.From, e.To} {
			switch node {
			case "load", "queue", "dispatch", "exec", "terminal":
			default:
				t.Errorf("capture edge references unexpected component %q", node)
			}
		}
	}
	if !foundEnqueue {
		t.Error("missing load→queue enqueue edge")
	}
	if !foundDeliver {
		t.Error("missing queue→dispatch deliver edge")
	}
	if !foundTake {
		t.Error("missing dispatch→exec take edge")
	}
	if !foundTerminal {
		t.Error("missing exec→terminal complete edge")
	}

	// A run with failures must show retry re-entry: exec → load retry.
	foundRetry := false
	for _, e := range g.Edges {
		if e.Kind == "retry" && e.From == "exec" && e.To == "load" {
			foundRetry = true
		}
	}
	if res.Stats.Retried > 0 && !foundRetry {
		t.Error("missing exec→load retry edge despite Retried > 0")
	}

	// Observational: no capture artifact in the digest.
	if res.Digest == "" {
		t.Fatal("expected a digest")
	}
	for _, m := range res.Metrics.Counters {
		if strings.Contains(m.Name, "capture") {
			t.Errorf("capture leaked into counters: %s", m.Name)
		}
	}
}

// TestTasqueueCaptureDigestUnchanged asserts capture ON does not perturb
// the digest vs capture OFF.
func TestTasqueueCaptureDigestUnchanged(t *testing.T) {
	run := func(captureOn bool) (digest string, n int) {
		cfg := DefaultConfig()
		cfg.Seed = 7
		cfg.BurstCount = 20
		cfg.SimEndMs = 5000
		cfg.Capture = captureOn
		cfg.FailP = 0.3
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		res := top.Run()
		if captureOn {
			n = len(top.Capture().Events())
		}
		return res.Digest, n
	}
	offDigest, _ := run(false)
	onDigest, n := run(true)
	if offDigest == "" {
		t.Fatal("expected non-empty digest")
	}
	if offDigest != onDigest {
		t.Fatalf("capture perturbed digest:\noff=%q\non =%q", offDigest, onDigest)
	}
	if n == 0 {
		t.Fatal("expected captured events with capture on")
	}
}
