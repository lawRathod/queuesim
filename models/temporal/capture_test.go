package temporal

import (
	"strings"
	"testing"

	"github.com/lawRathod/queuesim/model/capture"
)

// TestTemporalCaptureDisabledNil asserts Config.Capture=false leaves the
// Topology recorder nil — the byte-neutral default.
func TestTemporalCaptureDisabledNil(t *testing.T) {
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

// TestTemporalCaptureWiring exercises a 1-workflow calm run with capture on
// and asserts the recorded transitions form the canonical chain:
// load → history → matching → exec, with the wf-complete re-entering
// matching. It also checks the digest is unchanged vs capture off and the
// recorder is observational (no capture artifact in metrics).
func TestTemporalCaptureWiring(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 1
	cfg.BurstCount = 1
	cfg.SimEndMs = 20000
	cfg.Capture = true

	top, err := NewTopology(cfg)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	res := top.Run()

	evs := top.Capture().Events()
	if len(evs) == 0 {
		t.Fatal("expected captured events")
	}
	// Seq must be exactly the sim order (monotonic).
	for i := 1; i < len(evs); i++ {
		if evs[i].Seq != evs[i-1].Seq+1 {
			t.Fatalf("seq gap at %d: %d -> %d", i, evs[i-1].Seq, evs[i].Seq)
		}
	}

	// Aggregate and require the canonical calm lifecycle: a workflow_start
	// from load into history, at least one history→matching wf add, and a
	// matching→exec dispatch. The map must show ONLY the model's own
	// component names.
	g := capture.Aggregate(evs)
	var foundStart, foundWfAdd, foundDispatch, foundComplete bool
	for _, e := range g.Edges {
		switch {
		case e.From == "load" && e.Kind == "workflow_start":
			foundStart = true
		case e.Kind == "wf_add":
			foundWfAdd = true
		case e.Kind == "wf_dispatch" || e.Kind == "act_dispatch":
			foundDispatch = true
		case e.Kind == "wf_complete":
			foundComplete = true
		}
		for _, node := range []string{e.From, e.To} {
			switch node {
			case "load", "history", "matching", "exec", "terminal":
			default:
				t.Errorf("capture edge references unexpected component %q", node)
			}
		}
	}
	if !foundStart {
		t.Error("missing load→history workflow_start edge")
	}
	if !foundWfAdd {
		t.Error("missing history→matching wf_add edge")
	}
	if !foundDispatch {
		t.Error("missing matching→exec dispatch edge")
	}
	if !foundComplete {
		t.Error("missing wf_complete terminal edge")
	}

	// 1 workflow calm: exactly 1 workflow_start.
	if count := countKind(g, "workflow_start"); count != 1 {
		t.Errorf("workflow_start count = %d, want 1", count)
	}
	// The workflow completed.
	if res.Stats.Completed != 1 {
		t.Errorf("Completed = %d, want 1", res.Stats.Completed)
	}

	// Observational: no capture artifact in the metrics digest.
	if res.Digest == "" {
		t.Fatal("expected a digest")
	}
	for _, m := range res.Metrics.Counters {
		if strings.Contains(m.Name, "capture") {
			t.Errorf("capture leaked into counters: %s", m.Name)
		}
	}
	for _, g := range res.Metrics.Gauges {
		if strings.Contains(g.Name, "capture") {
			t.Errorf("capture leaked into gauges: %s", g.Name)
		}
	}
}

// TestTemporalCaptureDigestUnchanged asserts capture ON does not perturb
// the digest vs capture OFF (byte-neutral observation).
func TestTemporalCaptureDigestUnchanged(t *testing.T) {
	run := func(captureOn bool) (digest string, n int) {
		cfg := DefaultConfig()
		cfg.Seed = 7
		cfg.BurstCount = 20
		cfg.SimEndMs = 20000
		cfg.Capture = captureOn
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

func countKind(g *capture.Graph, kind string) int {
	n := 0
	for _, e := range g.Edges {
		if e.Kind == kind {
			n += e.Count
		}
	}
	return n
}
