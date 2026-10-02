package tasqueue

import (
	"bytes"
	"strings"
	"testing"
)

// TestTasqueueDotStructuralDebug is the honest-schematics gate: after the
// dead port-shell + Connect-edge removal (WP2), BuildSystem's WriteDot is a
// STRUCTURAL-DEBUG artifact, not a mechanism picture. The tasqueue model's
// real dynamics (the "tasqueue:tasks" broker queue, ETA timer queue,
// poller fleet, and the runtime dispatch/complete/retry loop) are
// constructed directly on the shared runtime's simulation and driven by the
// runtime tick — they are invisible to the DOT renderer, which draws only
// REGISTERED builder primitives. The two service clusters render but are
// EMPTY: no node declarations, no edges, and none of the deleted schematic
// ports may reappear. The trustworthy architecture picture is DERIVED FROM
// RUNTIME CAPTURE (Config.Capture → model/capture recorder →
// capture.Aggregate), keyed to the component ↔ Tasqueue source table in the
// package doc.
func TestTasqueueDotStructuralDebug(t *testing.T) {
	render := func() string {
		sys := BuildSystem(DefaultConfig())
		if err := sys.Build(); err != nil {
			t.Fatalf("Build: %v", err)
		}
		var buf bytes.Buffer
		if err := sys.WriteDot(&buf); err != nil {
			t.Fatalf("WriteDot: %v", err)
		}
		return buf.String()
	}
	a, b := render(), render()
	if a != b {
		t.Fatal("WriteDot not byte-identical across runs")
	}

	// The service clusters still render — the shells are gone but the
	// Service values (and therefore the clusters) remain.
	for _, want := range []string{
		`subgraph cluster_queue`,
		`subgraph cluster_worker`,
		// The System graph label (the system name) is still rendered.
		`graph [label="tasqueue"`,
	} {
		if !strings.Contains(a, want) {
			t.Fatalf("DOT missing %q:\n%s", want, a)
		}
	}

	// Honesty gate 1: the clusters are EMPTY — the renderer draws only
	// REGISTERED builder primitives, and this model registers none. No
	// `[shape=` node declaration and no `->` edge anywhere in the output.
	for _, line := range strings.Split(a, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.Contains(trimmed, "[shape=") {
			t.Fatalf("DOT contains a node declaration %q — expected empty service clusters:\n%s", trimmed, a)
		}
		if strings.Contains(trimmed, "->") {
			t.Fatalf("DOT contains an edge %q — expected no edges (runtime-mediated model):\n%s", trimmed, a)
		}
	}

	// Honesty gate 2 (guard against resurrection): none of the deleted
	// schematic ports may reappear. A port shell would render as a queue
	// node whose id is q_<service>_<port> (dotID maps '.'/'-' to '_'), so
	// the forbid list uses those exact rendered forms:
	//   queue ports in (in) + out (out); worker ports in (in) + done (out)
	for _, forbid := range []string{
		"q_queue_in", "q_queue_out",
		"q_worker_in", "q_worker_done",
	} {
		if strings.Contains(a, forbid) {
			t.Fatalf("DOT contains deleted port node %q (shell resurrected?):\n%s", forbid, a)
		}
	}
	t.Logf("DOT sample (structural debug — empty service clusters):\n%s", a)
}
