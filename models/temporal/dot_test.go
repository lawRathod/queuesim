package temporal

import (
	"bytes"
	"strings"
	"testing"
)

// TestTemporalDotStructuralDebug is the honest-schematics gate: after the
// dead port-shell + Connect-edge removal (WP1), BuildSystem's WriteDot is a
// STRUCTURAL-DEBUG artifact, not a mechanism picture. The temporal model's
// real dynamics (MatchQueues, poller fleets, history shards, sticky queue,
// transfer/timer/reschedule loop) are constructed directly on the shared
// runtime's simulation and driven by the runtime tick — they are invisible
// to the DOT renderer, which draws only REGISTERED builder primitives. The
// two service clusters render but are EMPTY: no node declarations, no
// edges, and none of the deleted schematic ports may reappear. The
// trustworthy architecture picture is DERIVED FROM RUNTIME CAPTURE
// (Config.Capture → model/capture recorder → capture.Aggregate), keyed to
// the component ↔ file:line table in the package doc / ADR-078.
func TestTemporalDotStructuralDebug(t *testing.T) {
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
		`subgraph cluster_matching`,
		`subgraph cluster_history`,
		// The System graph label (the system name) is still rendered.
		`graph [label="temporal"`,
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
	//   matching ports wf-in/act-in (in) + wf-done/act-done (out)
	//   history ports start/transfer (in) + timer (out)
	for _, forbid := range []string{
		"q_matching_wf_in", "q_matching_act_in",
		"q_matching_wf_done", "q_matching_act_done",
		"q_history_start", "q_history_transfer", "q_history_timer",
	} {
		if strings.Contains(a, forbid) {
			t.Fatalf("DOT contains deleted port node %q (shell resurrected?):\n%s", forbid, a)
		}
	}
	t.Logf("DOT sample (structural debug — empty service clusters):\n%s", a)
}
