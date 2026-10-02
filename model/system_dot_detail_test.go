package model

import (
	"strings"
	"testing"
)

// detailTestSystem builds a two-service system whose services register REAL
// generic primitives (queue+processor+worker) so the detailed renderer has
// real intra-service wiring to derive, plus model-declared components.
func detailTestSystem() *System {
	sys := NewSystem("detail_test", SystemConfig{Seed: 1})
	sys.AddService(Service{
		Name:       "a",
		Kind:       "a.kind",
		Label:      "service A",
		Components: []Component{{Name: "inner", Kind: "leaf", Label: "internal x"}},
		Ports:      []Port{{Name: "out"}},
		Build: func(b *TopologyBuilder) error {
			b.AddQueue("buf", 5)
			b.AddQueue("out", 0)
			b.ExportOut("out")
			b.AddProcessor(ProcessorConfig{Name: "proc", ServiceMs: 3, Budget: 2})
			b.Feed(b.Queue("buf"), b.Processor("proc"))
			b.Connect(b.Processor("proc"), b.Queue("out"), nil)
			b.AddWorker(WorkerConfig{Name: "pool", Queue: b.Queue("buf"), Count: 2, Budget: 1})
			return nil
		},
		Flow: []ComponentLink{{From: "buf", To: "inner", Label: "feeds"}},
	})
	sys.AddService(Service{
		Name:  "b",
		Kind:  "b.kind",
		Ports: []Port{{Name: "in", In: true}},
		Build: func(b *TopologyBuilder) error {
			b.AddQueue("in", 0)
			b.ExportIn("in")
			b.AddWorker(WorkerConfig{Name: "pool2", Queue: b.Queue("in"), Count: 3})
			return nil
		},
	})
	// A delayed boundary network so the cross-service diamond renders.
	sys.Connect("a", "out", "b", "in", NetworkConfig{DelayMs: 2})
	return sys
}

// TestWriteDotDetailDeterministic pins byte-identical rendering: the same
// built system must draw the same diagram every time (determinism contract).
func TestWriteDotDetailDeterministic(t *testing.T) {
	sys := detailTestSystem()
	if err := sys.Build(); err != nil {
		t.Fatalf("build: %v", err)
	}
	var b1, b2 strings.Builder
	if err := sys.WriteDotDetail(&b1); err != nil {
		t.Fatalf("render 1: %v", err)
	}
	if err := sys.WriteDotDetail(&b2); err != nil {
		t.Fatalf("render 2: %v", err)
	}
	if b1.String() != b2.String() {
		t.Fatalf("WriteDotDetail not deterministic:\n--- first ---\n%s\n--- second ---\n%s", b1.String(), b2.String())
	}
}

// TestWriteDotDetailRendersInternals asserts the detail renderer emits the
// REAL derived structure: registered queues/processors/workers with knob
// annotations, intra-service wiring (feed/connect/poll), cross-service
// connections through a boundary network, and model-declared components
// (dashed). The legend must NOT be emitted: the diagram must not require a
// legend to read.
func TestWriteDotDetailRendersInternals(t *testing.T) {
	sys := detailTestSystem()
	if err := sys.Build(); err != nil {
		t.Fatalf("build: %v", err)
	}
	var out strings.Builder
	if err := sys.WriteDotDetail(&out); err != nil {
		t.Fatalf("render: %v", err)
	}
	got := out.String()

	want := []string{
		// Service clusters with Kind + Label subtitles.
		`subgraph cluster_a`,
		`label="a\n[a.kind]\nservice A";`,
		`subgraph cluster_b`,
		`label="b\n[b.kind]";`,
		// Real queue with capacity knob + port boundary style (filled lightblue).
		`q_a_buf [shape=box, label="buf\n[cap 5]"];`,
		`q_a_out [shape=box, style=filled, fillcolor=lightblue, label="out"];`,
		`q_b_in [shape=box, style=filled, fillcolor=lightblue, label="in"];`,
		// Real processor with service-time annotation.
		`p_a_proc [shape=ellipse, label="proc\n[svc 3ms budget 2]"];`,
		// Real worker fleet with size/budget annotations.
		`w_a_pool [shape=cylinder, label="pool\n[x2 budget 1]"];`,
		`w_b_pool2 [shape=cylinder, label="pool2\n[x3]"];`,
		// Intra-service wiring derived from the built structure.
		`q_a_buf -> p_a_proc [color=black];`, // feed
		`p_a_proc -> q_a_out [color=blue];`,  // connect
		`q_a_buf -> w_a_pool [color=green];`, // worker poll
		`q_b_in -> w_b_pool2 [color=green];`, // worker poll
		// Model-declared component, dashed and distinct.
		`c_a_inner [shape=box, style=dashed, label="inner\ninternal x\n[leaf]"];`,
		// Cross-service connection through the boundary network.
		`q_a_out -> n_net_a_out__b_in [color=blue];`,
		`n_net_a_out__b_in [shape=diamond, label="2ms"];`,
		`n_net_a_out__b_in -> q_b_in [color=blue];`,
		// Declared flow arc (dashed, distinct from derived wiring).
		`q_a_buf -> c_a_inner [style=dashed, color=gray, fontcolor=gray, label="feeds"];`,
	}
	for _, w := range want {
		if !strings.Contains(got, w) {
			t.Errorf("WriteDotDetail missing %q\n--- got ---\n%s", w, got)
		}
	}

	// No legend: the schematic must be readable on its own — every rendered
	// construct is real structure or a self-describing annotation.
	for _, forbid := range []string{"cluster_legend", "legend"} {
		if strings.Contains(got, forbid) {
			t.Errorf("WriteDotDetail contains %q — legend must not be rendered\n--- got ---\n%s", forbid, got)
		}
	}
}

// TestWriteDotDetailDerivedOnly pins the honesty contract: style=dashed
// appears ONLY on model-declared things (component nodes and declared-flow
// edges) — every real registered node (q_/p_/w_/n_ with a shape) renders
// solid. A reviewer can always tell derived structure from declared names.
func TestWriteDotDetailDerivedOnly(t *testing.T) {
	sys := detailTestSystem()
	if err := sys.Build(); err != nil {
		t.Fatalf("build: %v", err)
	}
	var out strings.Builder
	if err := sys.WriteDotDetail(&out); err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, line := range strings.Split(out.String(), "\n") {
		trimmed := strings.TrimSpace(line)
		realNode := (strings.HasPrefix(trimmed, "q_") || strings.HasPrefix(trimmed, "p_") ||
			strings.HasPrefix(trimmed, "w_") || strings.HasPrefix(trimmed, "n_")) &&
			strings.Contains(line, "shape=")
		if realNode && strings.Contains(line, "style=dashed") {
			t.Errorf("real registered node rendered dashed (honesty contract broken): %s", trimmed)
		}
	}
}

// TestWriteDotUnchanged pins the summary renderer's byte-identical contract:
// WriteDot must remain the lean service-graph view (no legend, no internals,
// no components) and deterministic.
func TestWriteDotUnchanged(t *testing.T) {
	sys := detailTestSystem()
	if err := sys.Build(); err != nil {
		t.Fatalf("build: %v", err)
	}
	var b1, b2 strings.Builder
	if err := sys.WriteDot(&b1); err != nil {
		t.Fatalf("render 1: %v", err)
	}
	if err := sys.WriteDot(&b2); err != nil {
		t.Fatalf("render 2: %v", err)
	}
	if b1.String() != b2.String() {
		t.Fatalf("WriteDot not deterministic")
	}
	got := b1.String()
	for _, want := range []string{"cluster_a", "cluster_b", "q_a_out -> n_net_a_out__b_in [color=blue]"} {
		if !strings.Contains(got, want) {
			t.Errorf("WriteDot missing %q\n--- got ---\n%s", want, got)
		}
	}
	for _, forbid := range []string{"legend", "style=dashed", "(internals)", "color=black"} {
		if strings.Contains(got, forbid) {
			t.Errorf("WriteDot now contains detail-only marker %q — summary contract broken\n--- got ---\n%s", forbid, got)
		}
	}
}
