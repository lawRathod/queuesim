package model

import (
	"strings"
	"testing"

	"github.com/lawRathod/queuesim/godes/core"
)

// TestWriteDotStructure: a small topology (queue → processor → network →
// queue) emits a deterministic DOT digraph with all nodes and edges.
func TestWriteDotStructure(t *testing.T) {
	sim, err := core.NewSimulation(1, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation: %v", err)
	}
	b := NewTopologyBuilder(sim)
	b.AddQueue("in", 10)
	b.AddQueue("out", 0)
	b.AddProcessor(ProcessorConfig{Name: "proc", ServiceMs: 5})
	b.AddNetwork(NetworkConfig{Name: "link", DelayMs: 2})
	proc := b.Processor("proc")
	in := b.Queue("in")
	out := b.Queue("out")
	link := b.Network("link")
	b.Feed(in, proc)           // queue -> processor
	b.Connect(proc, out, link) // processor -> network -> queue
	top, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	var sb strings.Builder
	if err := top.WriteDot(&sb); err != nil {
		t.Fatalf("WriteDot: %v", err)
	}
	dot := sb.String()

	for _, want := range []string{
		"digraph godes_topology",
		"q_in [shape=box, label=\"in\\n[cap 10]\"]",
		"q_out [shape=box, label=\"out\"]",
		"p_proc [shape=ellipse, label=\"proc\\n[svc 5ms]\"]",
		"n_link [shape=diamond, label=\"link\\n[2ms drop 0.00]\"]",
		"q_in -> p_proc",   // feed edge
		"p_proc -> n_link", // connect via network
		"n_link -> q_out",  // network -> queue
	} {
		if !strings.Contains(dot, want) {
			t.Errorf("DOT missing %q\n---\n%s", want, dot)
		}
	}
	if !strings.HasPrefix(dot, "digraph godes_topology {") {
		t.Errorf("DOT must start with the digraph header")
	}
	if !strings.HasSuffix(strings.TrimSpace(dot), "}") {
		t.Errorf("DOT must end with }")
	}
}

// TestWriteDotDeterministic: the same topology emits identical DOT (byte
// for byte) — the exporter is deterministic.
func TestWriteDotDeterministic(t *testing.T) {
	build := func() string {
		sim, _ := core.NewSimulation(2, core.FIFOPolicy{})
		b := NewTopologyBuilder(sim)
		b.AddQueue("q1", 5)
		b.AddQueue("q2", 5)
		b.AddProcessor(ProcessorConfig{Name: "p1"})
		b.AddProcessor(ProcessorConfig{Name: "p2"})
		q1 := b.Queue("q1")
		q2 := b.Queue("q2")
		p1 := b.Processor("p1")
		p2 := b.Processor("p2")
		b.Feed(q1, p1)
		b.Feed(q2, p2)
		b.Connect(p1, q2, nil)
		b.Connect(p2, q1, nil)
		top, _ := b.Build()
		var sb strings.Builder
		_ = top.WriteDot(&sb)
		return sb.String()
	}
	a, b2 := build(), build()
	if a != b2 {
		t.Fatalf("DOT not deterministic:\n%s\n---\n%s", a, b2)
	}
}

// TestWriteDotWorker: a topology with a Worker fleet emits a cylinder node
// and a green pull edge from its queue.
func TestWriteDotWorker(t *testing.T) {
	sim, _ := core.NewSimulation(3, core.FIFOPolicy{})
	b := NewTopologyBuilder(sim)
	b.AddQueue("matching", 10)
	b.AddWorker(WorkerConfig{Name: "workers", Queue: b.Queue("matching"), ServiceMs: 5, Count: 2})
	top, err := b.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	var sb strings.Builder
	_ = top.WriteDot(&sb)
	dot := sb.String()
	for _, want := range []string{
		"w_workers [shape=cylinder, label=\"workers\\n[svc 5ms]\"]",
		"q_matching -> w_workers [color=green]",
	} {
		if !strings.Contains(dot, want) {
			t.Errorf("DOT missing %q\n---\n%s", want, dot)
		}
	}
}
