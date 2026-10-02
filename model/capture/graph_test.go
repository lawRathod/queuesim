package capture

import (
	"strings"
	"testing"
)

// TestAggregateEmpty returns an empty graph for no events.
func TestAggregateEmpty(t *testing.T) {
	g := Aggregate(nil)
	if len(g.Nodes) != 0 || len(g.Edges) != 0 {
		t.Fatalf("empty aggregate: nodes=%d edges=%d want 0/0", len(g.Nodes), len(g.Edges))
	}
	if g.Summary() == "" {
		t.Fatal("Summary of empty graph should not be empty")
	}
}

// TestAggregateSortsNodesAndEdges asserts nodes sort by name and edges by
// (from, to, kind), regardless of event input order.
func TestAggregateSortsNodesAndEdges(t *testing.T) {
	evs := []Event{
		{Seq: 0, From: "matching", To: "exec", Kind: "dispatch"},
		{Seq: 1, From: "load", To: "history", Kind: "workflow_start"},
		{Seq: 2, From: "history", To: "matching", Kind: "wf_add"},
		{Seq: 3, From: "exec", To: "history", Kind: "act_retry"},
		{Seq: 4, From: "matching", To: "matching", Kind: "sticky_dispatch"},
	}
	g := Aggregate(evs)
	wantNodes := []string{"exec", "history", "load", "matching"}
	if len(g.Nodes) != len(wantNodes) {
		t.Fatalf("nodes=%d want %d", len(g.Nodes), len(wantNodes))
	}
	for i, n := range g.Nodes {
		if n.Name != wantNodes[i] {
			t.Errorf("node %d = %q want %q", i, n.Name, wantNodes[i])
		}
	}
	wantEdges := [][3]string{
		{"exec", "history", "act_retry"},
		{"history", "matching", "wf_add"},
		{"load", "history", "workflow_start"},
		{"matching", "exec", "dispatch"},
		{"matching", "matching", "sticky_dispatch"},
	}
	if len(g.Edges) != len(wantEdges) {
		t.Fatalf("edges=%d want %d", len(g.Edges), len(wantEdges))
	}
	for i, e := range g.Edges {
		w := wantEdges[i]
		if e.From != w[0] || e.To != w[1] || e.Kind != w[2] {
			t.Errorf("edge %d = %s->%s %s want %s->%s %s", i, e.From, e.To, e.Kind, w[0], w[1], w[2])
		}
	}
}

// TestAggregateCountsAndTiming folds identical (from,to,kind) events into
// one edge carrying the count and the first/last sim timestamps.
func TestAggregateCountsAndTiming(t *testing.T) {
	evs := []Event{
		{From: "history", To: "matching", Kind: "act_add", TsMs: 40},
		{From: "history", To: "matching", Kind: "act_add", TsMs: 10},
		{From: "history", To: "matching", Kind: "act_add", TsMs: 90},
	}
	g := Aggregate(evs)
	if len(g.Edges) != 1 {
		t.Fatalf("edges=%d want 1", len(g.Edges))
	}
	e := g.Edges[0]
	if e.Count != 3 {
		t.Errorf("count=%d want 3", e.Count)
	}
	if e.FirstTsMs != 10 || e.LastTsMs != 90 {
		t.Errorf("timing wrong: first=%d last=%d want 10/90", e.FirstTsMs, e.LastTsMs)
	}
}

// TestAggregateKindDistinguishesEdges asserts the graph is a multigraph:
// distinct kinds between the same pair remain distinct edges.
func TestAggregateKindDistinguishesEdges(t *testing.T) {
	evs := []Event{
		{From: "history", To: "matching", Kind: "wf_add", TsMs: 1},
		{From: "history", To: "matching", Kind: "act_add", TsMs: 2},
		{From: "history", To: "matching", Kind: "wf_add", TsMs: 3},
	}
	g := Aggregate(evs)
	if len(g.Edges) != 2 {
		t.Fatalf("edges=%d want 2", len(g.Edges))
	}
	if g.Edges[0].Kind != "act_add" || g.Edges[0].Count != 1 {
		t.Errorf("edge0 wrong: %+v", g.Edges[0])
	}
	if g.Edges[1].Kind != "wf_add" || g.Edges[1].Count != 2 {
		t.Errorf("edge1 wrong: %+v", g.Edges[1])
	}
}

// TestAggregateNodeTimingAndSelfLoop exercises the concrete lifecycle shape
// a mechanism-map figure must support: a single-workflow calm chain with a
// retry loop.
func TestAggregateNodeTimingAndSelfLoop(t *testing.T) {
	evs := []Event{
		{From: "load", To: "history", Kind: "workflow_start", TsMs: 1},
		{From: "history", To: "matching", Kind: "wf_add", TsMs: 2},
		{From: "matching", To: "exec", Kind: "dispatch", TsMs: 3},
		{From: "exec", To: "history", Kind: "act_retry", TsMs: 4},
		{From: "history", To: "matching", Kind: "act_add", TsMs: 5},
		{From: "matching", To: "exec", Kind: "dispatch", TsMs: 6},
		{From: "matching", To: "matching", Kind: "sticky", TsMs: 7},
	}
	g := Aggregate(evs)
	if len(g.Nodes) != 4 {
		t.Errorf("nodes=%d want 4", len(g.Nodes))
	}
	if len(g.Edges) != 6 {
		t.Fatalf("edges=%d want 6", len(g.Edges))
	}
	// Sorted edge order: exec->history act_retry, history->matching act_add,
	// history->matching wf_add, load->history workflow_start,
	// matching->exec dispatch ×2, matching->matching sticky.
	if e := g.Edges[0]; e.From != "exec" || e.To != "history" || e.Kind != "act_retry" || e.Count != 1 {
		t.Errorf("exec edge wrong: %+v", e)
	}
	if e := g.Edges[4]; e.From != "matching" || e.To != "exec" || e.Kind != "dispatch" || e.Count != 2 {
		t.Errorf("dispatch edge wrong: %+v", e)
	}
	if e := g.Edges[5]; e.From != "matching" || e.To != "matching" || e.Kind != "sticky" || e.Count != 1 {
		t.Errorf("sticky edge wrong: %+v", e)
	}
}

// TestSummaryDeterministicAndContent asserts the Summary is a stable,
// sorted, human-readable rendering of the graph.
func TestSummaryDeterministicAndContent(t *testing.T) {
	evs := []Event{
		{From: "history", To: "matching", Kind: "wf_add", TsMs: 2},
		{From: "history", To: "matching", Kind: "wf_add", TsMs: 5},
		{From: "matching", To: "exec", Kind: "dispatch", TsMs: 3},
	}
	g1 := Aggregate(evs)
	g2 := Aggregate(reverseEvents(evs))
	if g1.Summary() != g2.Summary() {
		t.Fatalf("Summary not input-order deterministic:\n---\n%s\n---\n%s", g1.Summary(), g2.Summary())
	}
	s := g1.Summary()
	for _, want := range []string{"history", "matching", "exec", "wf_add", "dispatch", "2", "1"} {
		if !strings.Contains(s, want) {
			t.Errorf("Summary missing %q:\n%s", want, s)
		}
	}
	// Nodes listed before edges.
	if !(strings.Index(s, "exec") < strings.Index(s, "->")) {
		t.Errorf("Summary should list nodes before edges:\n%s", s)
	}
}

func reverseEvents(evs []Event) []Event {
	out := make([]Event, len(evs))
	for i, e := range evs {
		out[len(evs)-1-i] = e
	}
	return out
}
