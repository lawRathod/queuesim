package capture

import (
	"fmt"
	"sort"
	"strings"
)

// Graph is the aggregated mechanism multigraph: one node per observed
// component and one edge per observed (from, to, kind) transition, folded
// across every recorded Event. Nodes and edges are sorted (by name, and by
// from/to/kind) so the graph — and everything rendered from it — is
// deterministic regardless of the order events were recorded in.
type Graph struct {
	// Nodes are the observed components, sorted by name.
	Nodes []Node
	// Edges are the observed transitions, sorted by From, To, Kind.
	Edges []Edge
}

// Node is one observed component.
type Node struct {
	// Name is the model-chosen component name.
	Name string
}

// Edge is one observed (from, to, kind) transition family, aggregated
// across all recorded events with the same triple.
type Edge struct {
	From      string
	To        string
	Kind      string
	Count     int
	FirstTsMs int64
	LastTsMs  int64
}

// Aggregate folds recorded events into a sorted mechanism multigraph.
// Events with the same (From, To, Kind) are merged into one Edge carrying
// the event count and the first/last sim timestamps. A nil or empty event
// slice yields an empty graph. Aggregate performs no I/O and no RNG draws.
func Aggregate(events []Event) *Graph {
	type key struct{ from, to, kind string }
	edges := map[key]*edgeAgg{}
	nodes := map[string]struct{}{}
	for _, ev := range events {
		nodes[ev.From] = struct{}{}
		nodes[ev.To] = struct{}{}
		k := key{ev.From, ev.To, ev.Kind}
		agg, ok := edges[k]
		if !ok {
			agg = &edgeAgg{}
			edges[k] = agg
		}
		agg.count++
		if agg.first == 0 || ev.TsMs < agg.first {
			agg.first = ev.TsMs
		}
		if ev.TsMs > agg.last {
			agg.last = ev.TsMs
		}
	}

	g := &Graph{}
	for name := range nodes {
		g.Nodes = append(g.Nodes, Node{Name: name})
	}
	sort.Slice(g.Nodes, func(i, j int) bool { return g.Nodes[i].Name < g.Nodes[j].Name })

	for k, agg := range edges {
		g.Edges = append(g.Edges, Edge{
			From:      k.from,
			To:        k.to,
			Kind:      k.kind,
			Count:     agg.count,
			FirstTsMs: agg.first,
			LastTsMs:  agg.last,
		})
	}
	sort.Slice(g.Edges, func(i, j int) bool {
		a, b := g.Edges[i], g.Edges[j]
		if a.From != b.From {
			return a.From < b.From
		}
		if a.To != b.To {
			return a.To < b.To
		}
		return a.Kind < b.Kind
	})
	return g
}

// edgeAgg is the internal accumulation state for one (from,to,kind) triple.
// first==0 doubles as "unset" — sim time is non-negative, and a transition
// recorded at ts 0 legitimately keeps first==0.
type edgeAgg struct {
	count int
	first int64
	last  int64
}

// Summary renders a deterministic, human-readable digest of the graph:
// the observed nodes, then each observed transition with its count and
// first/last sim timestamps.
func (g *Graph) Summary() string {
	if g == nil || (len(g.Nodes) == 0 && len(g.Edges) == 0) {
		return "mechanism map: (no observed transitions)"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "mechanism map: %d nodes, %d edges\n", len(g.Nodes), len(g.Edges))
	b.WriteString("nodes:\n")
	for _, n := range g.Nodes {
		fmt.Fprintf(&b, "  %s\n", n.Name)
	}
	b.WriteString("edges:\n")
	for _, e := range g.Edges {
		fmt.Fprintf(&b, "  %s -> %s [%s] count=%d first_ts_ms=%d last_ts_ms=%d\n",
			e.From, e.To, e.Kind, e.Count, e.FirstTsMs, e.LastTsMs)
	}
	return b.String()
}
