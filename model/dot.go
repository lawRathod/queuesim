package model

import (
	"fmt"
	"io"
	"strings"
)

// WriteDot emits a Graphviz DOT description of the topology's structure:
// queues (boxes), processors (ellipses), networks (diamonds), and the
// directed edges between them — Feed edges (queue → processor) and Connect
// edges (processor → queue, through a network when one is attached).
//
// For hierarchical topologies, WriteDot renders each child as a subgraph
// cluster_<path> recursively, path-prefixes node IDs for global uniqueness,
// and colors cross-boundary edges red. All iteration is sorted for
// byte-identical determinism.
//
// The output is deterministic (nodes emitted in sorted-name order, edges in
// registration order) and self-contained: `dot -Tsvg topology.dot > x.svg`
// renders it. This is the thesis figure for "what the model IS".
func (t *Topology) WriteDot(w io.Writer) error {
	if w == nil {
		return fmt.Errorf("model: WriteDot requires a non-nil writer")
	}
	b := &strings.Builder{}
	b.WriteString("digraph godes_topology {\n")
	b.WriteString("  rankdir=LR;\n")
	b.WriteString("  node [fontname=\"Helvetica\"];\n")
	b.WriteString("  edge [fontname=\"Helvetica\"];\n")
	b.WriteString("  graph [label=\"GoDES topology\", labelloc=t, fontname=\"Helvetica\"];\n")
	t.writeDotRecursive(b, "  ")
	b.WriteString("}\n")
	_, err := io.WriteString(w, b.String())
	return err
}

func (t *Topology) writeDotRecursive(b *strings.Builder, indent string) {
	// Emit nodes for this level.
	for _, q := range t.sortedQueues() {
		label := q.Name()
		extra := ""
		if q.cfg.Capacity > 0 {
			extra = fmt.Sprintf("\\n[cap %d]", q.cfg.Capacity)
		}
		id := dotNodeID(t.path, q.Name())
		b.WriteString(fmt.Sprintf("%sq_%s [shape=box, label=\"%s%s\"];\n", indent, id, label, extra))
	}
	for _, p := range t.sortedProcessors() {
		label := p.Name()
		if p.cfg.ServiceMs > 0 {
			label += fmt.Sprintf("\\n[svc %dms]", p.cfg.ServiceMs)
		} else if p.cfg.ServiceRate > 0 {
			label += "\\n[rate]"
		}
		id := dotNodeID(t.path, p.Name())
		b.WriteString(fmt.Sprintf("%sp_%s [shape=ellipse, label=\"%s\"];\n", indent, id, label))
	}
	for _, n := range t.sortedNetworks() {
		label := n.Name()
		if n.cfg.DelayMs > 0 || n.cfg.DropRate > 0 {
			label += fmt.Sprintf("\\n[%dms drop %.2f]", n.cfg.DelayMs, n.cfg.DropRate)
		}
		id := dotNodeID(t.path, n.Name())
		b.WriteString(fmt.Sprintf("%sn_%s [shape=diamond, label=\"%s\"];\n", indent, id, label))
	}
	for _, w := range t.sortedWorkers() {
		label := w.Name()
		if w.cfg.ServiceMs > 0 {
			label += fmt.Sprintf("\\n[svc %dms]", w.cfg.ServiceMs)
		}
		id := dotNodeID(t.path, w.Name())
		b.WriteString(fmt.Sprintf("%sw_%s [shape=cylinder, label=\"%s\"];\n", indent, id, label))
		if w.cfg.Queue != nil {
			qID := dotNodeID(w.cfg.Queue.SubstreamPrefix(), w.cfg.Queue.Name())
			b.WriteString(fmt.Sprintf("%sq_%s -> w_%s [color=green];\n", indent, qID, id))
		}
	}
	// Feed edges: queue → processor
	for _, q := range t.sortedQueues() {
		for _, p := range t.queueProcs[q.Name()] {
			qID := dotNodeID(t.path, q.Name())
			pID := dotNodeID(t.path, p.Name())
			b.WriteString(fmt.Sprintf("%sq_%s -> p_%s [color=black];\n", indent, qID, pID))
		}
	}
	// Connect edges: processor → queue via network
	for _, p := range t.sortedProcessors() {
		for _, out := range p.outputs {
			pID := dotNodeID(t.path, p.Name())
			dstPrefix := out.dst.SubstreamPrefix()
			dstID := dotNodeID(dstPrefix, out.dst.Name())
			if out.net != nil {
				netID := dotNodeID(t.path, out.net.Name())
				b.WriteString(fmt.Sprintf("%sp_%s -> n_%s [color=blue];\n", indent, pID, netID))
				b.WriteString(fmt.Sprintf("%sn_%s -> q_%s [color=blue];\n", indent, netID, dstID))
			} else {
				b.WriteString(fmt.Sprintf("%sp_%s -> q_%s [color=blue];\n", indent, pID, dstID))
			}
		}
	}
	// Cross-boundary edges (red) at this level.
	for _, childName := range sortedNames(t.children) {
		child := t.children[childName]
		// child out-port → parent/sibling dst
		if edgeMap, ok := t.childOutEdges[childName]; ok {
			for _, portName := range sortedNames(edgeMap) {
				edge := edgeMap[portName]
				srcID := dotNodeID(child.path, portName)
				dstPrefix := edge.dst.SubstreamPrefix()
				dstID := dotNodeID(dstPrefix, edge.dst.Name())
				if edge.net != nil {
					netID := dotNodeID(t.path, edge.net.Name())
					b.WriteString(fmt.Sprintf("%sq_%s -> n_%s [color=red];\n", indent, srcID, netID))
					b.WriteString(fmt.Sprintf("%sn_%s -> q_%s [color=red];\n", indent, netID, dstID))
				} else {
					b.WriteString(fmt.Sprintf("%sq_%s -> q_%s [color=red];\n", indent, srcID, dstID))
				}
			}
		}
		// parent → child in-port
		if feedMap, ok := t.feedEdges[childName]; ok {
			for _, portName := range sortedNames(feedMap) {
				src := feedMap[portName]
				srcID := dotNodeID(src.SubstreamPrefix(), src.Name())
				dstID := dotNodeID(child.path, portName)
				b.WriteString(fmt.Sprintf("%sq_%s -> q_%s [color=red];\n", indent, srcID, dstID))
			}
		}
	}
	// Recurse into children clusters in sorted order.
	for _, childName := range sortedNames(t.children) {
		child := t.children[childName]
		clusterID := dotID(child.path)
		b.WriteString(fmt.Sprintf("%ssubgraph cluster_%s {\n", indent, clusterID))
		b.WriteString(fmt.Sprintf("%s  label=\"%s\";\n", indent, childName))
		child.writeDotRecursive(b, indent+"  ")
		b.WriteString(fmt.Sprintf("%s}\n", indent))
	}
}

// dotNodeID returns a sanitized node ID for a queue/processor/network/worker
// given its topology path prefix and name. For root path "" it is dotID(name);
// for child path "a" and name "q" it is dotID("a.q") → "a_q".
func dotNodeID(path, name string) string {
	if path == "" {
		return dotID(name)
	}
	return dotID(path + "." + name)
}

// dotID sanitizes a name for use as a Graphviz node ID (alphanumeric +
// underscore; everything else replaced by underscore). Names in GoDES are
// already safe (queue names, processor names), but this guards against
// exotic inputs.
func dotID(name string) string {
	var sb strings.Builder
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' {
			sb.WriteRune(r)
		} else {
			sb.WriteByte('_')
		}
	}
	return sb.String()
}
