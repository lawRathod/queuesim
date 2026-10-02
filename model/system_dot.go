package model

import (
	"fmt"
	"io"
	"strings"
)

// WriteDot renders the SERVICE GRAPH as a Graphviz DOT description: one
// cluster per Service (sorted-name order), the service's REGISTERED queues
// (boxes, with port direction annotated), Processors (ellipses) and Worker
// fleets (cylinders), and the cross-service connections as directed edges
// between the registered source out-port queue and destination in-port
// queue.
//
// Honest-schematics scope: this is a STRUCTURAL-DEBUG view of REGISTERED
// builder primitives. It cannot show machinery a Service built directly on
// the simulation (runtime-mediated models such as models/temporal and
// models/tasqueue register no ports, so their clusters render empty) — the
// trustworthy architecture picture for such models is DERIVED FROM RUNTIME
// CAPTURE (set SystemConfig.EnableCapture; the model records transitions at
// its real chokepoints and capture.Aggregate renders only what traversed).
// Port shells + zero-cost Connect edges that carry no tasks must NOT be
// added to make the diagram look fuller.
//
// The rendering is deterministic (nodes in sorted order, connections in
// sorted order) and derived directly from the BUILT service fragments —
// every node is a live TaskQueue/Processor/Worker owned by a registered
// Service; there are no dummy registrations. Requires Build (or Run) to
// have populated the real entities.
func (s *System) WriteDot(w io.Writer) error {
	if w == nil {
		return fmt.Errorf("model: WriteDot requires a non-nil writer")
	}
	if !s.built {
		return fmt.Errorf("model: WriteDot requires Build before rendering")
	}
	b := &strings.Builder{}
	b.WriteString("digraph godes_system {\n")
	b.WriteString("  rankdir=LR;\n")
	b.WriteString("  node [fontname=\"Helvetica\"];\n")
	b.WriteString("  edge [fontname=\"Helvetica\"];\n")
	fmt.Fprintf(b, "  graph [label=\"%s\", labelloc=t, fontname=\"Helvetica\"];\n", dotID(s.name))

	// Service clusters in sorted-name order, from the built children.
	for _, svcName := range sortedServices(s.services) {
		child := s.topo.Child(svcName)
		clusterID := dotID(svcName)
		fmt.Fprintf(b, "  subgraph cluster_%s {\n", clusterID)
		fmt.Fprintf(b, "    label=\"%s\";\n", svcName)
		// Real queues (sorted by name), annotated with port direction.
		for _, qName := range sortedNames(child.queues) {
			q := child.queues[qName]
			label := q.Name()
			if _, ok := child.inPorts[qName]; ok {
				label += " (in)"
			} else if _, ok := child.outPorts[qName]; ok {
				label += " (out)"
			}
			qID := dotID(svcName + "." + q.Name())
			fmt.Fprintf(b, "    q_%s [shape=box, label=\"%s\"];\n", qID, label)
		}
		// Real processors (sorted by name).
		for _, pName := range sortedNames(child.processors) {
			p := child.processors[pName]
			pID := dotID(svcName + "." + p.Name())
			label := p.Name()
			if p.cfg.ServiceMs > 0 {
				label += fmt.Sprintf("\\n[svc %dms]", p.cfg.ServiceMs)
			}
			fmt.Fprintf(b, "    p_%s [shape=ellipse, label=\"%s\"];\n", pID, label)
		}
		// Real worker fleets (sorted by name).
		for _, wName := range sortedNames(child.workers) {
			w := child.workers[wName]
			wID := dotID(svcName + "." + w.Name())
			fmt.Fprintf(b, "    w_%s [shape=cylinder, label=\"%s\"];\n", wID, w.Name())
		}
		b.WriteString("  }\n")
	}

	// Connections (sorted) between the real source out-port and destination
	// in-port queues.
	for _, c := range sortedConnections(s.conns) {
		srcID := dotID(c.Src + "." + c.SrcPort)
		dstID := dotID(c.Dst + "." + c.DstPort)
		if needsNetwork(c.Net) {
			netID := dotID(boundaryNetName(c))
			fmt.Fprintf(b, "  q_%s -> n_%s [color=blue];\n", srcID, netID)
			label := "link"
			if c.Net.DelayMs > 0 {
				label = fmt.Sprintf("%dms", c.Net.DelayMs)
			} else if c.Net.DropRate > 0 {
				label = fmt.Sprintf("drop %.2f", c.Net.DropRate)
			}
			fmt.Fprintf(b, "  n_%s [shape=diamond, label=\"%s\"];\n", netID, label)
			fmt.Fprintf(b, "  n_%s -> q_%s [color=blue];\n", netID, dstID)
		} else {
			fmt.Fprintf(b, "  q_%s -> q_%s [color=blue];\n", srcID, dstID)
		}
	}
	b.WriteString("}\n")
	_, err := io.WriteString(w, b.String())
	return err
}

// sortedConnections returns the System's connections in deterministic
// sorted order (by src, srcPort, dst, dstPort).
func sortedConnections(conns []*ServiceConnect) []*ServiceConnect {
	out := make([]*ServiceConnect, len(conns))
	copy(out, conns)
	sortConns(out)
	return out
}
