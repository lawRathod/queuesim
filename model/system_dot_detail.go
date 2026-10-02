package model

import (
	"fmt"
	"io"
	"strings"
)

// WriteDotDetail renders the full system schematic: one cluster per Service
// with the service's REGISTERED primitives and the REAL intra-service
// wiring the builder recorded — Feed edges (queue → processor), Connect
// edges (processor → queue, through a network when attached), worker polling
// edges (worker → its queue) — plus cross-service connections, per-entity
// knob annotations (queue capacity/max-wait/delay/drop, processor service
// time, network delay/drop, worker fleet size), and model-declared component
// names (dashed, distinct from real structure). No legend is rendered: the
// schematic is deliberately self-describing — shapes, edge colors, and knob
// annotations carry the meaning — so the diagram never requires a legend to
// read.
//
// Everything rendered is DERIVED from the built topology: nodes are the real
// TaskQueue/Processor/Network/Worker entities registered by the Services'
// Build closures. The only model-supplied decoration is Service.Label /
// Service.Components (declarative names for internals that are not generic
// registered primitives) — rendered dashed so a reviewer can always tell
// derived structure from declared names.
//
// Honest-schematics scope: like WriteDot, this is a STRUCTURAL-DEBUG view of
// REGISTERED builder primitives. A runtime-mediated model (models/temporal,
// models/tasqueue) builds its real machinery directly on the simulation and
// registers no ports, so its clusters render (near-)empty; the trustworthy
// architecture picture is DERIVED FROM RUNTIME CAPTURE (model/capture +
// SystemConfig.EnableCapture). Dead port
// shells / zero-cost Connect edges must not be registered to fill the
// diagram — if the code doesn't traverse it, the diagram doesn't draw it.
//
// Determinism: identical inputs → byte-identical output. All services,
// entities and connections are iterated in sorted order (registration order
// for a queue's feed processors and a processor's output edges, which are
// built deterministically). The renderer is read-only — it consumes no RNG
// and never touches the simulation, so drawing the diagram can never perturb
// a run.
//
// Requires Build (or Run) to have populated the real entities.
func (s *System) WriteDotDetail(w io.Writer) error {
	if w == nil {
		return fmt.Errorf("model: WriteDotDetail requires a non-nil writer")
	}
	if !s.built {
		return fmt.Errorf("model: WriteDotDetail requires Build before rendering")
	}
	b := &strings.Builder{}
	b.WriteString("digraph godes_system_detail {\n")
	b.WriteString("  rankdir=LR;\n")
	b.WriteString("  node [fontname=\"Helvetica\"];\n")
	b.WriteString("  edge [fontname=\"Helvetica\"];\n")
	fmt.Fprintf(b, "  graph [label=\"%s (internals)\", labelloc=t, fontname=\"Helvetica\"];\n", dotID(s.name))

	// Service clusters in sorted-name order, from the built children.
	for _, svcName := range sortedServices(s.services) {
		svc := s.services[svcName]
		child := s.topo.Child(svcName)
		clusterID := dotID(svcName)
		fmt.Fprintf(b, "  subgraph cluster_%s {\n", clusterID)
		label := svcName
		if svc.Kind != "" {
			label += "\\n[" + svc.Kind + "]"
		}
		if svc.Label != "" {
			label += "\\n" + svc.Label
		}
		fmt.Fprintf(b, "    label=\"%s\";\n", label)

		// Real queues (sorted), annotated with port boundary style + knobs. A port
		// queue (service boundary) renders filled lightblue and is labeled by NAME
		// only — the edge arrows already show direction, so "(in)/(out)" text is
		// redundant and confusing.
		for _, q := range child.sortedQueues() {
			label := q.Name()
			_, isIn := child.inPorts[q.Name()]
			_, isOut := child.outPorts[q.Name()]
			var knobs []string
			if q.cfg.Capacity > 0 {
				knobs = append(knobs, fmt.Sprintf("cap %d", q.cfg.Capacity))
			}
			if q.MaxWaitMs() > 0 {
				knobs = append(knobs, fmt.Sprintf("maxwait %dms", q.MaxWaitMs()))
			}
			if q.delayOn && q.delayMs > 0 {
				knobs = append(knobs, fmt.Sprintf("delay %dms", q.delayMs))
			}
			if q.dropRate > 0 {
				knobs = append(knobs, fmt.Sprintf("drop %.2f", q.dropRate))
			}
			if len(knobs) > 0 {
				label += "\\n[" + strings.Join(knobs, " ") + "]"
			}
			qID := queueID(svcName, q)
			if isIn || isOut {
				fmt.Fprintf(b, "    q_%s [shape=box, style=filled, fillcolor=lightblue, label=\"%s\"];\n", qID, label)
			} else {
				fmt.Fprintf(b, "    q_%s [shape=box, label=\"%s\"];\n", qID, label)
			}
		}
		// Real processors (sorted), annotated with service time/budget.
		for _, p := range child.sortedProcessors() {
			label := p.Name()
			var knobs []string
			if p.cfg.ServiceMs > 0 {
				knobs = append(knobs, fmt.Sprintf("svc %dms", p.cfg.ServiceMs))
			} else if p.cfg.ServiceRate > 0 {
				knobs = append(knobs, "rate")
			}
			if p.cfg.Budget > 0 {
				knobs = append(knobs, fmt.Sprintf("budget %d", p.cfg.Budget))
			}
			if len(knobs) > 0 {
				label += "\\n[" + strings.Join(knobs, " ") + "]"
			}
			pID := dotNodeID(svcName, p.Name())
			fmt.Fprintf(b, "    p_%s [shape=ellipse, label=\"%s\"];\n", pID, label)
		}
		// Real networks (sorted), annotated with delay/drop/jitter.
		for _, n := range child.sortedNetworks() {
			label := n.Name()
			var parts []string
			if n.cfg.DelayMs > 0 {
				parts = append(parts, fmt.Sprintf("%dms", n.cfg.DelayMs))
			}
			if n.cfg.DropRate > 0 {
				parts = append(parts, fmt.Sprintf("drop %.2f", n.cfg.DropRate))
			}
			if n.cfg.JitterMs > 0 {
				parts = append(parts, fmt.Sprintf("jitter %dms", n.cfg.JitterMs))
			}
			if len(parts) > 0 {
				label += "\\n[" + strings.Join(parts, " ") + "]"
			}
			nID := dotNodeID(svcName, n.Name())
			fmt.Fprintf(b, "    n_%s [shape=diamond, label=\"%s\"];\n", nID, label)
		}
		// Real worker fleets (sorted), annotated with fleet size/budget.
		for _, w := range child.sortedWorkers() {
			label := w.Name()
			var knobs []string
			if w.cfg.Count > 0 {
				knobs = append(knobs, fmt.Sprintf("x%d", w.cfg.Count))
			}
			if w.cfg.Budget > 0 {
				knobs = append(knobs, fmt.Sprintf("budget %d", w.cfg.Budget))
			}
			if w.cfg.ServiceMs > 0 {
				knobs = append(knobs, fmt.Sprintf("svc %dms", w.cfg.ServiceMs))
			}
			if len(knobs) > 0 {
				label += "\\n[" + strings.Join(knobs, " ") + "]"
			}
			wID := dotNodeID(svcName, w.Name())
			fmt.Fprintf(b, "    w_%s [shape=cylinder, label=\"%s\"];\n", wID, label)
		}
		// Model-declared component names (dashed — annotations, not
		// structure). Sorted by Name.
		for _, c := range sortedComponents(svc.Components) {
			label := c.Name
			if c.Label != "" {
				label += "\\n" + c.Label
			}
			if c.Kind != "" {
				label += "\\n[" + c.Kind + "]"
			}
			cID := dotID(svcName + "." + c.Name)
			fmt.Fprintf(b, "    c_%s [shape=box, style=dashed, label=\"%s\"];\n", cID, label)
		}

		// Real intra-service wiring.
		s.writeDotDetailEdges(b, svcName, child)

		// Model-declared flow arcs (dashed, gray, distinct from derived
		// wiring). From/To resolve to port queues or declared Components.
		s.writeDotDetailFlow(b, svcName, svc, child)

		b.WriteString("  }\n")
	}

	// Cross-service connections (sorted) between real out-port and in-port
	// queues, through a boundary Network when one is attached.
	for _, c := range sortedConnections(s.conns) {
		svc, ok := s.services[c.Src]
		if !ok {
			continue
		}
		p := svc.Port(c.SrcPort)
		if p == nil || p.Queue == nil {
			continue
		}
		svcDst, ok := s.services[c.Dst]
		if !ok {
			continue
		}
		pDst := svcDst.Port(c.DstPort)
		if pDst == nil || pDst.Queue == nil {
			continue
		}
		srcID := queueID(c.Src, p.Queue)
		dstID := queueID(c.Dst, pDst.Queue)
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

// writeDotDetailEdges renders the real intra-service wiring of one child:
// Feed edges (queue → processor, black), Connect edges (processor → queue,
// blue, through a network when attached), and worker polling edges
// (worker → its queue, green). Iteration is deterministic: entities in
// sorted order; a queue's feed processors and a processor's output edges in
// registration order (both built deterministically).
func (s *System) writeDotDetailEdges(b *strings.Builder, svcName string, child *Topology) {
	// Feed edges: queue → processor.
	for _, q := range child.sortedQueues() {
		qID := queueID(svcName, q)
		for _, p := range child.queueProcs[q.Name()] {
			pID := dotNodeID(svcName, p.Name())
			fmt.Fprintf(b, "    q_%s -> p_%s [color=black];\n", qID, pID)
		}
	}
	// Connect edges: processor → queue via network.
	for _, p := range child.sortedProcessors() {
		pID := dotNodeID(svcName, p.Name())
		for _, out := range p.outputs {
			dstID := queueID(svcName, out.dst)
			if out.net != nil {
				netID := dotNodeID(svcName, out.net.Name())
				fmt.Fprintf(b, "    p_%s -> n_%s [color=blue];\n", pID, netID)
				fmt.Fprintf(b, "    n_%s -> q_%s [color=blue];\n", netID, dstID)
			} else {
				fmt.Fprintf(b, "    p_%s -> q_%s [color=blue];\n", pID, dstID)
			}
		}
	}
	// Worker polling edges: worker → its queue (green).
	for _, w := range child.sortedWorkers() {
		if w.cfg.Queue == nil {
			continue
		}
		wID := dotNodeID(svcName, w.Name())
		qID := queueID(svcName, w.cfg.Queue)
		fmt.Fprintf(b, "    q_%s -> w_%s [color=green];\n", qID, wID)
	}
}

// queueID resolves a queue's deterministic DOT node id: the queue's own
// substream prefix when set (the owning service path), else the rendering
// service name. Using the queue's prefix keeps cross-service references
// (worker polls, processor outputs, connections) consistent with the
// cluster in which the queue is rendered.
func queueID(svcName string, q *TaskQueue) string {
	if p := q.SubstreamPrefix(); p != "" {
		return dotNodeID(p, q.Name())
	}
	return dotNodeID(svcName, q.Name())
}

// writeDotDetailFlow renders the service's declared ComponentLink arcs.
// From/To resolve to a port queue (q_* node) or a declared Component
// (c_* node); unresolvable names are skipped deterministically. Arcs are
// dashed/gray so declared flow never masquerades as derived wiring.
func (s *System) writeDotDetailFlow(b *strings.Builder, svcName string, svc Service, child *Topology) {
	for _, link := range sortedFlowLinks(svc.Flow) {
		fromID, fromOK := s.resolveDetailNode(svcName, link.From, child)
		toID, toOK := s.resolveDetailNode(svcName, link.To, child)
		if !fromOK || !toOK {
			continue
		}
		if link.Label != "" {
			fmt.Fprintf(b, "    %s -> %s [style=dashed, color=gray, fontcolor=gray, label=\"%s\"];\n", fromID, toID, link.Label)
		} else {
			fmt.Fprintf(b, "    %s -> %s [style=dashed, color=gray];\n", fromID, toID)
		}
	}
}

// resolveDetailNode resolves a service-level entity name to its detailed
// schematic node id: a registered queue (q_*) or a declared Component
// (c_*). Returns ok=false when the name matches neither.
func (s *System) resolveDetailNode(svcName, name string, child *Topology) (string, bool) {
	if q, ok := child.queues[name]; ok {
		return "q_" + queueID(svcName, q), true
	}
	svc := s.services[svcName]
	for _, c := range svc.Components {
		if c.Name == name {
			return "c_" + dotID(svcName+"."+c.Name), true
		}
	}
	return "", false
}

// sortedFlowLinks returns the service's declared flow arcs in deterministic
// (From, To, Label) order.
func sortedFlowLinks(links []ComponentLink) []ComponentLink {
	out := make([]ComponentLink, len(links))
	copy(out, links)
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && flowLess(out[j], out[j-1]); j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

// flowLess orders ComponentLinks by (From, To, Label).
func flowLess(a, b ComponentLink) bool {
	if a.From != b.From {
		return a.From < b.From
	}
	if a.To != b.To {
		return a.To < b.To
	}
	return a.Label < b.Label
}

// sortedComponents returns the Service's declared components in sorted
// (by Name) order for byte-identical rendering.
func sortedComponents(comps []Component) []Component {
	out := make([]Component, len(comps))
	copy(out, comps)
	// Insertion sort — tiny slices, deterministic, no allocation churn.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1].Name > out[j].Name; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}
