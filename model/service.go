package model

import "sort"

// Port declares one directed communication boundary of a Service. A port is
// backed by a real TaskQueue inside the service's fragment: an input port
// (In == true) is drained by the service's own primitives (e.g. a Feed into
// a Processor, or a parked Worker); an output port (In == false) is drained
// by the System's boundary pass and forwarded to a connected downstream
// in-port.
//
// The Queue field is filled in by the System after the service's Build runs
// (from the port's name) on the System's stored copy of the Service, so the
// author never needs to wire it by hand.
type Port struct {
	Name  string
	In    bool
	Queue *TaskQueue
}

// Component is a model-supplied declarative NAME for an internal part of a
// Service that is not a registered generic primitive (e.g. a model-specific
// leaf type such as a MatchQueue). Components are annotations, NOT
// structure: the schematic renders them dashed and visually distinct from
// real registered nodes (queues/processors/workers/networks), so a reviewer
// can always tell "what the library derived" from "what the model named".
type Component struct {
	// Name is the component identity (sorted when rendered).
	Name string
	// Kind is a free-form type tag (e.g. "MatchQueue", "TokenBucket").
	Kind string
	// Label is a one-line human description shown in the diagram.
	Label string
}

// ComponentLink is a model-declared data-flow arc between two of the
// service's named entities (a registered port queue or a declared
// Component). Like Component, a link is an ANNOTATION: the schematic
// renders it dashed and distinct from real registered edges, so the reader
// knows it declares the model's intended flow (e.g. "actQ → actPool:
// sync-match/dispatch") rather than derived wiring. From/To reference names
// in the service's port queues or Components; unresolvable names are
// skipped (deterministically).
type ComponentLink struct {
	From  string
	To    string
	Label string
}

// Service is a VALUE describing one named unit of a distributed system: a
// declarative spec (Name/Kind/Label, plus optional declared Ports and
// optional declared Components/Flow annotations) and a Build closure that
// materializes the service's real primitives (TaskQueues, Processors,
// Workers, Networks) onto a fragment TopologyBuilder.
//
// The System materializes each Service as a child topology (path-prefixed
// substreams, independent namespace), iterating services in sorted-name
// order for determinism. Multiple Services may be defined in the same Go
// package — a Service is a value, not a package boundary.
//
// Ports are declared in the Service literal BEFORE AddService (name +
// direction) ONLY when the service's tasks genuinely cross its boundary
// through a builder queue. The Build closure receives the fragment builder
// and MAY add the service's queues/processors/workers/networks and export
// every declared port (ExportIn for in-ports, ExportOut for out-ports).
// A runtime-mediated service (whose real machinery is constructed directly
// on the simulation and driven by a tick hook — see models/temporal,
// models/tasqueue) declares NO ports: a port shell would render in the DOT
// schematic without any task traversing it ("if the code doesn't traverse
// it, the diagram doesn't draw it"). The System binds each declared Port's
// Queue to its real TaskQueue at Build time; automatic monitoring and the
// DOT schematic are derived from the BUILT fragment (never from a
// hand-maintained list), so a Service cannot silently under-report what it
// owns.
type Service struct {
	Name  string
	Kind  string
	Ports []Port
	// Label is an optional one-line human description of the service,
	// rendered as the cluster subtitle in the detailed schematic.
	Label string
	// Components lists optional declarative NAMES for the service's
	// internal parts that are not registered generic primitives. They are
	// annotations only — the detailed schematic renders them dashed and
	// distinct from real structure. Sorted by Name when rendered.
	Components []Component
	// Flow lists optional declarative data-flow arcs between the
	// service's named entities (port queues and Components). Annotations
	// only — rendered dashed/distinct from real wiring; sorted by
	// (From, To) when rendered.
	Flow  []ComponentLink
	Build func(b *TopologyBuilder) error
}

// NewService returns a Service with the given name and kind. Ports and
// Build are filled in by the caller.
func NewService(name, kind string) Service {
	return Service{Name: name, Kind: kind}
}

// Port returns the declared port with the given name, or nil if absent.
func (s *Service) Port(name string) *Port {
	for i := range s.Ports {
		if s.Ports[i].Name == name {
			return &s.Ports[i]
		}
	}
	return nil
}

// HasPort reports whether the service declares a port with the given name
// and direction.
func (s *Service) HasPort(name string, in bool) bool {
	p := s.Port(name)
	return p != nil && p.In == in
}

// ServiceConnectionName is the deterministic name of a system-level
// connection (used for the boundary network and diagnostics).
func ServiceConnectionName(src, srcPort, dst, dstPort string) string {
	return src + "." + srcPort + "->" + dst + "." + dstPort
}

// sortedServices returns the sorted names of a name→service map.
func sortedServices[S any](m map[string]S) []string {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}
