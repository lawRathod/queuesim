package model

import (
	"sort"

	"github.com/lawRathod/queuesim/godes/core"
)

// hookedTick is a pump hook at a deterministic position.
type hookedTick struct {
	pos float64
	fn  func(core.SimTime)
}

// Topology is a buildable, pump-driven queuing network. It owns a core
// simulation, a set of named queues and processors, links between them,
// and a pump event that advances the network deterministically.
//
// Construction is via TopologyBuilder (NewTopologyBuilder → node methods →
// Build → Run). The builder validates wiring at build time; the topology
// is read-only afterward (attach faults via DecisionVector).
type Topology struct {
	sim        *core.Simulation
	path       string
	parent     *Topology
	children   map[string]*Topology
	outPorts   map[string]*TaskQueue
	inPorts    map[string]*TaskQueue
	queues     map[string]*TaskQueue
	processors map[string]*Processor
	networks   map[string]*Network
	workers    map[string]*Worker      // worker fleets (long-poll on a queue)
	queueProcs map[string][]*Processor // queue name → processors fed by its drained tasks
	injectors  []FaultInjector
	pumpMs     int64
	endMs      int64
	vector     *DecisionVector
	// Cross-topology edges (WP2): child out-port → parent/sibling destination.
	childOutEdges map[string]map[string]*boundaryEdge // childName -> portName -> edge
	feedEdges     map[string]map[string]*TaskQueue    // childName -> portName -> srcQueue (parent → child in-port)
	hooks         []hookedTick                        // per-level custom ticks sorted by pos (2.5 reserved for Matcher)
}

type boundaryEdge struct {
	dst *TaskQueue
	net *Network
}

// TopologyBuilder accumulates nodes and links before Run.
type TopologyBuilder struct {
	sim           *core.Simulation
	path          string
	parent        *TopologyBuilder
	children      map[string]*TopologyBuilder
	outPorts      map[string]*TaskQueue
	inPorts       map[string]*TaskQueue
	queues        map[string]*TaskQueue
	processors    map[string]*Processor
	networks      map[string]*Network
	workers       map[string]*Worker
	queueProcs    map[string][]*Processor
	injectors     []FaultInjector
	pumpMs        int64
	endMs         int64
	pumpSet       bool
	endSet        bool
	vector        *DecisionVector
	childOutEdges map[string]map[string]*boundaryEdge
	feedEdges     map[string]map[string]*TaskQueue
	hooks         []hookedTick
}

// NewTopologyBuilder creates a builder over sim.
func NewTopologyBuilder(sim *core.Simulation) *TopologyBuilder {
	return &TopologyBuilder{
		sim:           sim,
		queues:        map[string]*TaskQueue{},
		processors:    map[string]*Processor{},
		networks:      map[string]*Network{},
		workers:       map[string]*Worker{},
		queueProcs:    map[string][]*Processor{},
		injectors:     []FaultInjector{},
		children:      map[string]*TopologyBuilder{},
		outPorts:      map[string]*TaskQueue{},
		inPorts:       map[string]*TaskQueue{},
		childOutEdges: map[string]map[string]*boundaryEdge{},
		feedEdges:     map[string]map[string]*TaskQueue{},
		pumpMs:        1,
	}
}

// AddQueue registers a TaskQueue by name with the given capacity. Panics
// on duplicate name or nil sim.
func (b *TopologyBuilder) AddQueue(name string, capacity int) *TopologyBuilder {
	b.checkSim()
	if name == "" {
		panic("model: AddQueue requires a non-empty name")
	}
	if _, ok := b.queues[name]; ok {
		panic("model: duplicate queue name " + name)
	}
	if _, ok := b.processors[name]; ok {
		panic("model: name collision " + name)
	}
	cfg := TaskQueueConfig{Name: name, Capacity: capacity, substreamPrefix: b.path, SubstreamPrefix: b.path}
	b.queues[name] = NewTaskQueue(b.sim, cfg)
	return b
}

// AddProcessor registers a Processor by name with the given config.
func (b *TopologyBuilder) AddProcessor(cfg ProcessorConfig) *TopologyBuilder {
	b.checkSim()
	if _, ok := b.processors[cfg.Name]; ok {
		panic("model: duplicate processor name " + cfg.Name)
	}
	if _, ok := b.queues[cfg.Name]; ok {
		panic("model: name collision " + cfg.Name)
	}
	b.processors[cfg.Name] = NewProcessor(b.sim, cfg)
	return b
}

// AddNetwork registers a Network by name with the given config.
func (b *TopologyBuilder) AddNetwork(cfg NetworkConfig) *TopologyBuilder {
	b.checkSim()
	if cfg.Name == "" {
		panic("model: AddNetwork requires a name")
	}
	if _, ok := b.networks[cfg.Name]; ok {
		panic("model: duplicate network name " + cfg.Name)
	}
	b.networks[cfg.Name] = NewNetwork(b.sim, cfg)
	return b
}

// AddWorker registers a Worker fleet (long-poll on its queue).
func (b *TopologyBuilder) AddWorker(cfg WorkerConfig) *TopologyBuilder {
	b.checkSim()
	if cfg.Name == "" {
		panic("model: AddWorker requires a name")
	}
	if _, ok := b.workers[cfg.Name]; ok {
		panic("model: duplicate worker name " + cfg.Name)
	}
	if cfg.Queue == nil {
		panic("model: AddWorker requires a queue")
	}
	b.workers[cfg.Name] = NewWorker(b.sim, cfg)
	return b
}

// Worker returns a named worker fleet.
func (b *TopologyBuilder) Worker(name string) *Worker {
	w, ok := b.workers[name]
	if !ok {
		panic("model: unknown worker " + name)
	}
	return w
}

// Processor returns a named processor node (for wiring outputs).
func (b *TopologyBuilder) Processor(name string) *Processor {
	p, ok := b.processors[name]
	if !ok {
		panic("model: unknown processor " + name)
	}
	return p
}

// Queue returns a named queue node.
func (b *TopologyBuilder) Queue(name string) *TaskQueue {
	q, ok := b.queues[name]
	if !ok {
		panic("model: unknown queue " + name)
	}
	return q
}

// Network returns a named network node.
func (b *TopologyBuilder) Network(name string) *Network {
	n, ok := b.networks[name]
	if !ok {
		panic("model: unknown network " + name)
	}
	return n
}

// Connect wires a processor's output edge to a downstream queue,
// optionally through a Network (nil = direct zero-cost edge).
func (b *TopologyBuilder) Connect(proc *Processor, q *TaskQueue, net *Network) *TopologyBuilder {
	proc.AddOutput(q, net)
	return b
}

// Feed wires a queue's drained tasks into a processor's input buffer
// (queue → processor edge). The pump enqueues drained tasks from that
// queue into the processor each boundary.
func (b *TopologyBuilder) Feed(q *TaskQueue, proc *Processor) *TopologyBuilder {
	if q == nil || proc == nil {
		panic("model: Feed requires non-nil queue and processor")
	}
	b.queueProcs[q.Name()] = append(b.queueProcs[q.Name()], proc)
	return b
}

// ExportOut registers an existing queue as an output port (AD-2: ports are
// TaskQueues). The queue must already exist via AddQueue; the port name is
// the queue's name. Panics on unknown queue, duplicate out-port, or if the
// queue is already an in-port. Build validates out-port ∩ feed-source (AD-3).
func (b *TopologyBuilder) ExportOut(name string) *TopologyBuilder {
	b.checkSim()
	q, ok := b.queues[name]
	if !ok {
		panic("model: ExportOut unknown queue " + name)
	}
	if _, dup := b.outPorts[name]; dup {
		panic("model: duplicate out-port " + name)
	}
	if _, dup := b.inPorts[name]; dup {
		panic("model: queue already exported as in-port " + name)
	}
	b.outPorts[name] = q
	return b
}

// ExportIn registers an existing queue as an input port (AD-2). The queue
// must already exist via AddQueue. Panics on unknown queue, duplicate
// in-port, or if the queue is already an out-port.
func (b *TopologyBuilder) ExportIn(name string) *TopologyBuilder {
	b.checkSim()
	q, ok := b.queues[name]
	if !ok {
		panic("model: ExportIn unknown queue " + name)
	}
	if _, dup := b.inPorts[name]; dup {
		panic("model: duplicate in-port " + name)
	}
	if _, dup := b.outPorts[name]; dup {
		panic("model: queue already exported as out-port " + name)
	}
	b.inPorts[name] = q
	return b
}

// AddChild composes a child builder under the parent with the given name.
// Panics on empty/duplicate name, nil child, self-edge, mismatched
// simulation, or child already parented. Determinism: children are stored
// in sorted name order for pump and DOT. Reference ADR-086 AD-5, AD-8.
func (b *TopologyBuilder) AddChild(name string, child *TopologyBuilder) *TopologyBuilder {
	b.checkSim()
	if name == "" {
		panic("model: AddChild requires a non-empty name")
	}
	if child == nil {
		panic("model: AddChild requires a non-nil child")
	}
	if child == b {
		panic("model: AddChild self-edge not allowed")
	}
	if _, dup := b.children[name]; dup {
		panic("model: duplicate child name " + name)
	}
	if child.sim != b.sim {
		panic("model: AddChild mismatched simulation")
	}
	if child.parent != nil {
		panic("model: child already has a parent")
	}
	child.parent = b
	b.children[name] = child
	return b
}

// isWorkerQueue reports whether a queue name is pulled by a worker in this builder (sorted for determinism).
func (b *TopologyBuilder) isWorkerQueue(name string) bool {
	for _, n := range sortedNames(b.workers) {
		w := b.workers[n]
		if w.cfg.Queue != nil && w.cfg.Queue.Name() == name {
			return true
		}
	}
	return false
}

// ConnectFromChildPort wires a child's out-port to a parent queue (or
// sibling in-port), optionally through a Network (nil = direct).
// Panics on unknown child/port, nil destination, duplicate edge, or self-edge
// (dst is the same queue as the source out-port). Determinism: edges are
// drained in sorted child/port order. Reference ADR-086 AD-2, AD-5.
func (b *TopologyBuilder) ConnectFromChildPort(childName, portName string, dst *TaskQueue, net *Network) *TopologyBuilder {
	b.checkSim()
	if dst == nil {
		panic("model: ConnectFromChildPort requires non-nil destination")
	}
	child, ok := b.children[childName]
	if !ok {
		panic("model: ConnectFromChildPort unknown child " + childName)
	}
	if _, ok := child.outPorts[portName]; !ok {
		panic("model: ConnectFromChildPort unknown out-port " + portName + " on child " + childName)
	}
	// Self-edge check: dst is the same queue as source out-port
	if dst == child.outPorts[portName] {
		panic("model: ConnectFromChildPort self-edge not allowed for " + childName + "." + portName)
	}
	if b.childOutEdges[childName] == nil {
		b.childOutEdges[childName] = map[string]*boundaryEdge{}
	}
	if _, dup := b.childOutEdges[childName][portName]; dup {
		panic("model: duplicate ConnectFromChildPort for " + childName + "." + portName)
	}
	b.childOutEdges[childName][portName] = &boundaryEdge{dst: dst, net: net}
	return b
}

// FeedChildPort wires a parent queue to a child's in-port.
// Panics on unknown child/port, nil source, duplicate edge, self-edge, or
// if src is already a feed source or worker queue (would cause double-drain,
// AD-3). Reference ADR-086 AD-2, AD-5.
func (b *TopologyBuilder) FeedChildPort(src *TaskQueue, childName, portName string) *TopologyBuilder {
	b.checkSim()
	if src == nil {
		panic("model: FeedChildPort requires non-nil source")
	}
	child, ok := b.children[childName]
	if !ok {
		panic("model: FeedChildPort unknown child " + childName)
	}
	if _, ok := child.inPorts[portName]; !ok {
		panic("model: FeedChildPort unknown in-port " + portName + " on child " + childName)
	}
	if src == child.inPorts[portName] {
		panic("model: FeedChildPort self-edge not allowed for " + childName + "." + portName)
	}
	if _, isFeed := b.queueProcs[src.Name()]; isFeed {
		panic("model: FeedChildPort source " + src.Name() + " cannot be a feed source (would double-drain)")
	}
	if b.isWorkerQueue(src.Name()) {
		panic("model: FeedChildPort source " + src.Name() + " cannot be a worker queue (would double-drain)")
	}
	if b.feedEdges[childName] == nil {
		b.feedEdges[childName] = map[string]*TaskQueue{}
	}
	if _, dup := b.feedEdges[childName][portName]; dup {
		panic("model: duplicate FeedChildPort for " + childName + "." + portName)
	}
	b.feedEdges[childName][portName] = src
	return b
}

// SetPump sets the pump cadence in sim ticks (default 1).
// Panics if called on a child builder that already has a parent (AD-1:
// only the root owns the pump); also validated at Build time for late
// mutations before AddChild.
func (b *TopologyBuilder) SetPump(ms int64) *TopologyBuilder {
	if ms <= 0 {
		panic("model: pump must be > 0")
	}
	if b.parent != nil {
		panic("model: SetPump not allowed on child builder with a parent; set pump on the root")
	}
	b.pumpMs = ms
	b.pumpSet = true
	return b
}

// FaultInjector is the minimal seam the topology pump uses to apply
// fault-injection state each tick. Implemented by faults.Injector.
type FaultInjector interface {
	Apply()
}

// Inject registers a fault Injector to be applied by the pump each tick.
func (b *TopologyBuilder) Inject(inj FaultInjector) *TopologyBuilder {
	if inj == nil {
		panic("model: Inject requires a non-nil injector")
	}
	b.injectors = append(b.injectors, inj)
	return b
}

// SetEnd sets the sim horizon in sim ticks.
// Panics if called on a child builder that already has a parent (AD-1).
func (b *TopologyBuilder) SetEnd(ms int64) *TopologyBuilder {
	if b.parent != nil {
		panic("model: SetEnd not allowed on child builder with a parent; set end on the root")
	}
	b.endMs = ms
	b.endSet = true
	return b
}

// AddHook registers a per-level custom tick invoked at pump pos 2.5
// (after parent queues/processors, before child recursion). Hooks run in
// registration order, single-goroutine, no RNG draws beyond the hook's
// own substreams. Used by v2 to drive Matcher/TimerWheel via the
// composable pump (retires the hand-wired pump). 2.5 is reserved for
// Matcher leaf drain per ADR-091.
func (b *TopologyBuilder) AddHook(fn func(core.SimTime)) *TopologyBuilder {
	return b.AddHookAt(2.5, fn)
}

// AddHookAt registers a hook at an explicit pump position in [0,8).
// Position 2.5 is reserved for Matcher leaf drain (between Processor
// drains and child recursion). Hooks are executed in sorted pos order;
// equal pos preserves registration order (stable sort).
func (b *TopologyBuilder) AddHookAt(pos float64, fn func(core.SimTime)) *TopologyBuilder {
	b.checkSim()
	if fn == nil {
		panic("model: AddHook requires non-nil func")
	}
	b.hooks = append(b.hooks, hookedTick{pos: pos, fn: fn})
	return b
}

// AddHookWithPos is an alias for AddHookAt (ADR-091 frozen signature).
func (b *TopologyBuilder) AddHookWithPos(pos float64, fn func(core.SimTime)) *TopologyBuilder {
	return b.AddHookAt(pos, fn)
}

// SubstreamPrefix returns the hierarchical prefix for this builder's
// RNG substreams ("" for root, "matching" for child, "a.b" for grandchild).
// Constructor-only; used for path-prefixed substream isolation (ADR-089/091).
func (b *TopologyBuilder) SubstreamPrefix() string { return b.path }

// WithVector attaches a fault DecisionVector to the topology.
func (b *TopologyBuilder) WithVector(v *DecisionVector) *TopologyBuilder {
	b.vector = v
	return b
}

// Build validates and returns the constructed Topology.
func (b *TopologyBuilder) Build() (*Topology, error) {
	if b.sim == nil {
		return nil, errNilSim
	}
	return b.buildRecursive("")
}

func (b *TopologyBuilder) buildRecursive(parentPath string) (*Topology, error) {
	// Compute own path.
	var myPath string
	if b.parent == nil {
		myPath = "" // root
	} else {
		// Find own name via parent's children map.
		// Use b.path if already set (via AddChild propagation), else derive.
		if b.path != "" {
			myPath = b.path
		} else {
			// fallback: shouldn't happen, but compute from parent
			myPath = parentPath
		}
	}
	// If this builder is a child, ensure its path is set correctly.
	// For root, myPath is "".
	// For children, parentPath is already the parent's path.
	// We need to ensure children get correct paths before recursing.
	// However this builder's own queues need prefix update.
	b.path = myPath
	// Validate out-port ∩ feed-source panic (AD-3 row 1)
	for portName := range b.outPorts {
		if _, isFeed := b.queueProcs[portName]; isFeed {
			panic("model: out-port " + portName + " cannot be a feed source")
		}
	}
	// Validate child pump/end not set before adoption (AD-1)
	for _, childName := range sortedNames(b.children) {
		child := b.children[childName]
		if child.pumpSet {
			panic("model: child " + childName + " has custom pump; only root may set pump (AD-1)")
		}
		if child.endSet {
			panic("model: child " + childName + " has custom end; only root may set end (AD-1)")
		}
	}
	// Update substream prefix for own queues (constructor-only, AD-4).
	for _, q := range b.queues {
		desired := myPath
		current := q.SubstreamPrefix()
		if current != desired {
			if q.rng != nil {
				panic("model: late SubstreamPrefix mutation after RNG creation for queue " + q.Name())
			}
			q.cfg.substreamPrefix = desired
			q.cfg.SubstreamPrefix = desired
		}
	}

	// Build children post-order in sorted name order.
	builtChildren := map[string]*Topology{}
	// Assign child paths before building them.
	childNames := sortedNames(b.children)
	for _, childName := range childNames {
		childBuilder := b.children[childName]
		var childPath string
		if myPath == "" {
			childPath = childName
		} else {
			childPath = myPath + "." + childName
		}
		childBuilder.path = childPath
		childTopo, err := childBuilder.buildRecursive(myPath)
		if err != nil {
			return nil, err
		}
		builtChildren[childName] = childTopo
	}

	topo := &Topology{
		sim:           b.sim,
		path:          myPath,
		children:      builtChildren,
		outPorts:      b.outPorts,
		inPorts:       b.inPorts,
		queues:        b.queues,
		processors:    b.processors,
		networks:      b.networks,
		workers:       b.workers,
		queueProcs:    b.queueProcs,
		injectors:     b.injectors,
		pumpMs:        b.pumpMs,
		endMs:         b.endMs,
		vector:        b.vector,
		childOutEdges: b.childOutEdges,
		feedEdges:     b.feedEdges,
		hooks:         b.hooks,
	}
	// Set parent pointers on built children.
	for _, childTopo := range builtChildren {
		childTopo.parent = topo
	}
	return topo, nil
}

func (b *TopologyBuilder) checkSim() {
	if b.sim == nil {
		panic("model: TopologyBuilder requires a non-nil simulation")
	}
}

type buildError struct{ msg string }

func (e *buildError) Error() string { return e.msg }

var errNilSim = &buildError{msg: "model: TopologyBuilder requires a non-nil simulation"}

// ErrNilSim is returned by Build when the builder has a nil simulation.
var ErrNilSim = errNilSim

// Path returns the hierarchical path of the topology ("" for root).
// For a child named "frontend" under root, Path returns "frontend";
// for a grandchild "matching" under "frontend", it returns
// "frontend.matching".
func (t *Topology) Path() string { return t.path }

// Child returns a named child topology. Panics on unknown child.
func (t *Topology) Child(name string) *Topology {
	c, ok := t.children[name]
	if !ok {
		panic("model: unknown child " + name)
	}
	return c
}

// IsRoot reports whether the topology is the root (no parent).
func (t *Topology) IsRoot() bool { return t.parent == nil }

// Run drives the topology to its end horizon (or default 1e6 if unset).
// Panics if called on a child topology that has a parent (AD-1: only the
// root owns the pump; child Run must panic before scheduling any events,
// with message containing "has a parent; run the parent").
func (t *Topology) Run() {
	if t.sim == nil {
		panic("model: nil simulation in Run")
	}
	if t.parent != nil {
		panic("model: child topology has a parent; run the parent")
	}
	end := t.endMs
	if end <= 0 {
		end = 1_000_000
	}
	t.sim.ScheduleIn(t.pumpMs, func(s *core.Simulation) {
		t.pump(end)
	})
	t.sim.Run()
}

// pump drains every queue and network to now, ticks processors, applies
// vector knobs, and self-reschedules until end.
func (t *Topology) pump(end int64) {
	now := t.sim.Now()
	t.pumpNode(now, end)
	if now.Int64() >= end {
		return
	}
	t.sim.ScheduleIn(t.pumpMs, func(s *core.Simulation) {
		t.pump(end)
	})
}

// pumpNode is the recursive per-level drain implementing ADR-086 AD-1.
// Order per level (AD-1 invariant): (1) injectors/vector per level during
// recursion, (2) parent queues, (3) parent processors, (4) child recursion
// (each child: in-port drains → internal pass → out-port transmits),
// (5) boundary-network drain (implemented as the next tick's network drain
// at the start of pumpNode, giving exactly one-tick boundary latency),
// (6) worker park, (7) reschedule. One-tick cross-boundary vs same-tick
// intra-topology (queue→processor) asymmetry is documented in ADR-086.
func (t *Topology) pumpNode(now core.SimTime, end int64) {
	t.applyVectorAt(now.Int64())
	for _, inj := range t.injectors {
		inj.Apply()
	}
	// Build outPortSet for skipping child's own drain of out-ports?
	// For this node's own queues: drain those that feed processors and are
	// not out-ports? Actually parent queues: out-ports are drained by
	// parent boundary, but for leaf node, out-ports should be skipped in
	// its own drain. However for root, outPorts are destinations? Root
	// has no parent, but if root has outPorts (unlikely), they would be
	// boundary? For now, handle general: skip outPortSet in own drain,
	// boundary will handle them.
	outPortSet := t.outPorts
	// Drain networks - deterministic. For each network, forward drained
	// tasks to their destination queues (intra-processor outputs or
	// boundary edges). This handles both intra and parent-level boundary
	// networks at the start of the next tick.
	for _, n := range t.sortedNetworks() {
		delivered := n.DrainToTick(now)
		if len(delivered) == 0 {
			continue
		}
		// Intra: processor outputs using this network
		for _, p := range t.sortedProcessors() {
			for _, out := range p.outputs {
				if out.net == n {
					for _, task := range delivered {
						out.dst.AddAt(task, now)
					}
				}
			}
		}
		// Boundary: child out-port edges using this network (sorted for determinism)
		for _, childName := range sortedNames(t.childOutEdges) {
			edgeMap := t.childOutEdges[childName]
			for _, portName := range sortedNames(edgeMap) {
				edge := edgeMap[portName]
				if edge.net == n {
					for _, task := range delivered {
						edge.dst.AddAt(task, now)
					}
				}
			}
		}
	}
	for _, q := range t.sortedQueues() {
		if _, isOut := outPortSet[q.Name()]; isOut {
			continue
		}
		if _, feeds := t.queueProcs[q.Name()]; !feeds || t.workerQueue(q.Name()) {
			continue
		}
		// In-port ∩ worker-queue allowed: worker drains first, in-port pass skips if workerQueue
		// Already handled by workerQueue check: skip if workerQueue.
		// For in-port that is also workerQueue, we skip here (worker path did it).
		// For out-port ∩ worker-queue: allowed, worker path first, boundary reads remainder.
		// So out-port skip above means we don't drain here; boundary will handle remainder.
		delivered, _, _ := q.DrainToTick(now)
		if len(delivered) > 0 {
			for _, proc := range t.queueProcs[q.Name()] {
				proc.Enqueue(delivered)
			}
		}
	}
	for _, p := range t.sortedProcessors() {
		p.Tick(now)
	}
	// Per-level hooks sorted by pos — 2.5 reserved for Matcher (after
	// Processors, before child recursion). Stable sort preserves registration
	// order at equal pos.
	for _, h := range t.sortedHooks() {
		h.fn(now)
	}
	// Child recursion in sorted name order.
	for _, childName := range sortedNames(t.children) {
		child := t.children[childName]
		child.pumpNode(now, end)
	}
	// Boundary pass: drain each child's out-ports in sorted order and forward.
	for _, childName := range sortedNames(t.children) {
		child := t.children[childName]
		// Sorted port names for determinism.
		portNames := sortedNames(child.outPorts)
		for _, portName := range portNames {
			portQ := child.outPorts[portName]
			edge := t.childOutEdges[childName][portName]
			if edge == nil {
				// No edge: still drain to prevent accumulation? But spec says out-port drained by parent; if no edge, tasks stay? We'll drain and drop.
				portQ.DrainToTick(now)
				continue
			}
			// Respect AD-3: out-port ∩ worker-queue allowed, worker path already drained inside child.pumpNode (workers parked there). Here we drain remainder.
			// For out-port ∩ feed-source, Build already panicked.
			delivered, _, _ := portQ.DrainToTick(now)
			if len(delivered) == 0 {
				continue
			}
			if edge.net != nil {
				for _, task := range delivered {
					edge.net.Transmit(task, now)
				}
			} else {
				for _, task := range delivered {
					edge.dst.AddAt(task, now)
				}
			}
		}
		// Feed edges: parent → child in-port. Drain src queue and forward to child in-port.
		// For determinism, process feedEdges in sorted port order.
		if feedMap, ok := t.feedEdges[childName]; ok {
			for _, portName := range sortedNames(feedMap) {
				srcQ := feedMap[portName]
				// srcQ is a parent queue; it may have been already drained in parent queue pass above if it feeds processors.
				// But for FeedChildPort, srcQ should not be a feed source? Actually it's a parent queue that we want to forward remainder.
				// We need to handle similarly: if src was already drained as part of parent queue pass, its delivered tasks would have been fed to processors.
				// For FeedChildPort, the src queue's drained tasks should be forwarded to child in-port instead of/in addition to processor feed?
				// Spec says Feed wires queue→processor, but FeedChildPort is parent→child in-port. The src queue may be a parent queue that otherwise has no processor feed.
				// To avoid double-drain, we check if src was already drained: we drained parent queues only if they were in queueProcs. If src is not in queueProcs, it was skipped before, so we can drain now.
				// If src is in queueProcs, it was already drained, so we would double-drain. But AD-3 says out-port ∩ feed-source panic; for feedEdges, src is parent queue, dst is child in-port. If src is also a feed source (queue→processor), would that be double-drain? Not specified, but we should handle by draining remainder only if not already drained.
				dstQ := child.inPorts[portName]
				// If src was already drained and had workerQueue skip, we should not double drain.
				// Check if src was drained in parent queue loop: it was drained if feede and not workerQueue and not outPort. So detect.
				alreadyDrained := false
				if _, feeds := t.queueProcs[srcQ.Name()]; feeds && !t.workerQueue(srcQ.Name()) {
					if _, isOut := outPortSet[srcQ.Name()]; !isOut {
						alreadyDrained = true
					}
				}
				if alreadyDrained {
					// Already drained; its tasks went to processors; boundary feed has no remainder to forward.
					continue
				}
				delivered, _, _ := srcQ.DrainToTick(now)
				if len(delivered) > 0 {
					for _, task := range delivered {
						dstQ.AddAt(task, now)
					}
				}
			}
		}
	}
	// Boundary-network drain at position (5): drain parent networks that are boundary edges.
	// For now, drain all parent networks again? But we already drained networks at top. Need to drain boundary networks that received Transmit in this tick's boundary pass.
	// Those networks' due may be now + delay; if delay 0, they are due now, but we already drained networks before boundary pass, so they would not be delivered until next tick.
	// To respect one-tick boundary latency, we should not drain them immediately in same tick; they'll be drained at next parent tick's network drain phase.
	// So we do not drain boundary networks here same tick; leave for next tick.
	// However spec says boundary-network drain at position (5) - maybe they intend to drain boundary networks in same tick after forwarding?
	// For one-tick latency, we want next tick delivery, so we should not drain immediately.
	// We'll keep as no-op for now; boundary networks will be drained at next pumpNode's network drain phase.

	// Park worker fleets (long-poll) for this level.
	for _, w := range t.sortedWorkers() {
		w.ParkUpTo(now.Int64())
	}
}

// applyVectorAt applies the active DecisionVector's knobs to the topology's
// queues at sim time now. Currently supports drop-rate kinds applied to all
// queues (add_drop), and MaxWaitMs (s2s_timeout) applied to all queues.
// Other kinds are topology-specific and out of the general builder's scope.
func (t *Topology) applyVectorAt(now int64) {
	v := t.vector
	if v == nil {
		return
	}
	for _, m := range v.Members() {
		active := m.ActiveAt(now)
		switch m.Kind {
		case KindAddDrop:
			for _, q := range t.sortedQueues() {
				if active {
					q.SetDropRate(m.Rate)
				} else {
					q.SetDropRate(0)
				}
			}
			// Also apply to child queues recursively? No, per-level vector applied at each node's pumpNode.
			// Child's own pumpNode will apply its vector.
		case KindS2STimeout:
			for _, q := range t.sortedQueues() {
				if active {
					q.SetMaxWaitMs(int64(m.Rate))
				} else {
					q.SetMaxWaitMs(0)
				}
			}
		}
	}
}

// SortedName helper: deterministic iteration over maps by sorted key.
func sortedNames[V any](m map[string]V) []string {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}

func (t *Topology) sortedQueues() []*TaskQueue {
	out := make([]*TaskQueue, 0, len(t.queues))
	for _, n := range sortedNames(t.queues) {
		out = append(out, t.queues[n])
	}
	return out
}

func (t *Topology) sortedProcessors() []*Processor {
	out := make([]*Processor, 0, len(t.processors))
	for _, n := range sortedNames(t.processors) {
		out = append(out, t.processors[n])
	}
	return out
}

func (t *Topology) sortedNetworks() []*Network {
	out := make([]*Network, 0, len(t.networks))
	for _, n := range sortedNames(t.networks) {
		out = append(out, t.networks[n])
	}
	return out
}

func (t *Topology) sortedWorkers() []*Worker {
	out := make([]*Worker, 0, len(t.workers))
	for _, n := range sortedNames(t.workers) {
		out = append(out, t.workers[n])
	}
	return out
}

func (t *Topology) sortedHooks() []hookedTick {
	out := make([]hookedTick, len(t.hooks))
	copy(out, t.hooks)
	// Stable sort by pos to preserve registration order at equal pos.
	// Simple insertion sort (hooks small) for determinism without extra import.
	for i := 1; i < len(out); i++ {
		j := i
		for j > 0 && out[j].pos < out[j-1].pos {
			out[j], out[j-1] = out[j-1], out[j]
			j--
		}
	}
	return out
}

// workerQueue reports whether a queue is pulled by a worker (dispatch via
// sync-match, not processor feed).
func (t *Topology) workerQueue(name string) bool {
	for _, w := range t.sortedWorkers() {
		if w.cfg.Queue != nil && w.cfg.Queue.Name() == name {
			return true
		}
	}
	return false
}

// QueueCount returns the number of queues in the topology.
func (t *Topology) QueueCount() int { return len(t.queues) }

// ProcessorCount returns the number of processors.
func (t *Topology) ProcessorCount() int { return len(t.processors) }

// NetworkCount returns the number of networks.
func (t *Topology) NetworkCount() int { return len(t.networks) }

// Sim returns the underlying simulation.
func (t *Topology) Sim() *core.Simulation { return t.sim }

// Queue returns a named queue.
func (t *Topology) Queue(name string) *TaskQueue {
	q, ok := t.queues[name]
	if !ok {
		panic("model: unknown queue " + name)
	}
	return q
}

// Processor returns a named processor.
func (t *Topology) Processor(name string) *Processor {
	p, ok := t.processors[name]
	if !ok {
		panic("model: unknown processor " + name)
	}
	return p
}

// Network returns a named network.
func (t *Topology) Network(name string) *Network {
	n, ok := t.networks[name]
	if !ok {
		panic("model: unknown network " + name)
	}
	return n
}
