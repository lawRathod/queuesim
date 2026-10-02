package model

import (
	"fmt"
	"sort"

	"github.com/law/GoDES/core"
	"github.com/law/GoDES/core/dists"
	"github.com/lawRathod/queuesim/model/capture"
	"github.com/lawRathod/queuesim/model/monitor"
)

// SystemConfig configures a System. Zero-value knobs default sensibly (see
// each field). Monitoring is ON by default and is determinism-neutral.
type SystemConfig struct {
	// Seed seeds the System-owned core.Simulation. Only used when the
	// System creates its own sim (i.e. WithSim was not called). 0 is a
	// valid seed.
	Seed int64
	// PumpMs is the pump cadence in sim ticks (0 → 1).
	PumpMs int64
	// SimEndMs is the sim horizon in sim ticks (0 → 1_000_000).
	SimEndMs int64
	// DisableMonitor turns OFF the automatic per-service monitoring.
	// Monitoring is read-only and never perturbs the simulation, so an
	// on/off system yields a bit-identical simulation trace.
	DisableMonitor bool
	// EnableCapture turns ON the runtime mechanism recorder (a nil
	// Recorder is exposed when disabled). Capture is observational only:
	// it performs zero RNG draws, zero I/O inside Run, and never
	// registers mechanisms as topology or monitor nodes — so capture
	// on/off leaves the simulation trace and digest byte-identical.
	EnableCapture bool
}

// ServiceConnect records one declared cross-service connection. Wired at
// Build time from the Source service's out-port to the Destination
// service's in-port, optionally through a lossy/delayed Network.
type ServiceConnect struct {
	Src     string
	SrcPort string
	Dst     string
	DstPort string
	Net     NetworkConfig
}

// System glues Services together and OWNS the core.Simulation: it
// materializes each Service as a child topology (path-prefixed substreams),
// wires cross-service connections between real port queues, applies
// injectors, and drives the deterministic pump. It provides free monitoring
// (per-service queue depth, throughput, percentile histograms — zero
// config) and a free DOT schematic (WriteDot rendered from the built
// service fragments — REGISTERED primitives only, no dummy registrations).
// Honest-schematics scope: the schematic draws only what the Services
// actually registered and traversed; a runtime-mediated model that
// registers no ports (models/temporal, models/tasqueue) renders empty
// service clusters, and its trustworthy architecture picture is DERIVED
// FROM RUNTIME CAPTURE (model/capture + SystemConfig.EnableCapture — see
// model/capture doc) — never fake the diagram with dead shells.
//
// Determinism contract: single-goroutine, pump-driven; no wall-clock; no
// map iteration (services iterated in sorted-name order); RNG only via
// core.RNG substreams; the monitor makes zero RNG draws and samples in
// sorted order — monitoring on/off is bit-identical.
type System struct {
	name     string
	cfg      SystemConfig
	services map[string]Service
	conns    []*ServiceConnect
	faults   []FaultInjector
	ticks    []func(core.SimTime)

	sim   *core.Simulation
	root  *TopologyBuilder
	topo  *Topology
	mon   *monitor.Monitor
	cap   *capture.Recorder
	built bool
	ran   bool
}

// NewSystem creates a System with the given name and config. The name
// prefixes the owned simulation's path-derived substreams, so distinct
// systems are independent.
func NewSystem(name string, cfg SystemConfig) *System {
	return &System{
		name:     name,
		cfg:      cfg,
		services: map[string]Service{},
	}
}

// AddService registers a Service (by value — a Service is a VALUE). Services
// are materialized in sorted-name order (deterministic). The System stores
// its own copy and binds each declared Port's backing queue at Build time.
// Panics on empty or duplicate name, or a nil Build.
func (s *System) AddService(svc Service) *System {
	if svc.Name == "" {
		panic("model: AddService requires a service with a non-empty name")
	}
	if svc.Build == nil {
		panic("model: AddService requires a service with a Build func")
	}
	if _, dup := s.services[svc.Name]; dup {
		panic("model: duplicate service name " + svc.Name)
	}
	s.services[svc.Name] = svc
	return s
}

// Connect declares a cross-service connection from src's out-port srcPort
// to dst's in-port dstPort, optionally through a lossy/delayed Network. A
// zero-value net is a direct zero-cost edge. Panics on invalid network
// config (negative delays, DropRate outside [0,1]). Connections are wired
// at Build time in a deterministic (sorted) order.
func (s *System) Connect(src, srcPort, dst, dstPort string, net NetworkConfig) *System {
	if net.DelayMs < 0 || net.JitterMs < 0 || net.DropRate < 0 || net.DropRate > 1 {
		panic("model: Connect requires a valid NetworkConfig (non-negative delays, DropRate in [0,1])")
	}
	if net.Name != "" && net.DelayMs == 0 && net.DropRate == 0 && net.JitterMs == 0 {
		panic("model: Connect network %q has a name but no delay/drop/jitter (use the zero value for a direct edge)")
	}
	s.conns = append(s.conns, &ServiceConnect{
		Src: src, SrcPort: srcPort, Dst: dst, DstPort: dstPort, Net: net,
	})
	return s
}

// AddFault registers a fault Injector applied by the pump each tick. The
// injector must have been constructed against the System's simulation (and,
// for queue-target faults, the target queue — see the System's sim escapes
// for post-Build access). Determinism-neutral when the injector has no
// active window.
func (s *System) AddFault(inj FaultInjector) *System {
	if inj == nil {
		panic("model: AddFault requires a non-nil injector")
	}
	s.faults = append(s.faults, inj)
	return s
}

// AddTickHook registers a read-only per-pump-tick observer hook (an escape
// hatch for tests and future visualization). Hooks run after the monitor
// sample in registration order; they must not mutate the simulation.
func (s *System) AddTickHook(fn func(core.SimTime)) *System {
	if fn == nil {
		panic("model: AddTickHook requires a non-nil func")
	}
	s.ticks = append(s.ticks, fn)
	return s
}

// WithSim is the escape hatch: substitute an externally-created
// core.Simulation (e.g. to inspect it, or to pre-seed events from the
// caller's own sim reference). Must be called before Build. Returns the
// System for chaining. Deterministic when given a fixed-seed sim.
func (s *System) WithSim(sim *core.Simulation) *System {
	if s.built {
		panic("model: WithSim must be called before Build")
	}
	s.sim = sim
	return s
}

// Build materializes the System: creates (or adopts) the sim, builds each
// service as a child topology, binds declared ports, wires connections,
// registers automatic monitoring, and builds the root topology. Safe to
// call once.
func (s *System) Build() error {
	if s.built {
		return fmt.Errorf("model: System %q already built", s.name)
	}
	if s.sim == nil {
		sim, err := core.NewSimulation(s.cfg.Seed, core.FIFOPolicy{}) //nolint:forbidigo // System OWNS the sim (ADR-093 supersedes ADR-091 authoring surface)
		if err != nil {
			return fmt.Errorf("model: NewSimulation: %w", err)
		}
		s.sim = sim
	}

	root := NewTopologyBuilder(s.sim)
	s.root = root

	// Materialize each service as a child builder in sorted-name order and
	// bind each declared port's backing queue from its fragment.
	for _, name := range sortedServices(s.services) {
		svc := s.services[name]
		child := NewTopologyBuilder(s.sim)
		if err := svc.Build(child); err != nil {
			return fmt.Errorf("model: build service %q: %w", name, err)
		}
		for i := range svc.Ports {
			svc.Ports[i].Queue = child.Queue(svc.Ports[i].Name)
		}
		s.services[name] = svc
		root.AddChild(name, child)
	}

	// Wire cross-service connections in deterministic order.
	if err := s.wireConnections(); err != nil {
		return err
	}

	// Fault injectors.
	for _, inj := range s.faults {
		root.Inject(inj)
	}

	// Automatic monitoring: the Sample hook must be on the root builder
	// before Build, but the metric closures only read the BUILT children
	// (registered after root.Build).
	if !s.cfg.DisableMonitor {
		s.mon = monitor.New()
		root.AddHook(func(now core.SimTime) { s.mon.Sample() })
	}
	// Runtime mechanism capture: created when enabled so a model can
	// record transitions into it during Run. Observational only — never
	// registered as a topology node or monitor metric.
	if s.cfg.EnableCapture {
		s.cap = capture.NewRecorder()
	}
	// Author tick hooks (after the monitor sample, registration order).
	for _, h := range s.ticks {
		root.AddHook(h)
	}

	// Pump cadence and horizon.
	pump := s.cfg.PumpMs
	if pump <= 0 {
		pump = 1
	}
	root.SetPump(pump)
	end := s.cfg.SimEndMs
	if end <= 0 {
		end = 1_000_000
	}
	root.SetEnd(end)

	topo, err := root.Build()
	if err != nil {
		return err
	}
	s.topo = topo

	// Zero-config monitoring, derived from the REAL built services.
	if s.mon != nil {
		s.registerAutomaticMetrics()
	}
	s.built = true
	return nil
}

// wireConnections wires each declared connection from a child's out-port to
// the destination child's in-port, optionally through a lossy/delayed
// boundary Network. Connections are processed in a sorted order for
// determinism.
func (s *System) wireConnections() error {
	for _, c := range sortedConnections(s.conns) {
		srcChild, ok := s.root.children[c.Src]
		if !ok {
			return fmt.Errorf("model: connect from unknown service %q", c.Src)
		}
		dstChild, ok := s.root.children[c.Dst]
		if !ok {
			return fmt.Errorf("model: connect to unknown service %q", c.Dst)
		}
		if _, ok := srcChild.outPorts[c.SrcPort]; !ok {
			return fmt.Errorf("model: service %q has no out-port %q", c.Src, c.SrcPort)
		}
		if _, ok := dstChild.inPorts[c.DstPort]; !ok {
			return fmt.Errorf("model: service %q has no in-port %q", c.Dst, c.DstPort)
		}
		dst := dstChild.Queue(c.DstPort)

		var net *Network
		if needsNetwork(c.Net) {
			netCfg, err := s.prepareBoundaryNetwork(c)
			if err != nil {
				return err
			}
			s.root.AddNetwork(netCfg)
			net = s.root.Network(netCfg.Name)
		}
		s.root.ConnectFromChildPort(c.Src, c.SrcPort, dst, net)
	}
	return nil
}

// needsNetwork reports whether a connection config represents a real
// (lossy/delayed) link rather than a direct zero-cost edge.
func needsNetwork(cfg NetworkConfig) bool {
	return cfg.DelayMs > 0 || cfg.DropRate > 0 || cfg.JitterMs > 0
}

// boundaryNetName resolves the deterministic name of a connection's
// boundary Network: the author's explicit name when provided, else a name
// derived from the connection endpoints. Shared by wiring and schematics so
// the DOT diamond always matches the real network.
func boundaryNetName(c *ServiceConnect) string {
	if c.Net.Name != "" {
		return c.Net.Name
	}
	return "net_" + dotID(ServiceConnectionName(c.Src, c.SrcPort, c.Dst, c.DstPort))
}

// prepareBoundaryNetwork assigns a deterministic name and (when a drop or
// jitter draw is required) a path-prefixed deterministic LinkSrc substream
// for a connection's boundary Network.
func (s *System) prepareBoundaryNetwork(c *ServiceConnect) (NetworkConfig, error) {
	cfg := c.Net
	cfg.Name = boundaryNetName(c)
	if s.root.networks[cfg.Name] != nil {
		return NetworkConfig{}, fmt.Errorf("model: duplicate network name %q for connection", cfg.Name)
	}
	if (cfg.DropRate > 0 || cfg.JitterMs > 0) && cfg.LinkSrc == nil {
		cfg.LinkSrc = dists.FuncSource(s.sim.Substream("model." + s.name + ".net." + cfg.Name).Float64)
	}
	return cfg, nil
}

// registerAutomaticMetrics wires the zero-config per-service queue-depth
// gauges and throughput counters into the monitor. The metric closures read
// the BUILT child topologies directly (the real entities), iterating
// services/queues/workers/processors in sorted order — deterministic,
// sim-neutral, and immune to under-reporting.
func (s *System) registerAutomaticMetrics() {
	for _, svcName := range sortedServices(s.services) {
		child := s.topo.Child(svcName)
		for _, qName := range sortedNames(child.queues) {
			q := child.queues[qName]
			s.mon.AddGauge(svcName+"."+qName+".depth", func() int64 { return int64(q.Len()) })
		}
		for _, wName := range sortedNames(child.workers) {
			w := child.workers[wName]
			s.mon.AddCounter(svcName+"."+wName+".completed", func() uint64 { return w.Completed() })
		}
		for _, pName := range sortedNames(child.processors) {
			p := child.processors[pName]
			s.mon.AddCounter(svcName+"."+pName+".served", func() uint64 { return p.Served() })
		}
	}
}

// Run drives the System to its horizon and finalizes metrics. If Build has
// not run, the System is built first. Seeding between Build and Run is
// supported: call Build() explicitly, seed the service port queues (via
// sys.Queue), then call Run(). A System may be run exactly once.
//
// Metrics finalization: after the pump stops and the owned simulation runs
// to queue exhaustion (events scheduled past the horizon still fire), one
// final monitor sample records that tail — counters and histograms cover
// the WHOLE trace, not just the pump window.
func (s *System) Run() Result {
	if s.ran {
		panic("model: System already ran")
	}
	if !s.built {
		if err := s.Build(); err != nil {
			panic(err)
		}
	}
	s.ran = true
	s.topo.Run()
	res := Result{SimEndMs: s.sim.Now().Int64()}
	if s.mon != nil {
		// Final sample at true exhaustion (the last pump tick precedes the
		// tail events): drains pending histogram batches and refreshes
		// counters/gauges to their final state. Read-only and
		// deterministic — the simulation is already complete.
		s.mon.Sample()
		res.Metrics = s.mon.Summary()
		res.Digest = s.mon.Digest()
	}
	return res
}

// Result is the finalized outcome of a System run: the ending sim time and
// the deterministic monitor metrics (empty when monitoring is disabled).
type Result struct {
	SimEndMs int64
	Metrics  monitor.Metrics
	Digest   string
}

// Monitor returns the System's monitor (nil until Build, and nil when
// monitoring is disabled).
func (s *System) Monitor() *monitor.Monitor { return s.mon }

// Recorder returns the System's runtime mechanism recorder. It is nil
// until Build, and nil when capture is disabled (SystemConfig.EnableCapture
// false — the byte-neutral default). A model records transitions into it
// during Run and the caller flushes the buffer (capture.Recorder.FlushTo)
// after Run; capture never performs I/O inside Run.
func (s *System) Recorder() *capture.Recorder { return s.cap }

// Sim returns the System-owned simulation.
func (s *System) Sim() *core.Simulation { return s.sim }

// Topology returns the built root topology (nil until Build).
func (s *System) Topology() *Topology { return s.topo }

// Service returns the System's stored service by name (a copy of the
// registered value; zero Service if absent).
func (s *System) Service(name string) Service { return s.services[name] }

// Queue returns a service port's backing queue by port name (for seeding
// and inspection). Panics if the port is unknown or not yet built.
func (s *System) Queue(svcName, portName string) *TaskQueue {
	svc, ok := s.services[svcName]
	if !ok {
		panic("model: unknown service " + svcName)
	}
	p := svc.Port(portName)
	if p == nil {
		panic("model: unknown port " + portName + " on service " + svcName)
	}
	if p.Queue == nil {
		panic("model: port queue not bound; call Build before Queue")
	}
	return p.Queue
}

// Worker returns a worker fleet by service and fleet name (for inspection).
// Panics if the service or fleet is unknown, or if the System is not built.
func (s *System) Worker(svcName, workerName string) *Worker {
	if !s.built {
		panic("model: Worker requires Build")
	}
	child := s.topo.Child(svcName)
	w, ok := child.workers[workerName]
	if !ok {
		panic("model: unknown worker " + workerName + " on service " + svcName)
	}
	return w
}

// --- deterministic slice sorters (names are the source of truth) ---

// sortConns orders connections by (Src, SrcPort, Dst, DstPort) for
// deterministic wiring and rendering.
func sortConns(conns []*ServiceConnect) {
	sort.SliceStable(conns, func(i, j int) bool {
		a, b := conns[i], conns[j]
		if a.Src != b.Src {
			return a.Src < b.Src
		}
		if a.SrcPort != b.SrcPort {
			return a.SrcPort < b.SrcPort
		}
		if a.Dst != b.Dst {
			return a.Dst < b.Dst
		}
		return a.DstPort < b.DstPort
	})
}
