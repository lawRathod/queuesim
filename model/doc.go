// Package model is the GoDES modeling library: general, deterministic
// primitives for building queue-network DES models of distributed systems,
// plus a first-class authoring surface (Service/System) that turns those
// primitives into runnable, observable models.
//
// # Pillar one — primitives
//
// The general building blocks: a buffered TaskQueue (capacity/drop/timeout
// semantics), a Processor (queue→worker→queue stage), a lossy/delayed
// Network, an automaton Poller and its Worker fleet, a RetryController, a
// Topology builder for wiring primitives into arbitrary queuing graphs, and
// the stochastic distributions in core/dists plus the core/process and
// core/resource modeling packages. Fault injection is first-class via the
// faults subpackage (declarative, time-windowed, composable) and the
// DecisionVector search DSL (ADR-069), which share these contracts with the
// search and confirm legs.
//
// # Pillar two — Service/System authoring
//
// A Service (model/service.go) is a named unit of a distributed system with
// declared in/out ports and a Build closure that materializes its real
// primitives onto a fragment topology. A System (model/system.go) glues
// Services together: it materializes each as a path-prefixed child
// topology, wires cross-service connections through their real port queues,
// applies fault injectors, and OWNS the core.Simulation it drives with a
// single-shot Run. The pattern is: define Service values with ports →
// System.AddService/Connect → Build → Run → read the Result.Metrics and
// WriteDot schematic. Concrete walkthroughs live in the models/ directory
// (models/temporal, models/tasqueue) and in examples/models.
//
// Honest-scope note: WriteDot renders REGISTERED builder primitives only.
// A model whose real dynamics are runtime-mediated (constructed directly on
// the simulation and driven by a tick hook, e.g. models/temporal and
// models/tasqueue) should NOT declare port shells + Connect edges for the
// schematic — they would render without a task ever traversing them. Such
// models pair their structural-debug DOT with a mechanism map DERIVED FROM
// RUNTIME CAPTURE (model/capture + SystemConfig.EnableCapture): the model
// records transitions at its real chokepoints and capture.Aggregate folds
// them into a map that shows only what actually traversed.
//
// # Free monitoring and free schematic
//
// Because the System derives them from the BUILT service graph (never a
// hand-maintained list), every System automatically carries zero-config
// monitoring (model/monitor: queue-depth gauges, throughput counters,
// latency percentile histograms; extend via AddCounter/AddGauge/
// AddHistogram) and a free DOT schematic (System.WriteDot renders the real
// service graph — the registered ports/queues/processors/workers and
// cross-service edges actually wired through them; no dummy registrations).
// Generated DOT is a structural-debug view of REGISTERED builder
// primitives: for runtime-mediated models it renders only empty service
// shells, which is exactly why the honest-schematics discipline derives the
// thesis architecture figure from observed behavior instead: a model opts
// into runtime capture (SystemConfig.EnableCapture) and the mechanism-map
// figure is rendered from what the model executed — never hand-drawn from
// a component table.
//
// # Determinism
//
// Same model + same seed → bit-identical run. Single-goroutine, pump-driven;
// no wall-clock (time.Now is forbidden on simulation paths); no map
// iteration (services/queues/workers iterated in sorted order); RNG only via
// core.RNG substreams (path-prefixed). The free monitor makes zero RNG
// draws and samples in sorted order, so monitoring on/off is bit-identical.
//
// Experiment-analysis contracts shared between the model search leg and the
// bubble confirm leg — dose-response curve classification (ADR-074), dose-
// frontier wire formats, and recovery classification (ADR-073) — live in the
// analysis package, not here.
package model
