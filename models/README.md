# GoDES concrete models

This directory holds **concrete models** of real distributed systems, built
_on_ the GoDES modeling library. The split is:

- `model/` (singular) — the **modeling library**: general primitives
  (TaskQueue, Processor, Network, Worker, Poller, RetryController, faults,
  plus `core/dists`, `core/process`, `core/resource`) and the Service/System
  authoring surface, the free monitor, and the free DOT schematic.
  Deterministic by default.
- `models/` (plural, this directory) — **concrete, mechanism-faithful
  models**. Each picks up monitoring for free by using the library; the
  models in this directory are runtime-mediated (real dynamics constructed
  directly on the simulation and driven by a per-pump tick hook), so their
  generated DOT is deliberately sparse and structural-debug only — the
  trustworthy architecture picture for each model is the mechanism map
  DERIVED FROM RUNTIME CAPTURE (set `Config.Capture`, run, then
  `capture.Aggregate` the recorder's events into a map that shows only what
  actually traversed). Each model owns only its model logic.

## The models

- `temporal/` — Temporal's task-queue machinery (matching, history) as two
  Services; mechanism-faithful to the `temporal/` upstream submodule, with
  every constant pinned to a source `file:line` (`contract_test.go`).
  The model deliberately registers NO builder ports / `Connect` edges: its
  real dynamics are runtime-mediated (MatchQueues, poller fleets, history
  shards, sticky queue, transfer/timer/reschedule loop), so the DOT service
  graph is an empty-cluster structural-debug artifact. The architecture
  picture is DERIVED FROM OBSERVED BEHAVIOR — `Config.Capture` records
  every mechanism transition (load/history/matching/exec) and the
  mechanism-map figure renders from what actually traversed the model.

  Demo: `go run ./examples/models/temporal_metrics`
  (calm vs activity-failure storm: metrics + schematic note).
  Capture demo: `go run ./examples/models/mechanism_map`
  (calm vs storm: derived mechanism map → JSONL/DOT/SVG).

- `tasqueue/` — the Tasqueue job queue (queue/broker + worker services);
  mechanism-faithful to the `Tasqueue/` upstream submodule — immediate
  retries (no invented backoff), timeout deadline, OnError/OnSuccess chains
  and groups. Reported queue depth is the honest broker backlog. The model
  registers NO builder ports / `Connect` edges either: dispatch and
  completion/retry flow through the runtime mediator, and its DOT service
  graph is likewise an empty-cluster structural-debug artifact; the
  architecture picture is DERIVED FROM OBSERVED BEHAVIOR — `Config.Capture`
  records every mechanism transition (load/queue/dispatch/exec) and the
  mechanism-map figure renders from what actually traversed the model.

  Demo: `go run ./examples/models/tasqueue_metrics`
  (calm vs failure storm: metrics + schematic note).

## Build your own model

A model is built exactly like the two above — a few `model.Service` values
plus a small `run.go`:

1. **Define the `model.Service` values.** For each service, give it a
   `Name`/`Kind`, and write a `Build` closure that materializes the
   service's real primitives (TaskQueues, Processors, Workers, Networks)
   onto the fragment `*model.TopologyBuilder`. Declare `Ports` (name +
   in/out direction) and `ExportIn/ExportOut` them ONLY when tasks genuinely
   cross the service boundary through a builder queue (see the structural
   note in step 2); a runtime-mediated service declares none.
2. **Assemble the System.** `sys.AddService(...)` each service and
   `sys.Connect(src, srcPort, dst, dstPort, net)` each cross-service edge
   (optionally through a `NetworkConfig` for loss/delay). A per-pump
   mediator (an `AddTickHook`) advances shared runtime state once per pump
   tick.
   **Structural scope.** Register builder ports + `Connect` edges ONLY when
   tasks genuinely traverse them. If the model's real dynamics live in a
   runtime constructed directly on the simulation (the temporal/tasqueue
   pattern), declare no ports: a shell edge renders without carrying
   traffic, and the honest-schematics principle applies — if the code
   doesn't traverse it, the diagram doesn't draw it. Pair the resulting
   structural-debug DOT with a mechanism map DERIVED FROM RUNTIME CAPTURE:
   set `Config.Capture`, run, and `capture.Aggregate` the recorder's events
   into the trustworthy architecture picture — only what actually traversed
   (see the `doc.go` of each model).
3. **Build.** `sys.Build()` (or let `sys.Run()` auto-build after
   `BuildSystem`) — the System materializes the services, binds ports, wires
   connections, and registers the automatic monitor. The System owns the
   `core.Simulation`.
4. **Run.** `res := sys.Run()` — a single-shot run to the horizon + queue
   exhaustion.
5. **Read the result.** `res.Metrics` is the monitor's summary of the whole
   trace — queue depth, throughput, latency percentiles — and `res.Digest`
   its canonical, deterministic serialization. Call `sys.WriteDot(w)` for
   the DOT structural-debug view: it renders REGISTERED builder primitives
   only, so for a runtime-mediated model (no registered ports) the service
   clusters are empty by design — that is correct and honest. The
   trustworthy architecture picture is the mechanism map DERIVED FROM
   RUNTIME CAPTURE (`Config.Capture` → `model/capture` recorder →
   `capture.Aggregate`), not the generated DOT: it shows only transitions
   that actually traversed, keyed to the real-system source as in the
   temporal/tasqueue `doc.go` files.
6. **Wrap it in the conventional surface.** Provide `Config`,
   `DefaultConfig()`, `Run(cfg) Result`, and `BuildSystem(cfg) *System` in a
   `run.go` — mirror `models/temporal` and `models/tasqueue`.

## Determinism

Same model + same seed → bit-identical run. Single-goroutine (the System
owns the pump), no wall-clock (`time.Now` is forbidden), no map iteration
(services/queues/workers iterated in sorted order), RNG only via `core.RNG`
substreams (path-prefixed). The free monitor makes zero RNG draws and
samples in sorted order, so monitoring on/off is bit-identical.
