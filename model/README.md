# The modeling library

`model` is the **modeling library** of queuesim: general, deterministic
primitives for building discrete-event queue-network models of distributed
systems, plus a first-class authoring surface (`Service`/`System`) that turns
those primitives into runnable, observable models.

This is the *library*, not a model of any particular system. Concrete models
built on it live in [`models/`](../models/README.md): `models/temporal` and
`models/tasqueue` are the worked examples, mechanism-faithful to their real
upstream systems.

> **Provenance.** This document came from the GoDES project this repository
> was extracted from. References to ADRs and internal tracks are kept because
> they record why the semantics are what they are. The engine itself is
> vendored under [`godes/`](../godes/README.md).

Two mental models matter before you write anything:

1. **Two pillars.** Pillar one is the primitives (a `TaskQueue`, a
   `Processor`, a `Network`...). Pillar two is the authoring surface
   (`Service` values glued by a `System`) that gives you monitoring and a
   schematic for free.
2. **Determinism is the contract.** Same model + same seed → bit-identical
   run. Everything below is shaped by that promise.

---

## What modeling looks like

A `Task` is a unit of work flowing through the model: `{ID, Attempt, Class,
BornMs}`. A model is a graph of named components that receive, buffer,
transform, drop, delay, and forward `Task`s — driven by a **pump** that
advances simulated time in fixed ticks. There are no goroutines, no locks,
and no wall-clock reads on a simulation path.

Two levels of use:

### Level 1 — primitives and a `TopologyBuilder`

Wire raw primitives into a queueing graph by hand:

```go
b := model.NewTopologyBuilder(sim)
q := b.AddQueue("in", 100)                    // buffered queue, capacity 100
p := b.AddProcessor(model.ProcessorConfig{    // queue → worker → queue box
    Name:      "svc",
    ServiceMs: 5,                             // 5 ms service time
})
out := b.AddQueue("out", 0)
b.Feed(q, p)                                  // q drains into p
b.Connect(p, out, nil)                        // p's completions land in out
b.SetEnd(60_000)                              // run for 60 s of sim time
topo, err := b.Build()
topo.Run()
```

This level is fully general: any queueing graph you can wire is a model.

### Level 2 — `Service`/`System` authoring (recommended)

Describe each unit of the distributed system as a `Service` value — declared
name, in/out `Port`s, and a `Build` closure that materializes the service's
real primitives — then glue them with a `System`:

```go
sys := model.NewSystem("demo", model.SystemConfig{Seed: 42, SimEndMs: 60_000})

sys.AddService(model.Service{
    Name: "frontend",
    Ports: []model.Port{{Name: "start", In: false}},
    Build: func(b *model.TopologyBuilder) error {
        q := b.AddQueue("start", 0)   // real component
        b.ExportOut("start")          // bind the declared port to it
        return nil
    },
})
// ... matching, history services ...

sys.Connect("frontend", "start", "matching", "wf-in", model.NetworkConfig{DelayMs: 2})

res := sys.Run()          // Build + single-shot run
fmt.Println(res.Metrics)  // free monitoring summary
sys.WriteDot(os.Stdout)   // DOT structural-debug view (REGISTERED primitives)
```

The `System` builds each service as a path-prefixed child topology, binds
declared ports to their real queues, wires cross-service connections through
the port queues, owns the `core.Simulation`, and drives it once with `Run`.
The recommended model shape is the "run.go pattern" used by
[`models/temporal`](../models/temporal/run.go) and
[`models/tasqueue`](../models/tasqueue/run.go): `Config` +
`DefaultConfig()` + `Run(cfg) Result` + `BuildSystem(cfg) *System`.

Why prefer Level 2? Monitoring and the DOT schematic are **derived from the
built service graph** — never a hand-maintained list — so a service cannot
silently under-report what it owns, and you get observability with zero
configuration.

**Honest scope of the schematic.** `WriteDot` renders what was REGISTERED on
the builder: a service whose real dynamics are built directly on the sim and
driven by a runtime tick (as in `models/temporal` and `models/tasqueue`)
would render only EMPTY clusters if it registered nothing. Do not add
port-queue shells + zero-cost `Connect` edges just to make the diagram look
full — they render without any task traversing them, and the diagram
becomes a lie. If the code doesn't traverse it, the diagram shouldn't draw
it: keep the DOT as a structural-debug artifact and pair it with a mechanism
map DERIVED FROM RUNTIME CAPTURE for the trustworthy architecture picture
(`model/capture` + `SystemConfig.EnableCapture` — the model records
transitions at its real chokepoints; `capture.Aggregate` + `Graph.WriteDOT`
render only what actually traversed; see the `models/*/doc.go` mechanism
sections).

---

## Primitives

| Primitive | File | What it does |
|---|---|---|
| `Task` / `TaskClass` | `taskqueue.go` | The unit of work `{ID, Attempt, Class, BornMs}`; class routes to per-class queue partitions |
| `TaskQueue` | `taskqueue.go` | Buffered queue. Capacity/drop, `MaxWaitMs` residency expiry, `SetDelay`, per-name RNG substream, sync-match to parked workers |
| `Processor` | `processor.go` | "queue → worker → queue" stage: input buffer, in-flight budget, fixed or exponential service time, output edges (each optionally through a `Network`) |
| `Network` | `network.go` | Lossy/delayed link: `DelayMs` + `JitterMs`, `DropRate` (draws from a deterministic `LinkSrc`) |
| `Poller` | `poller.go` | Self-rescheduling automaton consumer (ADR-068): polls every `PollIntervalMs`, tracks in-flight, `SetEndMs`/`Revive` for dynamic scaling |
| `Worker` | `worker.go` | Fleet of long-polling workers pulling from a `TaskQueue`; sync-match rendezvous or backlog dispatch, per-execution service time, re-park |
| `RetryController` | `retry.go` | Stateless retry-policy math: `BackoffMs(attempt)` / `CanRetry(attempt)`; no RNG, no state |
| `TopologyBuilder` | `topology.go` | Wire primitives into graphs: `AddQueue/AddProcessor/AddNetwork/AddWorker`, `Feed`, `Connect`, `ExportIn/Out`, `AddChild`, `SetPump`, `SetEnd`, `AddHook`/`AddHookAt`, `Inject`, `WithVector` |

Supporting packages (outside `model/`, same library story):

- `core/dists` — 11 deterministic distributions over an injectable `Source`
- `core/process` — coroutine-style process modeling via `iter.Pull`
- `core/resource` — `Resource`/`Container`/`Store` with `AcquireOrTimeout`
- `model/faults` — declarative, time-windowed fault injection
  (`Injection{Kind, Start, Duration, Rate}` → drop/delay/timeout); composable
  and determinism-neutral when no window is active
- `model/monitor` — the free monitoring engine; zero RNG draws, sorted
  sampling, on/off is bit-identical (documented trade-off: gauges keep their
  full sampled series, so very long runs grow memory linearly)

---

## Free monitoring and free schematic

Because the `System` derives them from the built graph, every `System`
carries **zero-config** observability:

- **Monitoring** (`model/monitor`): automatic queue-depth gauges, throughput
  counters, and percentile histograms per service, plus an extension API —
  `sys.Monitor().AddCounter/AddGauge/AddHistogram`. `Run()` returns
  `Result{Metrics, Digest}` where `Digest` is a canonical deterministic
  serialization.
- **Schematic** (`System.WriteDot`): one cluster per `Service`, the
  REGISTERED queues/processors/workers as nodes, cross-service edges as
  directed arcs — all derived from live components, no dummy registrations.
  Generated DOT is a **structural-debug view**: it renders REGISTERED
  builder primitives only. A runtime-mediated model (whose real dynamics
  are constructed directly on the sim and driven by a tick hook — see
  `models/temporal`, `models/tasqueue`) deliberately registers no ports, so
  its DOT clusters are empty; the trustworthy architecture picture for such
  models is DERIVED FROM RUNTIME CAPTURE — set `SystemConfig.EnableCapture`
  and the runtime records each mechanism transition at its real chokepoints
  into a `model/capture` Recorder, then `capture.Aggregate` +
  `Graph.WriteDOT` render the map from what actually traversed (never
  fake the diagram with dead port shells — "if the code doesn't traverse
  it, the diagram doesn't draw it").

Both are read-only and never perturb the simulation: **monitoring on/off is
bit-identical**.

---

## Determinism rules

If you write a model, these are non-negotiable:

1. **Single-goroutine, pump-driven.** The `System`/`Topology` owns the pump;
   write no goroutines, channels, or locks on a simulation path.
2. **No wall-clock.** `time.Now` is forbidden on simulation paths. Time is
   sim time, advanced by the pump and scheduled events.
3. **No map iteration.** Services, queues, and workers are iterated in sorted
   name order — iteration order must never depend on Go map order.
4. **RNG only via `core.RNG` substreams**, path-prefixed and
   constructor-fixed. Never call a global RNG; a late mutation of a substream
   key silently changes the draw sequence and breaks determinism.
5. **The monitor draws nothing** and samples in sorted order.

---

## Composability

- **Primitives compose into graphs** via `TopologyBuilder`
  (`Feed`/`Connect`/`AddChild`). `ConnectFromChildPort`/`FeedChildPort` wire
  edges across topology boundaries (cross-topology and parent→child).
- **Services compose into a system** via `System.AddService`/`Connect`;
  each service is a path-prefixed child with an independent namespace, so the
  same service value can appear in multiple systems.
- **Faults compose.** Multiple `faults.Injection`s can target the same
  component; a `DecisionVector` (see below) carries composed fault members
  with overlay merge (`MergeOverlays`).
- **Systems nest** through `AddChild`, so a built topology can become a node
  inside a larger topology — the ns-3/Omnet++-style hierarchy used by the
  temporal model's frontend/matching/history layout.

### Faults and search

Fault injection is first-class. Two idioms:

1. **Declarative windows** — `model/faults`: attach an `Injector` to a
   `Target` (e.g. `NewQueueTarget(q)`) with time-windowed `Injection`s.
2. **Decision vectors** — `model/vector.go` (ADR-069): a
   `DecisionVector` names a fault kind (`KindAddDrop`, `KindActivityFailure`,
   ...) with a rate and window, for grid search. `ExpandGrid(axes)`
   enumerates a cartesian search space; `RegimeFromStatus` classifies a run
   outcome. This is the search/confirm duality the thesis experiments use:
   search cheap model runs for metastable candidates, then confirm them on a
   higher-fidelity leg.

---

## What's NOT a recommended use

- **Modeling code that is not a queueing/mechanism dynamic.** The library
  models throughput, backlog, latency, retries, and their failure modes. If
  your question is about byte-level protocol behavior, memory, or real code
  execution, use a different kind of simulation — not a queue model.
- **Reproducing every server constant.** A model is an abstraction. The
  discipline is to pin the *mechanisms that matter* to the real source
  (see the `contract_test.go` file:line pins in `models/temporal`) and
  document the divergences — not to copy every config value.
- **Large-scale, sub-millisecond fidelity.** Sim time is integer
  milliseconds; the pump cadence is your time granularity. Sub-ms effects
  are out of scope.
- **Hand-maintained observability.** If you find yourself registering dummy
  nodes for the schematic or hand-listing metrics, you are fighting the
  library — derive from the real built graph instead.
- **Anything non-deterministic.** No `time.Now`, no map iteration, no global
  RNG. If you need a "random" behavior, draw from a named substream.
- **`WithSim` for normal use.** It is an escape hatch for tests and
  pre-seeded events; let the `System` own the simulation.

---

## See also

- `doc.go` — the package documentation (concise version of this file)
- [`models/README.md`](../models/README.md) — what a concrete model is and a
  step-by-step "build your own model" guide
- `models/temporal`, `models/tasqueue` — the two worked example models
- `examples/models/` — runnable demos (`temporal_metrics`, `tasqueue_metrics`)
- `core/dists`, `core/process`, `core/resource` — supporting modeling
  packages
