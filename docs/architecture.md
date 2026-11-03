# Architecture

`queuesim` is four layers stacked on one rule: **a simulation never reads the
clock, never spawns goroutines, and never iterates a map on a simulation
path.**

```
┌──────────────────────────────────────────────────────────────┐
│ cmd/queuesim        serve · list · run · export              │
├──────────────────────────────────────────────────────────────┤
│ internal/webui      HTTP API, embedded dashboard             │
│ internal/lab        scenarios, knobs, trace capture, sweeps   │
├──────────────────────────────────────────────────────────────┤
│ models/temporal     Temporal matching + history model         │
│ models/tasqueue     Tasqueue queue + worker model             │
│ model/              TaskQueue, Processor, Worker, Network...  │
├──────────────────────────────────────────────────────────────┤
│ godes/              deterministic discrete-event engine       │
└──────────────────────────────────────────────────────────────┘
```

## The engine

`godes/core` is a discrete-event simulator. Components schedule callbacks at
simulated times; `Simulation.Step()` pops the earliest event and runs it.
Ties are broken by `FIFOPolicy` on `(ComponentID, TaskID, Seq)`, so even two
events at the same simulated millisecond have a deterministic order.

All randomness comes from `Simulation.Substream(name)` — a named RNG stream
derived from the run seed. Two runs with the same seed and the same draw
sequence are identical. There is no `time.Now()`, no `math/rand` global, and no
goroutine on a simulation path.

`godes/` is a source fork of the GoDES engine; see
[`../godes/README.md`](../godes/README.md).

## The pump

The modeling library's `System` wraps the event loop with a **pump**: a
callback every `PumpMs` simulated milliseconds until `SimEndMs`. The models use
that tick as their clock. This is a discrete-*time* simulation riding on a
discrete-event core:

- `System.Run()` runs the pump to the horizon, then lets remaining events fire
  (the tail), so counters cover the whole trace.
- `System.AddTickHook(fn)` registers a read-only observer that runs after the
  model's own tick. The lab attaches its sampler there.
- Monitoring is registered automatically; `monitor.Sample()` is a pure read.

The pump cadence is a deliberate trade-off: exact-time events would be faster
and cadence-free, but a fixed tick makes the mechanism order easy to reason
about and the trace easy to sample. The cost is linear in
`SimEndMs / PumpMs`.

## The models

Both models are **runtime-mediated**: their real data loop is driven by a
shared `runtime` object from the tick hook, not by traffic through the general
`TaskQueue`/`Processor` primitives. They still own a `model.System`, so they get
monitoring, the DOT schematic, and lifecycle management for free.

- `models/temporal` builds matching and history services with their own
  mechanism-faithful components (`MatchQueue`, `worker.Pool`, history shards,
  sticky queues). See [models.md](models.md).
- `models/tasqueue` builds a broker queue and a polling worker fleet on the
  library primitives.

## From a run to a trace

```
lab.Execute(Request)
  1. resolve the scenario + merge preset ← user knobs
  2. validate every knob (unknown key, range, finite)
  3. build the model with the effective config
  4. attach a tick hook: every stride ticks, read Snapshot()
  5. Run() to the horizon
  6. derive per-second rates from the counter series
  7. derive observations (insights) from the summary and trace
  8. assemble Run{timeline, summary, insights, percentiles, digest, annotations}
```

`Snapshot()` on both topologies is a pure read: queue lengths, parked pollers,
in-flight counters, head-of-backlog ages, and cumulative lifecycle counters.
The hook samples about `maxFrames = 1200` points (fewer for short horizons),
choosing a stride from the horizon, so a 5 s run and a 120 s run both return a
smooth timeline.

### Insights

`insights.go` is a small rule engine over the finished run: if arrivals exceed
the dispatch rate and a backlog formed, say so; if attempts per start is above
1 and retries exist, call it retry amplification; if the poller fleet is
saturated, name the drain-rate bound. Each rule is a deterministic function of
the config and the trace, so it is covered by the same tests as the run. The
rules are deliberately narrow — an insight that is not clearly supported by
the numbers is not emitted.

Rate series are derived after the run using a trailing window of ~2% of the
trace (bounded to 64 samples). Without smoothing, a Poisson arrival stream
sampled every 16 ms reads as alternating 0 and 62/s.

### The determinism digest

`Result.Digest` is `monitor.Metrics.String()`: a sorted, canonical
serialization of every counter, gauge (`last/max/mean/n`), and latency
histogram (`n/max/mean/p50/p90/p99`). It is byte-identical across bit-identical
runs and deliberately excludes observational extras such as p99.9 and stddev.

The UI's *Verify replay* runs the same request again and compares this string.
The model packages pin the same digests in their own test suites.

## The HTTP layer

`internal/webui` is a `net/http` mux with four JSON routes and an embedded
static file system (`//go:embed assets`). It has no dependencies beyond the
standard library.

- Request bodies are capped at 1 MiB.
- JSON responses are gzipped when the client accepts it.
- `MaxConcurrentRuns` (default 2) bounds CPU-bound simulations; excess requests
  get `429` rather than queueing.
- Handlers recover panics into a `500`.
- Security headers: `nosniff`, `no-referrer`, a self-only CSP. The frontend
  references no external URLs, so the dashboard works offline.

The server stores nothing. A response is the full artifact, and the frontend
encodes the scenario, seed, and every knob in the URL fragment, so any view is
a permalink. `Download trace` saves the same JSON.

## Adding a scenario

A scenario is a preset plus a run function. Add an entry to
`internal/lab/scenarios.go`:

```go
{
    id: "my-scenario", model: "temporal", group: "Temporal",
    title: "My scenario", subtitle: "One line",
    blurb: "What happens and why it matters.",
    watch: []string{"First thing to notice.", "Second.", "Third."},
    defs: temporalKnobs,
    preset: map[string]float64{
        "burst_count": 50, "arrival_rate": 20, "sim_end_ms": 20000,
    },
    defaultSeed: 42,
    run: executeTemporal,
},
```

The catalog, knob form, chart metadata, and CLI all derive from this entry.
If the scenario needs a knob that does not exist yet, add it to
`temporalKnobs`/`tasqueueKnobs`, apply it in `applyTemporal`/`applyTasqueue`,
and expose it from the model config.

To visualize a new live quantity, add it to the model's `Snapshot`, map it in
the corresponding `execute*` hook, list it in `series.go`, and reference it
from a chart or the diagram (`internal/webui/assets/app.js`, `DIAGRAMS`).
`Snapshot` must stay a pure read — the neutrality test will catch a mutation
that moves the digest.

## Testing

```bash
go test ./model/... ./models/... ./godes/...   # engine and model semantics
go test ./internal/...                          # scenarios, traces, HTTP
go vet ./...
```

The important gates:

- **Determinism**: `internal/lab.TestSameConfigSameDigestAndTrace` and the
  model packages' `determinism_test.go` (N=5 bit-identical runs).
- **Neutrality**: `internal/lab.TestSnapshotHookIsNeutral` proves the UI's
  sampling hook does not move the digest.
- **Contract**: every scenario runs, every chart references a real series, and
  every knob default is inside its own range.
- **API**: handler tests cover validation, gzip, sweeps, and the concurrency
  cap.

## Performance notes

A run's wall time is roughly proportional to `SimEndMs × events-per-tick`.
Typical scenarios take 10–200 ms; a 30 s sim with 30 arrivals/s takes about
2 s. The 200 ms slow path is the outage scenario's retry storm, not a leak.
Traces are 100–150 KB uncompressed and roughly 15 KB gzipped.
