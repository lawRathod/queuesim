# models/temporal — a queue model of Temporal's task queues

A mechanism-faithful model of **Temporal's matching + history machinery** —
the parts that decide how workflow/activity tasks get from a `StartWorkflow`
call into a worker and back. It is built on the queuesim modeling library
(`model/`) and models only what mechanically matters for task-queue dynamics:
task queues, poller fleets, sync-match, retries, persistence, transfer/timer
processing. Constants are pinned to the real server (`temporal/` submodule)
in `contract_test.go`, or honestly labeled as calibrated/neutral.

There is **no frontend service**: the `StartWorkflow` RPC adds no mechanical
chip here (its latency is folded into `NetworkMs`), so the workload source is
a runtime scheduling behavior, not a Service.

> **Reading this doc.** The library's general primitives are `TaskQueue`,
> `Processor`, `Worker`, `Poller`, `Network`, `RetryController` (see
> `model/README.md`). This model does **not** consume those general
> primitives for its queues/workers — it predates/parallels them with
> mechanism-faithful variants under `components/` (the library types were
> *generalized out of* this model — e.g. `model/worker.go` "brings the v2
> model's worker semantics into the general builder"). The mapping section
> below tells you, for every component here, which library primitive it is
> (if any) or exactly what it is if it has none.

## How these components map to the library primitives

| Temporal component (`models/temporal/`) | What it is | Library counterpart |
|---|---|---|
| `matcher.Task` / `TaskClass` | Unit of work `{ID, Attempt, Class, BornMs}` + fanout lineage (`Depth`, `ParentID`) and a `Rescheduled` flag. Classes: `TaskWfStart` / `TaskActivity` / `TaskWfComplete` | `model.Task`/`TaskClass` — same shape minus the lineage fields; library splits `TaskWfComplete` into `TaskWfSchedule` + `TaskWfComplete` |
| `matcher.MatchQueue` | A **task queue**: a FIFO backlog of spooled tasks plus a list of parked (long-polling) pollers. An add tries the **sync-match fast path** (hand the task straight to a parked poller) and otherwise **spools** to the backlog; a parked poller later drains the backlog. The richer, original variant of the library queue | `model.TaskQueue` — same job, same `Park`/`AddSync` mechanics. MatchQueue keeps Temporal-specific knobs the general queue dropped: sync-match *window* timing, backlog-age gate, token-bucket dispatch, child/root `forwardReliable` flag. `MatchingCapacity` is vestigial (matching is persistence-backed → adds never reject); the library queue does enforce capacity |
| `worker.Pool` | A **fleet of long-polling workers** over one queue: `N` pollers parked on the queue, each handed one task at a time, executes it after a service time, then **re-parks**. The "poller" is not a separate component — it is a parked slot in the pool | `model.Worker` — same semantics; the library `Worker` is this pool generalized (`model/worker.go`). The library's *other* consumer, `model.Poller` (fixed-interval polling automaton, ADR-068), is what **tasqueue** uses — not this model |
| `matcher.TokenBucket` | A **deterministic rate limiter**: tokens refill lazily against sim time at consume time. Used for the dispatch throttle and the child-partition drain bound | none — no library token bucket; a small bespoke limiter |
| `model.RetryController` | The activity **retry policy** (initial interval, coefficient, attempt cap, `BackoffMs`/`CanRetry`) | **used directly** — the library type, not a reimplementation (`rt.retry`) |
| `history.HistoryShard` | The **persistence + scheduling stage** for the workflows a shard owns (see below — bespoke) | none |
| `history.StickyQueue` | **Worker-cache affinity buffers** for wf tasks (see below — bespoke) | none |
| `history.PersistenceBucket` | **Rate limiter on persistence writes** (S4 seam): a token bucket that delays a write until its QPS token | none — bespoke (`matcher.TokenBucket` is not reused; the bucket reserves ahead with negative tokens) |
| `runtime` mediator + `load` | The per-pump tick that advances every mechanism in real-server order; the burst/arrival workload source | none — the documented "runtime-mediated model" authoring pattern from `models/README.md` (dynamics built directly on the sim, driven by `AddTickHook`) |

### The bespoke ones, precisely

- **`HistoryShard`** — one shard owns a range of workflows (`task → shard =
  `ID % Shards`). Every mutation first "writes to persistence": a sim delay
  of `PersistMs` (rate-limited by `PersistenceBucket`), then lands in one of
  three internal per-shard queues, drained per tick:
  - **transfer** — persistence landed, task must move into matching
    (`AddWorkflowTask`/`AddActivityTask`); drained `TransferBatch`/tick.
  - **timer** — tasks scheduled for a *future* time (activity retries, wf
    timers); delivered when `fireAt <= now`, prefetched up to
    `TimerMaxShiftMs`.
  - **resched** — tasks NACKed after a failed execution, waiting out the
    queues-framework reschedule backoff (`ReschedulerBackoffMs`), then
    re-entering matching.
  There is no library primitive because the library has no persistence
  concept, and the transfer/timer separation *is* the thing the thesis
  studies — folding the shard into a `Processor` would hide it.
- **`StickyQueue`** — Temporal's cache affinity: after a workflow's activity
  succeeds, its next wf task (`TaskWfComplete`) is routed to the *same*
  worker that just ran it (a per-worker buffer), so its workflow state is
  warm. If that worker is busy it falls back to the normal `matching-wf`
  queue after `StickyFallbackMs`. Pure Temporal mechanism, no library analog.

## Task lifecycle (the data loop)

```text
load (burst/arrivals)
  └─ startWorkflow ──► history: persist + transfer queue
                          └─ transfer ──► matching-wf queue (spool or sync-match)
                                             └─ poller takes task ──► history: runWF
                                                └─ schedules first activity ──► matching-act
                                                                                  └─ poller takes ──► runAct (service time)
                                                                                     ├─ success ──► TaskWfComplete → matching-wf / sticky → runWF → done (terminal)
                                                                                     └─ failure ──► retry timer → matching-act again (or terminal if past cap/S2C)
```

Every hop between the services is **runtime-mediated** — the two services
share one Go package and one `runtime` mediator, and there are no schematic
ports/edges (honest schematics: the real loop doesn't traverse builder
queues, so the DOT service graph is an empty structural-debug artifact; the
trustworthy architecture picture is derived from runtime capture, see
`Config.Capture`).

## Services

### 1. `matching` — task queues + poller fleets

Owns the matching queues (`matcher.MatchQueue`) and the poller fleets
(`worker.Pool`) that drain them. Built in `matchingService` (`matching.go`).

Three queues:

| Queue | Role |
|---|---|
| `matching-wf` (`wfQ`) | workflow tasks (`TaskWfStart`/`TaskWfComplete`); guard = `WorkflowTaskTimeoutMs` (server default 10 s) |
| `matching-act` ROOT (`actQ`) | the polled activity partition — SDK poller fleet parks & drains here; guard = `ActivityS2STimeoutMs` |
| `matching-act-child` (`childQ`) | a write/persistence partition: activity adds land here when forwarding is dead (see window mechanisms below); no pollers of its own, it only re-offers to the root via its own drain |

Why two activity partitions (the child/root split, 2026-09-07): at a healthy
sync-match window the child forwards every add straight to the root — the
root behaves exactly like one collapsed queue. When the window is below
`ForwardReliableMinMs`, forwarding dies (`TrySyncMatch` builds an
instantly-expired context) and adds are absorbed into the child, which can
only leave at the child's own drain rate (`ChildDrainFraction` ×
`DispatchRate`). That structural drain-bound — not a rate multiplier — is
what reproduces the real server's degraded-window drop.

Poller mechanics:

- Pollers **long-poll** their queue: parked while idle, re-park immediately
  after an execution finishes (`SetOnDone` → re-poll).
- **Sync-match fast path**: an add with a parked poller + a dispatch token
  hands the task over immediately; a throttled or un-parked add spools.
- **Sticky-first dispatch**: a poller checks its sticky queue before the
  normal queues (wf tasks only, when `StickyEnabled`).
- **Elastic scaling**: when a backlog persists past
  `PollerScaleUpBacklogMs`, the active poller target scales up toward
  `PollerScaleMax` (the server's `makePollerScalingDecisionImpl`).

### 2. `history` — shards, persistence, processors, lifecycle execution

Built in `historyService` (`history.go`). Owns the stateful side: one
`HistoryShard` per `Shards` value, the `StickyQueue`, and the lifecycle
execution (`runWF` / `runAct` / `complete`) — executes the workflow/activity
code, routes success (→ next activity or wf-complete) and failure (→ retry
timer, or terminal). The wf fleet's "execution" goes through history too;
the act fleet runs the activity's own service time (`ActivityMs`).

### The shared runtime (not a Service)

`runtime.go` holds all mechanism state and a `tick()` driven once per pump
tick by the System. Tick order (this order is the determinism backbone):

```
history shards (transfer → timer → rescheduler, rotating visit order)
→ maintain poller fleet (long-poll parking)
→ dispatch sticky → dispatch wf queue → child→root drain → dispatch act queue
→ fault/vector knobs → gate observation → depth bookkeeping (DrainMs)
```

Workload inflow (burst/arrivals/surge) is scheduled from `runtime.start`,
not from a Service (`load.go`).

## Component config

Config surface lives in one place — `Config` (`config.go`); zero value = the
mechanism is off. `DefaultConfig()` is the recommended starting point.

### matching (`components/matcher` + `components/worker`)

| Component | Key config | Default |
|---|---|---|
| `MatchQueue` (3 of them) | `SyncMatchWindowMs` (sync-match fast-path window), `WorkflowTaskTimeoutMs` / `ActivityS2STimeoutMs` (backlog residency expiry at dispatch), `BacklogGateMs` (age gate), `DispatchRate`/`DispatchBurst` (dispatch token bucket) | window 200; wf S2S 10 000; act S2S 0; gate 5000; rate 0 (unlimited) |
| `worker.Pool` (wf + act fleets) | `PollersWF`/`PollersAct`, `PollerBudget` (in-flight cap per poller), `LongPollMs` (park expiry; 0 = keep polling) | 4 / 4; budget 1; long-poll 60 000 |
| elastic scaling | `PollerScaleMax` (> configured = off), `PollerScaleUpBacklogMs` | off; 200 when enabled |

Sync-match degradation mechanisms (the C1 story):

- **Mechanism A — backlog-age gate** (`BacklogGateMs`): the real
  `isBacklogNegligible` gate. When the backlog head is older than the
  threshold, the sync-match fast path is disabled. **Observational in this
  model** — measured every tick (`Topology.ChildGate`) but not a routing
  input at the calibrated loads.
- **Mechanism B — window-mediated forwarding** (`ForwardReliableMinMs`,
  default 200): below this sync-match window the child partition can't
  offload to root pollers → adds stay local and accumulate.
- Child drain: `ChildDrainFraction` (0.75 — calibrated structural drain),
  `ChildReSpoolMs` (0 = off; a persistence write→read round-trip that keeps
  a task's create-time counting while it drains).

### history (`components/history`)

| Component | Key config | Default |
|---|---|---|
| `HistoryShard` | `Shards` (count), `PersistMs` (write latency), `TransferBatch` (per-tick transfer drain), `TransferWorkers`, `TimerBatch`, `TimerWorkers`, `TimerMaxShiftMs`, `ReschedulerBackoffMs` | 1 shard; PersistMs 0; TransferBatch **1** (calibrated — server ref 100); TransferWorkers 512; TimerBatch 100; TimerWorkers 512; shift 1000; backoff 1000 |
| `StickyQueue` | `StickyEnabled`, `StickyCacheMs`, `StickyFallbackMs`, `StickyS2STimeoutMs` | off |
| `PersistenceBucket` (S4) | `PersistenceMaxQPS`, `PersistenceQPSBurstRatio` | off |

Note `TransferWorkers`/`TimerWorkers` are carried for fidelity only — the
serial model drains by batch, it has no parallel workers.

### Load, transport, retry

| Config | Default | Meaning |
|---|---|---|
| `BurstCount`, `BurstAtMs` | 50, 0 | initial burst of `StartWorkflow`s |
| `ArrivalRate` (+ `ArrivalRate2`, `ArrivalSwitchMs`) | 0 | sustained arrivals (workflows/s, Poisson); time-varying switch |
| `NetworkMs`, `ActivityMs` | 2, 0 | per-RPC propagation; per-activity service time (library `Network`/`Processor` roles, folded into sim delays here) |
| `FanoutK` | 0 | fan-out: on success a workflow spawns K child activities (join) |
| `CascadeBadFraction` | -1 (off) | mixed population: that fraction of workflows fail during the fault window |
| retry | `RetryInitialMs` 100, `RetryCoeff` 1.0, `RetryMaxAttempts` 0 = unlimited | the `model.RetryController` policy |
| timeouts | `ScheduleToCloseMs` 0, `HeartbeatTimeoutMs` 0 | S2C expiry is terminal (bounds the whole attempt chain); heartbeat = service-time bound |

The queues-framework **reschedule** policy (drops / NACKs) is a flat
1 s / 1.1× / 3 min backoff (`rescheduleBackoffMs`) — retry policy, not a
fault. A drop never consumes the activity retry budget.

### Faults

Two mutually-exclusive sources (both route through the same dispatcher):
`Config.Vector` (a `model.DecisionVector` — the library's ADR-069 surface)
or `Config.FaultSchedule` (a shared schedule, ADR-100). The kinds are the
library's vector kinds (`model.KindAddDrop`, …); the *application* lives in
the model-local `faults/` subpackage because the injection chokepoints are
this model's bespoke components, not library `Target`s. Kinds:
`add_drop`, `wf_drop`, `poll_drop`, `s2s_timeout`, `transfer_delay`,
`poller_min`, `timer_load`, `activity_failure` (window + cascade),
`persistence_stall`, `poll_jitter`, `s2c_shorten`, `backlog_surge`.
The default activity-failure window is `FaultStartMs`/`FaultEndMs`.

### Performance seams (ADR-106, all zero = off)

S1 forwarder caps (`ForwarderMaxOutstandingPolls/Tasks`), S2 append
threshold (`AppendThreshold`), S3 poller-scaling gates
(`PollerScaleWaitMs`, `PollerDecisionsPerSec`), S4 persistence QPS
(`PersistenceMaxQPS`, `PersistenceQPSBurstRatio`).

### Run surface

`PumpMs` (1), `SimEndMs` (60 000), `Seed` (42), `DisableMonitor`,
`Capture`.

## Output

`Topology.Run()` / `Run(cfg)` return a `Result`:

- **Stats** (`TopologyStats`): Started, ActivityAdds, ActivityAttempts,
  AttemptsExecuted, Retries, Completed, Failed, SyncMatched, Spooled,
  Dropped, Expired, RejectedAdds, MaxQueue, PollEvents →
  `AttemptsPerStart()` (retry-amplification headline), `RetryAmp()`.
- **Status** + **DrainMs**: internal monitor classification; the sim time
  the backlog last drained (the SLA-recovery signal).
- **Metrics/Digest** (free monitor, zero config): S2S-latency histogram
  (`temporal.s2s.latency` — p50/p90/p99/max), counters
  (started/activity_adds/completed/retries/failed), and queue-depth gauges
  (matching wf/act/sticky/child depths + pollers/in-flight, history
  shard backlogs).

Determinism is a hard contract: single-goroutine pump, no wall-clock, RNG
only via fixed substreams, monitor-neutral — enforced by
`determinism_test.go` (N=5 bit-exact). See the package doc (`doc.go`) for
the mechanism surface and traceability labels; `contract_test.go` for the
server pins.

## Run it

```sh
# from the queuesim repository root
go test ./models/temporal/...                 # tests incl. determinism gates
go run ./cmd/queuesim run -scenario calm      # calm baseline
go run ./cmd/queuesim run -scenario retry-storm
go run ./cmd/queuesim serve -open             # explore and tune it live
```
