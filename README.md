# queuesim

Deterministic queue-network models of distributed systems, in Go.

This was `GoDES/model` + `GoDES/models` until 2026-10, when the abstract-model leg
was moved out of GoDES into its own module. It depends on `GoDES` for the
simulation engine (`core`, `core/dists`), the seeded run seed (`seed`), the shared
fault schedule (`faultsched`), the metastability classifier (`observe`) and the
determinism test helper (`simtest`). Locally that is a `replace` directive, so a
sibling `GoDES` checkout is required.

## Layout

- `model/` — the modeling library: `TaskQueue`, `Processor`, `Network`,
  `Poller`/`Worker`, `RetryController`, `Topology`, the first-class `Service`/`System`
  authoring surface, plus the free `monitor` (counters/gauges/histograms) and
  `capture` (runtime mechanism recorder).
- `models/temporal/` — Temporal matching/history/frontend expressed as a queue model.
- `models/tasqueue/` — Tasqueue queue/worker expressed as a queue model.

`model` is the library and `models` holds the concrete models. Yes, that is
confusing. It predates this move and was left alone so the extraction stayed
mechanical.

## Status — read before using

This is the **abstract-model leg**. It does not run real systems; it models them
from primitives. It exists to produce cheap candidate scenarios that were then
confirmed against real servers in GoDES's `spotlight/` benches. That
search/confirm method is no longer part of the live evidence pipeline, and the
numbers it contributed to `writing/temporal-report.tex` (`tab:gap`) are recorded
evidence with no reproducing path outside this repository.

Its determinism is real and gated. `go test ./models/temporal` runs N=5
bit-identical and pins:

    zero-draw sha256 a72689e91d1c1ca6feb5b79c30ed2aed52df1f1e88bfc446a1dcb59bdf3ea87e
    draws     sha256 c7b0d97d145079f610ce447adb1f2afa2f580031ae7a2f3328f43684d860a5bf

Both were verified unmoved by the move out of GoDES (2026-10).

## Known limitation

`model.Topology` drives the simulation with a fixed-cadence pump tick
(`SystemConfig.PumpMs`, default 1 ms) instead of scheduling exact-time events. A
model is therefore a discrete-*time* simulation at `PumpMs` resolution riding on a
discrete-event scheduler: results are cadence-dependent, and a 65 s run costs
~65,000 full queue/processor/network sweeps.

## Build

    go build ./...
    go vet ./...
    go test -count=1 -timeout 300s ./models/temporal
