# godes — the simulation engine (vendored fork)

These packages are a source copy of the **GoDES** deterministic
discrete-event engine, vendored into `queuesim` so the repository builds
standalone. Upstream: GoDES (MIT, Copyright © 2026 Prateek Rathod), commit
`8817f3d9c15bc544829d373e0205dba38318299a`.

| Package | What it provides |
|---|---|
| `core` | Simulation loop, event heap, seeded RNG substreams, tie-breaking policy |
| `core/dists` | Deterministic distributions (exponential, normal, empirical, ...) |
| `core/process`, `core/request`, `core/resource`, `core/simclock`, `core/task`, `core/component` | General DES building blocks |
| `seed` | `DSTSEED` run-seed resolution |
| `faultsched` | Shared, time-windowed fault schedules |
| `observe` | Sliding-window monitor and metastability classifier |
| `simtest` | Determinism test helper |

## Fork notes

- Only the packages above were copied. `core/logging` and GoDES's
  transport/benchmark packages are not used and are not present.
- Every import was rewritten from `github.com/law/GoDES/...` to
  `github.com/lawRathod/queuesim/godes/...`.
- Upstream unit tests are kept, so `go test ./godes/...` validates the fork.
- This is a **fork**, not a submodule: upstream changes are not picked up
  automatically. The engine is small and stable; if you need an upstream fix,
  copy the file and note the change here.

The MIT license text for this code is in [`../LICENSE`](../LICENSE); see
[`../THIRD_PARTY_NOTICES.md`](../THIRD_PARTY_NOTICES.md) for attribution.
