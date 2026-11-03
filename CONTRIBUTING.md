# Contributing

Thanks for taking a look. Issues and pull requests are welcome.

## Ground rules

1. **Determinism is the product.** A change that moves a pinned digest is a
   behavioural change, not a refactor. If a digest moves on purpose, update the
   pinned value in the same commit and explain why.
2. **Sampling is a pure read.** `Snapshot()` (and anything a tick hook calls)
   must not draw RNG, mutate model state, or iterate a map. The test
   `internal/lab.TestSnapshotHookIsNeutral` enforces this for the Temporal
   path.
3. **No new dependencies.** The module has zero third-party requirements and
   the dashboard loads nothing from the network. Keep it that way unless there
   is a very good reason.
4. **No wall clock, no goroutines on a simulation path.** Randomness goes
   through the seeded substreams. Ordering must not depend on map iteration.
5. **Be honest in the docs.** If a value is calibrated rather than pinned to a
   real server constant, say so.

## Good first contributions

- Add a scenario: a new preset in `internal/lab/scenarios.go` that isolates one
  mechanism. A short "what to watch" list is more valuable than another chart.
- Improve a chart or the mechanism map in
  `internal/webui/assets/app.js` (vanilla JS, no build step).
- Tighten a model against its real upstream: a constant, a mechanism, or a
  contract test with a `file:line` reference.
- Write about the mechanics. `docs/models.md` is the place.

## Adding a scenario

1. Add an entry to `temporalScenarios` or `tasqueueScenarios` in
   `internal/lab/scenarios.go` (id, text, preset, `run`).
2. Run it from the CLI to sanity-check the numbers:

   ```bash
   go run ./cmd/queuesim run -scenario <id>
   ```

3. Open the dashboard and check the charts are informative, the diagram
   animates, and the "what to watch" claims are true.
4. `go test ./internal/lab` — the catalog test verifies ids, knob defaults,
   and that every chart references a real series.

## Adding a knob

1. Add metadata to `temporalKnobs`/`tasqueueKnobs` in
   `internal/lab/knobs.go`.
2. Apply it in `applyTemporal`/`applyTasqueue`, and seed its default in the
   corresponding `*Defaults` map.
3. If the model config has no field for it, add one and thread it through the
   model. Keep the zero value neutral (off), as the rest of the model does.
4. Add the knob to the scenario presets where it matters.

## Before opening a PR

```bash
make check     # gofmt check, go vet, full test suite
```

If you changed a model mechanism, run the model tests directly:

```bash
go test -count=1 -timeout 300s ./models/temporal
go test -count=1 -timeout 120s ./models/tasqueue ./model
```

Keep commits focused. If a change has a trade-off, write it down in the PR
description — the next reader will need it.
