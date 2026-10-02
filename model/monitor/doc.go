// Package monitor provides FREE (zero-config) per-service monitoring for a
// GoDES System: automatic queue-depth gauges, throughput counters, and
// percentile histograms, with an extension API (AddCounter/AddGauge/
// AddHistogram for unlabeled metrics; AddCounterKey/AddGaugeKey/
// AddHistogramKey to key a metric by a canonical label set) for custom
// metrics. It is a passive recording engine: it makes ZERO RNG draws,
// samples every registered metric in sorted (name, then labels) order, and
// never mutates simulation state — so monitoring can be toggled on or off
// without perturbing the simulation (determinism-neutral).
//
// The package is intentionally decoupled from model (no model import): a
// System wires each metric's sampling closures; monitor only records and
// summarizes. All integer math is deterministic; histograms use pure
// integer log-scale buckets, and expose a tail surface (P999Ms, gated by
// P999MinCount, and StdDevMs) that is deliberately NOT rendered into the
// digest serialization (the digest is a frozen compatibility surface).
//
// Known trade-off: every gauge keeps its full sampled series (for
// depth-sequence inspection and determinism evidence), so very long runs
// grow memory linearly with the number of pump ticks. A bounded-series mode
// is a future knob; WP1 deliberately favors evidence over memory.
package monitor
