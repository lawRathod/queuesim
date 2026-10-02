package monitor

import (
	"fmt"
	"sort"
	"strings"
)

// Monitor is a passive, deterministic metric recorder. Metrics are
// registered under a stable name and an optional label set
// (AddCounter/AddGauge/AddHistogram register unlabeled; the *Key variants
// take labels), and sampled at each pump tick via Sample. Sample iterates
// every metric in sorted (name, labels) order and makes no RNG draws and no
// simulation mutations, so monitoring never perturbs the simulated system.
type Monitor struct {
	counters   map[string]*counter
	gauges     map[string]*gauge
	histograms map[string]*histogramMetric
}

// New creates an empty Monitor.
func New() *Monitor {
	return &Monitor{
		counters:   map[string]*counter{},
		gauges:     map[string]*gauge{},
		histograms: map[string]*histogramMetric{},
	}
}

// AddCounter registers a monotonically-growing total under a stable name.
// The `at` closure is called each Sample and must be a pure read.
func (m *Monitor) AddCounter(name string, at func() uint64) {
	m.AddCounterKey(name, nil, at)
}

// AddCounterKey is AddCounter with a label set: the counter is identified by
// (name, labels), so the same name can be sliced per shard / queue / kind.
// Labels are canonicalized by (Key, Value); a duplicate key panics.
func (m *Monitor) AddCounterKey(name string, labels []Label, at func() uint64) {
	if name == "" || at == nil {
		panic("monitor: AddCounterKey requires a non-empty name and a non-nil sampler")
	}
	labels = canonicalLabels(labels)
	key := encodeKey(name, labels)
	if _, dup := m.counters[key]; dup {
		panic("monitor: duplicate counter " + name + labelSet(labels))
	}
	m.counters[key] = &counter{name: name, labelSet: labelSet(labels), at: at}
}

// AddGauge registers an instantaneous sample under a stable name (e.g. a
// queue depth). The `at` closure is called each Sample and must be a pure
// read.
func (m *Monitor) AddGauge(name string, at func() int64) {
	m.AddGaugeKey(name, nil, at)
}

// AddGaugeKey is AddGauge with a label set. Labels are canonicalized by
// (Key, Value); a duplicate key panics.
func (m *Monitor) AddGaugeKey(name string, labels []Label, at func() int64) {
	if name == "" || at == nil {
		panic("monitor: AddGaugeKey requires a non-empty name and a non-nil sampler")
	}
	labels = canonicalLabels(labels)
	key := encodeKey(name, labels)
	if _, dup := m.gauges[key]; dup {
		panic("monitor: duplicate gauge " + name + labelSet(labels))
	}
	m.gauges[key] = &gauge{name: name, labelSet: labelSet(labels), at: at}
}

// AddHistogram registers a value-batch sampler under a stable name. The
// `at` closure is called each Sample and returns the batch of non-negative
// integer samples accumulated since the previous sample; the monitor folds
// them into a log-scale integer histogram (pure bucket math, sorted
// internally).
func (m *Monitor) AddHistogram(name string, at func() []int64) {
	m.AddHistogramKey(name, nil, at)
}

// AddHistogramKey is AddHistogram with a label set. Labels are canonicalized
// by (Key, Value); a duplicate key panics.
func (m *Monitor) AddHistogramKey(name string, labels []Label, at func() []int64) {
	if name == "" || at == nil {
		panic("monitor: AddHistogramKey requires a non-empty name and a non-nil sampler")
	}
	labels = canonicalLabels(labels)
	key := encodeKey(name, labels)
	if _, dup := m.histograms[key]; dup {
		panic("monitor: duplicate histogram " + name + labelSet(labels))
	}
	m.histograms[key] = &histogramMetric{name: name, labelSet: labelSet(labels), at: at, h: NewHistogram()}
}

// Sample reads every registered metric once, in sorted (name, then labels)
// order, and records the values. It performs no RNG draws and no simulation
// mutation — the neutrality contract. Call once per pump tick.
func (m *Monitor) Sample() {
	for _, name := range sortedNames(m.counters) {
		m.counters[name].value = m.counters[name].at()
	}
	for _, name := range sortedNames(m.gauges) {
		g := m.gauges[name]
		v := g.at()
		g.last = v
		if v > g.max {
			g.max = v
		}
		g.count++
		g.sum += uint64(v)
		g.series = append(g.series, v)
	}
	for _, name := range sortedNames(m.histograms) {
		for _, v := range m.histograms[name].at() {
			m.histograms[name].h.Add(v)
		}
	}
}

// GaugeSeries returns a copy of the sampled series for an UNLABELED gauge
// name, or nil if the gauge is unknown. A labeled gauge is not addressable
// here (its identity carries labels); it still appears in Summary. Read-only
// and deterministic.
func (m *Monitor) GaugeSeries(name string) []int64 {
	g, ok := m.gauges[name]
	if !ok {
		return nil
	}
	out := make([]int64, len(g.series))
	copy(out, g.series)
	return out
}

// Summary finalizes the current monitor state into a deterministic,
// sorted Metrics value.
func (m *Monitor) Summary() Metrics {
	ms := Metrics{}
	for _, key := range sortedNames(m.counters) {
		c := m.counters[key]
		ms.Counters = append(ms.Counters, CounterSummary{Name: c.name, LabelSet: c.labelSet, Value: c.value})
	}
	for _, key := range sortedNames(m.gauges) {
		g := m.gauges[key]
		ms.Gauges = append(ms.Gauges, GaugeSummary{
			Name:     g.name,
			LabelSet: g.labelSet,
			Last:     g.last,
			Max:      g.max,
			Count:    g.count,
			Mean:     mean(g.sum, g.count),
		})
	}
	for _, key := range sortedNames(m.histograms) {
		h := m.histograms[key].h
		p999, _ := h.P999Ms()
		ms.Histograms = append(ms.Histograms, HistogramSummary{
			Name:     m.histograms[key].name,
			LabelSet: m.histograms[key].labelSet,
			Count:    h.Count(),
			MaxMs:    h.MaxMs(),
			MeanMs:   h.MeanMs(),
			P50Ms:    h.PercentileMs(50, 100),
			P90Ms:    h.PercentileMs(90, 100),
			P99Ms:    h.PercentileMs(99, 100),
			P999Ms:   p999,
			StdDevMs: h.StdDevMs(),
		})
	}
	return ms
}

// Digest returns a canonical, deterministic string serialization of the
// current summary — byte-identical across bit-identical runs. It is the
// determinism evidence seam.
func (m *Monitor) Digest() string {
	return m.Summary().String()
}

// CounterSummary is the finalized state of one counter. LabelSet is the
// canonical rendered label suffix ("" when unlabeled, else "{k=v,k2=v2}")
// — Name + LabelSet is the metric's display identity. It is a plain string
// so the summary stays comparable.
type CounterSummary struct {
	Name     string
	LabelSet string
	Value    uint64
}

// GaugeSummary is the finalized state of one gauge: last/max/mean of its
// sampled series plus the sample count. See CounterSummary.LabelSet.
type GaugeSummary struct {
	Name     string
	LabelSet string
	Last     int64
	Max      int64
	Count    uint64
	Mean     int64
}

// HistogramSummary is the finalized percentile summary of one histogram.
// See CounterSummary.LabelSet. P999Ms is 0 unless Count >= P999MinCount (the
// gate in Histogram.P999Ms); StdDevMs is the integer population standard
// deviation. Neither is rendered by Metrics.String() — that serialization is
// frozen (ADR-115).
type HistogramSummary struct {
	Name     string
	LabelSet string
	Count    uint64
	MaxMs    int64
	MeanMs   int64
	P50Ms    int64
	P90Ms    int64
	P99Ms    int64
	P999Ms   int64
	StdDevMs int64
}

// Metrics holds the sorted final summaries of every registered metric.
type Metrics struct {
	Counters   []CounterSummary
	Gauges     []GaugeSummary
	Histograms []HistogramSummary
}

// Counter returns the summary for a counter name (ok=false when absent).
// The summaries are sorted by name, so lookups are deterministic. When
// several counters share a name, the first in sorted order is returned — the
// unlabeled entry, if present; inspect LabelSet to disambiguate.
func (ms Metrics) Counter(name string) (CounterSummary, bool) {
	for _, c := range ms.Counters {
		if c.Name == name {
			return c, true
		}
	}
	return CounterSummary{}, false
}

// Gauge returns the summary for a gauge name (ok=false when absent).
// Same-name entries resolve to the first in sorted order (the unlabeled one,
// if present); see MetricKey / LabelSet.
func (ms Metrics) Gauge(name string) (GaugeSummary, bool) {
	for _, g := range ms.Gauges {
		if g.Name == name {
			return g, true
		}
	}
	return GaugeSummary{}, false
}

// Histogram returns the percentile summary for a histogram name (ok=false
// when absent) — the p50/p90/p99/max surface models surface in their
// run-level results. Same-name entries resolve to the first in sorted order
// (the unlabeled one, if present); see MetricKey / LabelSet.
func (ms Metrics) Histogram(name string) (HistogramSummary, bool) {
	for _, h := range ms.Histograms {
		if h.Name == name {
			return h, true
		}
	}
	return HistogramSummary{}, false
}

// String returns a stable, sorted, deterministic serialization of Metrics.
// An unlabeled metric renders byte-identically to the pre-label format (bare
// name); a labeled metric renders name{k=v,k2=v2}. This serialization is the
// determinism evidence seam and is FROZEN (ADR-115) — the tail fields
// P999Ms/StdDevMs are deliberately not rendered here.
func (ms Metrics) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "counters[%d]", len(ms.Counters))
	for _, c := range ms.Counters {
		fmt.Fprintf(&sb, "{%s=%d}", c.Name+c.LabelSet, c.Value)
	}
	fmt.Fprintf(&sb, " gauges[%d]", len(ms.Gauges))
	for _, g := range ms.Gauges {
		fmt.Fprintf(&sb, "{%s=last:%d,max:%d,mean:%d,n:%d}", g.Name+g.LabelSet, g.Last, g.Max, g.Mean, g.Count)
	}
	fmt.Fprintf(&sb, " histograms[%d]", len(ms.Histograms))
	for _, h := range ms.Histograms {
		fmt.Fprintf(&sb, "{%s=n:%d,max:%d,mean:%d,p50:%d,p90:%d,p99:%d}",
			h.Name+h.LabelSet, h.Count, h.MaxMs, h.MeanMs, h.P50Ms, h.P90Ms, h.P99Ms)
	}
	return sb.String()
}

func mean(sum uint64, count uint64) int64 {
	if count == 0 {
		return 0
	}
	return int64(sum / count)
}

// sortedNames returns the sorted keys of a string-keyed map (deterministic
// iteration — never range directly over a map on a simulation path).
func sortedNames[V any](m map[string]V) []string {
	names := make([]string, 0, len(m))
	for k := range m {
		names = append(names, k)
	}
	sort.Strings(names)
	return names
}
