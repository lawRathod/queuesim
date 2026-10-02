package monitor

import (
	"os/exec"
	"strings"
	"testing"
)

// TestMonitorSampleSummary verifies Sampled values flow into a deterministic,
// sorted Summary.
func TestMonitorSampleSummary(t *testing.T) {
	m := New()
	depths := map[string]int64{"b": 0, "a": 0}
	m.AddCounter("throughput", func() uint64 { return 42 })
	m.AddGauge("queue.depth", func() int64 { return depths["a"] })
	m.AddGauge("b.queue.depth", func() int64 { return depths["b"] })
	m.AddHistogram("latency", func() []int64 { return []int64{1, 3, 1000} })

	for i := int64(0); i < 3; i++ {
		depths["a"] = i
		depths["b"] = 2 * i
		m.Sample()
	}

	sum := m.Summary()
	if len(sum.Counters) != 1 || sum.Counters[0].Name != "throughput" || sum.Counters[0].Value != 42 {
		t.Fatalf("counter summary wrong: %+v", sum.Counters)
	}
	// Sorted gauge order: b.queue.depth before queue.depth.
	if len(sum.Gauges) != 2 {
		t.Fatalf("gauges=%d want 2", len(sum.Gauges))
	}
	if sum.Gauges[0].Name != "b.queue.depth" || sum.Gauges[1].Name != "queue.depth" {
		t.Fatalf("gauges not sorted: %+v", sum.Gauges)
	}
	// queue.depth (a) sampled as 0,1,2 -> max 2, mean 1.
	if sum.Gauges[1].Max != 2 || sum.Gauges[1].Mean != 1 || sum.Gauges[1].Count != 3 {
		t.Fatalf("gauge a summary wrong: %+v", sum.Gauges[1])
	}
	if len(sum.Histograms) != 1 || sum.Histograms[0].Name != "latency" {
		t.Fatalf("histogram summary wrong: %+v", sum.Histograms)
	}
	if sum.Histograms[0].Count != 9 || sum.Histograms[0].MaxMs != 1000 {
		t.Fatalf("histogram stats wrong: %+v", sum.Histograms[0])
	}
	// Series access.
	if got := m.GaugeSeries("queue.depth"); len(got) != 3 || got[0] != 0 || got[2] != 2 {
		t.Fatalf("series wrong: %v", got)
	}
	if m.GaugeSeries("missing") != nil {
		t.Fatal("missing gauge should return nil")
	}
}

// TestMonitorDeterminism asserts an identical sampling schedule yields a
// byte-identical digest across independent monitors.
func TestMonitorDeterminism(t *testing.T) {
	build := func() *Monitor {
		m := New()
		vals := map[string]int64{"a": 0}
		m.AddGauge("depth", func() int64 { return vals["a"] })
		m.AddCounter("total", func() uint64 { return uint64(vals["a"]) })
		m.AddHistogram("res", func() []int64 { return []int64{vals["a"], vals["a"] + 1} })
		for i := int64(0); i < 20; i++ {
			vals["a"] = i * 7 % 13
			m.Sample()
		}
		return m
	}
	if a, b := build().Digest(), build().Digest(); a != b {
		t.Fatalf("monitor digest not deterministic:\n%s\n%s", a, b)
	}
}

// TestMonitorSortedSampling verifies samples are read in sorted-name order
// (iteration order is observable through a shared buffer).
func TestMonitorSortedSampling(t *testing.T) {
	m := New()
	var order []string
	m.AddGauge("zz", func() int64 { order = append(order, "zz"); return 0 })
	m.AddGauge("aa", func() int64 { order = append(order, "aa"); return 0 })
	m.AddGauge("mm", func() int64 { order = append(order, "mm"); return 0 })
	m.Sample()
	got := strings.Join(order, "")
	if got != "aammzz" {
		t.Fatalf("sample order=%q want aammzz", got)
	}
}

func TestMonitorDuplicatePanic(t *testing.T) {
	m := New()
	m.AddGauge("x", func() int64 { return 0 })
	assertPanic(t, func() { m.AddGauge("x", func() int64 { return 0 }) })
	assertPanic(t, func() { m.AddCounter("", func() uint64 { return 0 }) })
	assertPanic(t, func() { m.AddHistogram("h", nil) })
}

// TestMetricsFinders verifies the sorted-summary name lookups: found
// metrics return their summary with ok=true; unknown names return ok=false
// (the surface the demo and models use to pull named metrics out of a
// Result without hand-scanning the slices).
func TestMetricsFinders(t *testing.T) {
	m := New()
	m.AddCounter("c", func() uint64 { return 3 })
	m.AddGauge("g", func() int64 { return 2 })
	m.AddHistogram("h", func() []int64 { return []int64{5, 7} })
	m.Sample()
	sum := m.Summary()

	if c, ok := sum.Counter("c"); !ok || c.Value != 3 {
		t.Fatalf("Counter(\"c\") = %+v, %v; want 3, true", c, ok)
	}
	if _, ok := sum.Counter("missing"); ok {
		t.Fatal("Counter(\"missing\") ok=true, want false")
	}
	if g, ok := sum.Gauge("g"); !ok || g.Last != 2 {
		t.Fatalf("Gauge(\"g\") = %+v, %v; want last 2, true", g, ok)
	}
	if _, ok := sum.Gauge("missing"); ok {
		t.Fatal("Gauge(\"missing\") ok=true, want false")
	}
	if h, ok := sum.Histogram("h"); !ok || h.Count != 2 || h.MaxMs != 7 {
		t.Fatalf("Histogram(\"h\") = %+v, %v; want count 2 max 7, true", h, ok)
	}
	if _, ok := sum.Histogram("missing"); ok {
		t.Fatal("Histogram(\"missing\") ok=true, want false")
	}
}

func assertPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	fn()
}

// TestMonitorZeroDrawsStructural is the WP1 zero-RNG-draw gate (structural
// leg): the monitor package must not import the simulation engine or any
// RNG-bearing package, so it cannot draw or schedule even in principle. The
// behavioral leg is the System-level neutrality test (monitor on/off
// bit-identical).
func TestMonitorZeroDrawsStructural(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", "github.com/lawRathod/queuesim/model/monitor")
	out, err := cmd.Output()
	if err != nil {
		t.Skipf("go list unavailable: %v", err)
	}
	for _, dep := range strings.Fields(string(out)) {
		if strings.HasPrefix(dep, "github.com/law/GoDES/core") {
			t.Fatalf("monitor imports core (%s) — zero-draw contract violated", dep)
		}
		if strings.Contains(dep, "math/rand") {
			t.Fatalf("monitor imports math/rand (%s) — zero-draw contract violated", dep)
		}
	}
}
