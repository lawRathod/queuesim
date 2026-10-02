package monitor

import (
	"strings"
	"testing"
)

// TestLabelCanonicalizationOrderInsensitive verifies metric identity is the
// label SET, not its order: the same labels in any order canonicalize to the
// same key, an empty set is the bare name, and distinct sets never collide.
func TestLabelCanonicalizationOrderInsensitive(t *testing.T) {
	a := MetricKey{Name: "m", Labels: []Label{{Key: "zone", Value: "b"}, {Key: "shard", Value: "1"}}}
	b := MetricKey{Name: "m", Labels: []Label{{Key: "shard", Value: "1"}, {Key: "zone", Value: "b"}}}
	if a.Canonical() != b.Canonical() {
		t.Fatalf("canonical keys differ by label order:\n%q\n%q", a.Canonical(), b.Canonical())
	}
	if got := (MetricKey{Name: "m"}).Canonical(); got != "m" {
		t.Fatalf("empty-label canonical = %q, want %q", got, "m")
	}
	empty := (MetricKey{Name: "m"}).Canonical()
	oneEmpty := (MetricKey{Name: "m", Labels: []Label{{Key: "a", Value: ""}}}).Canonical()
	if empty == oneEmpty {
		t.Fatalf("empty label set collided with a one-empty-label set: %q", empty)
	}
	// A prefix name must not be confusable with a labeled name.
	if (MetricKey{Name: "ab"}).Canonical() == (MetricKey{Name: "a", Labels: []Label{{Key: "b", Value: ""}}}).Canonical() {
		t.Fatal("name prefix collided with a labeled key")
	}
}

// TestLabelDuplicateKeyPanics pins the programming-error contract: a label
// set with two labels sharing a key panics at registration and at
// canonicalization (consistent with the package's empty/dup-name panics).
func TestLabelDuplicateKeyPanics(t *testing.T) {
	m := New()
	assertPanic(t, func() {
		m.AddCounterKey("c", []Label{{Key: "k", Value: "1"}, {Key: "k", Value: "2"}}, func() uint64 { return 0 })
	})
	assertPanic(t, func() {
		MetricKey{Name: "m", Labels: []Label{{Key: "k", Value: "1"}, {Key: "k", Value: "1"}}}.Canonical()
	})
}

// TestLabeledRendering pins the deterministic non-empty render form:
// name{k=v,k2=v2} sorted by key.
func TestLabeledRendering(t *testing.T) {
	m := New()
	m.AddCounterKey("hits", []Label{{Key: "zone", Value: "b"}, {Key: "shard", Value: "2"}}, func() uint64 { return 7 })
	m.Sample()
	s := m.Summary().String()
	if !strings.Contains(s, "{hits{shard=2,zone=b}=7}") {
		t.Fatalf("labeled counter rendering missing/wrong:\n%s", s)
	}
}

// TestEmptyLabelByteIdentity is the WP1 byte-identity gate: registering via
// the labeled API with an empty (or nil) label set must produce a digest
// byte-identical to the legacy unlabeled API. This is the foundation the
// digest freeze rests on.
func TestEmptyLabelByteIdentity(t *testing.T) {
	schedule := func(m *Monitor) {
		m.Sample()
		m.Sample()
	}
	legacy := func() *Monitor {
		m := New()
		m.AddCounter("c", func() uint64 { return 3 })
		m.AddGauge("g", func() int64 { return 4 })
		m.AddHistogram("h", func() []int64 { return []int64{1, 2, 3} })
		schedule(m)
		return m
	}
	keyNil := func() *Monitor {
		m := New()
		m.AddCounterKey("c", nil, func() uint64 { return 3 })
		m.AddGaugeKey("g", nil, func() int64 { return 4 })
		m.AddHistogramKey("h", nil, func() []int64 { return []int64{1, 2, 3} })
		schedule(m)
		return m
	}
	keyEmpty := func() *Monitor {
		m := New()
		m.AddCounterKey("c", []Label{}, func() uint64 { return 3 })
		m.AddGaugeKey("g", []Label{}, func() int64 { return 4 })
		m.AddHistogramKey("h", []Label{}, func() []int64 { return []int64{1, 2, 3} })
		schedule(m)
		return m
	}
	if a, b := legacy().Digest(), keyNil().Digest(); a != b {
		t.Fatalf("nil-label digest differs from legacy:\nlegacy  %q\nnil-key %q", a, b)
	}
	if a, b := legacy().Digest(), keyEmpty().Digest(); a != b {
		t.Fatalf("empty-label digest differs from legacy:\nlegacy    %q\nempty-key %q", a, b)
	}
}

// TestMetricsStringUnlabeledGolden locks the legacy serialization byte for
// byte — the empty-label path must not move.
func TestMetricsStringUnlabeledGolden(t *testing.T) {
	m := New()
	m.AddCounter("c", func() uint64 { return 5 })
	m.AddGauge("g", func() int64 { return 9 })
	m.AddHistogram("h", func() []int64 { return []int64{4} })
	m.Sample()
	want := "counters[1]{c=5} gauges[1]{g=last:9,max:9,mean:9,n:1} histograms[1]{h=n:1,max:4,mean:4,p50:4,p90:4,p99:4}"
	if got := m.Digest(); got != want {
		t.Fatalf("unlabeled digest moved:\ngot  %q\nwant %q", got, want)
	}
}

// TestLabeledSortedIteration verifies sampling/iteration order is (name,
// then labels in canonical order): the unlabeled entry sorts first, then
// labels by value, then the next name.
func TestLabeledSortedIteration(t *testing.T) {
	m := New()
	var order []string
	m.AddGauge("b", func() int64 { order = append(order, "b"); return 0 })
	m.AddGaugeKey("a", []Label{{Key: "k", Value: "2"}}, func() int64 { order = append(order, "a{k=2}"); return 0 })
	m.AddGaugeKey("a", []Label{{Key: "k", Value: "1"}}, func() int64 { order = append(order, "a{k=1}"); return 0 })
	m.AddGauge("a", func() int64 { order = append(order, "a"); return 0 })
	m.Sample()
	got := strings.Join(order, ",")
	if got != "a,a{k=1},a{k=2},b" {
		t.Fatalf("sample order = %q, want %q", got, "a,a{k=1},a{k=2},b")
	}
}

// TestLabeledRegistrationNeutral is the WP1 in-package neutrality gate for
// the labeled path: registering an ADDITIONAL labeled metric must not
// perturb the existing (unlabeled) metrics' sampled values or read counts.
// The zero-RNG-draw half is structural (TestMonitorZeroDrawsStructural: the
// package imports no engine and no math/rand, so the labeled path cannot
// draw). The model-level labeled-run gate lives next to the temporal
// neutrality tests (TestLabeledMetricsNeutral).
func TestLabeledRegistrationNeutral(t *testing.T) {
	build := func(labeled bool) (*Monitor, *int) {
		m := New()
		tick := int64(0)
		labeledReads := 0
		m.AddCounter("total", func() uint64 { return uint64(tick) })
		m.AddGauge("depth", func() int64 { return tick })
		if labeled {
			m.AddGaugeKey("depth", []Label{{Key: "shard", Value: "0"}}, func() int64 {
				labeledReads++
				return tick
			})
		}
		for i := int64(0); i < 3; i++ {
			tick = i
			m.Sample()
		}
		return m, &labeledReads
	}
	base, _ := build(false)
	lab, labeledReads := build(true)

	bs, ls := base.Summary(), lab.Summary()
	if len(ls.Gauges) != len(bs.Gauges)+1 {
		t.Fatalf("labeled run gauges=%d want %d", len(ls.Gauges), len(bs.Gauges)+1)
	}
	bg, _ := bs.Gauge("depth")
	lg, _ := ls.Gauge("depth")
	if bg != lg {
		t.Fatalf("labeled registration perturbed the unlabeled gauge:\nbase %+v\nlab  %+v", bg, lg)
	}
	bc, _ := bs.Counter("total")
	lc, _ := ls.Counter("total")
	if bc != lc {
		t.Fatalf("labeled registration perturbed the unlabeled counter:\nbase %+v\nlab  %+v", bc, lc)
	}
	if *labeledReads != 3 {
		t.Fatalf("labeled sampler read %d times, want 3 (one per Sample)", *labeledReads)
	}
	// The extra labeled gauge itself is correct and rendered deterministically.
	all := ls.String()
	if !strings.Contains(all, "depth{shard=0}=last:2,max:2,mean:1,n:3") {
		t.Fatalf("labeled gauge summary wrong:\n%s", all)
	}
}
