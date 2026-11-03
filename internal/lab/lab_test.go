package lab

import (
	"reflect"
	"strings"
	"testing"

	"github.com/lawRathod/queuesim/models/tasqueue"
	"github.com/lawRathod/queuesim/models/temporal"
)

func TestCatalogIsConsistent(t *testing.T) {
	infos := Catalog()
	if len(infos) < 10 {
		t.Fatalf("catalog has %d scenarios, want at least 10", len(infos))
	}
	seen := map[string]bool{}
	for _, info := range infos {
		if seen[info.ID] {
			t.Errorf("duplicate scenario id %q", info.ID)
		}
		seen[info.ID] = true
		if info.Title == "" || info.Blurb == "" || len(info.WhatToWatch) == 0 {
			t.Errorf("%s: incomplete catalog text", info.ID)
		}
		seriesKeys := map[string]bool{}
		for _, s := range info.Series {
			seriesKeys[s.Key] = true
		}
		for _, c := range info.Charts {
			for _, k := range c.Series {
				if !seriesKeys[k] {
					t.Errorf("%s: chart %q references unknown series %q", info.ID, c.Title, k)
				}
			}
		}
		for _, k := range info.Knobs {
			if _, ok := info.Preset[k.Key]; !ok {
				t.Errorf("%s: knob %q has no preset value", info.ID, k.Key)
				continue
			}
			v := info.Preset[k.Key]
			if v < k.Min || v > k.Max {
				t.Errorf("%s: knob %q default %g outside [%g,%g]", info.ID, k.Key, v, k.Min, k.Max)
			}
			if k.Step <= 0 {
				t.Errorf("%s: knob %q step must be positive", info.ID, k.Key)
			}
		}
	}
}

func TestEveryScenarioRuns(t *testing.T) {
	for _, info := range Catalog() {
		info := info
		t.Run(info.ID, func(t *testing.T) {
			r, err := Execute(Request{Scenario: info.ID})
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}
			if len(r.T) < 2 {
				t.Fatalf("trace has %d points", len(r.T))
			}
			for _, s := range info.Series {
				vs, ok := r.V[s.Key]
				if !ok {
					t.Errorf("missing series %q", s.Key)
					continue
				}
				if len(vs) != len(r.T) {
					t.Errorf("series %q has %d values, timeline has %d", s.Key, len(vs), len(r.T))
				}
			}
			if r.Digest == "" {
				t.Error("empty determinism digest")
			}
			if len(r.Metrics) == 0 {
				t.Error("no summary metrics")
			}
			if r.SimEndMs <= 0 {
				t.Errorf("sim end %d", r.SimEndMs)
			}
			if got := metricValue(r, "attempts_per_start"); got < 0 {
				t.Errorf("negative attempts per start %v", got)
			}
		})
	}
}

func TestSameConfigSameDigestAndTrace(t *testing.T) {
	for _, id := range []string{"retry-storm", "throttle", "tasq-flaky"} {
		id := id
		t.Run(id, func(t *testing.T) {
			a, err := Execute(Request{Scenario: id, Seed: int64p(7)})
			if err != nil {
				t.Fatal(err)
			}
			b, err := Execute(Request{Scenario: id, Seed: int64p(7)})
			if err != nil {
				t.Fatal(err)
			}
			if a.Digest != b.Digest {
				t.Fatalf("digest moved: %q vs %q", a.Digest, b.Digest)
			}
			if !reflect.DeepEqual(a.V, b.V) {
				t.Fatal("timelines differ for the same seed")
			}
		})
	}
}

// TestSnapshotHookIsNeutral proves the UI's sampling path does not perturb the
// model: a plain run and a lab run (which attaches a tick hook) share a digest.
func TestSnapshotHookIsNeutral(t *testing.T) {
	info := scenarios["throttle"]
	vals := effectiveValues(info.defs, info.preset, map[string]float64{"arrival_rate": 12})
	cfg := applyTemporal(vals)
	cfg.Seed = 42
	plain, err := temporal.NewTopology(cfg)
	if err != nil {
		t.Fatal(err)
	}
	plainRes := plain.Run()

	withHook, err := Execute(Request{Scenario: "throttle", Seed: int64p(42), Knobs: map[string]float64{"arrival_rate": 12}})
	if err != nil {
		t.Fatal(err)
	}
	if plainRes.Digest != withHook.Digest {
		t.Fatalf("snapshot hook changed the digest:\nplain %s\nhook  %s", plainRes.Digest, withHook.Digest)
	}
}

func TestKnobValidation(t *testing.T) {
	if _, err := Execute(Request{Scenario: "calm", Knobs: map[string]float64{"nope": 1}}); err == nil {
		t.Error("unknown knob accepted")
	}
	if _, err := Execute(Request{Scenario: "calm", Knobs: map[string]float64{"pollers_wf": 0}}); err == nil {
		t.Error("out-of-range knob accepted")
	}
	if _, err := Execute(Request{Scenario: "nope"}); err == nil {
		t.Error("unknown scenario accepted")
	}
}

func TestKnobOverrideReachesTheModel(t *testing.T) {
	r, err := Execute(Request{Scenario: "throttle", Knobs: map[string]float64{"dispatch_rate": 40, "arrival_rate": 10}})
	if err != nil {
		t.Fatal(err)
	}
	if r.Knobs["dispatch_rate"] != 40 || r.Knobs["arrival_rate"] != 10 {
		t.Fatalf("knobs not echoed: %v", r.Knobs)
	}
	// With dispatch (40/s) above arrivals (10/s) the backlog must not grow to
	// the hundreds the throttled preset reaches.
	if peak := metricValue(r, "peak_backlog"); peak > 60 {
		t.Errorf("peak backlog %v with dispatch above arrivals", peak)
	}
}

func TestSweepVariesBySeed(t *testing.T) {
	points, err := Sweep("retry-storm", nil, []int64{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 3 {
		t.Fatalf("got %d points", len(points))
	}
	distinct := map[string]bool{}
	for _, p := range points {
		distinct[p.Digest] = true
		if p.Digest == "" {
			t.Error("empty digest in sweep")
		}
	}
	if len(distinct) < 2 {
		t.Errorf("all seeds produced one digest: %v", distinct)
	}
}

func TestTemporalSnapshotFieldsAreSane(t *testing.T) {
	info := scenarios["bottleneck"]
	vals := effectiveValues(info.defs, info.preset, nil)
	cfg := applyTemporal(vals)
	cfg.Seed = 42
	top, err := temporal.NewTopology(cfg)
	if err != nil {
		t.Fatal(err)
	}
	res := top.Run()
	snap := top.Snapshot()
	if snap.TMs <= 0 {
		t.Errorf("snapshot time %d", snap.TMs)
	}
	if snap.Started != res.Stats.Started {
		t.Errorf("snapshot started %d, stats %d", snap.Started, res.Stats.Started)
	}
}

func int64p(v int64) *int64 { return &v }

func TestInsightsNameTheMechanism(t *testing.T) {
	cases := []struct {
		scenario string
		want     string
	}{
		{"throttle", "dispatch throttle"},
		{"retry-storm", "retry amplification"},
		{"tasq-poison", "failed"},
		{"bottleneck", "saturated"},
		{"fanout", "fan-out"},
	}
	for _, c := range cases {
		r, err := Execute(Request{Scenario: c.scenario})
		if err != nil {
			t.Fatalf("%s: %v", c.scenario, err)
		}
		if len(r.Insights) == 0 {
			t.Errorf("%s: no insights", c.scenario)
			continue
		}
		var all string
		for _, in := range r.Insights {
			all += strings.ToLower(in.Title + " " + in.Detail + ". ")
		}
		if !strings.Contains(all, c.want) {
			t.Errorf("%s: insights do not mention %q:\n%s", c.scenario, c.want, all)
		}
		if len(r.Insights) > 4 {
			t.Errorf("%s: %d insights, want at most 4", c.scenario, len(r.Insights))
		}
	}
}

func TestEveryRunHasBoundedInsights(t *testing.T) {
	for _, info := range Catalog() {
		r, err := Execute(Request{Scenario: info.ID})
		if err != nil {
			t.Fatal(err)
		}
		if len(r.Insights) > 4 {
			t.Errorf("%s: %d insights", info.ID, len(r.Insights))
		}
		for _, in := range r.Insights {
			if in.Title == "" || in.Detail == "" {
				t.Errorf("%s: empty insight %+v", info.ID, in)
			}
		}
	}
}

// TestSnapshotBeforeRunIsSafe guards the public Snapshot API: before the
// system is built the mechanism components do not exist yet, and sampling
// must return an empty snapshot rather than panic.
func TestSnapshotBeforeRunIsSafe(t *testing.T) {
	tt, err := temporal.NewTopology(temporal.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if s := tt.Snapshot(); s.Started != 0 {
		t.Errorf("temporal pre-run snapshot: %+v", s)
	}
	tq, err := tasqueue.NewTopology(tasqueue.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	if s := tq.Snapshot(); s.Started != 0 {
		t.Errorf("tasqueue pre-run snapshot: %+v", s)
	}
}
