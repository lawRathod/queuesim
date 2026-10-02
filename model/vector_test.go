package model

import (
	"reflect"
	"testing"

	"github.com/law/GoDES/observe"
)

// TestVectorActiveAt: half-open window [start, start+duration); duration 0
// is open-ended.
func TestVectorActiveAt(t *testing.T) {
	v := DecisionVector{Name: "v", Kind: KindAddDrop, StartMs: 100, DurationMs: 200}
	cases := []struct {
		now  int64
		want bool
	}{
		{99, false}, {100, true}, {299, true}, {300, false},
	}
	for _, c := range cases {
		if got := v.ActiveAt(c.now); got != c.want {
			t.Errorf("ActiveAt(%d) = %v, want %v", c.now, got, c.want)
		}
	}
	open := DecisionVector{Name: "o", Kind: KindPollDrop, StartMs: 500, DurationMs: 0}
	if !open.ActiveAt(500) || !open.ActiveAt(1<<30) {
		t.Fatal("duration 0 must be open-ended")
	}
	if open.ActiveAt(499) {
		t.Fatal("open-ended vector active before start")
	}
}

// TestExpandGrid: combinatorial cross product in deterministic order
// (durations outer, rates inner, axes in input order).
func TestExpandGrid(t *testing.T) {
	got := ExpandGrid([]VectorAxis{
		{Kind: KindAddDrop, Target: "matching", StartMs: 100, Durations: []int64{10, 20}, Rates: []float64{0.1, 0.5}},
		{Kind: KindPollDrop, Target: "matching", StartMs: 200, Durations: []int64{30}, Rates: []float64{0.8}},
	})
	if len(got) != 5 {
		t.Fatalf("ExpandGrid returned %d vectors, want 5", len(got))
	}
	if got[0].Name != "add_drop-matching-100-10-0.1" || got[1].Name != "add_drop-matching-100-10-0.5" {
		t.Fatalf("order wrong: %s %s", got[0].Name, got[1].Name)
	}
	if got[2].Name != "add_drop-matching-100-20-0.1" {
		t.Fatalf("duration outer loop broken: %s", got[2].Name)
	}
	if got[4].Name != "poll_drop-matching-200-30-0.8" {
		t.Fatalf("axis order broken: %s", got[4].Name)
	}
}

// TestToConfirmJSONMerge: Confirm overrides merge recursively into the base
// config map; the base is not mutated.
func TestToConfirmJSONMerge(t *testing.T) {
	base := map[string]any{
		"workload": map[string]any{
			"workflow": "fault_amp",
			"fault":    map[string]any{"start_offset_ms": int64(1), "duration_ms": int64(10000)},
		},
		"harness": map[string]any{"seed": int64(42)},
	}
	v := DecisionVector{Name: "v", Kind: KindAddDrop, Confirm: map[string]any{
		"workload": map[string]any{"fault": map[string]any{"drop_rate": 0.3, "start_offset_ms": int64(5000)}},
	}}
	merged, err := v.ToConfirmJSON(base)
	if err != nil {
		t.Fatalf("ToConfirmJSON: %v", err)
	}
	wl := merged["workload"].(map[string]any)
	f := wl["fault"].(map[string]any)
	if f["drop_rate"] != 0.3 || f["start_offset_ms"] != int64(5000) || f["duration_ms"] != int64(10000) {
		t.Fatalf("merge wrong: %v", f)
	}
	if merged["harness"].(map[string]any)["seed"] != int64(42) {
		t.Fatalf("unrelated section mutated: %v", merged["harness"])
	}
	if base["workload"].(map[string]any)["fault"].(map[string]any)["drop_rate"] != nil {
		t.Fatal("base map mutated by ToConfirmJSON")
	}
	// Determinism: same input, byte-identical output.
	m2, _ := v.ToConfirmJSON(base)
	if !reflect.DeepEqual(merged, m2) {
		t.Fatal("ToConfirmJSON not a pure function")
	}
	// nil Confirm: returns a copy of the base, no error.
	nv := DecisionVector{Name: "n"}
	m3, err := nv.ToConfirmJSON(base)
	if err != nil || !reflect.DeepEqual(m3, base) {
		t.Fatalf("nil-Confirm merge: %v %v", m3, err)
	}
}

// TestMembers: a plain vector is its own member set; a composed vector
// returns its members in declared order (determinism contract — slice
// order, never a map).
func TestMembers(t *testing.T) {
	solo := DecisionVector{Name: "solo", Kind: KindAddDrop}
	if got := solo.Members(); len(got) != 1 || !reflect.DeepEqual(got[0], solo) {
		t.Fatalf("solo Members() = %v, want [self]", got)
	}
	m1 := DecisionVector{Name: "m1", Kind: KindAddDrop, Rate: 0.3}
	m2 := DecisionVector{Name: "m2", Kind: KindS2STimeout, Rate: 100}
	comp := DecisionVector{Name: "compose-m1-m2", Compose: []DecisionVector{m1, m2}}
	got := comp.Members()
	if len(got) != 2 || !reflect.DeepEqual(got[0], m1) || !reflect.DeepEqual(got[1], m2) {
		t.Fatalf("composed Members() order wrong: %v", got)
	}
	// Members() must not alias the Compose slice (callers must be free to
	// mutate the returned slice without corrupting the vector).
	got[0].Name = "mutated"
	if comp.Compose[0].Name != "m1" {
		t.Fatal("Members() aliases the Compose slice")
	}
}

// TestMergeOverlays: sequential overlay merge — base copied, overlays
// applied in order (last wins), overlays and base never mutated, nil base
// treated as empty.
func TestMergeOverlays(t *testing.T) {
	base := map[string]any{
		"workload": map[string]any{"fault": map[string]any{"drop_rate": 0.0}},
		"harness":  map[string]any{"pollers": float64(4)},
	}
	o1 := map[string]any{"workload": map[string]any{"fault": map[string]any{"drop_rate": 0.3, "start_offset_ms": int64(5000)}}}
	o2 := map[string]any{"workload": map[string]any{"fault": map[string]any{"start_offset_ms": int64(8000)}}, "harness": map[string]any{"pollers": float64(1)}}
	merged := MergeOverlays(base, o1, o2)
	f := merged["workload"].(map[string]any)["fault"].(map[string]any)
	if f["drop_rate"] != 0.3 || f["start_offset_ms"] != int64(8000) {
		t.Fatalf("last-wins merge wrong: %v", f)
	}
	if merged["harness"].(map[string]any)["pollers"] != float64(1) {
		t.Fatalf("overlay 2 harness not applied: %v", merged["harness"])
	}
	// Immutability: base and both overlays untouched.
	if base["workload"].(map[string]any)["fault"].(map[string]any)["start_offset_ms"] != nil {
		t.Fatal("base mutated by MergeOverlays")
	}
	if o2["harness"].(map[string]any)["pollers"] != float64(1) || o1["workload"].(map[string]any)["fault"].(map[string]any)["drop_rate"] != 0.3 {
		t.Fatal("overlay mutated by MergeOverlays")
	}
	// Overlay-order sensitivity: swapping overlays changes the result.
	swapped := MergeOverlays(base, o2, o1)
	if swapped["workload"].(map[string]any)["fault"].(map[string]any)["start_offset_ms"] != int64(5000) {
		t.Fatal("merge not order-sensitive — sequential override broken")
	}
	// Nil base: result is the merged overlays, no panic.
	if got := MergeOverlays(nil, o1); got["workload"].(map[string]any)["fault"].(map[string]any)["drop_rate"] != 0.3 {
		t.Fatalf("nil base broken: %v", got)
	}
	// Determinism: pure function of inputs.
	if !reflect.DeepEqual(merged, MergeOverlays(base, o1, o2)) {
		t.Fatal("MergeOverlays not a pure function")
	}
}

// TestExpandGridComposed: a composed axis expands to one vector whose
// members are the expanded member singletons, named "compose-<m1>-<m2>".
// Member axes with multiple durations/rates cross-combine (product, member
// order preserved).
func TestExpandGridComposed(t *testing.T) {
	got := ExpandGrid([]VectorAxis{
		{
			Compose: []VectorAxis{
				{Kind: KindAddDrop, Target: "matching", StartMs: 1, Durations: []int64{10000}, Rates: []float64{0.3}},
				{Kind: KindActivityFailure, Target: "activity", StartMs: 1, Durations: []int64{10000}, Rates: []float64{0}},
			},
		},
	})
	if len(got) != 1 {
		t.Fatalf("ExpandGrid composed returned %d vectors, want 1", len(got))
	}
	v := got[0]
	if v.Name != "compose-add_drop-matching-1-10000-0.3-activity_failure-activity-1-10000-0" {
		t.Fatalf("composed name wrong: %s", v.Name)
	}
	if v.Kind != "" || len(v.Compose) != 2 {
		t.Fatalf("composed shape wrong: %+v", v)
	}
	if v.Compose[0].Name != "add_drop-matching-1-10000-0.3" || v.Compose[1].Name != "activity_failure-activity-1-10000-0" {
		t.Fatalf("member expansion wrong: %v %v", v.Compose[0].Name, v.Compose[1].Name)
	}
	// Axis Confirm + FaultWindow propagate to the expanded vector.
	withCfg := ExpandGrid([]VectorAxis{{
		Compose: []VectorAxis{
			{Kind: KindPollerMin, Target: "matching", StartMs: 0, Durations: []int64{5000}, Rates: []float64{1}},
			{Kind: KindS2STimeout, Target: "matching", StartMs: 0, Durations: []int64{5000}, Rates: []float64{100}},
		},
		Confirm: map[string]any{"harness": map[string]any{"pollers": float64(1)}},
	}})
	if withCfg[0].Confirm["harness"].(map[string]any)["pollers"] != float64(1) {
		t.Fatalf("axis Confirm not propagated: %v", withCfg[0].Confirm)
	}
}

// TestRegimeFromStatus: metastable = Metastable && !Recovered; recoverable =
// Metastable && Recovered; nothing otherwise.
func TestRegimeFromStatus(t *testing.T) {
	if got := RegimeFromStatus(observe.MonitorStatus{Metastable: true, Recovered: false}); got != RegimeMetastable {
		t.Fatalf("metastable-only status → %q", got)
	}
	if got := RegimeFromStatus(observe.MonitorStatus{Metastable: true, Recovered: true}); got != RegimeRecoverable {
		t.Fatalf("metastable+recovered status → %q", got)
	}
	if got := RegimeFromStatus(observe.MonitorStatus{Metastable: false, Recovered: true}); got != RegimeNothing {
		t.Fatalf("recovered-without-metastable → %q", got)
	}
	if got := RegimeFromStatus(observe.MonitorStatus{}); got != RegimeNothing {
		t.Fatalf("empty status → %q", got)
	}
}

// TestMonitorConfigShared: the shared config is byte-for-byte the bubble's
// monitor literal (ADR-069 window fields; the rest zero) so both legs
// observe identical semantics.
func TestMonitorConfigShared(t *testing.T) {
	cfg := MonitorConfig()
	if cfg.WindowMs != 100 || cfg.BaselineWarmupWindows != 50 || cfg.MetastablePersistWindows != 50 {
		t.Fatalf("ADR-069 fields wrong: %+v", cfg)
	}
	// Zero-valued elsewhere — identical to retry_amp's monitor literal.
	if cfg.RecoveryHoldWindows != 0 || cfg.BaselineEpsilon != 0 {
		t.Fatalf("shared config deviates from the bubble literal: %+v", cfg)
	}
	// Must be constructible (NewMonitor panics on degenerate configs).
	m := observe.NewMonitor(cfg, 0)
	if m == nil {
		t.Fatal("NewMonitor returned nil")
	}
}
