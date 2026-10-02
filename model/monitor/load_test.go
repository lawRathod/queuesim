package monitor

import "testing"

// TestLoadVocabularyLeaves pins the shared leaf names — the contract that
// keeps models/temporal and models/tasqueue from drifting (WP3).
func TestLoadVocabularyLeaves(t *testing.T) {
	cases := map[string]string{
		LoadQueueDepth:  "queue_depth",
		LoadUtilization: "utilization",
		LoadInflight:    "inflight",
		LoadThroughput:  "throughput",
	}
	for got, want := range cases {
		if got != want {
			t.Fatalf("load leaf = %q, want %q", got, want)
		}
	}
}

// TestLoadNameNamespaces pins the single separator.
func TestLoadNameNamespaces(t *testing.T) {
	if got := LoadName("history.shards", LoadQueueDepth); got != "history.shards.queue_depth" {
		t.Fatalf("LoadName = %q, want history.shards.queue_depth", got)
	}
}

// TestUtilizationPermille pins the service-rate math, including the
// unlimited-budget gate (ok=false, value 0 — never a negative gauge sample).
func TestUtilizationPermille(t *testing.T) {
	cases := []struct {
		inflight, budget int
		want             int64
		ok               bool
	}{
		{0, 4, 0, true},
		{4, 4, 1000, true},
		{2, 4, 500, true},
		{1, 3, 333, true}, // integer truncation
		{3, 3, 1000, true},
		{5, 4, 1250, true}, // over budget must not panic or wrap
		{0, 0, 0, false},   // unlimited: undefined ratio
		{0, -1, 0, false},
	}
	for _, c := range cases {
		got, ok := UtilizationPermille(c.inflight, c.budget)
		if got != c.want || ok != c.ok {
			t.Fatalf("UtilizationPermille(%d,%d) = (%d,%v), want (%d,%v)", c.inflight, c.budget, got, ok, c.want, c.ok)
		}
	}
}

// TestNonEmptyFraction pins the queue-non-empty fraction: samples with
// depth > 0 over total samples, 0 for no data.
func TestNonEmptyFraction(t *testing.T) {
	cases := []struct {
		name   string
		series []int64
		want   float64
	}{
		{"nil", nil, 0},
		{"empty", []int64{}, 0},
		{"all zero", []int64{0, 0, 0}, 0},
		{"single zero", []int64{0}, 0},
		{"single positive", []int64{7}, 1},
		{"all positive", []int64{1, 2, 3, 4}, 1},
		{"half", []int64{0, 0, 1, 1}, 0.5},
		{"quarter", []int64{0, 0, 0, 5}, 0.25},
	}
	for _, c := range cases {
		if got := NonEmptyFraction(c.series); got != c.want {
			t.Fatalf("%s: NonEmptyFraction = %v, want %v", c.name, got, c.want)
		}
	}
}
