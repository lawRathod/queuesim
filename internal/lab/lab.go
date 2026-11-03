// Package lab turns the deterministic queue models into runnable,
// inspectable experiments for the web UI and the CLI.
//
// A Scenario is a named preset plus the knob surface a visitor may turn.
// Running one builds the model, samples its live state once per stride,
// and returns a JSON-ready trace: the timeline (T + V), the final summary,
// and the determinism digest. Sampling is read-only by construction (the
// model's Snapshot methods draw no RNG and mutate nothing), so a run with
// the UI's sampler attached is bit-identical to one without it.
package lab

// Knob is one adjustable parameter, rendered as a control in the web UI.
type Knob struct {
	Key     string   `json:"key"`
	Label   string   `json:"label"`
	Group   string   `json:"group"`
	Help    string   `json:"help"`
	Unit    string   `json:"unit,omitempty"`
	Kind    string   `json:"kind"` // "slider" | "number" | "toggle" | "select"
	Min     float64  `json:"min"`
	Max     float64  `json:"max"`
	Step    float64  `json:"step"`
	Default float64  `json:"default"`
	Options []Option `json:"options,omitempty"`
}

// Option is one choice of a select knob.
type Option struct {
	Label string  `json:"label"`
	Value float64 `json:"value"`
}

// Annotation marks an interesting interval on the timeline (a fault window,
// a load step). The UI shades it behind the charts.
type Annotation struct {
	Label   string `json:"label"`
	StartMs int64  `json:"start_ms"`
	EndMs   int64  `json:"end_ms"`
	Kind    string `json:"kind"` // "fault" | "load"
}

// SeriesMeta describes one timeline series.
type SeriesMeta struct {
	Key   string `json:"key"`
	Label string `json:"label"`
	Unit  string `json:"unit"`
	Kind  string `json:"kind"` // "gauge" | "counter" | "rate"
}

// Chart is a named multi-line chart over the shared timeline.
type Chart struct {
	Title  string   `json:"title"`
	Unit   string   `json:"unit"`
	Series []string `json:"series"`
	Note   string   `json:"note,omitempty"`
}

// Metric is one headline number in the summary panel.
type Metric struct {
	Key       string  `json:"key"`
	Label     string  `json:"label"`
	Value     float64 `json:"value"`
	Unit      string  `json:"unit,omitempty"`
	Precision int     `json:"precision"`
	Help      string  `json:"help,omitempty"`
	Tone      string  `json:"tone,omitempty"` // "good" | "warn" | "bad"
}

// Percentile is one latency distribution row (from the model's monitor).
type Percentile struct {
	Name  string `json:"name"`
	Label string `json:"label"`
	Count uint64 `json:"count"`
	P50Ms int64  `json:"p50_ms"`
	P90Ms int64  `json:"p90_ms"`
	P99Ms int64  `json:"p99_ms"`
	MaxMs int64  `json:"max_ms"`
}

// Status is the model's internal metastability classification, when the
// model produces one.
type Status struct {
	Metastable     bool  `json:"metastable"`
	Recovered      bool  `json:"recovered"`
	MetastableAtMs int64 `json:"metastable_at_ms"`
	RecoveryTimeMs int64 `json:"recovery_time_ms"`
}

// Insight is one rule-derived observation about a run — a short, honest
// reading of what the numbers mean, shown above the charts.
type Insight struct {
	Tone   string `json:"tone,omitempty"` // "good" | "warn" | "bad"
	Title  string `json:"title"`
	Detail string `json:"detail"`
}

// Run is a complete, self-contained experiment result. Nothing is stored
// server-side: the response is the artifact.
type Run struct {
	Scenario    string               `json:"scenario"`
	Model       string               `json:"model"`
	Seed        int64                `json:"seed"`
	SimEndMs    int64                `json:"sim_end_ms"`
	WallMs      int64                `json:"wall_ms"`
	Knobs       map[string]float64   `json:"knobs"`
	Annotations []Annotation         `json:"annotations"`
	Series      []SeriesMeta         `json:"series"`
	Charts      []Chart              `json:"charts"`
	T           []int64              `json:"t"`
	V           map[string][]float64 `json:"v"`
	Metrics     []Metric             `json:"metrics"`
	Insights    []Insight            `json:"insights"`
	Percentiles []Percentile         `json:"percentiles"`
	Status      *Status              `json:"status,omitempty"`
	Digest      string               `json:"digest"`
	Notes       []string             `json:"notes,omitempty"`
}

// Info is a scenario's catalog entry: enough for the UI to render the
// picker and build its knob form without knowing the model internals.
type Info struct {
	ID          string             `json:"id"`
	Model       string             `json:"model"`
	Group       string             `json:"group"`
	Title       string             `json:"title"`
	Subtitle    string             `json:"subtitle"`
	Blurb       string             `json:"blurb"`
	WhatToWatch []string           `json:"what_to_watch"`
	Knobs       []Knob             `json:"knobs"`
	Preset      map[string]float64 `json:"preset"`
	Charts      []Chart            `json:"charts"`
	Series      []SeriesMeta       `json:"series"`
	DefaultSeed int64              `json:"default_seed"`
}

// Request is a run request from the UI or CLI.
type Request struct {
	Scenario string             `json:"scenario"`
	Seed     *int64             `json:"seed,omitempty"`
	Knobs    map[string]float64 `json:"knobs,omitempty"`
}

// SweepPoint is one seed's outcome in a determinism sweep.
type SweepPoint struct {
	Seed      int64   `json:"seed"`
	Digest    string  `json:"digest"`
	Value     float64 `json:"value"`
	Completed uint64  `json:"completed"`
	Failed    uint64  `json:"failed"`
	MaxDepth  float64 `json:"max_depth"`
}
