package capture

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
)

// TestWriteDOTDeterministicAndNoLegend asserts WriteDOT renders a
// byte-identical graph regardless of event input order, with no legend
// node and no cluster subgraph.
func TestWriteDOTDeterministicAndNoLegend(t *testing.T) {
	evs := []Event{
		{From: "history", To: "matching", Kind: "wf_add", TsMs: 2},
		{From: "matching", To: "exec", Kind: "dispatch", TsMs: 3},
		{From: "load", To: "history", Kind: "workflow_start", TsMs: 1},
	}
	var b1, b2 bytes.Buffer
	if err := Aggregate(evs).WriteDOT(&b1); err != nil {
		t.Fatalf("WriteDOT: %v", err)
	}
	if err := Aggregate(reverseEvents(evs)).WriteDOT(&b2); err != nil {
		t.Fatalf("WriteDOT: %v", err)
	}
	if b1.String() != b2.String() {
		t.Fatalf("WriteDOT not input-order deterministic:\n---\n%s\n---\n%s", b1.String(), b2.String())
	}
	dot := b1.String()
	for _, forbidden := range []string{"legend", "cluster"} {
		if strings.Contains(strings.ToLower(dot), forbidden) {
			t.Errorf("DOT contains forbidden %q:\n%s", forbidden, dot)
		}
	}
	// The single-style rule: the ONLY node/edge style statements are the
	// two graph-level defaults — no per-element style blocks may appear.
	for _, perElement := range []string{"node [", "edge ["} {
		if strings.Count(dot, perElement) > 1 {
			t.Errorf("DOT repeats default %q — styles must stay single:\n%s", perElement, dot)
		}
	}
}

// TestWriteDOTStructure asserts the DOT header, node declarations and edge
// statements exist and use the configured graph name, the layout/style
// attributes, and the kind+count label.
func TestWriteDOTStructure(t *testing.T) {
	evs := []Event{
		{From: "load", To: "history", Kind: "workflow_start", TsMs: 1},
		{From: "load", To: "history", Kind: "workflow_start", TsMs: 9},
		{From: "history", To: "matching", Kind: "act_add", TsMs: 4},
	}
	var buf bytes.Buffer
	if err := Aggregate(evs).WriteDOT(&buf); err != nil {
		t.Fatalf("WriteDOT: %v", err)
	}
	dot := buf.String()
	for _, want := range []string{
		"digraph mechanism_map {",
		`rankdir=LR`,
		`ranksep=1.5`,
		`nodesep=0.9`,
		`splines=spline`,
		`pad=0.5`,
		`"history"`,
		`"load"`,
		`"matching"`,
		`"load" -> "history" [label="workflow_start x2"`,
		`"history" -> "matching" [label="act_add x1"`,
	} {
		if !strings.Contains(dot, want) {
			t.Errorf("DOT missing %q:\n%s", want, dot)
		}
	}
	// The kind+count label and the penwidth attribute are both present on
	// the same aggregated edge statement.
	attr := fullEdgeAttr("workflow_start x2", "2") // 1 + 2.2*log10(3) ≈ 2.0 → trimmed "2"
	if !strings.Contains(dot, `"load" -> "history" `+attr) {
		t.Errorf("load->history edge missing full attributes %q:\n%s", attr, dot)
	}
}

// TestWriteDOTNodeEdgeDefaults asserts the single shared node style and the
// single shared edge style are emitted as graph-level default statements
// (one node default, one edge default), so every node and edge renders
// uniformly without per-element styling or a legend.
func TestWriteDOTNodeEdgeDefaults(t *testing.T) {
	evs := []Event{
		{From: "load", To: "history", Kind: "workflow_start", TsMs: 1},
	}
	var buf bytes.Buffer
	if err := Aggregate(evs).WriteDOT(&buf); err != nil {
		t.Fatalf("WriteDOT: %v", err)
	}
	dot := buf.String()
	for _, want := range []string{
		`node [shape=box style="rounded,filled" fillcolor="#f0f2f7" color="#4a4f5a" fontname="Helvetica" fontsize=15]`,
		`edge [color="#5a5f6a" fontsize=11 fontname="Helvetica" arrowsize=0.8]`,
	} {
		if !strings.Contains(dot, want) {
			t.Errorf("DOT missing default %q:\n%s", want, dot)
		}
	}
	// Exactly one node default and one edge default statement.
	if got := strings.Count(dot, "node ["); got != 1 {
		t.Errorf("expected exactly one node default, found %d:\n%s", got, dot)
	}
	if got := strings.Count(dot, "edge ["); got != 1 {
		t.Errorf("expected exactly one edge default, found %d:\n%s", got, dot)
	}
}

// fullEdgeAttr is the complete attribute suffix WriteDOT emits for an edge
// statement, used to assert the label and penwidth appear together.
func fullEdgeAttr(label, pw string) string {
	return `[label="` + label + `" penwidth=` + pw + `]`
}

// TestWriteDOTPenwidth asserts the penwidth formula
// min(1 + 2.2*log10(count+1), 7), rounded to one decimal with a trailing
// .0 trimmed, at representative counts. The kind prefixes the count in the
// label (edgeLabel), and the count still drives penwidth.
func TestWriteDOTPenwidth(t *testing.T) {
	cases := []struct {
		count int
		want  string // the exact penwidth value after label="k xCOUNT" penwidth=
	}{
		{1, "1.7"},     // 1 + 2.2*log10(2) ≈ 1.7
		{2, "2"},       // 1 + 2.2*log10(3) ≈ 2.0 → trimmed "2"
		{9, "3.2"},     // 1 + 2.2*log10(10) = 3.2
		{50, "4.8"},    // 1 + 2.2*log10(51) ≈ 4.8
		{99, "5.4"},    // 1 + 2.2*log10(100) = 5.4
		{100, "5.4"},   // 1 + 2.2*log10(101) ≈ 5.4
		{1000, "7"},    // 1 + 2.2*log10(1001) ≈ 7.6 → capped at 7
		{1999, "7"},    // 1 + 2.2*log10(2000) ≈ 8.3 → capped at 7
		{2049, "7"},    // 1 + 2.2*log10(2050) ≈ 8.3 → capped at 7
		{1000000, "7"}, // 1 + 2.2*log10(1000001) ≈ 14.2 → capped at 7
	}
	// Counts below 1 keep the floor penwidth 1 (defensive: an aggregated
	// edge only exists once a transition was observed, so count>=1 in the
	// emitted graph).
	if got := penwidth(0); got != "1" {
		t.Errorf("penwidth(0) = %q, want %q", got, "1")
	}
	for _, tc := range cases {
		evs := make([]Event, tc.count)
		for i := range evs {
			evs[i] = Event{From: "a", To: "b", Kind: "k", TsMs: int64(i)}
		}
		var buf bytes.Buffer
		if err := Aggregate(evs).WriteDOT(&buf); err != nil {
			t.Fatalf("WriteDOT(count=%d): %v", tc.count, err)
		}
		want := `label="k x` + itoa(tc.count) + `" penwidth=` + tc.want
		if !strings.Contains(buf.String(), want) {
			t.Errorf("count=%d: DOT missing %q\n%s", tc.count, want, buf.String())
		}
	}
}

// TestWriteDOTKindsDistinguishable asserts two parallel edges — the same
// (from,to) pair with DIFFERENT kinds — carry labels that name their kind,
// so a rendered arrow is readable without a legend. The old count-only
// labels ("50"/"50") were indistinguishable; the regression pins the fix.
func TestWriteDOTKindsDistinguishable(t *testing.T) {
	evs := []Event{
		{From: "exec", To: "history", Kind: "act_ok", TsMs: 1},
		{From: "exec", To: "history", Kind: "wf_task_done", TsMs: 2},
		{From: "exec", To: "history", Kind: "act_retry", TsMs: 3},
	}
	var buf bytes.Buffer
	if err := Aggregate(evs).WriteDOT(&buf); err != nil {
		t.Fatalf("WriteDOT: %v", err)
	}
	dot := buf.String()
	for _, want := range []string{
		`"exec" -> "history" [label="act_ok x1"`,
		`"exec" -> "history" [label="wf_task_done x1"`,
		`"exec" -> "history" [label="act_retry x1"`,
	} {
		if !strings.Contains(dot, want) {
			t.Errorf("DOT missing distinguishable label %q:\n%s", want, dot)
		}
	}
	// No bare count label survives: every label must name its kind.
	if strings.Contains(dot, `[label="1" penwidth=`) {
		t.Errorf("bare count label without kind present:\n%s", dot)
	}
}

// TestWriteDOTEmpty renders a valid minimal graph with only the header and
// the unconditional graph attributes.
func TestWriteDOTEmpty(t *testing.T) {
	var buf bytes.Buffer
	if err := Aggregate(nil).WriteDOT(&buf); err != nil {
		t.Fatalf("WriteDOT(empty): %v", err)
	}
	dot := buf.String()
	if !strings.Contains(dot, "digraph mechanism_map {") || !strings.Contains(dot, "}") {
		t.Errorf("empty DOT malformed:\n%s", dot)
	}
	for _, want := range []string{`ranksep=1.5`, `nodesep=0.9`, `splines=spline`, `pad=0.5`, `node [`, `edge [`} {
		if !strings.Contains(dot, want) {
			t.Errorf("empty DOT missing unconditional attribute %q:\n%s", want, dot)
		}
	}
}

// TestWriteDOTNilReceiverAndNilWriter asserts a nil *Graph receiver renders
// an empty digraph instead of panicking, and a nil writer returns an error.
func TestWriteDOTNilReceiverAndNilWriter(t *testing.T) {
	var g *Graph
	var buf bytes.Buffer
	if err := g.WriteDOT(&buf); err != nil {
		t.Fatalf("WriteDOT(nil receiver): %v", err)
	}
	if !strings.Contains(buf.String(), "digraph mechanism_map {") || !strings.Contains(buf.String(), "}") {
		t.Errorf("nil-receiver DOT malformed:\n%s", buf.String())
	}
	if err := Aggregate(nil).WriteDOT(nil); err == nil {
		t.Fatal("WriteDOT(nil writer) should return an error")
	}
}

// TestWriteDOTSanitizesIdentifiers asserts component names with characters
// that are illegal in DOT identifiers (quotes, brackets, whitespace) are
// quoted/escaped rather than breaking the render.
func TestWriteDOTSanitizesIdentifiers(t *testing.T) {
	evs := []Event{
		{From: `weird"name`, To: `bracket[name]`, Kind: "k", TsMs: 1},
		{From: `load`, To: `space name`, Kind: "k", TsMs: 2},
	}
	var buf bytes.Buffer
	if err := Aggregate(evs).WriteDOT(&buf); err != nil {
		t.Fatalf("WriteDOT: %v", err)
	}
	dot := buf.String()
	// Every node and edge must be enclosed in double quotes (we quote
	// unconditionally) so raw DOT metacharacters cannot escape.
	if !strings.Contains(dot, `"weird\"name"`) {
		t.Errorf("backslash-escaped quote node missing:\n%s", dot)
	}
	if !strings.Contains(dot, `"space name"`) {
		t.Errorf("space node missing:\n%s", dot)
	}
}

// itoa renders the table counts as decimal strings for expected labels.
func itoa(i int) string {
	return strconv.Itoa(i)
}
