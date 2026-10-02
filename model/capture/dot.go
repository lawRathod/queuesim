package capture

import (
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

// graphName is the DOT graph name emitted by WriteDOT.
const graphName = "mechanism_map"

// graphAttributes are the graph-level DOT layout attributes emitted after
// rankdir, in this fixed order, so the emitted graph is deterministic. The
// values spread the mechanism-map stages apart (ranksep/nodesep), route
// parallel edges between the same (from,to) pair onto separate curved
// splines instead of stacking them, and add a margin so node labels never
// touch the image edge. splines=spline, NOT curved: graphviz dot collapses
// parallel edges onto one identical path under curved and drops their edge
// labels (verified against dot 16.0), while spline keeps every parallel
// route distinct and labeled.
var graphAttributes = []string{
	"ranksep=1.5",
	"nodesep=0.9",
	"splines=spline",
	"pad=0.5",
}

// nodeDefaults is the single shared node style emitted as one graph-level
// node default statement: every observed component renders as the same
// rounded, light-filled box with a dark gray border and sans-serif label.
var nodeDefaults = []string{
	`shape=box`,
	`style="rounded,filled"`,
	`fillcolor="#f0f2f7"`,
	`color="#4a4f5a"`,
	`fontname="Helvetica"`,
	`fontsize=15`,
}

// edgeDefaults is the single shared edge style emitted as one graph-level
// edge default statement: every observed transition renders in dark gray
// with a smaller sans-serif label and a slightly smaller arrowhead so many
// parallel edges stay legible. The per-edge label and penwidth remain the
// data — they are emitted per edge statement, not defaulted here.
var edgeDefaults = []string{
	`color="#5a5f6a"`,
	`fontsize=11`,
	`fontname="Helvetica"`,
	`arrowsize=0.8`,
}

// WriteDOT renders the aggregated mechanism map as a DOT digraph, one node
// style and one edge style:
//
//	label = "<kind> x<count>" — the transition category and the observed
//	        transition count (e.g. `act_ok x50`)
//	penwidth = min(1 + 2.2*log10(count+1), 7)
//	rankdir = LR, ranksep=1.5, nodesep=0.9, splines=spline, pad=0.5
//
// The kind prefixes the label because Aggregate emits ONE edge per
// (from,to,kind): without it, parallel mechanisms between the same pair
// would render as visually identical arrows that differ only by count and
// become indistinguishable without a legend. The count still drives
// penwidth. WriteDOT emits ONLY what Aggregate observed — no legend, no
// clusters, no port shells. Component names are unconditionally
// double-quoted and escaped so DOT metacharacters in model-chosen names
// cannot break the render. Rendering is deterministic: nodes and edges are
// pre-sorted by Aggregate and the attribute statements are emitted in
// fixed order. A nil receiver renders an empty digraph; a nil writer
// returns an error.
func (g *Graph) WriteDOT(w io.Writer) error {
	if w == nil {
		return errors.New("capture: WriteDOT requires a non-nil writer")
	}
	if g == nil {
		g = &Graph{}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "digraph %s {\n", graphName)
	b.WriteString("  rankdir=LR;\n")
	for _, a := range graphAttributes {
		fmt.Fprintf(&b, "  %s;\n", a)
	}
	b.WriteString("  node [")
	for i, a := range nodeDefaults {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(a)
	}
	b.WriteString("];\n")
	b.WriteString("  edge [")
	for i, a := range edgeDefaults {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(a)
	}
	b.WriteString("];\n")
	for _, n := range g.Nodes {
		fmt.Fprintf(&b, "  %s;\n", dotID(n.Name))
	}
	for _, e := range g.Edges {
		fmt.Fprintf(&b, "  %s -> %s [label=%s penwidth=%s];\n",
			dotID(e.From), dotID(e.To), strconv.Quote(edgeLabel(e.Kind, e.Count)), penwidth(e.Count))
	}
	b.WriteString("}\n")
	_, err := io.WriteString(w, b.String())
	return err
}

// maxPenwidth caps the capture edge thickness scale so very large observed
// counts (thousands of traversals) stay clearly thick but never cartoonish.
const maxPenwidth = 7.0

// penwidth implements the capture edge thickness scale
// min(1 + 2.2*log10(count+1), maxPenwidth), rounded to one decimal place
// with a trailing .0 trimmed. A count of 1 yields a visible hairline above
// the DOT minimum; each order of magnitude adds ~2.2 units of width until
// the scale reaches maxPenwidth (a count of ~2049 and above is capped at
// 7). Counts below 1 keep the floor width 1.
func penwidth(count int) string {
	if count < 1 {
		return "1"
	}
	pw := 1 + 2.2*math.Log10(float64(count)+1)
	if pw > maxPenwidth {
		pw = maxPenwidth
	}
	s := strconv.FormatFloat(pw, 'f', 1, 64)
	return strings.TrimSuffix(s, ".0")
}

// dotID returns a DOT-safe quoted identifier for a model-chosen component
// name: the name is unconditionally double-quoted and any embedded
// double-quote or backslash is backslash-escaped, so arbitrary component
// names render without breaking the DOT grammar.
func dotID(name string) string {
	esc := strings.ReplaceAll(name, `\`, `\\`)
	esc = strings.ReplaceAll(esc, `"`, `\"`)
	return `"` + esc + `"`
}

// itoaCap converts a count to its decimal string.
func itoaCap(count int) string {
	return strconv.Itoa(count)
}

// edgeLabel renders the DOT edge label for one (from,to,kind) transition
// family. The label carries BOTH the model-chosen kind and the observed
// count so parallel mechanisms between the same pair remain visually
// distinguishable without a legend, while the count keeps driving
// penwidth. Deterministic: kind is fixed by the model and count is a pure
// aggregate.
func edgeLabel(kind string, count int) string {
	return kind + " x" + itoaCap(count)
}
