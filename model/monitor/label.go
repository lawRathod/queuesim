package monitor

import (
	"sort"
	"strings"
)

// Label is one key/value pair attached to a metric's identity. Labels let
// the same statistic be sliced per shard / per queue / per kind without a
// second store or a parallel API (ADR-114).
type Label struct {
	Key   string
	Value string
}

// MetricKey is the identity of a metric: its name plus a canonical label
// set. Labels are sorted by (Key, Value), so two MetricKeys built from the
// same set in any order are identical. A label set with two labels sharing a
// key is a programming error and panics, consistent with the package's
// empty/duplicate-name panics.
type MetricKey struct {
	Name   string
	Labels []Label
}

// labelSep is the canonical key-component separator: the NUL byte. A
// metric name or a label key/value may not contain it (canonicalLabels /
// encodeKey panic), which is what makes the encoding injective.
const labelSep byte = 0

// canonicalLabels returns the labels sorted by (Key, Value), copying the
// caller's slice so the store never aliases caller memory. A duplicate key
// panics. An empty or nil set canonicalizes to nil.
func canonicalLabels(labels []Label) []Label {
	if len(labels) == 0 {
		return nil
	}
	out := make([]Label, len(labels))
	copy(out, labels)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key != out[j].Key {
			return out[i].Key < out[j].Key
		}
		return out[i].Value < out[j].Value
	})
	for i := 1; i < len(out); i++ {
		if out[i].Key == out[i-1].Key {
			panic("monitor: duplicate label key " + out[i].Key)
		}
	}
	return out
}

// labelSet renders an (already canonical) label set in the deterministic,
// human-readable suffix form used by the digest: "" when the set is empty
// (so the bare name renders byte-identically to today), else {k=v,k2=v2}.
func labelSet(labels []Label) string {
	if len(labels) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, l := range labels {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(l.Key)
		b.WriteByte('=')
		b.WriteString(l.Value)
	}
	b.WriteByte('}')
	return b.String()
}

// encodeKey builds the canonical, comparable map key from an already
// canonical label set: name + (NUL key NUL value)*. The NUL separator makes
// the encoding injective (see labelSep), and because NUL sorts below every
// printable byte the encoding is also monotonic in the desired iteration
// order — name first, then labels in canonical order — so sorting the map
// keys yields (name, labels) order directly.
func encodeKey(name string, labels []Label) string {
	guardSep(name)
	if len(labels) == 0 {
		return name
	}
	var b strings.Builder
	b.WriteString(name)
	for _, l := range labels {
		guardSep(l.Key)
		guardSep(l.Value)
		b.WriteByte(labelSep)
		b.WriteString(l.Key)
		b.WriteByte(labelSep)
		b.WriteString(l.Value)
	}
	return b.String()
}

// guardSep panics on a component containing the NUL separator, which would
// make the canonical encoding ambiguous.
func guardSep(s string) {
	if strings.IndexByte(s, labelSep) >= 0 {
		panic("monitor: metric name and label key/value must not contain NUL")
	}
}

// Canonical returns the canonical, comparable string encoding of k. It is
// stable across label order and injective over (name, label set); a duplicate
// label key panics. Useful for keying external maps and for assertions.
func (k MetricKey) Canonical() string {
	return encodeKey(k.Name, canonicalLabels(k.Labels))
}
