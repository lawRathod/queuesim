package capture

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRecordSeqOrder asserts Seq increments monotonically in Record call
// order — the single-goroutine sim order IS the sequence order.
func TestRecordSeqOrder(t *testing.T) {
	r := NewRecorder()
	for i := 0; i < 3; i++ {
		r.Record("a", "b", "kind", int64(i))
	}
	evs := r.Events()
	if len(evs) != 3 {
		t.Fatalf("events=%d want 3", len(evs))
	}
	for i, e := range evs {
		if e.Seq != uint64(i) {
			t.Errorf("event %d seq=%d want %d", i, e.Seq, i)
		}
		if e.TsMs != int64(i) {
			t.Errorf("event %d ts=%d want %d", i, e.TsMs, i)
		}
	}
}

// TestRecordFields asserts Record stores from/to/kind and applies Options.
func TestRecordFields(t *testing.T) {
	r := NewRecorder()
	r.Record("load", "history", "workflow_start", 42, WithTask("wf-0", 1, 0))
	evs := r.Events()
	if len(evs) != 1 {
		t.Fatalf("events=%d want 1", len(evs))
	}
	e := evs[0]
	if e.From != "load" || e.To != "history" || e.Kind != "workflow_start" || e.TsMs != 42 {
		t.Errorf("record fields wrong: %+v", e)
	}
	if e.TaskID != "wf-0" || e.Attempt != 1 || e.Class != 0 {
		t.Errorf("task fields wrong: %+v", e)
	}
}

// TestEventsReturnsCopy asserts Events returns a defensive copy: mutating
// the returned slice or the underlying structs must not affect the buffer.
func TestEventsReturnsCopy(t *testing.T) {
	r := NewRecorder()
	r.Record("a", "b", "k", 1)
	evs := r.Events()
	evs[0].TsMs = 999
	evs = append(evs, Event{Seq: 99})
	if got := r.Events(); len(got) != 1 || got[0].TsMs != 1 {
		t.Errorf("buffer mutated through Events(): %+v", got)
	}
}

// TestFlushToSortedBySeq writes buffered events as JSONL sorted ascending
// by Seq. The buffer preserves Record order, but FlushTo must re-sort so a
// deterministic output holds even if a caller ever reordered the slice.
func TestFlushToSortedBySeq(t *testing.T) {
	r := NewRecorder()
	r.Record("load", "history", "workflow_start", 3)
	r.Record("history", "matching", "wf_add", 1)
	r.Record("matching", "exec", "dispatch", 2)
	// Force out-of-order buffer by hand (simulates an external reorder).
	buf := r.events
	buf[0], buf[2] = buf[2], buf[0]

	dir := t.TempDir()
	path := filepath.Join(dir, "capture.jsonl")
	if err := r.FlushTo(path); err != nil {
		t.Fatalf("FlushTo: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(lines) != 3 {
		t.Fatalf("lines=%d want 3", len(lines))
	}
	// Sorted by Seq: recorded seqs 0,1,2 carry ts 3,1,2 and the buffer was
	// scrambled — after the seq sort the lines must show ts 3,1,2 again.
	for i, wantTS := range []string{`"ts_ms":3`, `"ts_ms":1`, `"ts_ms":2`} {
		if !strings.Contains(lines[i], wantTS) {
			t.Errorf("line %d not sorted by seq: %s", i, lines[i])
		}
	}
}

// TestFlushToEmptyWritesEmptyFile asserts an empty buffer produces an empty
// (zero-byte) file — no header line.
func TestFlushToEmptyWritesEmptyFile(t *testing.T) {
	r := NewRecorder()
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.jsonl")
	if err := r.FlushTo(path); err != nil {
		t.Fatalf("FlushTo: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(data) != 0 {
		t.Errorf("empty flush wrote %d bytes, want 0", len(data))
	}
}

// TestFlushToPathError propagates a filesystem error (unwritable path).
func TestFlushToPathError(t *testing.T) {
	r := NewRecorder()
	r.Record("a", "b", "k", 1)
	if err := r.FlushTo(filepath.Join(t.TempDir(), "no", "such", "dir", "x.jsonl")); err == nil {
		t.Fatal("FlushTo to a missing directory should error")
	}
}

// TestFlushToRoundTrip asserts each flushed JSONL line unmarshals back to
// the exact recorded event.
func TestFlushToRoundTrip(t *testing.T) {
	r := NewRecorder()
	r.Record("load", "history", "workflow_start", 1, WithTask("wf-0", 1, 0))
	r.Record("history", "matching", "wf_add", 2, WithClass(0))
	r.Record("matching", "exec", "dispatch", 3, WithTask("act-0", 1, 1), WithCount(2))
	r.Record("exec", "history", "act_retry", 4, WithTask("act-0", 2, 1))

	dir := t.TempDir()
	path := filepath.Join(dir, "c.jsonl")
	if err := r.FlushTo(path); err != nil {
		t.Fatalf("FlushTo: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer f.Close()

	want := r.Events()
	sc := bufio.NewScanner(f)
	i := 0
	for sc.Scan() {
		if i >= len(want) {
			t.Fatalf("extra line %d: %s", i, sc.Text())
		}
		var got Event
		if err := json.Unmarshal(sc.Bytes(), &got); err != nil {
			t.Fatalf("line %d unmarshal: %v", i, err)
		}
		if got != want[i] {
			t.Errorf("line %d: got %+v want %+v", i, got, want[i])
		}
		i++
	}
	if err := sc.Err(); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if i != len(want) {
		t.Errorf("lines=%d want %d", i, len(want))
	}
}
