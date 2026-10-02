package capture

import (
	"bufio"
	"encoding/json"
	"os"
	"sort"
)

// Recorder is a RAM-buffered, deterministic mechanism transition recorder.
// Models call Record at their real chokepoints with the transition's
// component names; Recorder assigns each event a monotonic Seq in call
// order. Because GoDES simulations are single-goroutine and pump-driven,
// call order IS sim order — the record is deterministic under a fixed
// seed with no wall-clock and no goroutine interleaving.
//
// A Recorder performs zero I/O during the simulation; the caller flushes
// the RAM buffer with FlushTo after Run completes.
type Recorder struct {
	events []Event
	seq    uint64
}

// NewRecorder creates an empty recorder.
func NewRecorder() *Recorder {
	return &Recorder{events: make([]Event, 0, 4096)}
}

// Record appends one observed transition from component "from" to
// component "to" of category "kind" at sim time tsMs (milliseconds).
// Options attach task identity and quantities. Record is cheap (an append)
// and performs no I/O and no RNG draws; when capture is disabled a model
// must nil-check the recorder instead of calling Record (byte-neutrality).
func (r *Recorder) Record(from, to, kind string, tsMs int64, opts ...Option) {
	e := Event{
		Seq:  r.seq,
		TsMs: tsMs,
		From: from,
		To:   to,
		Kind: kind,
	}
	for _, opt := range opts {
		opt(&e)
	}
	r.seq++
	r.events = append(r.events, e)
}

// Events returns a defensive copy of every buffered event in Record order
// (which is seq order for a well-behaved single-goroutine caller).
func (r *Recorder) Events() []Event {
	return append([]Event(nil), r.events...)
}

// FlushTo writes every buffered event as JSONL to path, one event per
// line sorted by Seq ascending, creating (or truncating) the file. The
// sort guarantees seq-monotonic output even if a caller reordered the
// buffered slice. An empty buffer produces an empty file. FlushTo is the
// caller's job AFTER the simulation — capture never performs I/O inside
// Run.
func (r *Recorder) FlushTo(path string) error {
	events := append([]Event(nil), r.events...)
	sort.Slice(events, func(i, j int) bool { return events[i].Seq < events[j].Seq })

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(f)
	for i := range events {
		line, err := json.Marshal(events[i])
		if err != nil {
			f.Close()
			return err
		}
		if _, err := bw.Write(append(line, '\n')); err != nil {
			f.Close()
			return err
		}
	}
	if err := bw.Flush(); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// Option customizes one recorded Event. Options are applied in order; the
// last write to a field wins.
type Option func(*Event)

// WithTask attaches workflow/activity task identity to a recorded event.
func WithTask(taskID string, attempt, class int) Option {
	return func(e *Event) {
		e.TaskID = taskID
		e.Attempt = attempt
		e.Class = class
	}
}

// WithClass attaches only the task class to a recorded event (when the
// identity is not yet known but the class is).
func WithClass(class int) Option {
	return func(e *Event) {
		e.Class = class
	}
}

// WithCount attaches a transition quantity (e.g. spooled batch size) to a
// recorded event.
func WithCount(count int) Option {
	return func(e *Event) {
		e.Count = count
	}
}
