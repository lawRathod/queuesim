package faultsched

import (
	"math"
	"reflect"
	"strings"
	"testing"
)

// TestKindValues pins the fault-schedule kind vocabulary to the model's
// VectorKind strings (ADR-100: plain strings shared with the model leg, no
// import needed) plus the three reserved kinds.
func TestKindValues(t *testing.T) {
	want := []struct {
		kind Kind
		str  string
	}{
		{KindAddDrop, "add_drop"},
		{KindWfDrop, "wf_drop"},
		{KindPollDrop, "poll_drop"},
		{KindActivityFailure, "activity_failure"},
		{KindS2STimeout, "s2s_timeout"},
		{KindTransferDelay, "transfer_delay"},
		{KindPollerMin, "poller_min"},
		{KindTimerLoad, "timer_load"},
		{KindPersistenceStall, "persistence_stall"},
		{KindPollJitter, "poll_jitter"},
		{KindS2CShorten, "s2c_shorten"},
		{KindBacklogSurge, "backlog_surge"},
		{KindPersistenceFail, "persistence_fail"},
		{KindDuplicate, "duplicate"},
		{KindPartition, "partition"},
	}
	for _, w := range want {
		if string(w.kind) != w.str {
			t.Errorf("Kind %q has string %q, want %q", w.kind, w.kind, w.str)
		}
	}
}

// TestStatePureFunction asserts State is a pure function of (seed,
// elapsedMs): the same query twice returns an identical, non-aliasing
// result, and a different seed may answer differently but is itself
// deterministic (ADR-100: no hidden state, no mutable seed on Schedule).
func TestStatePureFunction(t *testing.T) {
	sched, err := NewSchedule(1, []Window{
		{Kind: KindAddDrop, StartMs: 1000, EndMs: 2000, Rate: 0.3},
		{Kind: KindActivityFailure, StartMs: 1000, EndMs: 0, Rate: 1},
	})
	if err != nil {
		t.Fatalf("NewSchedule: %v", err)
	}
	for _, seed := range []int64{1, 7, 42} {
		a := sched.State(seed, 1500)
		b := sched.State(seed, 1500)
		if !reflect.DeepEqual(a, b) {
			t.Fatalf("State(%d,1500) not pure: %v vs %v", seed, a, b)
		}
		if len(a) > 0 && &a[0] == &b[0] {
			t.Fatal("State results must not alias each other")
		}
	}
}

// TestStateSeedIsParameter asserts State's seed parameter selects behavior:
// two schedules constructed with different NewSchedule seeds behave
// identically (the Schedule stores no mutable seed), and State's own seed
// is honored per call.
func TestStateSeedIsParameter(t *testing.T) {
	windows := []Window{
		{Kind: KindPollJitter, StartMs: 0, EndMs: 1000, Rate: 0.5},
		{Kind: KindAddDrop, StartMs: 0, EndMs: 1000, Rate: 0.5},
	}
	s1, err := NewSchedule(11, windows)
	if err != nil {
		t.Fatalf("NewSchedule s1: %v", err)
	}
	s2, err := NewSchedule(22, windows)
	if err != nil {
		t.Fatalf("NewSchedule s2: %v", err)
	}
	if !reflect.DeepEqual(s1.State(9, 500), s2.State(9, 500)) {
		t.Fatal("NewSchedule seed must not affect State: State(seed=9) must be identical across schedules")
	}
	// State's per-call seed must not be stored: interleaved calls with
	// different seeds do not perturb each other.
	if !reflect.DeepEqual(s1.State(9, 500), s1.State(9, 500)) {
		t.Fatal("repeated same-seed State calls must agree")
	}
}

// TestBoundarySemantics asserts the half-open [StartMs, EndMs) contract:
// before start → inactive, at start → active, inside → active, at end
// (exclusive) → inactive, after end → inactive. EndMs 0 is open-ended from
// StartMs.
func TestBoundarySemantics(t *testing.T) {
	sched, err := NewSchedule(1, []Window{
		{Kind: KindAddDrop, StartMs: 100, EndMs: 200, Rate: 0.5},
	})
	if err != nil {
		t.Fatalf("NewSchedule: %v", err)
	}
	cases := []struct {
		elapsedMs int64
		want      bool
	}{
		{99, false},   // before start
		{100, true},   // at start (inclusive)
		{150, true},   // inside
		{199, true},   // last inside ms
		{200, false},  // at end (exclusive)
		{1000, false}, // after end
	}
	for _, c := range cases {
		got := sched.State(1, c.elapsedMs)
		if active := len(got) == 1; active != c.want {
			t.Errorf("State(1,%d) active=%v (%v), want %v", c.elapsedMs, active, got, c.want)
		}
	}

	open, err := NewSchedule(1, []Window{
		{Kind: KindPollDrop, StartMs: 100, EndMs: 0, Rate: 0.1},
	})
	if err != nil {
		t.Fatalf("NewSchedule open: %v", err)
	}
	for _, ms := range []int64{100, 1 << 40} {
		if got := open.State(1, ms); len(got) != 1 || got[0].Kind != KindPollDrop {
			t.Errorf("open-ended window inactive at %d: %v", ms, got)
		}
	}
	if got := open.State(1, 99); len(got) != 0 {
		t.Errorf("open-ended window active before start at 99: %v", got)
	}

	atZero, err := NewSchedule(1, []Window{
		{Kind: KindWfDrop, StartMs: 0, EndMs: 50, Rate: 0.2},
	})
	if err != nil {
		t.Fatalf("NewSchedule atZero: %v", err)
	}
	if got := atZero.State(1, 0); len(got) != 1 {
		t.Errorf("window with StartMs 0 inactive at elapsedMs 0: %v", got)
	}
	if got := atZero.State(1, 50); len(got) != 0 {
		t.Errorf("window with StartMs 0 active at EndMs 50: %v", got)
	}
}

// TestOverlapLastWins asserts overlapping windows of the SAME kind resolve
// to the LAST active by sorted (StartMs, Kind) order — deterministic, no
// map iteration. Different kinds coexist.
func TestOverlapLastWins(t *testing.T) {
	sched, err := NewSchedule(1, []Window{
		{Kind: KindAddDrop, StartMs: 0, EndMs: 200, Rate: 0.1},
		{Kind: KindAddDrop, StartMs: 100, EndMs: 300, Rate: 0.9},
		{Kind: KindWfDrop, StartMs: 50, EndMs: 250, Rate: 0.4},
	})
	if err != nil {
		t.Fatalf("NewSchedule: %v", err)
	}
	// 150: add_drop 0.1 + 0.9 both active → 0.9 wins (last by StartMs),
	// and the surviving add_drop keeps its sorted position AFTER wf_drop
	// (sorted windows: add_drop@0, wf_drop@50, add_drop@100 — the first
	// add_drop is shadowed, so survivors in order are wf_drop, add_drop).
	got := sched.State(1, 150)
	want := []ActiveFault{
		{Kind: KindWfDrop, Rate: 0.4},
		{Kind: KindAddDrop, Rate: 0.9},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("State(1,150) = %v, want %v", got, want)
	}
	// 250: add_drop second window active (0.9), first inactive; wf_drop
	// inactive at its exclusive end.
	got = sched.State(1, 250)
	want = []ActiveFault{{Kind: KindAddDrop, Rate: 0.9}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("State(1,250) = %v, want %v", got, want)
	}
	// Result order must follow sorted window order even when the caller
	// listed windows unsorted.
	unsorted, err := NewSchedule(1, []Window{
		{Kind: KindWfDrop, StartMs: 100, EndMs: 200, Rate: 0.4},
		{Kind: KindAddDrop, StartMs: 50, EndMs: 150, Rate: 0.1},
	})
	if err != nil {
		t.Fatalf("NewSchedule unsorted: %v", err)
	}
	got = unsorted.State(1, 120)
	want = []ActiveFault{
		{Kind: KindAddDrop, Rate: 0.1},
		{Kind: KindWfDrop, Rate: 0.4},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unsorted State(1,120) = %v, want sorted %v", got, want)
	}
}

// TestShadowingWinnerKeepsOwnPosition pins the same-kind overlap semantics
// precisely: a shadowing winner (the LAST active window of its kind by
// sorted order) is emitted at its OWN sorted position, NOT at its kind's
// first-active position. add_drop@0 is shadowed by add_drop@100, so the
// surviving add_drop is emitted AFTER wf_drop@50 (sorted positions: 0, 1,
// 2 → survivors in window order are wf_drop at position 1, then add_drop
// at position 2).
func TestShadowingWinnerKeepsOwnPosition(t *testing.T) {
	sched, err := NewSchedule(1, []Window{
		{Kind: KindAddDrop, StartMs: 0, EndMs: 200, Rate: 0.1},
		{Kind: KindWfDrop, StartMs: 50, EndMs: 250, Rate: 0.4},
		{Kind: KindAddDrop, StartMs: 100, EndMs: 300, Rate: 0.9},
	})
	if err != nil {
		t.Fatalf("NewSchedule: %v", err)
	}
	got := sched.State(1, 150)
	want := []ActiveFault{
		{Kind: KindWfDrop, Rate: 0.4},
		{Kind: KindAddDrop, Rate: 0.9},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("State(1,150) = %v, want shadowing winner at own position %v", got, want)
	}
}

// TestNewScheduleSortsByStartThenKind asserts State output order is window
// order after a deterministic (StartMs, Kind) sort.
func TestNewScheduleSortsByStartThenKind(t *testing.T) {
	sched, err := NewSchedule(1, []Window{
		{Kind: KindPollDrop, StartMs: 200, EndMs: 300, Rate: 0.5},
		{Kind: KindAddDrop, StartMs: 100, EndMs: 300, Rate: 0.3},
		{Kind: KindWfDrop, StartMs: 150, EndMs: 300, Rate: 0.4},
	})
	if err != nil {
		t.Fatalf("NewSchedule: %v", err)
	}
	got := sched.State(1, 250)
	want := []ActiveFault{
		{Kind: KindAddDrop, Rate: 0.3},
		{Kind: KindWfDrop, Rate: 0.4},
		{Kind: KindPollDrop, Rate: 0.5},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("State(1,250) = %v, want sorted %v", got, want)
	}
}

// TestNewScheduleTiebreakByKind asserts windows sharing a StartMs order by
// Kind (deterministic string tiebreak — no map iteration).
func TestNewScheduleTiebreakByKind(t *testing.T) {
	sched, err := NewSchedule(1, []Window{
		{Kind: KindWfDrop, StartMs: 100, EndMs: 300, Rate: 0.4},
		{Kind: KindAddDrop, StartMs: 100, EndMs: 300, Rate: 0.3},
	})
	if err != nil {
		t.Fatalf("NewSchedule: %v", err)
	}
	got := sched.State(1, 200)
	want := []ActiveFault{
		{Kind: KindAddDrop, Rate: 0.3},
		{Kind: KindWfDrop, Rate: 0.4},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("State(1,200) = %v, want %v", got, want)
	}
}

// TestNewScheduleValidation is the validation table: NaN/±Inf rates,
// inverted windows, negative starts, unknown kinds, and probability bounds
// for the probability kinds. Each case asserts an error mentioning the
// offending fact (error content is the debugging UX).
func TestNewScheduleValidation(t *testing.T) {
	cases := []struct {
		name    string
		windows []Window
		wantErr string // substring the error must mention
	}{
		{
			name:    "NaN rate",
			windows: []Window{{Kind: KindAddDrop, StartMs: 0, EndMs: 100, Rate: math.NaN()}},
			wantErr: "not finite",
		},
		{
			name:    "+Inf rate",
			windows: []Window{{Kind: KindAddDrop, StartMs: 0, EndMs: 100, Rate: math.Inf(1)}},
			wantErr: "not finite",
		},
		{
			name:    "-Inf rate",
			windows: []Window{{Kind: KindAddDrop, StartMs: 0, EndMs: 100, Rate: math.Inf(-1)}},
			wantErr: "not finite",
		},
		{
			name:    "inverted window",
			windows: []Window{{Kind: KindAddDrop, StartMs: 200, EndMs: 100, Rate: 0.5}},
			wantErr: "EndMs 100 < StartMs 200",
		},
		{
			name:    "negative start",
			windows: []Window{{Kind: KindAddDrop, StartMs: -1, EndMs: 100, Rate: 0.5}},
			wantErr: "StartMs -1 < 0",
		},
		{
			name:    "unknown kind",
			windows: []Window{{Kind: Kind("frobnicate"), StartMs: 0, EndMs: 100, Rate: 0.5}},
			wantErr: "unknown kind",
		},
		{
			name:    "probability below zero",
			windows: []Window{{Kind: KindAddDrop, StartMs: 0, EndMs: 100, Rate: -0.1}},
			wantErr: "outside [0,1]",
		},
		{
			name:    "probability above one",
			windows: []Window{{Kind: KindWfDrop, StartMs: 0, EndMs: 100, Rate: 1.1}},
			wantErr: "outside [0,1]",
		},
		{
			name:    "poll_jitter probability above one",
			windows: []Window{{Kind: KindPollJitter, StartMs: 0, EndMs: 100, Rate: 2.0}},
			wantErr: "outside [0,1]",
		},
		{
			name:    "negative scalar rate",
			windows: []Window{{Kind: KindTimerLoad, StartMs: 0, EndMs: 100, Rate: -5}},
			wantErr: "rate -5 < 0",
		},
		{
			name:    "finite window at MaxInt64 start",
			windows: []Window{{Kind: KindAddDrop, StartMs: math.MaxInt64, EndMs: math.MaxInt64, Rate: 0.5}},
			wantErr: "unreachable",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := NewSchedule(1, c.windows)
			if err == nil {
				t.Fatalf("NewSchedule(%s) = %v, want error", c.name, s)
			}
			if !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("NewSchedule(%s) error %q does not mention %q", c.name, err, c.wantErr)
			}
		})
	}
}

// TestNewScheduleProbabilityKindsBound checks all four probability kinds
// and their boundary acceptance.
func TestNewScheduleProbabilityKindsBound(t *testing.T) {
	probKinds := []Kind{KindAddDrop, KindWfDrop, KindPollDrop, KindPollJitter}
	for _, k := range probKinds {
		for _, rate := range []float64{0.0, 1.0} {
			if _, err := NewSchedule(1, []Window{{Kind: k, StartMs: 0, EndMs: 10, Rate: rate}}); err != nil {
				t.Errorf("%s rate %v rejected: %v", k, rate, err)
			}
		}
		for _, rate := range []float64{-0.01, 1.01} {
			if _, err := NewSchedule(1, []Window{{Kind: k, StartMs: 0, EndMs: 10, Rate: rate}}); err == nil {
				t.Errorf("%s rate %v accepted, want error", k, rate)
			}
		}
	}
}

// TestFromTraceOverflowRejected asserts the 1 ms singleton reconstruction
// [ElapsedMs, ElapsedMs+1) rejects an ElapsedMs whose +1 would overflow,
// rather than silently wrapping to an open-ended or inverted window.
func TestFromTraceOverflowRejected(t *testing.T) {
	tr := []TracePoint{{ElapsedMs: math.MaxInt64, Faults: []ActiveFault{{Kind: KindAddDrop, Rate: 0.5}}}}
	s, err := FromTrace(1, tr)
	if err == nil {
		t.Fatalf("FromTrace(MaxInt64) = %v, want overflow error", s)
	}
	if !strings.Contains(err.Error(), "overflow") {
		t.Fatalf("FromTrace(MaxInt64) error %q does not mention overflow", err)
	}
}

// TestEmptyWindowsNilSchedule asserts nil and empty windows produce the
// byte-neutral (nil, nil) no-schedule state, and a nil Schedule answers no
// faults for every query.
func TestEmptyWindowsNilSchedule(t *testing.T) {
	for _, w := range [][]Window{nil, {}} {
		s, err := NewSchedule(1, w)
		if err != nil {
			t.Fatalf("NewSchedule(empty) err=%v, want nil", err)
		}
		if s != nil {
			t.Fatalf("NewSchedule(empty) = non-nil %v, want nil", s)
		}
	}
	var nilSched *Schedule
	for _, ms := range []int64{0, 500, 1 << 40} {
		if got := nilSched.State(1, ms); got != nil {
			t.Errorf("nil Schedule State(%d) = %v, want nil", ms, got)
		}
	}
}

// TestNoActiveReturnsNil asserts State returns nil (not an empty non-nil
// slice) when no window is active at elapsedMs.
func TestNoActiveReturnsNil(t *testing.T) {
	sched, err := NewSchedule(1, []Window{
		{Kind: KindAddDrop, StartMs: 100, EndMs: 200, Rate: 0.5},
	})
	if err != nil {
		t.Fatalf("NewSchedule: %v", err)
	}
	for _, ms := range []int64{0, 50, 99, 200, 300} {
		if got := sched.State(1, ms); got != nil {
			t.Errorf("State(1,%d) = %v, want nil", ms, got)
		}
	}
}

// TestFromTraceRoundTrip asserts the replay seam: a recorded trace
// reconstructs a schedule whose State at each recorded elapsedMs
// reproduces the recorded faults exactly, via the same State fast path.
func TestFromTraceRoundTrip(t *testing.T) {
	trace := []TracePoint{
		{ElapsedMs: 100, Faults: []ActiveFault{{Kind: KindAddDrop, Rate: 0.3}}},
		{ElapsedMs: 200, Faults: []ActiveFault{{Kind: KindWfDrop, Rate: 0.5}}},
		{ElapsedMs: 300, Faults: []ActiveFault{{Kind: KindAddDrop, Rate: 0.3}, {Kind: KindWfDrop, Rate: 0.5}}},
		{ElapsedMs: 400, Faults: nil},
	}
	s, err := FromTrace(42, trace)
	if err != nil {
		t.Fatalf("FromTrace: %v", err)
	}
	if s == nil {
		t.Fatal("FromTrace returned nil schedule for non-empty trace")
	}
	for _, tp := range trace {
		got := s.State(42, tp.ElapsedMs)
		if !reflect.DeepEqual(got, tp.Faults) {
			t.Errorf("State(42,%d) = %v, want recorded %v", tp.ElapsedMs, got, tp.Faults)
		}
	}
	// 1 ms singleton windows: active only inside [ElapsedMs, ElapsedMs+1).
	if got := s.State(42, 101); got != nil {
		t.Errorf("State(42,101) = %v, want nil (singleton ended)", got)
	}
}

// TestFromTraceNilFaultInstant asserts a recorded no-fault instant
// round-trips: FromTrace records it as an (always-inactive) singleton
// window, and replay State at that instant returns nil.
func TestFromTraceNilFaultInstant(t *testing.T) {
	trace := []TracePoint{
		{ElapsedMs: 100, Faults: []ActiveFault{{Kind: KindAddDrop, Rate: 0.3}}},
		{ElapsedMs: 200, Faults: nil}, // recorded no-fault instant
	}
	s, err := FromTrace(42, trace)
	if err != nil {
		t.Fatalf("FromTrace: %v", err)
	}
	if got := s.State(42, 200); got != nil {
		t.Errorf("State(42,200) = %v, want nil (no faults recorded)", got)
	}
	if got := s.State(42, 100); len(got) != 1 || got[0].Kind != KindAddDrop {
		t.Errorf("State(42,100) = %v, want [add_drop]", got)
	}
}

// TestFromTraceEmpty asserts nil and empty traces produce (nil, nil) — the
// byte-neutral no-schedule state.
func TestFromTraceEmpty(t *testing.T) {
	for _, tr := range [][]TracePoint{nil, {}} {
		s, err := FromTrace(1, tr)
		if err != nil {
			t.Fatalf("FromTrace(empty) err=%v, want nil", err)
		}
		if s != nil {
			t.Fatalf("FromTrace(empty) = non-nil %v, want nil", s)
		}
	}
}

// TestFromTraceValidates asserts FromTrace rejects non-monotonic elapsedMs
// (including an equal repeat) and negative elapsedMs.
func TestFromTraceValidates(t *testing.T) {
	t.Run("non-monotonic", func(t *testing.T) {
		cases := [][]TracePoint{
			{
				{ElapsedMs: 100, Faults: nil},
				{ElapsedMs: 99, Faults: nil},
			},
			{
				{ElapsedMs: 100, Faults: nil},
				{ElapsedMs: 100, Faults: nil},
			},
		}
		for _, tr := range cases {
			if s, err := FromTrace(1, tr); err == nil {
				t.Fatalf("FromTrace(%v) = %v, want error", tr, s)
			} else if !strings.Contains(err.Error(), "monotonic") {
				t.Fatalf("FromTrace error %q does not mention monotonic", err)
			}
		}
	})
	t.Run("negative elapsed", func(t *testing.T) {
		tr := []TracePoint{{ElapsedMs: -1, Faults: nil}}
		if s, err := FromTrace(1, tr); err == nil {
			t.Fatalf("FromTrace(negative) = %v, want error", s)
		}
	})
}

// TestWindowsEnumeration pins the Windows() enumeration seam: a nil
// receiver returns nil, the result shares the sorted (StartMs, Kind) order,
// and callers may not mutate the schedule through it.
func TestWindowsEnumeration(t *testing.T) {
	var nilSched *Schedule
	if got := nilSched.Windows(); got != nil {
		t.Fatalf("nil Schedule.Windows() = %v, want nil", got)
	}
	sched, err := NewSchedule(1, []Window{
		{Kind: KindWfDrop, StartMs: 200, EndMs: 300, Rate: 0.4},
		{Kind: KindAddDrop, StartMs: 100, EndMs: 300, Rate: 0.3},
	})
	if err != nil {
		t.Fatalf("NewSchedule: %v", err)
	}
	got := sched.Windows()
	want := []Window{
		{Kind: KindAddDrop, StartMs: 100, EndMs: 300, Rate: 0.3},
		{Kind: KindWfDrop, StartMs: 200, EndMs: 300, Rate: 0.4},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Windows() = %v, want sorted %v", got, want)
	}
}

// TestReservedKinds asserts NewSchedule accepts windows naming the three
// reserved kinds (persistence_fail, duplicate, partition) and State returns
// them. They are RESERVED — no leg behavior exists yet beyond appearing in
// State output (ADR-100); validation must accept them so the kind set is
// closed and round-trips.
func TestReservedKinds(t *testing.T) {
	reserved := []Kind{KindPersistenceFail, KindDuplicate, KindPartition}
	for _, k := range reserved {
		s, err := NewSchedule(1, []Window{{Kind: k, StartMs: 0, EndMs: 100, Rate: 1}})
		if err != nil {
			t.Errorf("NewSchedule with reserved %s rejected: %v", k, err)
			continue
		}
		got := s.State(1, 50)
		if len(got) != 1 || got[0].Kind != k {
			t.Errorf("State with reserved %s = %v, want [{%s 1}]", k, got, k)
		}
	}
}

// TestDeterminismManyCalls asserts State hides no state: 1000 repeated
// calls over an active window return identical results every time.
func TestDeterminismManyCalls(t *testing.T) {
	sched, err := NewSchedule(99, []Window{
		{Kind: KindAddDrop, StartMs: 0, EndMs: 0, Rate: 0.3},
		{Kind: KindWfDrop, StartMs: 5000, EndMs: 6000, Rate: 0.7},
		{Kind: KindPersistenceStall, StartMs: 1000, EndMs: 0, Rate: 3},
		{Kind: KindS2STimeout, StartMs: 0, EndMs: 9000, Rate: 500},
	})
	if err != nil {
		t.Fatalf("NewSchedule: %v", err)
	}
	want := sched.State(42, 5500)
	if len(want) == 0 {
		t.Fatal("expected active faults at 5500")
	}
	for i := 0; i < 1000; i++ {
		got := sched.State(42, 5500)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("call %d diverged: %v vs %v", i, got, want)
		}
	}
}

// TestScheduleSatisfiesSource asserts *Schedule satisfies the Source query
// seam and a nil *Schedule (typed nil Source) is the no-schedule state at
// every call site: a nil Source must be treated as "no schedule" by
// callers, who nil-check the interface before querying (the State method
// itself nil-checks its receiver).
func TestScheduleSatisfiesSource(t *testing.T) {
	var _ Source = (*Schedule)(nil)
	var typedNil *Schedule
	if got := typedNil.State(1, 100); got != nil {
		t.Fatalf("nil *Schedule State = %v, want nil", got)
	}
	s, err := NewSchedule(1, []Window{{Kind: KindAddDrop, StartMs: 0, EndMs: 100, Rate: 0.5}})
	if err != nil {
		t.Fatalf("NewSchedule: %v", err)
	}
	var src Source = s
	if got := src.State(1, 50); len(got) != 1 {
		t.Fatalf("Source State(1,50) = %v, want 1 fault", got)
	}
}

// TestStateDoesNotAliasSchedule asserts mutating the returned slice never
// corrupts the schedule (the schedule is immutable after NewSchedule).
func TestStateDoesNotAliasSchedule(t *testing.T) {
	sched, err := NewSchedule(1, []Window{
		{Kind: KindAddDrop, StartMs: 0, EndMs: 0, Rate: 0.3},
	})
	if err != nil {
		t.Fatalf("NewSchedule: %v", err)
	}
	got := sched.State(1, 10)
	if len(got) != 1 {
		t.Fatalf("State = %v, want 1 fault", got)
	}
	got[0].Rate = 0.99 // mutate the caller's slice
	again := sched.State(1, 10)
	if again[0].Rate != 0.3 {
		t.Fatalf("mutating State result corrupted schedule: rate = %v, want 0.3", again[0].Rate)
	}
}
