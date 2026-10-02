package model

import (
	"reflect"
	"testing"

	"github.com/law/GoDES/core"
)

// newTestQueue builds a TaskQueue over a fresh seeded simulation.
func newTestQueue(t *testing.T, cfg TaskQueueConfig) (*core.Simulation, *TaskQueue) {
	t.Helper()
	sim, err := core.NewSimulation(42, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation: %v", err)
	}
	return sim, NewTaskQueue(sim, cfg)
}

// TestAddDeliversFIFO: tasks added at the current tick are drained in FIFO
// order once the pump reaches their due time.
func TestAddDeliversFIFO(t *testing.T) {
	sim, q := newTestQueue(t, TaskQueueConfig{Name: "q"})
	if !q.Add(Task{ID: 1, Attempt: 1}) {
		t.Fatal("Add rejected on unbounded queue")
	}
	if !q.Add(Task{ID: 2, Attempt: 1}) {
		t.Fatal("Add rejected on unbounded queue")
	}
	delivered, dropped, _ := q.DrainToTick(sim.Now())
	if len(dropped) != 0 {
		t.Fatalf("dropped = %d, want 0", len(dropped))
	}
	if got := len(delivered); got != 2 {
		t.Fatalf("delivered = %d, want 2", got)
	}
	if delivered[0].ID != 1 || delivered[1].ID != 2 {
		t.Fatalf("delivery order = [%d %d], want [1 2]", delivered[0].ID, delivered[1].ID)
	}
	if q.Len() != 0 {
		t.Fatalf("Len = %d after full drain, want 0", q.Len())
	}
}

// TestAddAtDelayedDelivery: AddAt schedules delivery at an absolute time;
// early drains deliver nothing.
func TestAddAtDelayedDelivery(t *testing.T) {
	sim, q := newTestQueue(t, TaskQueueConfig{Name: "q"})
	sim.Advance(10)
	if !q.AddAt(Task{ID: 7, Attempt: 1}, sim.Now().Add(5)) {
		t.Fatal("AddAt rejected on unbounded queue")
	}
	// Drain at t=12: not yet due.
	delivered, _, _ := q.DrainToTick(core.NewSimTime(12))
	if len(delivered) != 0 {
		t.Fatalf("early drain delivered %d tasks, want 0", len(delivered))
	}
	if q.Len() != 1 {
		t.Fatalf("Len = %d before due, want 1", q.Len())
	}
	// Drain at t=15: due.
	delivered, _, _ = q.DrainToTick(core.NewSimTime(15))
	if len(delivered) != 1 || delivered[0].ID != 7 {
		t.Fatalf("due drain delivered %+v, want [7]", delivered)
	}
}

// TestCapacityRejects: a full queue rejects the newest add (DropNewest →
// RejectedAdds semantics) without mutating the queue.
func TestCapacityRejects(t *testing.T) {
	sim, q := newTestQueue(t, TaskQueueConfig{Name: "q", Capacity: 2})
	if !q.Add(Task{ID: 1}) || !q.Add(Task{ID: 2}) {
		t.Fatal("Adds within capacity rejected")
	}
	if q.Add(Task{ID: 3}) {
		t.Fatal("Add accepted at full capacity")
	}
	if q.Rejected() != 1 {
		t.Fatalf("Rejected = %d, want 1", q.Rejected())
	}
	if q.Len() != 2 {
		t.Fatalf("Len = %d, want 2", q.Len())
	}
	delivered, _, _ := q.DrainToTick(sim.Now())
	if len(delivered) != 2 || delivered[0].ID != 1 || delivered[1].ID != 2 {
		t.Fatalf("delivered = %+v, want [1 2]", delivered)
	}
	// Capacity frees after drain.
	if !q.Add(Task{ID: 4}) {
		t.Fatal("Add rejected after capacity freed")
	}
}

// TestDropRateAtDrain: drain-time fault drops a fraction of due tasks; the
// decision is evaluated in FIFO drain order.
func TestDropRateAtDrain(t *testing.T) {
	sim, q := newTestQueue(t, TaskQueueConfig{Name: "q"})
	q.SetDropRate(1.0) // deterministic: drop everything
	for i := uint64(1); i <= 3; i++ {
		q.Add(Task{ID: i})
	}
	delivered, dropped, _ := q.DrainToTick(sim.Now())
	if len(delivered) != 0 || len(dropped) != 3 {
		t.Fatalf("delivered=%d dropped=%d, want 0/3", len(delivered), len(dropped))
	}
	if q.Dropped() != 3 {
		t.Fatalf("Dropped = %d, want 3", q.Dropped())
	}
	if q.Len() != 0 {
		t.Fatalf("Len = %d, want 0", q.Len())
	}
}

// TestZeroDrawsWhenOff: with drop rate 0 the queue never touches its RNG
// substream — the fixed path must not consume randomness.
func TestZeroDrawsWhenOff(t *testing.T) {
	_, q := newTestQueue(t, TaskQueueConfig{Name: "q"})
	q.Add(Task{ID: 1})
	q.DrainToTick(core.NewSimTime(0))
	if q.rng != nil {
		t.Fatal("drop-rate substream created while drop rate is 0 (zero draws when off violated)")
	}
	q.SetDropRate(0)
	if q.rng != nil {
		t.Fatal("SetDropRate(0) created a substream")
	}
}

// TestDropRateDeterministic: same seed, same drop rate → identical
// delivered/dropped split across repeated runs.
func TestDropRateDeterministic(t *testing.T) {
	run := func() ([]uint64, int) {
		sim, err := core.NewSimulation(7, core.FIFOPolicy{})
		if err != nil {
			t.Fatalf("NewSimulation: %v", err)
		}
		q := NewTaskQueue(sim, TaskQueueConfig{Name: "q"})
		q.SetDropRate(0.5)
		for i := uint64(1); i <= 100; i++ {
			q.Add(Task{ID: i})
		}
		delivered, dropped, _ := q.DrainToTick(core.NewSimTime(0))
		ids := make([]uint64, len(delivered))
		for i, d := range delivered {
			ids[i] = d.ID
		}
		return ids, len(dropped)
	}
	firstIDs, firstDropped := run()
	if firstDropped == 0 || firstDropped == 100 {
		t.Fatalf("drop split degenerate: dropped=%d, want 0 < n < 100", firstDropped)
	}
	for i := 0; i < 10; i++ {
		ids, dropped := run()
		if dropped != firstDropped || !reflect.DeepEqual(ids, firstIDs) {
			t.Fatalf("run %d diverged: dropped %d vs %d or delivery ids differ", i, dropped, firstDropped)
		}
	}
}

// TestTransferDelay: SetDelay(ms) defers each due task once by ms; after
// clearing, undelayed tasks flow normally.
func TestTransferDelay(t *testing.T) {
	sim, q := newTestQueue(t, TaskQueueConfig{Name: "xfer"})
	q.Add(Task{ID: 1})
	q.Add(Task{ID: 2})
	q.SetDelay(100)
	q.SetDelayActive(true)
	delivered, _, _ := q.DrainToTick(sim.Now())
	if len(delivered) != 0 {
		t.Fatalf("delivered %d while delay active, want 0", len(delivered))
	}
	if q.Len() != 2 {
		t.Fatalf("Len = %d after deferral, want 2", q.Len())
	}
	// At t=100 the deferred tasks are due again.
	delivered, _, _ = q.DrainToTick(core.NewSimTime(100))
	if len(delivered) != 2 {
		t.Fatalf("delivered %d after deferral elapses, want 2", len(delivered))
	}
}

// TestDrainTickMinAtFastPath: draining to a time before the earliest due
// task delivers nothing and leaves the queue intact.
func TestDrainTickMinAtFastPath(t *testing.T) {
	sim, q := newTestQueue(t, TaskQueueConfig{Name: "q"})
	q.AddAt(Task{ID: 1}, sim.Now().Add(50))
	delivered, _, _ := q.DrainToTick(sim.Now().Add(49))
	if len(delivered) != 0 {
		t.Fatalf("pre-due drain delivered %d", len(delivered))
	}
	if q.Len() != 1 {
		t.Fatalf("Len = %d, want 1", q.Len())
	}
}

// ---- D2 (plan 2026-08-12): MaxWaitMs + 3-way drain ----

// TestMaxWaitMsExpires: with MaxWaitMs set, a task whose queue residency
// exceeds the deadline is EXPIRED at drain (removed, returned in the third
// slice — non-retryable schedule-to-start semantics), not delivered.
func TestMaxWaitMsExpires(t *testing.T) {
	sim, q := newTestQueue(t, TaskQueueConfig{Name: "q"})
	q.SetMaxWaitMs(100)
	sim.Advance(0)
	q.Add(Task{ID: 1}) // enqueued at t=0
	sim.Advance(150)
	delivered, dropped, expired := q.DrainToTick(sim.Now())
	if len(delivered) != 0 || len(dropped) != 0 {
		t.Fatalf("delivered=%d dropped=%d, want 0/0 (expired)", len(delivered), len(dropped))
	}
	if len(expired) != 1 || expired[0].ID != 1 {
		t.Fatalf("expired = %+v, want [1]", expired)
	}
	if q.Len() != 0 {
		t.Fatalf("Len = %d after expiry, want 0", q.Len())
	}
}

// TestMaxWaitMsUnderDeadline: a task drained before the deadline is
// delivered normally; the deadline counts from ENQUEUE time (AddAt), not
// due time.
func TestMaxWaitMsUnderDeadline(t *testing.T) {
	sim, q := newTestQueue(t, TaskQueueConfig{Name: "q"})
	q.SetMaxWaitMs(100)
	q.AddAt(Task{ID: 2}, sim.Now().Add(50))
	sim.Advance(80) // enqueued 80ms ago, due at 50
	delivered, _, expired := q.DrainToTick(sim.Now())
	if len(delivered) != 1 || delivered[0].ID != 2 {
		t.Fatalf("delivered = %+v, want [2] (residency 80 < 100)", delivered)
	}
	if len(expired) != 0 {
		t.Fatalf("expired = %+v, want none", expired)
	}
}

// TestMaxWaitMsOff: MaxWaitMs 0 (default) never expires — zero behavior
// change.
func TestMaxWaitMsOff(t *testing.T) {
	sim, q := newTestQueue(t, TaskQueueConfig{Name: "q"})
	q.Add(Task{ID: 3})
	sim.Advance(100000)
	delivered, _, expired := q.DrainToTick(sim.Now())
	if len(expired) != 0 {
		t.Fatalf("expired = %+v with MaxWaitMs 0, want none", expired)
	}
	if len(delivered) != 1 {
		t.Fatalf("delivered = %d, want 1", len(delivered))
	}
}

// TestMaxWaitMsDeterministic: expiry is a pure function of enqueue time —
// same seed, same schedule → identical 3-way split across repeated runs.
func TestMaxWaitMsDeterministic(t *testing.T) {
	run := func() int {
		sim, err := core.NewSimulation(11, core.FIFOPolicy{})
		if err != nil {
			t.Fatalf("NewSimulation: %v", err)
		}
		q := NewTaskQueue(sim, TaskQueueConfig{Name: "q"})
		q.SetMaxWaitMs(50)
		for i := uint64(1); i <= 5; i++ {
			q.AddAt(Task{ID: i}, sim.Now().Add(10*int64(i)))
			sim.Advance(20)
		}
		_, _, expired := q.DrainToTick(core.NewSimTime(1000))
		return len(expired)
	}
	first := run()
	for i := 0; i < 5; i++ {
		if got := run(); got != first {
			t.Fatalf("run %d expired=%d vs %d", i, got, first)
		}
	}
}

// TestSyncMatchParked: a parked worker receives a task directly via Add
// (rendezvous, no buffering) — the general-builder sync-match.
func TestSyncMatchParked(t *testing.T) {
	sim, q := newTestQueue(t, TaskQueueConfig{Name: "q"})
	var got []uint64
	sim.ScheduleAt(sim.Now(), func(*core.Simulation) {
		q.Park(sim.Now().Add(60000).Int64(), func(tt Task) { got = append(got, tt.ID) })
	})
	sim.ScheduleAt(sim.Now(), func(*core.Simulation) {
		sm, acc := q.AddSync(Task{ID: 1, Attempt: 1})
		if !sm || !acc {
			t.Fatalf("AddSync = %v,%v want true,true (sync-match)", sm, acc)
		}
	})
	sim.Run()
	if len(got) != 1 || got[0] != 1 {
		t.Fatalf("taken = %v, want [1]", got)
	}
	if q.Len() != 0 {
		t.Fatalf("backlog len = %d, want 0 (sync-match must not buffer)", q.Len())
	}
	if q.Parked() != 0 {
		t.Fatalf("parked = %d, want 0 (worker consumed)", q.Parked())
	}
}

// TestAddBuffersWhenNoParked: no parked worker → Add buffers (no sync).
func TestAddBuffersWhenNoParked(t *testing.T) {
	_, q := newTestQueue(t, TaskQueueConfig{Name: "q"})
	sm, acc := q.AddSync(Task{ID: 1, Attempt: 1})
	if sm || !acc {
		t.Fatalf("AddSync = %v,%v want false,true (buffer)", sm, acc)
	}
	if q.Len() != 1 {
		t.Fatalf("backlog len = %d, want 1", q.Len())
	}
}
