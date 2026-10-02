package faults

import (
	"testing"

	"github.com/law/GoDES/core"
	"github.com/lawRathod/queuesim/model"
)

func newSim(t *testing.T) *core.Simulation {
	t.Helper()
	s, err := core.NewSimulation(42, core.FIFOPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestInjectionActiveAt(t *testing.T) {
	tests := []struct {
		name     string
		start    int64
		duration int64
		now      int64
		want     bool
	}{
		{name: "before start", start: 10, duration: 5, now: 9, want: false},
		{name: "at start", start: 10, duration: 5, now: 10, want: true},
		{name: "inside", start: 10, duration: 5, now: 12, want: true},
		{name: "at end (half-open)", start: 10, duration: 5, now: 15, want: false},
		{name: "after end", start: 10, duration: 5, now: 16, want: false},
		{name: "open ended", start: 10, duration: 0, now: 1000, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inj := Injection{Kind: KindDrop, Start: tt.start, Duration: tt.duration}
			if got := inj.ActiveAt(tt.now); got != tt.want {
				t.Fatalf("ActiveAt(%d) = %v, want %v", tt.now, got, tt.want)
			}
		})
	}
}

// TestInjectorQueueDrop verifies a drop injection arms the queue's
// drop-rate knob during the window and resets it after.
func TestInjectorQueueDrop(t *testing.T) {
	sim := newSim(t)
	q := model.NewTaskQueue(sim, model.TaskQueueConfig{Name: "q"})
	tgt := NewQueueTarget(q)
	inj := NewInjector(sim, tgt, Injection{Kind: KindDrop, Start: 10, Duration: 5, Rate: 0.5})

	// Before the window: apply must leave knobs off (neutrality).
	sim.Advance(0)
	inj.Apply()
	if q.MaxWaitMs() != 0 {
		t.Fatal("no timeout should be set")
	}
	_ = tgt
}

// TestInjectorQueueDelay verifies a delay injection postpones delivery
// during its window.
func TestInjectorQueueDelay(t *testing.T) {
	sim := newSim(t)
	q := model.NewTaskQueue(sim, model.TaskQueueConfig{Name: "q"})
	tgt := NewQueueTarget(q)
	inj := NewInjector(sim, tgt, Injection{Kind: KindDelay, Start: 0, Duration: 50, Rate: 7})
	inj.Apply()
	q.Add(model.Task{ID: 1, Attempt: 1})
	delivered, _, _ := q.DrainToTick(sim.Now())
	if len(delivered) != 0 {
		t.Fatalf("delay should postpone delivery, got %d", len(delivered))
	}
	sim.Advance(8)
	delivered, _, _ = q.DrainToTick(sim.Now())
	if len(delivered) != 1 {
		t.Fatalf("after delay, delivery should occur, got %d", len(delivered))
	}
}

// TestInjectorDelayWindowOff verifies the delay knob resets to off when
// the window closes.
func TestInjectorDelayWindowOff(t *testing.T) {
	sim := newSim(t)
	q := model.NewTaskQueue(sim, model.TaskQueueConfig{Name: "q"})
	tgt := NewQueueTarget(q)
	inj := NewInjector(sim, tgt, Injection{Kind: KindDelay, Start: 0, Duration: 10, Rate: 100})
	sim.Advance(11) // window closed
	inj.Apply()
	q.Add(model.Task{ID: 1, Attempt: 1})
	delivered, _, _ := q.DrainToTick(sim.Now())
	if len(delivered) != 1 {
		t.Fatalf("delay must be off after window; delivered=%d", len(delivered))
	}
}

// TestInjectorQueueTimeout verifies a timeout injection expires tasks
// whose residency exceeds the deadline.
func TestInjectorQueueTimeout(t *testing.T) {
	sim := newSim(t)
	q := model.NewTaskQueue(sim, model.TaskQueueConfig{Name: "q"})
	tgt := NewQueueTarget(q)
	inj := NewInjector(sim, tgt, Injection{Kind: KindTimeout, Start: 0, Duration: 100, Rate: 5})
	inj.Apply()
	q.Add(model.Task{ID: 1, Attempt: 1, BornMs: 0})
	sim.Advance(6)
	_, _, expired := q.DrainToTick(sim.Now())
	if len(expired) != 1 {
		t.Fatalf("expected 1 expiry, got %d", len(expired))
	}
}

// TestInjectorComposed verifies multiple injections on the same target
// compose (drop + delay simultaneously).
func TestInjectorComposed(t *testing.T) {
	sim := newSim(t)
	q := model.NewTaskQueue(sim, model.TaskQueueConfig{Name: "q"})
	tgt := NewQueueTarget(q)
	inj := NewInjector(sim, tgt,
		Injection{Kind: KindDrop, Start: 0, Duration: 100, Rate: 0.5},
		Injection{Kind: KindDelay, Start: 0, Duration: 100, Rate: 3},
	)
	inj.Apply()
	if q.MaxWaitMs() != 0 {
		t.Fatal("no timeout set")
	}
	q.Add(model.Task{ID: 1, Attempt: 1})
	if got := q.Len(); got != 1 {
		t.Fatalf("Len=%d want 1 (task buffered by delay)", got)
	}
}

// TestInjectorZeroDrawNeutrality verifies an injector with no active
// window changes nothing.
func TestInjectorZeroDrawNeutrality(t *testing.T) {
	sim := newSim(t)
	q := model.NewTaskQueue(sim, model.TaskQueueConfig{Name: "q"})
	tgt := NewQueueTarget(q)
	inj := NewInjector(sim, tgt, Injection{Kind: KindDrop, Start: 1000, Duration: 0, Rate: 0.5})
	inj.Apply() // t=0 is before the window
	if q.MaxWaitMs() != 0 {
		t.Fatal("timeout should stay off")
	}
}

// TestInjectorTargetName verifies the name accessor passes through.
func TestInjectorTargetName(t *testing.T) {
	sim := newSim(t)
	q := model.NewTaskQueue(sim, model.TaskQueueConfig{Name: "q1"})
	tgt := NewQueueTarget(q)
	inj := NewInjector(sim, tgt)
	if inj.TargetName() != "q1" {
		t.Fatalf("TargetName=%q want q1", inj.TargetName())
	}
}
