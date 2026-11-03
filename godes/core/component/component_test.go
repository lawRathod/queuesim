package component

import (
	"testing"

	"github.com/lawRathod/queuesim/godes/core"
)

type pingEvent struct{ Count int }

type pingComponent struct{ Seen []int }

func (c *pingComponent) ProcessEvent(_ Key[pingEvent], ev pingEvent, _ *core.Simulation) {
	c.Seen = append(c.Seen, ev.Count)
}

type ackEvent struct{ ID string }

type ackComponent struct{ Received []string }

func (c *ackComponent) ProcessEvent(_ Key[ackEvent], ev ackEvent, _ *core.Simulation) {
	c.Received = append(c.Received, ev.ID)
}

func TestComponentDispatch(t *testing.T) {
	sim, err := core.NewSimulation(1, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation error: %v", err)
	}

	reg := NewRegistry(sim)
	key := Add(reg, &pingComponent{})

	sim.ScheduleAt(0, func(s *core.Simulation) {
		Dispatch(reg, key, pingEvent{Count: 1})
	})
	sim.Run()

	comp := Get(reg, key).(*pingComponent)
	if len(comp.Seen) != 1 || comp.Seen[0] != 1 {
		t.Fatalf("unexpected component state: %+v", comp.Seen)
	}
}

func TestGetNonExistentKey(t *testing.T) {
	sim, err := core.NewSimulation(1, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation error: %v", err)
	}

	reg := NewRegistry(sim)
	// Key with id=0 — no component was ever registered with this id.
	key := Key[pingEvent]{}

	if got := Get(reg, key); got != nil {
		t.Fatalf("expected nil for unregistered key, got %v", got)
	}
}

func TestDispatchNonExistentKey(t *testing.T) {
	sim, err := core.NewSimulation(1, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation error: %v", err)
	}

	reg := NewRegistry(sim)
	key := Key[pingEvent]{}

	// Dispatch to an unregistered key must not panic.
	Dispatch(reg, key, pingEvent{Count: 42})
}

func TestMultipleComponents(t *testing.T) {
	sim, err := core.NewSimulation(1, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation error: %v", err)
	}

	reg := NewRegistry(sim)
	pingKey := Add(reg, &pingComponent{})
	ackKey := Add(reg, &ackComponent{})

	sim.ScheduleAt(0, func(s *core.Simulation) {
		Dispatch(reg, pingKey, pingEvent{Count: 7})
		Dispatch(reg, ackKey, ackEvent{ID: "hello"})
	})
	sim.Run()

	pingComp := Get(reg, pingKey).(*pingComponent)
	if len(pingComp.Seen) != 1 || pingComp.Seen[0] != 7 {
		t.Fatalf("unexpected ping state: %+v", pingComp.Seen)
	}
	ackComp := Get(reg, ackKey).(*ackComponent)
	if len(ackComp.Received) != 1 || ackComp.Received[0] != "hello" {
		t.Fatalf("unexpected ack state: %+v", ackComp.Received)
	}
}

func TestSequentialDispatches(t *testing.T) {
	sim, err := core.NewSimulation(1, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation error: %v", err)
	}

	reg := NewRegistry(sim)
	key := Add(reg, &pingComponent{})

	sim.ScheduleAt(0, func(s *core.Simulation) {
		Dispatch(reg, key, pingEvent{Count: 10})
		Dispatch(reg, key, pingEvent{Count: 20})
		Dispatch(reg, key, pingEvent{Count: 30})
	})
	sim.Run()

	comp := Get(reg, key).(*pingComponent)
	if len(comp.Seen) != 3 {
		t.Fatalf("expected 3 events, got %+v", comp.Seen)
	}
	if comp.Seen[0] != 10 || comp.Seen[1] != 20 || comp.Seen[2] != 30 {
		t.Fatalf("unexpected event order: %+v", comp.Seen)
	}
}

// orchestratorEvent triggers component A which dispatches to component B.
type orchestratorEvent struct{}

type orchestratorComponent struct {
	pingKey Key[pingEvent]
	reg     *Registry
}

func (c *orchestratorComponent) ProcessEvent(_ Key[orchestratorEvent], _ orchestratorEvent, _ *core.Simulation) {
	Dispatch(c.reg, c.pingKey, pingEvent{Count: 99})
}

func TestGetTypeMismatchReturnsNil(t *testing.T) {
	sim, err := core.NewSimulation(1, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation error: %v", err)
	}
	reg := NewRegistry(sim)
	pingKey := Add(reg, &pingComponent{})
	// Construct a Key[ackEvent] with the same internal id to simulate
	// a type confusion scenario. Should return nil gracefully (not panic).
	got := Get[ackEvent](reg, Key[ackEvent]{id: pingKey.id})
	if got != nil {
		t.Fatal("expected nil for type-mismatched Get")
	}
}

func TestDispatchNilSimPanics(t *testing.T) {
	reg := &Registry{comps: make(map[uint64]any)}
	key := Key[pingEvent]{}

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic from Dispatch on Registry with nil sim")
		}
	}()
	Dispatch(reg, key, pingEvent{Count: 1})
}

func TestNestedDispatch(t *testing.T) {
	sim, err := core.NewSimulation(1, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation error: %v", err)
	}

	reg := NewRegistry(sim)
	pingKey := Add(reg, &pingComponent{})
	orchKey := Add(reg, &orchestratorComponent{
		pingKey: pingKey,
		reg:     reg,
	})

	sim.ScheduleAt(0, func(s *core.Simulation) {
		Dispatch(reg, orchKey, orchestratorEvent{})
	})
	sim.Run()

	pingComp := Get(reg, pingKey).(*pingComponent)
	if len(pingComp.Seen) != 1 || pingComp.Seen[0] != 99 {
		t.Fatalf("expected nested dispatch to reach ping component: %+v", pingComp.Seen)
	}
}
