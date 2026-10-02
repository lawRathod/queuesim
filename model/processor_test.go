package model

import (
	"testing"

	"github.com/law/GoDES/core"
	"github.com/law/GoDES/core/dists"
)

func newSim(t *testing.T) *core.Simulation {
	t.Helper()
	s, err := core.NewSimulation(42, core.FIFOPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestProcessorFixedService(t *testing.T) {
	sim := newSim(t)
	out := NewTaskQueue(sim, TaskQueueConfig{Name: "out"})
	p := NewProcessor(sim, ProcessorConfig{Name: "svc", Budget: 1, ServiceMs: 5})
	p.AddOutput(out, nil)

	p.Enqueue([]Task{{ID: 1, Attempt: 1}, {ID: 2, Attempt: 1}})
	p.Tick(sim.Now())

	// Before service completes, in-flight is budget-limited to 1.
	if p.InFlight() != 1 {
		t.Fatalf("InFlight=%d want 1", p.InFlight())
	}
	if p.InputLen() != 1 {
		t.Fatalf("InputLen=%d want 1 (second task buffered)", p.InputLen())
	}
	if p.Served() != 0 {
		t.Fatalf("Served=%d want 0 yet", p.Served())
	}

	sim.Advance(6) // service of task 1 completes at t=5

	if p.Served() != 1 {
		t.Fatalf("Served=%d want 1", p.Served())
	}
	delivered, _, _ := out.DrainToTick(sim.Now())
	if len(delivered) != 1 || delivered[0].ID != 1 {
		t.Fatalf("delivered=%v want [1]", delivered)
	}

	// Processor now schedules the second task.
	p.Tick(sim.Now())
	if p.InFlight() != 1 {
		t.Fatalf("InFlight=%d want 1 (task 2 in flight)", p.InFlight())
	}
}

func TestProcessorExponentialService(t *testing.T) {
	sim := newSim(t)
	out := NewTaskQueue(sim, TaskQueueConfig{Name: "out"})
	rate := 1.0
	src := dists.FuncSource(func() float64 { return 0.5 }) // -ln(0.5)=ln2 ~ 0.693
	p := NewProcessor(sim, ProcessorConfig{Name: "svc", Budget: 0, ServiceRate: rate, ServiceSrc: src})
	p.AddOutput(out, nil)
	p.Enqueue([]Task{{ID: 1, Attempt: 1}})
	p.Tick(sim.Now())
	// service ~ ln2 ticks => round -> 1 tick.
	sim.Advance(2)
	if p.Served() != 1 {
		t.Fatalf("Served=%d want 1", p.Served())
	}
	delivered, _, _ := out.DrainToTick(sim.Now())
	if len(delivered) != 1 {
		t.Fatal("no delivery")
	}
}

func TestProcessorDropViaNetwork(t *testing.T) {
	sim := newSim(t)
	out := NewTaskQueue(sim, TaskQueueConfig{Name: "out"})
	dropSrc := dists.FuncSource(func() float64 { return 0.1 }) // < DropRate 0.5 => always drop
	nw := NewNetwork(sim, NetworkConfig{Name: "link", DelayMs: 1, DropRate: 0.5, LinkSrc: dropSrc})
	p := NewProcessor(sim, ProcessorConfig{Name: "svc", Budget: 0, ServiceMs: 0})
	p.AddOutput(out, nw)
	p.Enqueue([]Task{{ID: 1, Attempt: 1}})
	p.Tick(sim.Now())
	sim.Advance(1)
	if p.Served() != 1 {
		t.Fatalf("Served=%d want 1 (service completed)", p.Served())
	}
	if p.Dropped() != 1 {
		t.Fatalf("Dropped=%d want 1 (network dropped)", p.Dropped())
	}
	if nw.Dropped() != 1 {
		t.Fatalf("network Dropped=%d want 1", nw.Dropped())
	}
	if out.Len() != 0 {
		t.Fatal("dropped task must not reach output")
	}
}

func TestProcessorBudgetCapsInFlight(t *testing.T) {
	sim := newSim(t)
	out := NewTaskQueue(sim, TaskQueueConfig{Name: "out"})
	p := NewProcessor(sim, ProcessorConfig{Name: "svc", Budget: 2, ServiceMs: 100})
	p.AddOutput(out, nil)
	// Enqueue 5 tasks; a single Tick should only start 2.
	p.Enqueue([]Task{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}, {ID: 5}})
	p.Tick(sim.Now())
	if p.InFlight() != 2 {
		t.Fatalf("InFlight=%d want 2 (budget)", p.InFlight())
	}
	if p.InputLen() != 3 {
		t.Fatalf("InputLen=%d want 3", p.InputLen())
	}
}

func TestProcessorDeterminism(t *testing.T) {
	run := func() []uint64 {
		sim := newSim(t)
		out := NewTaskQueue(sim, TaskQueueConfig{Name: "out"})
		p := NewProcessor(sim, ProcessorConfig{Name: "svc", Budget: 1, ServiceMs: 3})
		p.AddOutput(out, nil)
		p.Enqueue([]Task{{ID: 1}, {ID: 2}, {ID: 3}})
		var served []uint64
		for sim.Now().Int64() < 20 {
			p.Tick(sim.Now())
			sim.Advance(1)
		}
		// Collect service order via completions.
		served = append(served, p.Served())
		return served
	}
	a := run()
	b := run()
	if len(a) != len(b) {
		t.Fatalf("length mismatch")
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("determinism violation")
		}
	}
}

func TestProcessorPanics(t *testing.T) {
	sim := newSim(t)
	tests := []struct {
		name string
		cfg  ProcessorConfig
	}{
		{name: "empty name", cfg: ProcessorConfig{Name: ""}},
		{name: "negative budget", cfg: ProcessorConfig{Name: "x", Budget: -1}},
		{name: "negative service", cfg: ProcessorConfig{Name: "x", ServiceMs: -1}},
		{name: "rate without source", cfg: ProcessorConfig{Name: "x", ServiceRate: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("expected panic")
				}
			}()
			NewProcessor(sim, tt.cfg)
		})
	}
}
