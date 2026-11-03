package resource

import (
	"testing"

	"github.com/lawRathod/queuesim/godes/core"
)

func TestAcquireOrTimeoutImmediateGrant(t *testing.T) {
	sim := newSim(t, 1)
	r := NewResource(sim, 2)
	granted, timedOut := false, false
	r.AcquireOrTimeout(10, func(*core.Simulation) { granted = true }, func(*core.Simulation) { timedOut = true })
	if !granted || timedOut {
		t.Fatalf("free slot should grant immediately: granted=%v timeout=%v", granted, timedOut)
	}
	sim.Run()
	if timedOut {
		t.Fatal("timeout must not fire after an immediate grant")
	}
	if r.Used() != 1 {
		t.Fatalf("Used=%d want 1", r.Used())
	}
}

func TestAcquireOrTimeoutRenege(t *testing.T) {
	sim := newSim(t, 2)
	r := NewResource(sim, 1)
	r.TryAcquire() // hold the only slot
	granted, timedOut := false, false
	r.AcquireOrTimeout(5, func(*core.Simulation) { granted = true }, func(*core.Simulation) { timedOut = true })
	sim.Run()
	if granted || !timedOut {
		t.Fatalf("expired patience should renege: granted=%v timeout=%v", granted, timedOut)
	}
	if r.Used() != 1 {
		t.Fatalf("Used=%d want 1 (renege holds nothing)", r.Used())
	}
	if len(r.waiters) != 0 {
		t.Fatalf("reneged waiter should be removed from queue, len=%d", len(r.waiters))
	}
}

func TestAcquireOrTimeoutGrantBeforeTimeout(t *testing.T) {
	sim := newSim(t, 3)
	r := NewResource(sim, 1)
	r.TryAcquire()
	granted, timedOut := false, false
	r.AcquireOrTimeout(100, func(*core.Simulation) { granted = true }, func(*core.Simulation) { timedOut = true })
	// Release at t=10 — well before the 100-tick patience.
	sim.ScheduleIn(10, func(*core.Simulation) { r.Release() })
	sim.Run()
	if !granted || timedOut {
		t.Fatalf("release before patience should grant: granted=%v timeout=%v", granted, timedOut)
	}
	// The granted waiter holds the slot.
	if r.Used() != 1 {
		t.Fatalf("Used=%d want 1 (granted waiter holds)", r.Used())
	}
}

func TestAcquireOrTimeoutExactlyOneOutcome(t *testing.T) {
	sim := newSim(t, 4)
	r := NewResource(sim, 1)
	r.TryAcquire()
	calls := 0
	r.AcquireOrTimeout(20, func(*core.Simulation) { calls++ }, func(*core.Simulation) { calls++ })
	// Release AFTER the timeout (late grant must be ignored).
	sim.ScheduleIn(50, func(*core.Simulation) { r.Release() })
	sim.Run()
	if calls != 1 {
		t.Fatalf("exactly one continuation should run, got %d", calls)
	}
	// The late Release found an empty waiter queue and decremented level.
	if r.Used() != 0 {
		t.Fatalf("Used=%d want 0 after the lone release", r.Used())
	}
}

func TestAcquireOrTimeoutFIFOWithMixedWaiters(t *testing.T) {
	sim := newSim(t, 5)
	r := NewResource(sim, 1)
	r.TryAcquire()
	var order []string
	// Patient waiter (plain Acquire) first...
	r.Acquire(func(*core.Simulation) { order = append(order, "patient") })
	// ...then an impatient one that reneges at t=30.
	r.AcquireOrTimeout(30, func(*core.Simulation) { order = append(order, "impatient-granted") }, func(*core.Simulation) { order = append(order, "impatient-renege") })
	// Releases at t=10 and t=20: grant the patient waiter, then the slot
	// passes to the (still-queued) impatient waiter before its timeout.
	sim.ScheduleIn(10, func(*core.Simulation) { r.Release() })
	sim.ScheduleIn(10, func(*core.Simulation) { r.Release() })
	sim.Run()
	// Impatient is granted at t=20 (< 30 patience) — timeout cancelled.
	want := []string{"patient", "impatient-granted"}
	if len(order) != 2 {
		t.Fatalf("order=%v want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order=%v want %v", order, want)
		}
	}
}

func TestAcquireOrTimeoutZeroPatienceReneges(t *testing.T) {
	sim := newSim(t, 6)
	r := NewResource(sim, 1)
	r.TryAcquire()
	timedOut := false
	r.AcquireOrTimeout(0, func(*core.Simulation) { t.Fatal("grant must not happen with zero patience") }, func(*core.Simulation) { timedOut = true })
	sim.Run()
	if !timedOut {
		t.Fatal("zero patience should renege")
	}
}
