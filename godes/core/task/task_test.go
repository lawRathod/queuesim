package task

import (
	"testing"

	"github.com/lawRathod/queuesim/godes/core"
	"github.com/lawRathod/queuesim/godes/simtest"
)

func TestTaskTimeout(t *testing.T) {
	sim, err := core.NewSimulation(1, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation error: %v", err)
	}
	fired := 0
	mgr := NewManager()
	mgr.Timeout(sim, 5, func(*core.Simulation) { fired++ })
	sim.Run()
	if fired != 1 {
		t.Fatalf("expected timeout to fire once, got %d", fired)
	}
}

func TestTaskRetry(t *testing.T) {
	sim, err := core.NewSimulation(1, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation error: %v", err)
	}
	attempts := 0
	mgr := NewManager()
	mgr.Retry(sim, 1, 3, 2, func(*core.Simulation) bool {
		attempts++
		return attempts >= 2
	})
	sim.Run()
	if attempts != 2 {
		t.Fatalf("expected retry attempts=2, got %d", attempts)
	}
}

func TestTaskPeriodic(t *testing.T) {
	sim, err := core.NewSimulation(1, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation error: %v", err)
	}
	count := 0
	mgr := NewManager()
	mgr.Periodic(sim, 2, 3, func(*core.Simulation) { count++ })
	sim.Run()
	if count != 3 {
		t.Fatalf("expected periodic to fire 3 times, got %d", count)
	}
}

func TestTaskCancellation(t *testing.T) {
	sim, err := core.NewSimulation(1, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation error: %v", err)
	}
	executed := false
	mgr := NewManager()
	handle := mgr.Timeout(sim, 10, func(*core.Simulation) { executed = true })
	handle.Cancel()
	sim.Run()
	if executed {
		t.Fatal("expected cancelled task to not execute")
	}
}

func TestTaskExecutionOrder(t *testing.T) {
	sim, err := core.NewSimulation(1, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation error: %v", err)
	}
	var order []int
	mgr := NewManager()
	mgr.Timeout(sim, 10, func(*core.Simulation) { order = append(order, 1) })
	mgr.Timeout(sim, 5, func(*core.Simulation) { order = append(order, 2) })
	mgr.Timeout(sim, 15, func(*core.Simulation) { order = append(order, 3) })
	sim.Run()
	if len(order) != 3 || order[0] != 2 || order[1] != 1 || order[2] != 3 {
		t.Fatalf("unexpected execution order: %v", order)
	}
}

func TestTaskDeterminism(t *testing.T) {
	simtest.AssertDeterministic(t, 5, 42, func(seed int64) string {
		sim, err := core.NewSimulation(seed, core.FIFOPolicy{})
		if err != nil {
			t.Fatalf("NewSimulation error: %v", err)
		}
		var events []string
		mgr := NewManager()
		mgr.Timeout(sim, 5, func(*core.Simulation) { events = append(events, "A") })
		mgr.Timeout(sim, 3, func(*core.Simulation) { events = append(events, "B") })
		mgr.Retry(sim, 1, 3, 2, func(*core.Simulation) bool {
			events = append(events, "retry")
			return true
		})
		sim.Run()
		return simtest.HashTrace(events)
	})
}

func TestRetryExhaustion(t *testing.T) {
	sim, err := core.NewSimulation(1, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation error: %v", err)
	}
	attempts := 0
	mgr := NewManager()
	mgr.Retry(sim, 1, 3, 2, func(*core.Simulation) bool {
		attempts++
		return false
	})
	sim.Run()
	if attempts != 3 {
		t.Fatalf("expected 3 attempts (max exhausted), got %d", attempts)
	}
}

func TestPeriodicInfiniteCancellation(t *testing.T) {
	sim, err := core.NewSimulation(1, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation error: %v", err)
	}
	count := 0
	mgr := NewManager()
	h := mgr.Periodic(sim, 2, 0, func(*core.Simulation) { count++ })
	sim.ScheduleIn(7, func(*core.Simulation) { h.Cancel() })
	sim.Run()
	if count < 2 || count > 5 {
		t.Fatalf("expected bounded fires after cancel, got %d", count)
	}
}

func TestTimeoutNilSimPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for nil sim")
		}
	}()
	mgr := NewManager()
	mgr.Timeout(nil, 5, func(*core.Simulation) {})
}

func TestRetryNilSimPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for nil sim")
		}
	}()
	mgr := NewManager()
	mgr.Retry(nil, 1, 3, 2, func(*core.Simulation) bool { return true })
}

func TestPeriodicNilSimPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for nil sim")
		}
	}()
	mgr := NewManager()
	mgr.Periodic(nil, 2, 3, func(*core.Simulation) {})
}

func TestTimeoutCancelDoesNotAdvanceSimTime(t *testing.T) {
	sim, _ := core.NewSimulation(1, core.FIFOPolicy{})
	mgr := NewManager()
	h := mgr.Timeout(sim, 10, func(*core.Simulation) { t.Fatal("should not fire") })
	if !sim.Cancel(h.taskHandle()) {
		t.Fatal("Cancel returned false on pending task")
	}
	sim.Run()
	if sim.Now() != 0 {
		t.Fatalf("expected sim.Now()=0 (no advance for cancelled timeout), got %d", sim.Now())
	}
}

// TestRetryCancelBeforeFirstAttempt verifies that cancelling a Retry chain
// before its first attempt fires actually prevents the chain from executing.
// This was broken by the placeholder-minting pattern which left a tombstone
// in cancelledTasks, causing Cancel to return false.
func TestRetryCancelBeforeFirstAttempt(t *testing.T) {
	sim, err := core.NewSimulation(1, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation error: %v", err)
	}
	attempts := 0
	mgr := NewManager()
	h := mgr.Retry(sim, 10, 3, 5, func(*core.Simulation) bool {
		attempts++
		return false
	})
	// Cancel before any attempt fires.
	if !h.Cancel() {
		t.Fatal("Retry.Cancel() returned false on pending chain")
	}
	sim.Run()
	if attempts != 0 {
		t.Fatalf("expected 0 attempts after cancel, got %d", attempts)
	}
}

// TestPeriodicCancelBeforeFirstTick verifies that cancelling a Periodic chain
// before its first tick fires actually prevents the chain from executing.
func TestPeriodicCancelBeforeFirstTick(t *testing.T) {
	sim, err := core.NewSimulation(1, core.FIFOPolicy{})
	if err != nil {
		t.Fatalf("NewSimulation error: %v", err)
	}
	count := 0
	mgr := NewManager()
	h := mgr.Periodic(sim, 10, 5, func(*core.Simulation) { count++ })
	// Cancel before any tick fires.
	if !h.Cancel() {
		t.Fatal("Periodic.Cancel() returned false on pending chain")
	}
	sim.Run()
	if count != 0 {
		t.Fatalf("expected 0 ticks after cancel, got %d", count)
	}
}
