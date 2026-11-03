package simclock

import (
	"context"
	"testing"
	"time"
)

func TestDeadlineContext_Err_NilBeforeDeadline(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	deadline := time.UnixMilli(100)
	ctx, cancel := c.WithDeadline(context.Background(), deadline)
	defer cancel()

	if err := ctx.Err(); err != nil {
		t.Errorf("Err() before deadline = %v, want nil", err)
	}
}

func TestDeadlineContext_Err_DeadlineExceededAfter(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	deadline := time.UnixMilli(100)
	ctx, cancel := c.WithDeadline(context.Background(), deadline)
	defer cancel()

	// Advance past the deadline.
	c.Sleep(200 * time.Millisecond)

	if err := ctx.Err(); err != context.DeadlineExceeded {
		t.Errorf("Err() after deadline = %v, want DeadlineExceeded", err)
	}
}

func TestDeadlineContext_Err_AtDeadline_NotExceeded(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	deadline := time.UnixMilli(100)
	ctx, cancel := c.WithDeadline(context.Background(), deadline)
	defer cancel()

	// Advance exactly to the deadline.
	c.Sleep(100 * time.Millisecond)

	// At exactly the deadline, Err() should be nil (strict > check).
	if err := ctx.Err(); err != nil {
		t.Errorf("Err() at deadline = %v, want nil (strict > check)", err)
	}
}

func TestDeadlineContext_Deadline_ReturnsConfiguredTime(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	deadline := time.UnixMilli(100)
	ctx, cancel := c.WithDeadline(context.Background(), deadline)
	defer cancel()

	got, ok := ctx.Deadline()
	if !ok {
		t.Fatal("Deadline() returned ok=false, want true")
	}
	if !got.Equal(deadline) {
		t.Errorf("Deadline() = %v, want %v", got, deadline)
	}
}

func TestDeadlineContext_Done_ReturnsNil(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	deadline := time.UnixMilli(100)
	ctx, cancel := c.WithDeadline(context.Background(), deadline)
	defer cancel()

	if ch := ctx.Done(); ch != nil {
		t.Errorf("Done() = %v, want nil (sim-time-driven, no channel)", ch)
	}
}

func TestDeadlineContext_Value_DelegatesToParent(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	type key struct{}
	parent := context.WithValue(context.Background(), key{}, "hello")
	deadline := time.UnixMilli(100)
	ctx, cancel := c.WithDeadline(parent, deadline)
	defer cancel()

	got := ctx.Value(key{})
	if got != "hello" {
		t.Errorf("Value() = %v, want 'hello'", got)
	}
}

func TestDeadlineContext_ParentErr_Propagates(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	parent, parentCancel := context.WithCancel(context.Background())
	deadline := time.UnixMilli(100)
	ctx, cancel := c.WithDeadline(parent, deadline)
	defer cancel()

	// Cancel the parent.
	parentCancel()

	// The child should report the parent's error.
	if err := ctx.Err(); err != context.Canceled {
		t.Errorf("Err() after parent cancel = %v, want Canceled", err)
	}
}

func TestSimClock_WithTimeout_EquivalentToWithDeadline(t *testing.T) {
	sim := newTestSim(t)
	c := NewSimClock(sim)

	timeout := 100 * time.Millisecond
	ctx, cancel := c.WithTimeout(context.Background(), timeout)
	defer cancel()

	// The deadline should be Now() + timeout = 0 + 100ms = 100ms.
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("WithTimeout: Deadline() returned ok=false")
	}
	want := time.UnixMilli(100)
	if !deadline.Equal(want) {
		t.Errorf("WithTimeout deadline = %v, want %v", deadline, want)
	}
}
