package simclock

import (
	"context"
	"testing"
	"time"
)

func TestWallClock_Now_ReturnsReasonableTime(t *testing.T) {
	now := WallClock{}.Now()
	if now.IsZero() {
		t.Fatal("WallClock.Now() returned zero time")
	}
	after2020 := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	if now.Before(after2020) {
		t.Errorf("WallClock.Now() = %v, want after %v", now, after2020)
	}
}

func TestWallClock_Since_ReturnsPositiveDuration(t *testing.T) {
	past := time.Now().Add(-100 * time.Millisecond)
	d := WallClock{}.Since(past)
	if d < 90*time.Millisecond {
		t.Errorf("WallClock.Since(100ms ago) = %v, want >= 90ms", d)
	}
}

func TestWallClock_NewTimer_Fires(t *testing.T) {
	timer := WallClock{}.NewTimer(50 * time.Millisecond)
	select {
	case <-timer.C:
		// Success: timer fired.
	case <-time.After(200 * time.Millisecond):
		t.Fatal("WallClock.NewTimer(50ms) did not fire within 200ms")
	}
}

func TestWallClock_NewTicker_Fires(t *testing.T) {
	ticker := WallClock{}.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	select {
	case <-ticker.C:
		// Success: ticker fired at least once.
	case <-time.After(200 * time.Millisecond):
		t.Fatal("WallClock.NewTicker(20ms) did not fire within 200ms")
	}
}

func TestWallClock_Sleep_DoesNotHang(t *testing.T) {
	done := make(chan struct{})
	go func() {
		WallClock{}.Sleep(50 * time.Millisecond)
		close(done)
	}()
	select {
	case <-done:
		// Success: sleep returned.
	case <-time.After(500 * time.Millisecond):
		t.Fatal("WallClock.Sleep(50ms) did not return within 500ms")
	}
}

func TestWallClock_WithDeadline_CancelsContext(t *testing.T) {
	deadline := time.Now().Add(50 * time.Millisecond)
	ctx, cancel := WallClock{}.WithDeadline(context.Background(), deadline)
	defer cancel()
	select {
	case <-ctx.Done():
		// Success: context was cancelled.
	case <-time.After(500 * time.Millisecond):
		t.Fatal("WallClock.WithDeadline(50ms) did not cancel within 500ms")
	}
}

func TestWallClock_WithTimeout_CancelsContext(t *testing.T) {
	ctx, cancel := WallClock{}.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	select {
	case <-ctx.Done():
		// Success: context was cancelled.
	case <-time.After(500 * time.Millisecond):
		t.Fatal("WallClock.WithTimeout(50ms) did not cancel within 500ms")
	}
}

func TestWallClock_WithDeadline_AlreadyPassed(t *testing.T) {
	deadline := time.Now().Add(-1 * time.Second)
	ctx, cancel := WallClock{}.WithDeadline(context.Background(), deadline)
	defer cancel()
	// An already-expired deadline should be cancelled immediately.
	select {
	case <-ctx.Done():
		// Success: context is already done.
	default:
		t.Fatal("WallClock.WithDeadline(already passed) should be immediately cancelled")
	}
}

func TestWallClock_ImplementsClockInterface(t *testing.T) {
	var _ Clock = WallClock{}
}
