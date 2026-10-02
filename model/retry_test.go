package model

import (
	"math"
	"testing"
)

// TestRetryMaximumIntervalClamp: a positive MaximumIntervalMs replaces the
// legacy 60000 ceiling — growth above it clamps, and an initial above it
// collapses both bounds to it.
func TestRetryMaximumIntervalClamp(t *testing.T) {
	capped := RetryController{InitialIntervalMs: 100, Coefficient: 3.0, MaximumIntervalMs: 500}
	if got := capped.BackoffMs(20); got != 500 {
		t.Fatalf("BackoffMs(20) = %d, want 500 (custom ceiling)", got)
	}
	// Growth still applies below the custom ceiling.
	growing := RetryController{InitialIntervalMs: 100, Coefficient: 2.0, MaximumIntervalMs: 500}
	want := []int64{100, 200, 400, 500} // 100*2^3=800 clamps to 500
	for attempt := int32(1); attempt <= 4; attempt++ {
		if got := growing.BackoffMs(attempt); got != want[attempt-1] {
			t.Fatalf("BackoffMs(%d) = %d, want %d", attempt, got, want[attempt-1])
		}
	}
	// Initial above the custom cap: both bounds collapse to the cap.
	above := RetryController{InitialIntervalMs: 120000, Coefficient: 1.0, MaximumIntervalMs: 500}
	if got := above.BackoffMs(1); got != 500 {
		t.Fatalf("BackoffMs(1) = %d, want 500 (initial capped at custom ceiling)", got)
	}
	// Non-finite powers collapse to the custom ceiling, never NaN/Inf.
	nan := RetryController{InitialIntervalMs: 100, Coefficient: math.NaN(), MaximumIntervalMs: 500}
	if got := nan.BackoffMs(5); got != 500 {
		t.Fatalf("NaN coefficient BackoffMs(5) = %d, want 500 (custom ceiling)", got)
	}
}

// TestRetryLegacyDefaultUnchanged: MaximumIntervalMs == 0 keeps the legacy
// 60000 behavior byte-for-byte — identical to an explicit 60000 ceiling.
func TestRetryLegacyDefaultUnchanged(t *testing.T) {
	legacy := RetryController{InitialIntervalMs: 100, Coefficient: 2.0}
	explicit := RetryController{InitialIntervalMs: 100, Coefficient: 2.0, MaximumIntervalMs: 60000}
	for attempt := int32(1); attempt <= 25; attempt++ {
		if got, want := legacy.BackoffMs(attempt), explicit.BackoffMs(attempt); got != want {
			t.Fatalf("BackoffMs(%d): legacy = %d, explicit-60000 = %d (must match)", attempt, got, want)
		}
	}
	// Upper clamp still lands on 60000 without the field set.
	if got := legacy.BackoffMs(20); got != 60000 {
		t.Fatalf("legacy BackoffMs(20) = %d, want 60000", got)
	}
	// Negative ceilings are meaningless — they also select the legacy cap.
	negative := RetryController{InitialIntervalMs: 100, Coefficient: 2.0, MaximumIntervalMs: -7}
	if got := negative.BackoffMs(20); got != 60000 {
		t.Fatalf("negative-ceiling BackoffMs(20) = %d, want 60000 (legacy fallback)", got)
	}
}

// TestRetryBackoffDeterministic: pure math — same inputs give same outputs
// across repeated runs (no RNG, no state).
func TestRetryBackoffDeterministic(t *testing.T) {
	c := RetryController{InitialIntervalMs: 100, Coefficient: 2.0, MaximumIntervalMs: 500}
	first := make([]int64, 10)
	for attempt := int32(1); attempt <= 10; attempt++ {
		first[attempt-1] = c.BackoffMs(attempt)
	}
	for run := 0; run < 50; run++ {
		for attempt := int32(1); attempt <= 10; attempt++ {
			if got := c.BackoffMs(attempt); got != first[attempt-1] {
				t.Fatalf("run %d: BackoffMs(%d) = %d, want %d", run, attempt, got, first[attempt-1])
			}
		}
	}
}

// TestExceedsDeadlineBoundary: the gate fires only when now+backoff lands
// strictly after expiration — equality does NOT exceed.
func TestExceedsDeadlineBoundary(t *testing.T) {
	tests := []struct {
		name       string
		nowMs      int64
		backoffMs  int64
		expiryMs   int64
		wantExceed bool
	}{
		{"below deadline", 1000, 500, 1501, false},
		{"boundary equality does not exceed", 1000, 500, 1500, false},
		{"above deadline", 1000, 500, 1499, true},
		{"zero backoff at expiry", 1000, 0, 1000, false},
		{"zero backoff past expiry", 1001, 0, 1000, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ExceedsDeadline(tt.nowMs, tt.backoffMs, tt.expiryMs); got != tt.wantExceed {
				t.Fatalf("ExceedsDeadline(%d, %d, %d) = %v, want %v",
					tt.nowMs, tt.backoffMs, tt.expiryMs, got, tt.wantExceed)
			}
		})
	}
}

// TestExceedsDeadlineSaturation: int64 overflow saturates at MaxInt64 instead
// of wrapping; non-positive expirations mean "no deadline".
func TestExceedsDeadlineSaturation(t *testing.T) {
	// now+backoff overflows: saturates to MaxInt64, which exceeds any finite expiry.
	if !ExceedsDeadline(math.MaxInt64-10, 100, math.MaxInt64-1) {
		t.Fatal("overflowing now+backoff must exceed a finite expiry (saturating, no wrap)")
	}
	// Saturated fire time exactly at a MaxInt64 expiry: equality does not exceed.
	if ExceedsDeadline(math.MaxInt64-10, 100, math.MaxInt64) {
		t.Fatal("saturated fire time == MaxInt64 expiry must NOT exceed (boundary)")
	}
	// Negative clock/backoff inputs saturate to zero (non-negative time).
	if ExceedsDeadline(-5, -5, 0) {
		t.Fatal("negative now/backoff with no deadline must NOT exceed")
	}
	if got := ExceedsDeadline(-5, 100, 50); !got {
		t.Fatalf("ExceedsDeadline(-5, 100, 50) = %v, want true (now saturates to 0, 0+100 > 50)", got)
	}
	// Non-positive expiry means no deadline — never exceeds.
	if ExceedsDeadline(1000, 500, 0) {
		t.Fatal("zero expiry (no deadline) must NOT exceed")
	}
	if ExceedsDeadline(math.MaxInt64, math.MaxInt64, -1) {
		t.Fatal("negative expiry (no deadline) must NOT exceed")
	}
}
