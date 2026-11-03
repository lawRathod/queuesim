package model

import (
	"math"

	"github.com/lawRathod/queuesim/godes/core"
)

// RetryController is the stateless retry-policy math (ADR-068): backoff =
// InitialIntervalMs * Coefficient^(attempt-1), clamped to
// [min(initial, ceiling), ceiling], where ceiling is MaximumIntervalMs (or
// the legacy 60000 when unset). It contains no RNG and no state — the
// topology owns attempt counters; the controller only answers "how long"
// and "may I".
type RetryController struct {
	InitialIntervalMs int64   // base backoff before the second attempt
	Coefficient       float64 // backoff growth factor per retry
	MaxAttempts       int32   // total attempt cap; 0 = unlimited
	// MaximumIntervalMs is the backoff ceiling. Values <= 0 select the
	// legacy 60000 ceiling, so the zero value is byte-neutral with the
	// pre-field behavior.
	MaximumIntervalMs int64
}

// maxBackoffMs is the legacy retry backoff ceiling (plan calibration table).
// It applies whenever RetryController.MaximumIntervalMs is unset (<= 0).
const maxBackoffMs int64 = 60000

// ceiling returns the effective backoff cap: MaximumIntervalMs when positive,
// otherwise the legacy maxBackoffMs.
func (c RetryController) ceiling() int64 {
	if c.MaximumIntervalMs <= 0 {
		return maxBackoffMs
	}
	return c.MaximumIntervalMs
}

// BackoffMs returns the backoff before the next attempt, given the 1-based
// number of the attempt that just failed. Coefficients <= 0 and
// non-finite powers collapse to the lower clamp; zero initial returns 0
// (immediate retry).
func (c RetryController) BackoffMs(failedAttempt int32) int64 {
	if c.InitialIntervalMs <= 0 {
		return 0
	}
	capMs := c.ceiling()
	lower := c.InitialIntervalMs
	if lower > capMs {
		lower = capMs
	}
	if failedAttempt <= 1 {
		return lower
	}
	v := float64(c.InitialIntervalMs) * math.Pow(c.Coefficient, float64(failedAttempt-1))
	if math.IsNaN(v) || math.IsInf(v, 1) || v >= float64(capMs) {
		return capMs
	}
	if v < 0 {
		return lower
	}
	ms := int64(math.Round(v))
	if ms < lower {
		ms = lower
	}
	if ms > capMs {
		ms = capMs
	}
	return ms
}

// CanRetry reports whether the attempt that just failed (1-based) may be
// retried: always when MaxAttempts == 0, otherwise only while the completed
// attempt count is below the cap.
func (c RetryController) CanRetry(failedAttempt int32) bool {
	return c.MaxAttempts == 0 || failedAttempt < c.MaxAttempts
}

// ExceedsDeadline reports whether a retry scheduled backoffMs after nowMs
// would fire strictly after expirationMs — the per-policy deadline gate.
// Equality does NOT exceed: a retry landing exactly on the deadline is
// still allowed.
//
// All arithmetic is saturating int64 (via core.SimTime, ADR-005): nowMs +
// backoffMs clamps at math.MaxInt64 instead of wrapping, so an overflowing
// sum exceeds every finite expiration. Negative nowMs/backoffMs saturate to
// zero (non-negative time). A non-positive expirationMs means "no deadline"
// and never exceeds. The function is pure: no RNG, no wall-clock, no state.
func ExceedsDeadline(nowMs, backoffMs, expirationMs int64) bool {
	if expirationMs <= 0 {
		return false
	}
	if backoffMs < 0 {
		backoffMs = 0
	}
	fireAt := core.NewSimTime(nowMs).Add(backoffMs)
	return fireAt.Int64() > expirationMs
}
