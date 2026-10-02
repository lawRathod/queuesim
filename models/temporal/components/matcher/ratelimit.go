package matcher

import (
	"math"
)

// TokenBucket is a deterministic token-bucket rate limiter.
// Determinism: refill is lazy at Consume time using sim time.
//
// Sync-match accounting: the matching fast path consumes one token BEFORE
// handing a task to a parked poller (sync Consume before match). A throttled
// offer (Consume refused) spools instead of sync-matching.
//
// Recycle is a reserved cross-track primitive for forwarding paths that
// optimistically consume a sync token before knowing whether the offer will
// spool. No caller in matcher/ consumes-then-spools today — the throttled
// Add path spools WITHOUT consuming (refused takes no token) and the sync
// path never spools — so no double-charge occurs in the current tree; the
// method exists so a future optimistic-consume site can return the token
// (a spooled task is charged again at TryDispatch).
type TokenBucket struct {
	rate     float64
	burst    float64
	level    float64
	lastMs   int64
	refused  uint64
	consumed uint64
}

// NewTokenBucket creates a bucket with rate tokens/s and burst capacity.
// rate <= 0 means unlimited (Consume always returns true, zero draws).
func NewTokenBucket(rate float64, burst float64) *TokenBucket {
	if rate < 0 {
		rate = 0
	}
	if burst <= 0 {
		burst = 1
	}
	return &TokenBucket{rate: rate, burst: burst, level: burst}
}

func (b *TokenBucket) refill(nowMs int64) {
	if b.rate <= 0 {
		return
	}
	if nowMs <= b.lastMs {
		return
	}
	dt := float64(nowMs - b.lastMs)
	b.level = math.Min(b.burst, b.level+dt/1000.0*b.rate)
	b.lastMs = nowMs
}

// Consume attempts to take one token at sim time nowMs. The matching
// sync-match fast path calls Consume BEFORE handing a task to a parked
// poller; a refused offer spools (or waits out its Offer deadline) instead
// of sync-matching.
func (b *TokenBucket) Consume(nowMs int64) bool {
	if b.rate <= 0 {
		b.consumed++
		return true
	}
	b.refill(nowMs)
	if b.level >= 1 {
		b.level--
		b.consumed++
		return true
	}
	b.refused++
	return false
}

// Refused returns the number of refused (throttled) attempts.
func (b *TokenBucket) Refused() uint64 { return b.refused }

// Recycle returns one token to the bucket, capped at burst. It is the
// inverse of an optimistically consumed sync token that did not result in a
// dispatch (e.g. an offer that fell back to spool — the spooled task is
// charged again at TryDispatch). On an unlimited bucket (rate <= 0) it is a
// no-op. Recycle never draws RNG and never moves sim time.
func (b *TokenBucket) Recycle() {
	if b.rate <= 0 {
		return
	}
	b.level = math.Min(b.burst, b.level+1)
}

// Consumed returns the number of successful token takes.
func (b *TokenBucket) Consumed() uint64 { return b.consumed }
