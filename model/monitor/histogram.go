package monitor

import "math/bits"

// numBuckets is the number of log-scale integer buckets. Bucket i covers
// [2^(i-1), 2^i - 1] for i in [1, numBuckets); bucket 0 holds value 0.
const numBuckets = 64

// Histogram is a deterministic, pure-integer log-scale histogram of
// milliseconds (or any non-negative integer sample). Bucket i covers the
// range [2^(i-1), 2^i-1]; percentiles are derived from cumulative bucket
// counts using integer arithmetic only (no floating point in the hot path).
type Histogram struct {
	buckets [numBuckets]uint64
	count   uint64
	maxMs   int64
	sumMs   uint64
	// sumSq is the sum of squares of the samples, saturating at MaxUint64
	// (see Add) — the ceiling is the largest uint64; in a run of millisecond
	// samples it is unreachable, but the guard means a long storm can never
	// silently wrap it into a bogus stddev.
	sumSq uint64
}

// NewHistogram returns an empty histogram.
func NewHistogram() *Histogram { return &Histogram{} }

// Add records one sample. Negative values are clamped to 0. The
// second-moment accumulator sumSq saturates at MaxUint64 rather than wrapping
// (a value whose square exceeds uint64, or a total that would overflow).
func (h *Histogram) Add(v int64) {
	if v < 0 {
		v = 0
	}
	if v > h.maxMs {
		h.maxMs = v
	}
	h.sumMs += uint64(v)
	hi, lo := bits.Mul64(uint64(v), uint64(v))
	if hi != 0 || h.sumSq > ^uint64(0)-lo {
		h.sumSq = ^uint64(0) // saturate
	} else {
		h.sumSq += lo
	}
	h.count++
	h.buckets[bucketIndex(v)]++
}

// bucketIndex returns the bucket index for a non-negative value.
func bucketIndex(v int64) int {
	if v <= 0 {
		return 0
	}
	return 64 - bits.LeadingZeros64(uint64(v))
}

// Count returns the number of recorded samples.
func (h *Histogram) Count() uint64 { return h.count }

// MaxMs returns the largest recorded sample.
func (h *Histogram) MaxMs() int64 { return h.maxMs }

// MeanMs returns the integer-rounded mean of recorded samples.
func (h *Histogram) MeanMs() int64 {
	if h.count == 0 {
		return 0
	}
	return int64(h.sumMs / uint64(h.count))
}

// StdDevMs returns the integer population standard deviation (floor of the
// square root of the second central moment), computed with integer math
// only. It is 0 for 0 or 1 samples.
//
// Saturation note (ADR-116): once sumSq has saturated at MaxUint64 the
// accumulator clamps BELOW the true sum of squares, so the reading is a
// biased approximation — typically an UNDER-estimate — and saturation is not
// detectable from the return value. The only guarantees are that it never
// wraps and never goes negative (the degenerate mean² > uint64 case caps at
// math.MaxInt64).
func (h *Histogram) StdDevMs() int64 {
	if h.count <= 1 {
		return 0
	}
	mean := h.sumMs / h.count
	if mean != 0 {
		if hi, _ := bits.Mul64(mean, mean); hi != 0 {
			return int64(^uint64(0) >> 1) // mean² > uint64: capped, see saturation note
		}
	}
	e2 := h.sumSq / h.count
	m2 := mean * mean
	if e2 <= m2 {
		return 0
	}
	return int64(isqrt(e2 - m2))
}

// P999MinCount is the sample-count gate for P999Ms: a p99.9 is only
// reportable from at least this many samples. Below it the 99.9th-percentile
// target index collapses onto the maximum, so the reading is just the max
// bucket's lower bound — not a tail measurement. 1000 is the sample count
// needed for roughly one expected observation in the top 0.1%.
const P999MinCount = 1000

// P999Ms returns the bucket-lower-bound representative of the 99.9th
// percentile, and ok=false (value 0) when Count < P999MinCount.
//
// Honesty note (ADR-116): PercentileMs returns the bucket LOWER bound
// 2^(i-1), so p999 is bucket-resolution-limited — it cannot resolve a tail
// finer than a bucket, and at the gate boundary (or any uniform sample) it
// degenerates to the maximum bucket's lower bound. It is an approximate
// instrument and must be quoted as such.
func (h *Histogram) P999Ms() (int64, bool) {
	if h.count < P999MinCount {
		return 0, false
	}
	return h.PercentileMs(999, 1000), true
}

// PercentileMs returns the bucket-lower-bound representative of the
// (num/denom)th percentile using pure integer math. num/denom in (0,1].
// Returns 0 when the histogram is empty.
func (h *Histogram) PercentileMs(num, denom uint64) int64 {
	if h.count == 0 {
		return 0
	}
	target := h.count * num / denom
	if target == 0 {
		target = 1
	}
	var cum uint64
	for i := 0; i < numBuckets; i++ {
		cum += h.buckets[i]
		if cum >= target {
			if i == 0 {
				return 0
			}
			return int64(uint64(1) << uint(i-1))
		}
	}
	return h.maxMs
}

// Digest returns a deterministic string serialization of the histogram's
// full bucket state — byte-identical for identical insertion sequences.
func (h *Histogram) Digest() string {
	var b [numBuckets]byte
	for i := 0; i < numBuckets; i++ {
		b[i] = byte(h.buckets[i] & 0xff)
	}
	return string(b[:])
}

// isqrt returns floor(sqrt(v)) via Newton's method (integer math only).
func isqrt(v uint64) uint64 {
	if v == 0 {
		return 0
	}
	x := uint64(1) << ((bits.Len64(v) + 1) / 2)
	for {
		y := (x + v/x) / 2
		if y >= x {
			return x
		}
		x = y
	}
}
