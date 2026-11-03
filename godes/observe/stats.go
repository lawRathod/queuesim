package observe

import (
	"math"
	"sort"
)

// Stats is a streaming descriptive-statistics collector: count, mean,
// sample standard deviation, min, max, and a two-sided confidence interval
// for the mean. Feed values with Push; read with the accessors. The CI uses
// the t-distribution (exact small-df table + interpolation); for df > 120
// it falls back to the normal approximation.
type Stats struct {
	n      int64
	sum    float64
	sumSq  float64
	min    float64
	max    float64
	values []float64 // retained only when KeepValues is set
}

// StatsConfig controls optional Stats behavior.
type StatsConfig struct {
	// KeepValues retains all pushed values (enables exact percentiles via
	// Percentile). Memory: O(n).
	KeepValues bool
}

// NewStats creates an empty collector.
func NewStats(cfg ...StatsConfig) *Stats {
	s := &Stats{min: math.Inf(1), max: math.Inf(-1)}
	if len(cfg) > 0 && cfg[0].KeepValues {
		s.values = []float64{}
	}
	return s
}

// Push records one observation.
func (s *Stats) Push(v float64) {
	s.n++
	s.sum += v
	s.sumSq += v * v
	if v < s.min {
		s.min = v
	}
	if v > s.max {
		s.max = v
	}
	if s.values != nil {
		s.values = append(s.values, v)
	}
}

// N returns the number of observations.
func (s *Stats) N() int64 { return s.n }

// Mean returns the sample mean (0 when empty).
func (s *Stats) Mean() float64 {
	if s.n == 0 {
		return 0
	}
	return s.sum / float64(s.n)
}

// StdDev returns the sample standard deviation (n-1 denominator; 0 when
// n < 2).
func (s *Stats) StdDev() float64 {
	if s.n < 2 {
		return 0
	}
	variance := (s.sumSq - s.sum*s.sum/float64(s.n)) / float64(s.n-1)
	if variance < 0 {
		variance = 0 // guard against floating-point negative variance
	}
	return math.Sqrt(variance)
}

// Min returns the smallest observed value (+Inf when empty).
func (s *Stats) Min() float64 { return s.min }

// Max returns the largest observed value (-Inf when empty).
func (s *Stats) Max() float64 { return s.max }

// ConfidenceInterval returns the two-sided (1-alpha) confidence bounds for
// the mean using the t-distribution. For an empty collector it returns
// zeros; for n == 1 it returns the degenerate interval [mean, mean].
// alpha is typically 0.05 (95%).
func (s *Stats) ConfidenceInterval(alpha float64) (lo, hi float64) {
	if s.n == 0 {
		return 0, 0
	}
	mean := s.Mean()
	if s.n == 1 {
		return mean, mean
	}
	tc := tCritical(float64(s.n-1), alpha)
	half := tc * s.StdDev() / math.Sqrt(float64(s.n))
	return mean - half, mean + half
}

// Percentile returns the p-th percentile (p in [0,100]) of pushed values
// via linear interpolation on the sorted sample. Requires KeepValues;
// panics otherwise. Returns 0 when empty.
func (s *Stats) Percentile(p float64) float64 {
	if s.values == nil {
		panic("observe: Percentile requires StatsConfig{KeepValues: true}")
	}
	if len(s.values) == 0 {
		return 0
	}
	sorted := make([]float64, len(s.values))
	copy(sorted, s.values)
	sort.Float64s(sorted)
	if p <= 0 {
		return sorted[0]
	}
	if p >= 100 {
		return sorted[len(sorted)-1]
	}
	rank := (p / 100) * float64(len(sorted)-1)
	lo := int(math.Floor(rank))
	hi := int(math.Ceil(rank))
	frac := rank - float64(lo)
	return sorted[lo]*(1-frac) + sorted[hi]*frac
}

// BatchMeansEstimate is the steady-state mean estimate produced by
// BatchMeans: the number of non-overlapping batches k, the per-batch sample
// size, the grand mean of the batch means, and a normal-approximation CI for
// the mean.
type BatchMeansEstimate struct {
	Batches   int
	BatchSize int
	Mean      float64
	StdErr    float64
	CILow     float64
	CIHigh    float64
}

// BatchMeans estimates the steady-state mean of an autocorrelated series by
// the method of non-overlapping batch means: it splits samples into `batches`
// consecutive chunks, forms one mean per chunk, and treats the k batch means
// as approximately independent replications — then builds a normal CI from
// their sample variance (the correlation assumption is that batches are
// approximately stationary and weakly dependent).
//
// This is the correct estimator where a raw t-CI over the individual samples
// is NOT: consecutive window samples are positively correlated (e.g.
// Topology.MonitorWindows), so the sample variance of the raw series
// underestimates the true variance of the mean and the t-interval shrinks
// below its nominal coverage. Batching into fewer, larger chunks decorrelates
// them; it is not a cure for a trending or strongly periodic series.
//
// Preconditions, any of which returns ok=false: batches >= 2,
// len(samples) >= batches, confidence in (0,1). Any remainder samples beyond
// batches*batchSize are discarded (a floor split into equal-size batches). A
// constant series has zero batch-mean variance, so its interval is the
// degenerate [mean, mean] (not NaN). The interval uses the normal quantile;
// at small batch counts (k a few) this slightly under-covers, so prefer
// k >= ~10 (the reference's own guidance). Pure and deterministic: no RNG,
// no map iteration, stdlib only.
func BatchMeans(samples []float64, batches int, confidence float64) (BatchMeansEstimate, bool) {
	if batches < 2 || !(confidence > 0 && confidence < 1) || len(samples) < batches {
		return BatchMeansEstimate{}, false
	}
	batchSize := len(samples) / batches // >= 1: len(samples) >= batches >= 2
	means := make([]float64, 0, batches)
	for b := 0; b < batches; b++ {
		start := b * batchSize
		var sum float64
		for _, v := range samples[start : start+batchSize] {
			sum += v
		}
		means = append(means, sum/float64(batchSize))
	}
	k := len(means) // == batches (the guard above makes every batch non-empty)
	var grand float64
	for _, m := range means {
		grand += m
	}
	grand /= float64(k)
	var ss float64
	for _, m := range means {
		d := m - grand
		ss += d * d
	}
	variance := ss / float64(k-1)
	if variance < 0 {
		variance = 0 // guard against floating-point negative variance
	}
	stdErr := math.Sqrt(variance / float64(k))
	z := normalQuantile(0.5 + confidence/2)
	return BatchMeansEstimate{
		Batches:   k,
		BatchSize: batchSize,
		Mean:      grand,
		StdErr:    stdErr,
		CILow:     grand - z*stdErr,
		CIHigh:    grand + z*stdErr,
	}, true
}

// WilsonInterval returns the Wilson score interval for a Bernoulli proportion
// successes/trials at the given confidence. It is preferred over the normal
// (Wald) interval for small counts and proportions near 0 or 1, where Wald
// yields degenerate or out-of-range bounds. The bounds are clamped to [0,1].
//
// Preconditions, any of which returns ok=false: trials > 0,
// successes <= trials, confidence in (0,1). Pure and deterministic.
func WilsonInterval(successes, trials uint64, confidence float64) (lo, hi float64, ok bool) {
	if trials == 0 || successes > trials || !(confidence > 0 && confidence < 1) {
		return 0, 0, false
	}
	n := float64(trials)
	phat := float64(successes) / n
	z := normalQuantile(0.5 + confidence/2)
	z2 := z * z
	denom := 1 + z2/n
	center := (phat + z2/(2*n)) / denom
	radius := (z / denom) * math.Sqrt(phat*(1-phat)/n+z2/(4*n*n))
	lo = center - radius
	hi = center + radius
	if lo < 0 {
		lo = 0
	}
	if hi > 1 {
		hi = 1
	}
	return lo, hi, true
}

// tCritical returns the two-sided critical value t_{1-alpha/2} for the
// given degrees of freedom. Uses a small-df t-table with linear
// interpolation below df=120 and the normal quantile above (error < 0.2%
// at df=120). Supports alpha >= 0.01 exactly; smaller alphas fall back to
// the normal approximation.
func tCritical(df float64, alpha float64) float64 {
	z := normalQuantile(1 - alpha/2)
	if df > 120 || alpha < 0.01 {
		return z
	}
	// Rows: {df, t at alpha=0.05, t at alpha=0.01}.
	table := [][3]float64{
		{1, 12.706, 63.657},
		{2, 4.303, 9.925},
		{3, 3.182, 5.841},
		{4, 2.776, 4.604},
		{5, 2.571, 4.032},
		{6, 2.447, 3.707},
		{7, 2.365, 3.499},
		{8, 2.306, 3.355},
		{9, 2.262, 3.250},
		{10, 2.228, 3.169},
		{15, 2.131, 2.947},
		{20, 2.086, 2.845},
		{25, 2.060, 2.787},
		{30, 2.042, 2.750},
		{40, 2.021, 2.704},
		{60, 2.000, 2.660},
		{120, 1.980, 2.617},
	}
	col := 1
	if alpha <= 0.01 {
		col = 2
	}
	for i := 0; i < len(table)-1; i++ {
		lo, hi := table[i], table[i+1]
		if df >= lo[0] && df <= hi[0] {
			frac := (df - lo[0]) / (hi[0] - lo[0])
			return lo[col] + frac*(hi[col]-lo[col])
		}
	}
	return z
}

// normalQuantile computes the inverse standard-normal CDF: it solves
// Phi(x) = p for x via Newton iteration on math.Erfc, seeded by a
// logistic-style rational guess. Machine-precision accurate, no magic
// approximation constants.
func normalQuantile(p float64) float64 {
	if p <= 0 {
		return math.Inf(-1)
	}
	if p >= 1 {
		return math.Inf(1)
	}
	// Initial seed: logistic approximation to the probit (good to ~0.1).
	x := 0.0
	if p != 0.5 {
		x = 0.18 * math.Log(p/(1-p))
	}
	// Newton: f(x) = 0.5*erfc(-x/sqrt(2)) - p; f'(x) = pdf(x).
	for i := 0; i < 60; i++ {
		f := 0.5*math.Erfc(-x/math.Sqrt2) - p
		pdf := math.Exp(-0.5*x*x) / math.Sqrt(2*math.Pi)
		step := f / pdf
		x -= step
		if math.Abs(step) < 1e-14 {
			break
		}
	}
	return x
}
