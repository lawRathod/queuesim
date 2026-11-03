package observe

import (
	"math"
)

// QueueId identifies a queue/metric source.
type QueueId uint64

// ScoreWeights configures the weighted distance computation for each
// scoring dimension. The final score is the sum of per-dimension distances
// weighted by these coefficients: score = w_q*d_q + w_r*d_r + w_t*d_t + w_d*d_d
// where each d_x = |current_x - baseline_x| (per-window rate or mean).
type ScoreWeights struct {
	QueueMean          float64 // weight for queue-depth deviation
	RetryAmplification float64 // weight for retries/sec deviation
	TimeoutRateRps     float64 // weight for timeouts/sec deviation
	DropRateRps        float64 // weight for drops/sec deviation
	Distance           float64 // reserved for future use
}

func defaultScoreWeights() ScoreWeights {
	return ScoreWeights{
		QueueMean: 1.0, RetryAmplification: 1.0,
		TimeoutRateRps: 1.0, DropRateRps: 1.0, Distance: 1.0,
	}
}

// MonitorConfig configures the metastability monitor.
// All time values are in simulation milliseconds.
type MonitorConfig struct {
	WindowMs                 int64
	BaselineWarmupWindows    int
	RecoveryHoldWindows      int
	BaselineEpsilon          float64
	BaselineRatio            float64 // ratio threshold; 0 = disabled; checked as OR with BaselineEpsilon
	MetastablePersistWindows int
	RecoveryTimeLimitMs      *int64
	ScoreWeights             ScoreWeights
}

func defaultMonitorConfig() MonitorConfig {
	return MonitorConfig{
		WindowMs:                 1000,
		BaselineWarmupWindows:    10,
		RecoveryHoldWindows:      3,
		BaselineEpsilon:          3.0,
		BaselineRatio:            0.0, // disabled by default; opt-in per scenario
		MetastablePersistWindows: 20,
		RecoveryTimeLimitMs:      nil,
		ScoreWeights:             defaultScoreWeights(),
	}
}

type windowMetrics struct {
	startMs       int64
	endMs         int64
	queueIntegral float64 // accumulated depth × milliseconds in this window
	queueMax      uint64
	completedIn   uint64
	completedLt   uint64
	dropped       uint64
	timeouts      uint64
	retries       uint64
}

// MonitorWindowSummary is a public, post-processed view of a single closed
// monitoring window. It holds no references to the Monitor's internal state
// and is safe to marshal as JSON.
type MonitorWindowSummary struct {
	StartMs     int64   // window start (simulation ms)
	EndMs       int64   // window end (simulation ms)
	QueueMean   float64 // time-integral average queue depth
	QueueMax    uint64  // peak observed queue depth
	CompletedIn uint64  // requests completed within latency threshold
	CompletedLt uint64  // requests completed beyond latency threshold (long-tail)
	Dropped     uint64  // requests dropped
	Timeouts    uint64  // request timeouts
	Retries     uint64  // retry events
}

// MonitorStatus summarizes the current monitor state.
type MonitorStatus struct {
	// Score is the queue-depth distance from baseline at detection time
	// (or the live distance for non-metastable monitors).
	// It equals QueueMean × |currentMean − baselineMean| where QueueMean
	// is the ScoreWeights.QueueMean multiplier (default 1.0).
	// Score is 0 when baseline warmup is not yet complete.
	Score          float64
	Metastable     bool
	Recovered      bool
	WindowsSeen    int
	RecoveryTimeMs *int64
	MetastableAtMs int64 // simulation time (ms) when metastability was first declared; 0 if never
	BaselineEndMs  int64 // simulation time (ms) when baseline warmup completed; 0 if incomplete
}

// Monitor tracks queue depth and other metrics over sliding windows.
type Monitor struct {
	cfg     MonitorConfig
	startMs int64

	windows     []windowMetrics
	currentWin  windowMetrics
	windowLenMs int64

	// Time-integral queue tracking (persistent across windows)
	lastDepth    uint64 // queue depth since last observation
	lastChangeMs int64  // simulation time of last depth change

	// Single-dimension baseline (queue mean) — kept for backward compat.
	baseline     []float64
	baselineMean float64

	// Multi-dimension baselines for weighted L1 scoring.
	baselineRetries  float64 // mean retries per window during baseline
	baselineTimeouts float64 // mean timeouts per window during baseline
	baselineDrops    float64 // mean drops per window during baseline

	metastableWinCount int
	recoveryWinCount   int
	windowsSeen        int
	metastableAtMs     int64

	// scoreAtDetection captures the score when metastability is first declared.
	// This prevents score from being diluted by window pruning after detection.
	scoreAtDetection float64
}

// NewMonitor creates a Monitor with the given config, using nowMs as the
// simulation-time reference for aligning sliding windows.
// Panics if cfg has degenerate values: BaselineWarmupWindows <= 0,
// MetastablePersistWindows <= 0, or WindowMs <= 0.
func NewMonitor(cfg MonitorConfig, nowMs int64) *Monitor {
	if cfg.BaselineWarmupWindows <= 0 {
		panic("observe: MonitorConfig.BaselineWarmupWindows must be > 0")
	}
	if cfg.MetastablePersistWindows <= 0 {
		panic("observe: MonitorConfig.MetastablePersistWindows must be > 0")
	}
	if cfg.WindowMs <= 0 {
		panic("observe: MonitorConfig.WindowMs must be > 0")
	}
	// Apply default score weights when the caller leaves them zero-valued.
	// This prevents the common mistake of omitting ScoreWeights and getting
	// a score that is always zero (0.0 × distance = 0.0).
	if cfg.ScoreWeights == (ScoreWeights{}) {
		cfg.ScoreWeights = defaultScoreWeights()
	}

	m := &Monitor{
		cfg:         cfg,
		startMs:     nowMs,
		windowLenMs: cfg.WindowMs,
	}
	return m
}

func (m *Monitor) windowFor(nowMs int64) int64 {
	if nowMs < m.startMs {
		return 0
	}
	return (nowMs - m.startMs) / m.windowLenMs
}

// flushCurrent accumulates the time-weighted queue integral up to nowMs.
// If nowMs has crossed one or more window boundaries, each fully-elapsed
// window is committed to m.windows and the baseline/metastability detection
// logic is evaluated per-window. The partial window containing nowMs
// (if any) is left in currentWin for subsequent observations.
func (m *Monitor) flushCurrent(nowMs int64) {
	for {
		if m.currentWin.startMs == 0 && m.currentWin.endMs == 0 {
			// First-time initialization: no window has been set up yet.
			winIdx := m.windowFor(nowMs)
			m.currentWin.startMs = winIdx*m.windowLenMs + m.startMs
			m.currentWin.endMs = (winIdx+1)*m.windowLenMs + m.startMs
		}

		// Accumulate integral up to the window boundary (or nowMs if earlier).
		boundary := m.currentWin.endMs
		if nowMs < boundary {
			boundary = nowMs
		}
		if boundary > m.lastChangeMs {
			elapsed := boundary - m.lastChangeMs
			m.currentWin.queueIntegral += float64(m.lastDepth) * float64(elapsed)
			m.lastChangeMs = boundary
		}

		if nowMs < m.currentWin.endMs {
			// Still within the current window — nothing to commit yet.
			return
		}

		// The window is complete: save its endMs before resetting.
		prevEndMs := m.currentWin.endMs

		// Commit the completed window.
		m.windows = append(m.windows, m.currentWin)
		m.currentWin = windowMetrics{}
		m.windowsSeen++

		// Set up the next window contiguously from the committed window's end.
		// This avoids skipping windows when nowMs spans multiple boundaries.
		m.currentWin.startMs = prevEndMs
		m.currentWin.endMs = prevEndMs + m.windowLenMs

		windowLen := float64(m.windowLenMs)
		win := m.windows[len(m.windows)-1]
		qMean := win.queueIntegral / windowLen
		rPerWin := float64(win.retries)
		tPerWin := float64(win.timeouts)
		dPerWin := float64(win.dropped)

		if m.windowsSeen <= m.cfg.BaselineWarmupWindows {
			m.baseline = append(m.baseline, qMean)

			if m.windowsSeen == m.cfg.BaselineWarmupWindows {
				var sumQ, sumR, sumT, sumD float64
				for i, v := range m.baseline {
					sumQ += v
					w := m.windows[i]
					sumR += float64(w.retries)
					sumT += float64(w.timeouts)
					sumD += float64(w.dropped)
				}
				n := float64(len(m.baseline))
				m.baselineMean = sumQ / n
				m.baselineRetries = sumR / n
				m.baselineTimeouts = sumT / n
				m.baselineDrops = sumD / n
			}
		} else {
			// Multi-dimensional distance from baseline: weighted L1 norm.
			// Each dimension contributes |current - baseline| × weight.
			// All-zero baseline is valid (system starts from rest).
			dQ := math.Abs(qMean - m.baselineMean)
			dR := math.Abs(rPerWin - m.baselineRetries)
			dT := math.Abs(tPerWin - m.baselineTimeouts)
			dD := math.Abs(dPerWin - m.baselineDrops)
			score := m.cfg.ScoreWeights.QueueMean*dQ +
				m.cfg.ScoreWeights.RetryAmplification*dR +
				m.cfg.ScoreWeights.TimeoutRateRps*dT +
				m.cfg.ScoreWeights.DropRateRps*dD

			// Divergence is metastable if EITHER:
			//   (a) score >= BaselineEpsilon (multi-dim weighted distance), OR
			//   (b) ratio (queueMean/baseline) >= BaselineRatio (when >0)
			// The ratio check catches systems where the absolute distance
			// is small but relative growth is large (e.g., baseline queue=1,
			// post-spike=2, distance=1 < epsilon=3 but ratio=2×).
			diverged := score >= m.cfg.BaselineEpsilon
			if !diverged && m.cfg.BaselineRatio > 0 && m.baselineMean > 0 {
				diverged = qMean/m.baselineMean >= m.cfg.BaselineRatio
			}
			if diverged {
				m.metastableWinCount++
				m.recoveryWinCount = 0
				if m.metastableWinCount >= m.cfg.MetastablePersistWindows && m.metastableAtMs == 0 {
					m.metastableAtMs = win.endMs
					// Aggregate score from post-baseline windows at detection.
					warmup := m.cfg.BaselineWarmupWindows
					if warmup < 0 {
						warmup = 0
					}
					if warmup > len(m.windows) {
						warmup = len(m.windows)
					}
					var saQ, saR, saT, saD float64
					var saDur int64
					for _, w := range m.windows[warmup:] {
						saQ += w.queueIntegral
						saR += float64(w.retries)
						saT += float64(w.timeouts)
						saD += float64(w.dropped)
						saDur += w.endMs - w.startMs
					}
					sad := float64(saDur)
					var aggQ, aggR, aggT, aggD float64
					if sad > 0 {
						aggQ = saQ / sad // avg queue depth
						nWin := sad / float64(m.windowLenMs)
						aggR = saR / nWin // per-window retries
						aggT = saT / nWin // per-window timeouts
						aggD = saD / nWin // per-window drops
					}
					adQ := math.Abs(aggQ - m.baselineMean)
					adR := math.Abs(aggR - m.baselineRetries)
					adT := math.Abs(aggT - m.baselineTimeouts)
					adD := math.Abs(aggD - m.baselineDrops)
					m.scoreAtDetection = m.cfg.ScoreWeights.QueueMean*adQ +
						m.cfg.ScoreWeights.RetryAmplification*adR +
						m.cfg.ScoreWeights.TimeoutRateRps*adT +
						m.cfg.ScoreWeights.DropRateRps*adD
				}
			} else {
				m.metastableWinCount = 0
				m.recoveryWinCount++
			}
		}
		// Loop: next window may also be complete.
	}
}

// ObserveQueueLen records a queue-depth observation at simulation time nowMs
// for the given queue. Accumulates time-weighted integral since the last
// observation for exact average queue depth computation.
func (m *Monitor) ObserveQueueLen(nowMs int64, _ QueueId, depth uint64) {
	m.ensureWindow(nowMs)

	// Accumulate integral: previous depth × elapsed time since last change.
	elapsed := nowMs - m.lastChangeMs
	if elapsed > 0 {
		m.currentWin.queueIntegral += float64(m.lastDepth) * float64(elapsed)
	}

	m.lastDepth = depth
	m.lastChangeMs = nowMs

	if depth > m.currentWin.queueMax {
		m.currentWin.queueMax = depth
	}
}

// ObserveDrop records a dropped request at simulation time nowMs.
func (m *Monitor) ObserveDrop(nowMs int64) {
	m.ensureWindow(nowMs)
	m.currentWin.dropped++
}

// ObserveTimeout records a request timeout at simulation time nowMs.
func (m *Monitor) ObserveTimeout(nowMs int64) {
	m.ensureWindow(nowMs)
	m.currentWin.timeouts++
}

// ObserveRetry records a retry event at simulation time nowMs.
func (m *Monitor) ObserveRetry(nowMs int64) {
	m.ensureWindow(nowMs)
	m.currentWin.retries++
}

// ObserveComplete records a completed request at simulation time nowMs with the
// given latency. If active is true the completion is counted as in-flight; if false
// it is counted as long-tail (beyond a configured threshold).
func (m *Monitor) ObserveComplete(nowMs int64, _ int64, active bool) {
	m.ensureWindow(nowMs)
	if active {
		m.currentWin.completedIn++
	} else {
		m.currentWin.completedLt++
	}
}

func (m *Monitor) ensureWindow(nowMs int64) {
	if m.currentWin.startMs == 0 && m.currentWin.endMs == 0 {
		winIdx := m.windowFor(nowMs)
		m.currentWin.startMs = winIdx*m.windowLenMs + m.startMs
		m.currentWin.endMs = (winIdx+1)*m.windowLenMs + m.startMs
		return
	}
	// Advance if nowMs has crossed the current window boundary.
	if nowMs >= m.currentWin.endMs {
		m.flushCurrent(nowMs)
	}
}

// FlushUpTo flushes the current window up to simulation time nowMs and prunes
// windows older than the retention period from the tracked history.
func (m *Monitor) FlushUpTo(nowMs int64) {
	m.flushCurrent(nowMs)
	m.pruneOld(nowMs)
}

// FlushWindows commits any completed windows up to nowMs without pruning
// old windows. Use this during simulation stepping when you need to retain
// the full window history for later analysis (e.g., visualization).
// Call FlushUpTo periodically or at the end to limit memory growth.
func (m *Monitor) FlushWindows(nowMs int64) {
	m.flushCurrent(nowMs)
}

func (m *Monitor) pruneOld(nowMs int64) {
	keepMs := m.cfg.WindowMs * int64(m.cfg.MetastablePersistWindows+10)
	if keepMs > 0 && len(m.windows) > 0 {
		cutoff := nowMs - keepMs
		for len(m.windows) > 0 && m.windows[0].endMs < cutoff {
			m.windows = m.windows[1:]
		}
	}
}

// Status returns a snapshot of the current monitor state, including the
// aggregate distance from baseline, whether metastability or recovery has
// been declared, and the number of windows seen so far.
func (m *Monitor) Status() MonitorStatus {
	status := MonitorStatus{WindowsSeen: m.windowsSeen}

	if m.windowsSeen <= m.cfg.BaselineWarmupWindows {
		return status
	}

	// Compute live multi-dimensional score from ALL post-warmup windows.
	// This reflects the true system state even when metastability was
	// declared early (scoreAtDetection may be stale or 0 if declared
	// during the idle startup period).
	warmup := m.cfg.BaselineWarmupWindows
	var pwQ, pwR, pwT, pwD float64 // post-warmup sums
	var pwDur int64                // post-warmup duration
	for i, w := range m.windows {
		if i < warmup {
			continue // skip warmup windows
		}
		pwQ += w.queueIntegral
		pwR += float64(w.retries)
		pwT += float64(w.timeouts)
		pwD += float64(w.dropped)
		pwDur += w.endMs - w.startMs
	}
	pwd := float64(pwDur)
	var postQ, postR, postT, postD float64 // per-window means
	if pwd > 0 {
		postQ = pwQ / pwd // queueIntegral/ms = avg queue depth
		nWin := float64(pwDur) / float64(m.windowLenMs)
		postR = pwR / nWin // per-window retries
		postT = pwT / nWin // per-window timeouts
		postD = pwD / nWin // per-window drops
	}
	status.Score = m.cfg.ScoreWeights.QueueMean*math.Abs(postQ-m.baselineMean) +
		m.cfg.ScoreWeights.RetryAmplification*math.Abs(postR-m.baselineRetries) +
		m.cfg.ScoreWeights.TimeoutRateRps*math.Abs(postT-m.baselineTimeouts) +
		m.cfg.ScoreWeights.DropRateRps*math.Abs(postD-m.baselineDrops)

	// Read counters tracked per-window during flush — do not mutate here.
	if m.metastableWinCount >= m.cfg.MetastablePersistWindows {
		status.Metastable = true
	}

	if m.metastableAtMs > 0 && m.recoveryWinCount >= m.cfg.RecoveryHoldWindows {
		status.Recovered = true
	}

	status.MetastableAtMs = m.metastableAtMs
	status.BaselineEndMs = m.startMs + int64(m.cfg.BaselineWarmupWindows)*m.cfg.WindowMs

	return status
}

// WindowSummaries returns a snapshot of all closed monitoring windows in
// chronological order. The returned slice is a deep copy: mutating it has
// no effect on the Monitor, and the Monitor's continued operation will not
// mutate previously returned values.
//
// Returns nil when no windows have been closed yet. The Monitor is not
// goroutine-safe; WindowSummaries must not be called concurrently with
// Observe* or Flush* methods.
func (m *Monitor) WindowSummaries() []MonitorWindowSummary {
	if len(m.windows) == 0 {
		return nil
	}
	out := make([]MonitorWindowSummary, len(m.windows))
	for i, w := range m.windows {
		dur := w.endMs - w.startMs
		var mean float64
		if dur > 0 {
			mean = w.queueIntegral / float64(dur)
		}
		out[i] = MonitorWindowSummary{
			StartMs:     w.startMs,
			EndMs:       w.endMs,
			QueueMean:   mean,
			QueueMax:    w.queueMax,
			CompletedIn: w.completedIn,
			CompletedLt: w.completedLt,
			Dropped:     w.dropped,
			Timeouts:    w.timeouts,
			Retries:     w.retries,
		}
	}
	return out
}
