package observe

import (
	"testing"
)

func TestMonitorBaseline(t *testing.T) {
	cfg := defaultMonitorConfig()
	cfg.BaselineWarmupWindows = 3
	m := NewMonitor(cfg, 0)

	for i := 0; i < 3; i++ {
		ms := int64(i*1000 + 500)
		m.ObserveQueueLen(ms, 0, 5)
		m.FlushUpTo(ms + 500)
	}

	status := m.Status()
	if status.Score != 0 {
		t.Fatalf("expected score 0 during warmup, got %f", status.Score)
	}
}

func TestMonitorQueueIntegral(t *testing.T) {
	// Window 0-1000ms: queue at depth 5 from t=200 to t=800 (600ms)
	// Queue at depth 10 from t=800 to t=1000 (200ms, flushed)
	// Mean should be (5*600 + 10*200) / 1000 = 5000/1000 = 5.0
	cfg := defaultMonitorConfig()
	cfg.WindowMs = 1000
	cfg.BaselineWarmupWindows = 1
	cfg.MetastablePersistWindows = 100
	m := NewMonitor(cfg, 0)

	m.ObserveQueueLen(200, 0, 5)
	m.ObserveQueueLen(800, 0, 10)
	m.FlushUpTo(1000)

	status := m.Status()
	if status.WindowsSeen != 1 {
		t.Fatalf("expected 1 window, got %d", status.WindowsSeen)
	}
	if status.Score != 0 {
		t.Fatalf("expected score 0 during warmup, got %f", status.Score)
	}

	win := m.windows[0]
	mean := win.queueIntegral / float64(win.endMs-win.startMs)
	if mean != 5.0 {
		t.Fatalf("expected integral mean 5.0, got %f (integral=%f, duration=%d)",
			mean, win.queueIntegral, win.endMs-win.startMs)
	}
}

func TestMonitorIntegralMultiBoundary(t *testing.T) {
	cfg := defaultMonitorConfig()
	cfg.WindowMs = 1000
	cfg.BaselineWarmupWindows = 3
	m := NewMonitor(cfg, 0)

	// Observe at t=200, depth=5. FlushUpTo(3500) spans windows
	// [0,1000), [1000,2000), [2000,3000) and partially into [3000,4000).
	m.ObserveQueueLen(200, 0, 5)
	m.FlushUpTo(3500)

	if len(m.windows) != 3 {
		t.Fatalf("expected 3 committed windows, got %d", len(m.windows))
	}

	// Window 0 [0,1000): depth 5 from t=200 to t=1000 → integral=5*800=4000
	// Window 1 [1000,2000): depth 5 from t=1000 to t=2000 → integral=5*1000=5000
	// Window 2 [2000,3000): depth 5 from t=2000 to t=3000 → integral=5*1000=5000
	expected := []struct {
		integral float64
		mean     float64
	}{
		{4000, 4.0},
		{5000, 5.0},
		{5000, 5.0},
	}
	for i, exp := range expected {
		w := m.windows[i]
		if w.queueIntegral != exp.integral {
			t.Errorf("window %d: expected integral %f, got %f", i, exp.integral, w.queueIntegral)
		}
		mean := w.queueIntegral / float64(w.endMs-w.startMs)
		if mean != exp.mean {
			t.Errorf("window %d: expected mean %f, got %f", i, exp.mean, mean)
		}
	}
}

func TestMonitorDeterminism(t *testing.T) {
	var results [3]struct {
		windows int
		score   float64
	}
	for run := 0; run < 3; run++ {
		cfg := defaultMonitorConfig()
		cfg.BaselineWarmupWindows = 2
		cfg.MetastablePersistWindows = 1
		cfg.BaselineEpsilon = 1.0
		m := NewMonitor(cfg, 0)
		for i := 0; i < 5; i++ {
			m.ObserveQueueLen(int64(i*1000+200), 0, uint64(5+i*10))
			m.FlushUpTo(int64((i + 1) * 1000))
		}
		results[run].windows = m.windowsSeen
		results[run].score = m.Status().Score
	}
	for i := 1; i < 3; i++ {
		if results[i] != results[0] {
			t.Fatalf("run %d differs from run 0: %+v vs %+v", i, results[i], results[0])
		}
	}
}

func TestMonitorDetectsQueueSpike(t *testing.T) {
	cfg := defaultMonitorConfig()
	cfg.BaselineWarmupWindows = 3
	cfg.MetastablePersistWindows = 2
	cfg.BaselineEpsilon = 2.0
	m := NewMonitor(cfg, 0)

	for i := 0; i < 3; i++ {
		ms := int64(i*1000 + 500)
		m.ObserveQueueLen(ms, 0, 5)
		m.FlushUpTo(ms + 500)
	}

	for i := 0; i < 3; i++ {
		ms := int64((3+i)*1000 + 500)
		m.ObserveQueueLen(ms, 0, 50)
		m.FlushUpTo(ms + 500)
		m.Status() // simulate periodic polling during spike
	}

	status := m.Status()
	if status.Score <= 0 {
		t.Fatalf("expected positive score after spike, got %f", status.Score)
	}
	if !status.Metastable {
		t.Fatal("expected metastable=true after persistent spike")
	}
}

// TestMonitorRatioThreshold covers the chain_enqueue misclassification:
// baseline queue depth is small (1.0) and post-spike depth is 2.0 — that's a
// 2.0× growth (clearly metastable) but the absolute distance (1.0) is below
// BaselineEpsilon=2.0, so the absolute-only check says "stable". The ratio
// check (BaselineRatio=2.0) catches it.
func TestMonitorRatioThreshold(t *testing.T) {
	cfg := defaultMonitorConfig()
	cfg.WindowMs = 1000
	cfg.BaselineWarmupWindows = 3
	cfg.MetastablePersistWindows = 3
	cfg.BaselineEpsilon = 2.0 // absolute distance threshold (misses this case)
	cfg.BaselineRatio = 2.0   // ratio threshold (catches low-traffic systems)
	m := NewMonitor(cfg, 0)

	// 3 warmup windows at depth 1.0 — baselineMean = 1.0
	for i := 0; i < 3; i++ {
		startMs := int64(i * 1000)
		m.ObserveQueueLen(startMs, 0, 1)
		m.FlushUpTo(startMs + 1000)
	}

	// 3 spike windows at depth 2.0 — distance=1.0 (<2.0), ratio=2.0 (≥2.0)
	for i := 0; i < 3; i++ {
		startMs := int64((3 + i) * 1000)
		m.ObserveQueueLen(startMs, 0, 2)
		m.FlushUpTo(startMs + 1000)
	}

	status := m.Status()
	if !status.Metastable {
		t.Fatalf("expected metastable=true via ratio threshold (baseline=1.0, post-spike=2.0, ratio=2.0); got Metastable=false Score=%f", status.Score)
	}
}

// TestMonitorRecoveredRequiresPriorMetastability covers the recovery false
// positive: a monitor that never went metastable should not report Recovered.
// Currently, RecoveryHoldWindows consecutive non-metastable windows triggers
// Recovered=true regardless of whether the system was ever broken.
func TestMonitorRecoveredRequiresPriorMetastability(t *testing.T) {
	cfg := defaultMonitorConfig()
	cfg.WindowMs = 1000
	cfg.BaselineWarmupWindows = 2
	cfg.MetastablePersistWindows = 100 // never metastable
	cfg.RecoveryHoldWindows = 2
	cfg.BaselineEpsilon = 100.0 // impossible threshold
	m := NewMonitor(cfg, 0)

	// 5 stable windows: depth stays at 5, never diverges
	for i := 0; i < 5; i++ {
		ms := int64(i*1000 + 500)
		m.ObserveQueueLen(ms, 0, 5)
		m.FlushUpTo(ms + 500)
	}

	status := m.Status()
	if status.Metastable {
		t.Fatalf("expected Metastable=false (epsilon unreachable), got true")
	}
	if status.Recovered {
		t.Fatalf("expected Recovered=false (never metastable), got true")
	}
}

func TestMonitor_WindowSummaries_Empty(t *testing.T) {
	cfg := defaultMonitorConfig()
	m := NewMonitor(cfg, 0)
	summaries := m.WindowSummaries()
	if summaries != nil {
		t.Fatalf("expected nil from fresh monitor, got %d summaries", len(summaries))
	}
}

func TestMonitor_WindowSummaries_AfterObservations(t *testing.T) {
	cfg := defaultMonitorConfig()
	cfg.WindowMs = 100
	cfg.BaselineWarmupWindows = 1
	m := NewMonitor(cfg, 0)

	// Window [0,100): depth 0 from t=0 to t=50, depth 5 from t=50 to t=100
	m.ObserveQueueLen(0, 0, 0)
	m.ObserveQueueLen(50, 0, 5)
	m.FlushUpTo(100)

	summaries := m.WindowSummaries()
	if len(summaries) != 1 {
		t.Fatalf("expected 1 summary, got %d", len(summaries))
	}
	s := summaries[0]
	if s.StartMs != 0 || s.EndMs != 100 {
		t.Fatalf("unexpected window bounds: [%d, %d)", s.StartMs, s.EndMs)
	}

	// QueueMean: (0*50 + 5*50) / 100 = 250/100 = 2.5
	if s.QueueMean != 2.5 {
		t.Fatalf("expected QueueMean 2.5, got %f", s.QueueMean)
	}
	if s.QueueMax != 5 {
		t.Fatalf("expected QueueMax 5, got %d", s.QueueMax)
	}

	// Verify deep copy: modifying returned slice doesn't affect second call.
	summaries[0].QueueMean = 999.0
	summaries2 := m.WindowSummaries()
	if len(summaries2) != 1 || summaries2[0].QueueMean != 2.5 {
		t.Fatal("WindowSummaries did not return a deep copy")
	}
}
