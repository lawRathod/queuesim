package temporal

// S4 — Persistence QPS rate limiter (ADR-106, plan
// 2026-09-07-four-grounded-performance-seams-phase1.md, WP4).
//
// WP4 wires the two WP0 fields (PersistenceMaxQPS, PersistenceQPSBurstRatio)
// into a token bucket on the persistence-write dispatch path
// (HistoryShard.persistScaled — the model's single persistence-touching
// choke point, reached by startWorkflow/runWF/runAct/complete in history.go).
//
// Grounding (temporal submodule @ ffddf6fdc): the real history service
// builds ONE per-service persistence rate limiter — the golang
// x/time/rate token bucket (common/quotas/rate_limiter_impl.go:27), burst =
// int(rate × ratio) (common/quotas/dynamic.go:74-95, min-1 rule at :88-91),
// constructed only when PersistenceMaxQPS() > 0
// (common/persistence/client/fx.go:117-138), with the burst ratio wired at
// service/history/fx.go:293 and its 1.0 default at
// common/dynamicconfig/constants.go:251-254. The model mirrors that
// construction site in history.go and mirrors the burst math in
// components/history.NewPersistenceBucket — burst is derived from the ratio
// in CODE (ADR-106 decision 3, CODE-not-comment), never from a comment.
//
// Determinism: the bucket is pure deterministic float math on SimTime —
// zero RNG draws, zero wall-clock, bit-exact under a fixed seed (see
// TestS4DeterminismRepeats). Zero config (maxQPS = 0) = off = unbounded =
// today's behavior byte-identical (TestS4ZeroOff + TestS4NeutralityDigest).

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/law/GoDES/core"
	"github.com/lawRathod/queuesim/models/temporal/components/history"
)

// s4Shard builds a bare persistence shard with the given per-write latency.
func s4Shard(t *testing.T, persistMs int64) *history.HistoryShard {
	t.Helper()
	return history.NewHistoryShard(newSim(t), 0, persistMs, 1, 1, 1, 1, 0, 0)
}

// s4Landings drives n persistence writes, all arriving at the same sim time
// now (mult = 1), and returns the n landing times in write order.
func s4Landings(sh *history.HistoryShard, now core.SimTime, n int) []int64 {
	out := make([]int64, n)
	for i := 0; i < n; i++ {
		out[i] = sh.PersistScaled(now, 1).Int64()
	}
	return out
}

// TestS4ZeroOff pins the neutrality contract's off position: PersistenceMaxQPS
// = 0 must be byte-identical to an unwired shard (today's behavior) whatever
// the burst ratio — no delay is ever introduced and the shard write cursor
// advances exactly as before. The limiter is only constructed when maxQPS > 0
// (mirror of common/persistence/client/fx.go:117), so the zero bucket and the
// absent bucket must behave identically.
func TestS4ZeroOff(t *testing.T) {
	drive := func(sh *history.HistoryShard) []int64 {
		var out []int64
		// A mixed arrival pattern: same-instant backlog, then spaced writes,
		// across both the plain and scaled persist paths.
		for _, batch := range []struct {
			now  core.SimTime
			n    int
			mult float64
		}{
			{0, 5, 1},    // 5 writes at t=0 (Persist)
			{0, 5, 1},    // 5 more at the same instant
			{25, 3, 1},   // spaced arrival
			{40, 4, 2.5}, // scaled (mult) arrival
		} {
			for i := 0; i < batch.n; i++ {
				if batch.mult == 1 && len(out)%9 == 0 {
					out = append(out, sh.Persist(batch.now).Int64())
					continue
				}
				out = append(out, sh.PersistScaled(batch.now, batch.mult).Int64())
			}
		}
		return out
	}

	// persistMs = 7 so the unbounded path has a nonzero serialized landing
	// (the write cursor) to compare against.
	unwired := s4Shard(t, 7)
	zeroBucket := s4Shard(t, 7)
	zeroBucket.SetPersistenceBucket(history.NewPersistenceBucket(0, 0))
	zeroRatioBucket := s4Shard(t, 7)
	zeroRatioBucket.SetPersistenceBucket(history.NewPersistenceBucket(0, 5))

	want := drive(unwired)
	if got := drive(zeroBucket); fmt.Sprintf("%v", got) != fmt.Sprintf("%v", want) {
		t.Fatalf("maxQPS=0 bucket changed the write path:\nwant %v\ngot  %v", want, got)
	}
	if got := drive(zeroRatioBucket); fmt.Sprintf("%v", got) != fmt.Sprintf("%v", want) {
		t.Fatalf("maxQPS=0 with ratio 5 changed the write path:\nwant %v\ngot  %v", want, got)
	}
	if unwired.LastWriteMs() != zeroBucket.LastWriteMs() {
		t.Fatalf("write cursor moved at maxQPS=0: unwired %d, zero-bucket %d", unwired.LastWriteMs(), zeroBucket.LastWriteMs())
	}
}

// TestS4TokenBucketShape pins the token-bucket contour on the persistence
// write path: an initial burst of `burst` writes passes immediately, then the
// bucket paces writes at the configured QPS (a dry bucket admits one write
// every 1000/qps ms), landings stay monotone (arrival order preserved), and
// an idle interval refills the bucket up to its capacity (never beyond).
//
// Rate chosen for exact integer math on the ms sim clock: QPS = 100/s →
// one token per 10 ms, burst = 100 × 1.0 = 100.
func TestS4TokenBucketShape(t *testing.T) {
	sh := s4Shard(t, 0)
	sh.SetPersistenceBucket(history.NewPersistenceBucket(100, 1.0))

	land := s4Landings(sh, 0, 250)
	if len(land) != 250 {
		t.Fatalf("landings = %d, want 250", len(land))
	}
	// Burst: the first 100 writes land at t=0 (capacity = QPS × 1.0).
	for i := 0; i < 100; i++ {
		if land[i] != 0 {
			t.Fatalf("landing[%d] = %d, want 0 (within burst)", i, land[i])
		}
	}
	// Pacing: the 101st write waits for a token — one per 10ms thereafter.
	for i := 100; i < 250; i++ {
		want := int64((i - 99) * 10)
		if land[i] != want {
			t.Fatalf("landing[%d] = %d, want %d (paced at 100/s)", i, land[i], want)
		}
		if i > 100 && land[i] <= land[i-1] {
			t.Fatalf("landings not monotone at %d: %d <= %d", i, land[i], land[i-1])
		}
	}
	// Idle refill: after 3500ms of silence the bucket is full again — a
	// write at t=5000 lands immediately (tokens capped at burst, not beyond).
	if got := sh.PersistScaled(core.SimTime(5000), 1).Int64(); got != 5000 {
		t.Fatalf("write after idle landed at %d, want 5000 (burst refilled)", got)
	}
}

// TestS4BurstRatioCodePin is the CODE-not-comment burst-ratio pin
// (ADR-106 decision 3): the effective burst observed from behavior equals
// QPS × PersistenceQPSBurstRatio, with the ratio's server default 1.0 giving
// burst == QPS. The burst is derived in code at the rate-limiter construction
// call site (history.go historyService Build →
// history.NewPersistenceBucket — mirroring the server's
// common/quotas/dynamic.go:88 `burst := int(rate * ratio)`), never from the
// catalog comment. Each case drives maxQPS+burst+ε same-instant writes and
// counts how many land at t=0.
func TestS4BurstRatioCodePin(t *testing.T) {
	type want struct {
		name     string
		qps      float64 // maxQPS (tokens/s)
		ratio    float64 // PersistenceQPSBurstRatio
		burst    int     // expected immediate writes at t=0 = int(qps × ratio)
		first    int64   // expected landing of write #(burst+1)
		strictly bool    // ratio 0 = no burst credit: even write #1 is paced
	}
	cases := []want{
		// Server defaults: history.persistenceMaxQPS = 9000
		// (common/dynamicconfig/constants.go:1485) × system.
		// persistenceQPSBurstRatio = 1.0 (constants.go:251) → burst 9000.
		{"history default ratio 1.0", 9000, 1.0, 9000, 1, false},
		// ratio > 1 scales the burst up (QPS × ratio in code).
		{"ratio 2.0", 1000, 2.0, 2000, 1, false},
		// ratio < 1 scales the burst down (fractional product truncates).
		{"ratio 0.5", 1000, 0.5, 500, 1, false},
		// ratio 0 = burst 0: the ceiling paces every write (server burst-0
		// limiter — no initial credit, one write per 1000/qps ms).
		{"ratio 0", 1000, 0, 0, 1, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sh := s4Shard(t, 0)
			sh.SetPersistenceBucket(history.NewPersistenceBucket(c.qps, c.ratio))
			n := c.burst + 5
			land := s4Landings(sh, 0, n)
			immediate := 0
			for _, l := range land {
				if l == 0 {
					immediate++
				}
			}
			if immediate != c.burst {
				t.Fatalf("immediate landings = %d, want %d (effective burst = QPS × ratio, from behavior)", immediate, c.burst)
			}
			if c.strictly {
				for i := 0; i < n; i++ {
					if land[i] != int64(i+1) {
						t.Fatalf("ratio 0: landing[%d] = %d, want %d (strict pacing)", i, land[i], i+1)
					}
				}
				return
			}
			if got := land[c.burst]; got < c.first {
				t.Fatalf("write past the burst landed at %d, want >= %d (paced past capacity)", got, c.first)
			}
		})
	}
}

// TestS4ServiceCeilingAcrossShards pins that the rate limiter is ONE
// service-level ceiling shared by every shard of a runtime — mirroring the
// real server, which constructs a single per-service persistence limiter
// (service/history/fx.go:293 → common/persistence/client/fx.go:117). With
// two shards sharing one bucket the total burst across BOTH shards is `burst`
// (not 2×burst), and each shard's landings stay monotone in arrival order.
func TestS4ServiceCeilingAcrossShards(t *testing.T) {
	bucket := history.NewPersistenceBucket(100, 1.0) // burst 100, paced 10ms
	sh0 := s4Shard(t, 0)
	sh1 := s4Shard(t, 0)
	sh0.SetPersistenceBucket(bucket)
	sh1.SetPersistenceBucket(bucket)

	const perShard = 120
	land0 := make([]int64, 0, perShard)
	land1 := make([]int64, 0, perShard)
	for i := 0; i < 2*perShard; i++ {
		// Interleave arrivals across the shards (deterministic call order).
		if i%2 == 0 {
			land0 = append(land0, sh0.PersistScaled(0, 1).Int64())
		} else {
			land1 = append(land1, sh1.PersistScaled(0, 1).Int64())
		}
	}
	total0 := 0
	for _, l := range land0 {
		if l == 0 {
			total0++
		}
	}
	total1 := 0
	for _, l := range land1 {
		if l == 0 {
			total1++
		}
	}
	if total0+total1 != 100 {
		t.Fatalf("immediate landings across shards = %d, want 100 (one shared ceiling, not per-shard)", total0+total1)
	}
	// FIFO per shard: each shard's own write order is preserved.
	for i := 1; i < perShard; i++ {
		if land0[i] < land0[i-1] || land1[i] < land1[i-1] {
			t.Fatalf("per-shard landing order violated at %d: sh0 %v sh1 %v", i, land0[i], land1[i])
		}
	}
	// Global order is preserved too: the interleaved sequence is the
	// admission order (global write #i — 0-based — lands 10ms after write
	// #(i-99) once the shared burst is past, i > 99).
	for i := 100; i < 2*perShard; i++ {
		var got int64
		if i%2 == 0 {
			got = land0[i/2]
		} else {
			got = land1[i/2]
		}
		if want := int64((i - 99) * 10); got != want {
			t.Fatalf("global write #%d landed at %d, want %d (admission order)", i+1, got, want)
		}
	}
}

// TestS4DeterminismRepeats is the S4 determinism gate on the ACTIVE seam: a
// throttling ceiling (50/s, burst 50 — small enough that a 20-workflow burst
// drains the bucket and paces) must still yield a bit-identical digest across
// repeats. The token bucket draws zero RNG and reads zero wall-clock.
func TestS4DeterminismRepeats(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Seed = 42
	cfg.BurstCount = 20
	cfg.SimEndMs = 5000
	cfg.PersistenceMaxQPS = 50
	cfg.PersistenceQPSBurstRatio = 1.0
	run := func() string {
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run().Digest
	}
	first := run()
	for i := 1; i < 4; i++ {
		if got := run(); got != first {
			t.Fatalf("repeat %d: digest mismatch under an active S4 ceiling:\n%q\n%q", i, got, first)
		}
	}
	// Mechanism evidence: the ceiling must actually bite (the paced landings
	// shift the event stream) — a bit-identical digest to the unbounded run
	// would mean the seam is dead code.
	off := cfg
	off.PersistenceMaxQPS = 0
	top, err := NewTopology(off)
	if err != nil {
		t.Fatalf("NewTopology: %v", err)
	}
	if got := top.Run().Digest; got == first {
		t.Fatalf("S4 ceiling changed nothing: on-digest == off-digest %q", got)
	}
}

// TestS4NeutralityDigest is the WP4 byte-identity gate: with all seam fields
// at zero (S4 off), the two canonical N5 digests stay byte-identical to the
// pins recorded in seams_test.go — zero-draw sha256 a72689e9d1… and draws
// sha256 c7b0d97d14… (re-captured at the 2026-09-09 traceability closeout,
// commit 8c45016). WP4 must not change behavior at zero.
func TestS4NeutralityDigest(t *testing.T) {
	runZeroDraw := func() string {
		cfg := seamZeroConfig()
		cfg.Seed = 7
		cfg.BurstCount = 30
		cfg.SimEndMs = 20000
		cfg.FaultStartMs = 1
		cfg.FaultEndMs = 5001
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run().Digest
	}
	runDraws := func() string {
		cfg := rngConfig()
		cfg.ForwarderMaxOutstandingPolls = 0
		cfg.ForwarderMaxOutstandingTasks = 0
		cfg.AppendThreshold = 0
		cfg.PollerScaleWaitMs = 0
		cfg.PollerDecisionsPerSec = 0
		cfg.PersistenceMaxQPS = 0
		cfg.PersistenceQPSBurstRatio = 0
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		return top.Run().Digest
	}

	const pinZeroDraw = "a72689e91d1c1ca6feb5b79c30ed2aed52df1f1e88bfc446a1dcb59bdf3ea87e"
	const pinDraws = "c7b0d97d145079f610ce447adb1f2afa2f580031ae7a2f3328f43684d860a5bf"

	if got := fmt.Sprintf("%x", sha256.Sum256([]byte(runZeroDraw()))); got != pinZeroDraw {
		t.Fatalf("zero-draw digest moved with S4 off:\nsha256 = %s\npin     = %s", got, pinZeroDraw)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256([]byte(runDraws()))); got != pinDraws {
		t.Fatalf("draws digest moved with S4 off:\nsha256 = %s\npin     = %s", got, pinDraws)
	}
}
