package temporal

import (
	"crypto/sha256"
	"fmt"
	"math"
	"strings"
	"testing"
)

// Four Grounded Performance Seams — WP0 (shared infra, ADR-106 / plan
// 2026-09-07-four-grounded-performance-seams-phase1.md).
//
// WP0 adds the seven zero-default config fields (S1 forwarder caps,
// S2 append threshold, S3 poller wait + decisions cap, S4 persistence QPS)
// plus validation; seam BEHAVIOR lands in WP1-WP4. The neutrality contract
// (zero = off) is proven here: with all seams at their zero default the N5
// digest stays byte-identical to the recorded pins (zero-draw path
// a72689e9d1…, draws path c7b0d97d14… — the digests re-captured at the
// 2026-09-09 traceability closeout, commit 8c45016, and unchanged since).

// seamZeroConfig returns DefaultConfig with every seam field EXPLICITLY at
// zero (the neutrality contract's off position — same value the zero-value
// struct carries, stated so a future non-zero default trips loudly).
func seamZeroConfig() Config {
	cfg := DefaultConfig()
	cfg.ForwarderMaxOutstandingPolls = 0
	cfg.ForwarderMaxOutstandingTasks = 0
	cfg.AppendThreshold = 0
	cfg.PollerScaleWaitMs = 0
	cfg.PollerDecisionsPerSec = 0
	cfg.PersistenceMaxQPS = 0
	cfg.PersistenceQPSBurstRatio = 0
	return cfg
}

// TestSeamConfigFieldsZeroDefault pins the WP0 zero-default contract: every
// new seam field defaults to its zero value (= off) in DefaultConfig, so
// today's behavior is the unmodified behavior. A non-zero default here
// would be a neutrality violation by construction.
func TestSeamConfigFieldsZeroDefault(t *testing.T) {
	cfg := DefaultConfig()
	seams := []struct {
		name string
		got  any
	}{
		{"ForwarderMaxOutstandingPolls", cfg.ForwarderMaxOutstandingPolls},
		{"ForwarderMaxOutstandingTasks", cfg.ForwarderMaxOutstandingTasks},
		{"AppendThreshold", cfg.AppendThreshold},
		{"PollerScaleWaitMs", cfg.PollerScaleWaitMs},
		{"PollerDecisionsPerSec", cfg.PollerDecisionsPerSec},
		{"PersistenceMaxQPS", cfg.PersistenceMaxQPS},
		{"PersistenceQPSBurstRatio", cfg.PersistenceQPSBurstRatio},
	}
	for _, s := range seams {
		if got := fmt.Sprintf("%v", s.got); got != "0" {
			t.Fatalf("DefaultConfig().%s = %v, want 0 (zero default = seam off)", s.name, s.got)
		}
	}
}

// TestSeamConfigValidation pins WP0 validation: negative values, NaN, and
// ±Inf are rejected for every seam field; zero and positive values are
// accepted (fields are surface-only in WP0 — seam behavior lands WP1-WP4).
func TestSeamConfigValidation(t *testing.T) {
	base := func() Config {
		cfg := seamZeroConfig()
		cfg.Seed = 7
		cfg.BurstCount = 5
		cfg.SimEndMs = 3000
		return cfg
	}
	run := func(cfg Config) error {
		_, err := NewTopology(cfg)
		return err
	}
	wantReject := func(name string, cfg Config) {
		t.Helper()
		if err := run(cfg); err == nil {
			t.Fatalf("%s: NewTopology succeeded, want a validation error", name)
		} else if !strings.Contains(err.Error(), "seam") {
			t.Fatalf("%s: error must be the seam validation error, got: %v", name, err)
		}
	}

	// Negative counts / durations / rates / ratio are all rejected.
	neg := base()
	neg.ForwarderMaxOutstandingPolls = -1
	wantReject("ForwarderMaxOutstandingPolls=-1", neg)
	neg = base()
	neg.ForwarderMaxOutstandingTasks = -1
	wantReject("ForwarderMaxOutstandingTasks=-1", neg)
	neg = base()
	neg.AppendThreshold = -1
	wantReject("AppendThreshold=-1", neg)
	neg = base()
	neg.PollerScaleWaitMs = -1
	wantReject("PollerScaleWaitMs=-1", neg)
	neg = base()
	neg.PollerDecisionsPerSec = -1
	wantReject("PollerDecisionsPerSec=-1", neg)
	neg = base()
	neg.PersistenceMaxQPS = -1
	wantReject("PersistenceMaxQPS=-1", neg)
	neg = base()
	neg.PersistenceQPSBurstRatio = -1
	wantReject("PersistenceQPSBurstRatio=-1", neg)

	// NaN is rejected on the float-rate seam fields.
	for _, bad := range []struct {
		name  string
		apply func(cfg *Config)
	}{
		{"PollerDecisionsPerSec=NaN", func(c *Config) { c.PollerDecisionsPerSec = math.NaN() }},
		{"PersistenceMaxQPS=NaN", func(c *Config) { c.PersistenceMaxQPS = math.NaN() }},
		{"PersistenceQPSBurstRatio=NaN", func(c *Config) { c.PersistenceQPSBurstRatio = math.NaN() }},
	} {
		cfg := base()
		bad.apply(&cfg)
		wantReject(bad.name, cfg)
	}

	// ±Inf is rejected too (an infinite rate would break the future token
	// bucket's refill math just as surely as NaN).
	for _, bad := range []struct {
		name  string
		apply func(cfg *Config)
	}{
		{"PollerDecisionsPerSec=+Inf", func(c *Config) { c.PollerDecisionsPerSec = math.Inf(1) }},
		{"PollerDecisionsPerSec=-Inf", func(c *Config) { c.PollerDecisionsPerSec = math.Inf(-1) }},
		{"PersistenceMaxQPS=+Inf", func(c *Config) { c.PersistenceMaxQPS = math.Inf(1) }},
		{"PersistenceMaxQPS=-Inf", func(c *Config) { c.PersistenceMaxQPS = math.Inf(-1) }},
		{"PersistenceQPSBurstRatio=+Inf", func(c *Config) { c.PersistenceQPSBurstRatio = math.Inf(1) }},
		{"PersistenceQPSBurstRatio=-Inf", func(c *Config) { c.PersistenceQPSBurstRatio = math.Inf(-1) }},
	} {
		cfg := base()
		bad.apply(&cfg)
		wantReject(bad.name, cfg)
	}

	// Zero (all off) and positive values are accepted.
	if err := run(base()); err != nil {
		t.Fatalf("all-seams-zero config rejected: %v", err)
	}
	on := base()
	on.ForwarderMaxOutstandingPolls = 1
	on.ForwarderMaxOutstandingTasks = 1
	on.AppendThreshold = 250
	on.PollerScaleWaitMs = 1000
	on.PollerDecisionsPerSec = 10
	on.PersistenceMaxQPS = 2000
	on.PersistenceQPSBurstRatio = 1.0
	if err := run(on); err != nil {
		t.Fatalf("server-default seam values rejected (WP0: surface only): %v", err)
	}
}

// TestSeamZeroDefaultNeutralDigest is the WP0 byte-identity gate: with all
// seams at zero (DefaultConfig), the two canonical N5 digests stay
// byte-identical to the recorded pins — zero-draw sha256 a72689e9d1… and
// draws sha256 c7b0d97d14… (digests re-captured at the 2026-09-09
// traceability closeout, commit 8c45016). A WP that wires a seam must not
// change behavior at zero; if a digest legitimately moves (calibration), the
// pin is re-captured deliberately and recorded, never silently.
func TestSeamZeroDefaultNeutralDigest(t *testing.T) {
	// Zero-draw determinism path (TestDeterminismN5 scenario).
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
	// RNG-drawing storm path (rngConfig scenario).
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
		t.Fatalf("zero-draw digest moved with seams at zero:\nsha256 = %s\npin     = %s", got, pinZeroDraw)
	}
	if got := fmt.Sprintf("%x", sha256.Sum256([]byte(runDraws()))); got != pinDraws {
		t.Fatalf("draws digest moved with seams at zero:\nsha256 = %s\npin     = %s", got, pinDraws)
	}
}

// TestSeamZeroDefaultStableRepeats is the N=5 determinism gate on the
// all-seams-zero path: identical digest across repeats (the seam surface
// must not introduce any per-run variance).
func TestSeamZeroDefaultStableRepeats(t *testing.T) {
	cfg := seamZeroConfig()
	cfg.Seed = 7
	cfg.BurstCount = 20
	cfg.SimEndMs = 20000
	cfg.FaultStartMs = 1
	cfg.FaultEndMs = 5001
	var first string
	for i := 0; i < 5; i++ {
		top, err := NewTopology(cfg)
		if err != nil {
			t.Fatalf("NewTopology: %v", err)
		}
		res := top.Run()
		if i == 0 {
			first = res.Digest
		}
		if res.Digest != first {
			t.Fatalf("run %d: digest mismatch\n%q\n%q", i, res.Digest, first)
		}
	}
}
