package seed

import (
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// resetLoader clears the once-read cache so a test can drive os.Getenv itself.
// Not parallel-safe: every test that calls it resets and re-reads in one
// goroutine.
func resetLoader() {
	loader.once = sync.Once{}
	loader.seed, loader.set = 0, false
}

// TestResolveIdentityWithEnvUnset is gate (i) of ADR-122: with no global seed,
// Resolve is the identity for every non-zero config seed, so no leg's seed
// moves. No run is needed — the whole claim is that the resolved seed equals
// the declared one.
func TestResolveIdentityWithEnvUnset(t *testing.T) {
	for _, v := range []string{"", "0"} {
		t.Setenv(Env, v)
		resetLoader()
		for _, x := range []int64{1, 7, 42, 1234, 20260910, -5, 1<<62 + 3} {
			if got := Resolve(x); got != x {
				t.Errorf("%s=%q: Resolve(%d) = %d, want %d (identity gate)", Env, v, x, got, x)
			}
		}
		if got := Resolve(0); got != DefaultRunSeed {
			t.Errorf("%s=%q: Resolve(0) = %d, want the documented default %d", Env, v, got, DefaultRunSeed)
		}
	}
}

// TestGlobalTreatsEmptyAndZeroAsUnset pins the runtime-layer equivalence:
// dstSetSeed(0) leaves the seam off, so "" and "0" are both "no seed given"
// and neither may silently zero a leg's config seed.
func TestGlobalTreatsEmptyAndZeroAsUnset(t *testing.T) {
	for _, v := range []string{"", "0"} {
		t.Setenv(Env, v)
		resetLoader()
		got, ok := Global()
		if ok {
			t.Errorf("%s=%q: Global() = (%d, true), want unset", Env, v, got)
		}
	}
	t.Setenv(Env, "9")
	resetLoader()
	if got, ok := Global(); !ok || got != 9 {
		t.Fatalf("%s=9: Global() = (%d, %v), want (9, true)", Env, got, ok)
	}
}

// TestGlobalReadsEnvOnce pins the sync.Once contract (ADR-122 [C3]): the value
// is fixed at first use, so a later environment change cannot move a config
// seed away from the runtime's schedinit copy.
func TestGlobalReadsEnvOnce(t *testing.T) {
	t.Setenv(Env, "11")
	resetLoader()
	if got, ok := Global(); !ok || got != 11 {
		t.Fatalf("first Global() = (%d, %v), want (11, true)", got, ok)
	}
	t.Setenv(Env, "22")
	if got, ok := Global(); !ok || got != 11 {
		t.Fatalf("second Global() after Setenv = (%d, %v), want the once-read (11, true)", got, ok)
	}
}

// TestResolveEnvOverridesExplicit pins the precedence direction: the
// environment wins, because main cannot change the runtime's copy.
func TestResolveEnvOverridesExplicit(t *testing.T) {
	t.Setenv(Env, "7")
	resetLoader()
	if got := Resolve(42); got != 7 {
		t.Errorf("Resolve(42) with %s=7 = %d, want 7", Env, got)
	}
	if got := Resolve(0); got != 7 {
		t.Errorf("Resolve(0) with %s=7 = %d, want 7", Env, got)
	}
}

// TestDefaultNormalizesUnseededPath pins the one documented unseeded default
// every scenario config loader now routes through (ADR-122 T2).
func TestDefaultNormalizesUnseededPath(t *testing.T) {
	t.Setenv(Env, "")
	resetLoader()
	if got := Default(0); got != DefaultRunSeed {
		t.Errorf("Default(0) = %d, want %d", got, DefaultRunSeed)
	}
	for _, x := range []int64{1, 7, 42, 20260910, -5} {
		if got := Default(x); got != x {
			t.Errorf("Default(%d) = %d, want unchanged", x, got)
		}
	}
	// Default is a config-loader concern and must NOT consult the environment:
	// resolution happens at the run boundary, not at load time.
	t.Setenv(Env, "7")
	resetLoader()
	if got := Default(0); got != DefaultRunSeed {
		t.Errorf("Default(0) with %s=7 = %d, want %d (load-time default is env-free)", Env, got, DefaultRunSeed)
	}
}

// TestParseRejectsMalformed pins the loud-failure contract: a malformed
// DSTSEED panics rather than silently disabling the seam (the overlay's
// throw), and a non-decimal-digit literal is malformed even when Go's own
// parser would tolerate it.
func TestParseRejectsMalformed(t *testing.T) {
	for _, s := range []string{"+7", "-7", " 7", "7 ", "7.0", "0x7", "1_0", "07x", "seven", "7a"} {
		t.Run(s, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("parse(%q) did not panic — a malformed %s must fail loudly", s, Env)
				}
			}()
			parse(s)
		})
	}
}

// TestParseRejectsOutOfRange: the overlay wraps silently on overflow; GoDES
// panics, because a silently wrapped seed is a seed the run does not know it
// used.
func TestParseRejectsOutOfRange(t *testing.T) {
	for _, s := range []string{"9223372036854775808", "18446744073709551615", "18446744073709551616"} {
		t.Run(s, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Errorf("parse(%q) did not panic — an out-of-range %s must fail loudly", s, Env)
				}
			}()
			parse(s)
		})
	}
	if got, ok := parse("9223372036854775807"); !ok || got != 9223372036854775807 {
		t.Errorf("parse(MaxInt64) = (%d, %v), want (MaxInt64, true)", got, ok)
	}
}

// TestGuardDeclaredDisagreementOnly pins T6's guard semantics: it errors only
// when a global seed is set AND disagrees, so the CAL anchors (which legitimately
// agree with DSTSEED=42) and experiment.sh's every compare-arm run still work.
func TestGuardDeclaredDisagreementOnly(t *testing.T) {
	cases := []struct {
		env      string
		declared int64
		wantErr  bool
	}{
		{"", 42, false},       // no global seed: every declared set runs
		{"", 20260910, false}, // ditto
		{"42", 42, false},     // the CAL anchor agrees — must NOT be blocked
		{"0", 20260910, false},
		{"7", 7, false},
		{"7", 0, false}, // no declared seed yet: nothing to disagree with
		{"7", 13, true},
		{"42", 20260910, true},
	}
	for _, tc := range cases {
		t.Setenv(Env, tc.env)
		resetLoader()
		err := GuardDeclared(tc.declared, "test seed")
		if tc.wantErr != (err != nil) {
			t.Errorf("%s=%q declared=%d: err=%v, wantErr=%v", Env, tc.env, tc.declared, err, tc.wantErr)
			continue
		}
		if tc.wantErr {
			if !errors.Is(err, ErrDeclaredMismatch) {
				t.Errorf("%s=%q declared=%d: error does not wrap ErrDeclaredMismatch: %v", Env, tc.env, tc.declared, err)
			}
			// Both values and the remedy must be named.
			msg := err.Error()
			for _, want := range []string{tc.env, strconv.FormatInt(tc.declared, 10), Env} {
				if !strings.Contains(msg, want) {
					t.Errorf("%s=%q declared=%d: error %q does not name %q", Env, tc.env, tc.declared, msg, want)
				}
			}
		}
	}
}
