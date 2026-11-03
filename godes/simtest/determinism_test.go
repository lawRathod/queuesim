package simtest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"testing"
)

func TestAssertDeterministic(t *testing.T) {
	// A simple deterministic function
	deterministicFn := func(seed int64) string {
		return fmt.Sprintf("seed_%d_output", seed)
	}
	AssertDeterministic(t, 5, 42, deterministicFn)
}

func TestAssertTraceHash(t *testing.T) {
	events := []string{
		"event1",
		"event2",
	}

	// Compute manually to compare
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%+v\n", "event1")))
	h.Write([]byte(fmt.Sprintf("%+v\n", "event2")))
	expected := hex.EncodeToString(h.Sum(nil))

	AssertTraceHash(t, events, expected)
}

func FuzzTraceHash(f *testing.F) {
	f.Add("initial_event")
	f.Fuzz(func(t *testing.T, event string) {
		events := []string{event}
		hash := HashTrace(events)
		if len(hash) != 64 {
			t.Errorf("expected hash length 64, got %d", len(hash))
		}
	})
}
