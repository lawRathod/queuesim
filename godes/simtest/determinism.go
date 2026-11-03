package simtest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"reflect"
	"testing"
)

// AssertDeterministic Runs a given function `n` times with the same seed,
// capturing the output and ensuring that every execution yields exactly the same result.
func AssertDeterministic[T any](t *testing.T, n int, seed int64, fn func(seed int64) T) {
	t.Helper()

	if n < 2 {
		t.Fatalf("n must be at least 2 for determinism testing")
	}

	var firstOutput T
	for i := 0; i < n; i++ {
		out := fn(seed)
		if i == 0 {
			firstOutput = out
		} else {
			if !reflect.DeepEqual(firstOutput, out) {
				t.Fatalf("Non-deterministic execution detected at run %d with seed %d.\nExpected: %v\nGot: %v", i+1, seed, firstOutput, out)
			}
		}
	}
}

// HashTrace takes a slice of any type (usually string or struct events)
// and returns a SHA256 hex string of its formatted representation.
func HashTrace[T any](events []T) string {
	h := sha256.New()
	for _, e := range events {
		// Use a simple fmt string representation for hashing.
		// For complex structs, this ensures a consistent stringified form.
		h.Write([]byte(fmt.Sprintf("%+v\n", e)))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// AssertTraceHash compares the trace of events against a known golden hash.
func AssertTraceHash[T any](t *testing.T, events []T, expectedHash string) {
	t.Helper()
	actualHash := HashTrace(events)
	if actualHash != expectedHash {
		t.Fatalf("Trace hash mismatch.\nExpected: %s\nActual: %s", expectedHash, actualHash)
	}
}
