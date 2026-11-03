package core

import "testing"

func TestTaskHandleIDIsAccessible(t *testing.T) {
	h := TaskHandle{id: 42}
	if got := h.ID(); got != 42 {
		t.Fatalf("expected TaskID 42, got %d", got)
	}
}

func TestZeroTaskHandleIsInvalidByConvention(t *testing.T) {
	var h TaskHandle
	if h.ID() != 0 {
		t.Fatalf("zero TaskHandle should report TaskID 0, got %d", h.ID())
	}
}
