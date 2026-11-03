package request

import (
	"fmt"
	"testing"

	"github.com/lawRathod/queuesim/godes/core"
	"github.com/lawRathod/queuesim/godes/simtest"
)

func TestRequestCreation(t *testing.T) {
	id := RequestID(1)
	createdAt := core.SimTime(100)
	r := NewRequest(id, createdAt, []byte{1, 2, 3})
	if r.ID() != id {
		t.Fatalf("expected id %v, got %v", id, r.ID())
	}
	if r.AttemptCount() != 0 {
		t.Fatalf("expected 0 attempts")
	}
	if r.IsCompleted() {
		t.Fatal("expected not completed")
	}
	if r.Status() != RequestStatusPending {
		t.Fatal("expected pending status")
	}
}

func TestRequestLatency(t *testing.T) {
	r := NewRequest(RequestID(1), core.SimTime(100), []byte{})
	if _, ok := r.Latency(); ok {
		t.Fatal("expected no latency before completion")
	}
	r.Complete(core.SimTime(250), RequestStatusSuccess)
	lat, ok := r.Latency()
	if !ok {
		t.Fatal("expected latency after completion")
	}
	if lat != core.SimTime(150) {
		t.Fatalf("expected latency 150, got %v", lat)
	}
}

func TestRequestAttempts(t *testing.T) {
	r := NewRequest(RequestID(1), core.SimTime(100), []byte{})
	a1 := NewRequestAttempt(RequestAttemptID(1), RequestID(1), 1, core.SimTime(100), []byte{})
	a2 := NewRequestAttempt(RequestAttemptID(2), RequestID(1), 2, core.SimTime(110), []byte{})
	r.AddAttempt(a1)
	r.AddAttempt(a2)
	if r.AttemptCount() != 2 {
		t.Fatalf("expected 2 attempts, got %d", r.AttemptCount())
	}
	if len(r.Attempts()) != 2 {
		t.Fatalf("expected 2 attempts from getter, got %d", len(r.Attempts()))
	}
}

func TestAttemptCreation(t *testing.T) {
	a := NewRequestAttempt(RequestAttemptID(1), RequestID(1), 1, core.SimTime(100), []byte{1, 2, 3})
	if a.IsCompleted() {
		t.Fatal("expected not completed")
	}
	a.Complete(core.SimTime(150), AttemptStatusSuccess)
	if !a.IsCompleted() {
		t.Fatal("expected completed")
	}
	if !a.IsSuccessful() {
		t.Fatal("expected success")
	}
}

func TestAttemptStatusVariants(t *testing.T) {
	a := NewRequestAttempt(RequestAttemptID(1), RequestID(1), 1, core.SimTime(100), []byte{})
	a.Complete(core.SimTime(150), AttemptStatusTimeout)
	if a.IsSuccessful() {
		t.Fatal("timeout should not be success")
	}

	a2 := NewRequestAttempt(RequestAttemptID(2), RequestID(1), 1, core.SimTime(100), []byte{})
	a2.Complete(core.SimTime(150), AttemptStatusServerError)
	if a2.IsSuccessful() {
		t.Fatal("server error should not be success")
	}

	a3 := NewRequestAttempt(RequestAttemptID(3), RequestID(1), 1, core.SimTime(100), []byte{})
	a3.Complete(core.SimTime(150), AttemptStatusRejected)
	if a3.IsSuccessful() {
		t.Fatal("rejected should not be success")
	}
}

func TestAttemptDuration(t *testing.T) {
	a := NewRequestAttempt(RequestAttemptID(1), RequestID(1), 1, core.SimTime(100), []byte{})
	if _, ok := a.Duration(); ok {
		t.Fatal("expected no duration before completion")
	}
	a.Complete(core.SimTime(250), AttemptStatusSuccess)
	dur, ok := a.Duration()
	if !ok {
		t.Fatal("expected duration after completion")
	}
	if dur != core.SimTime(150) {
		t.Fatalf("expected duration 150, got %v", dur)
	}
}

func TestResponseSuccess(t *testing.T) {
	resp := ResponseSuccess(RequestAttemptID(1), RequestID(1), core.SimTime(150), []byte{4, 5, 6})
	if !resp.IsSuccess() {
		t.Fatal("expected success")
	}
	if resp.IsError() {
		t.Fatal("expected not error")
	}
	if resp.AttemptID() != RequestAttemptID(1) {
		t.Fatal("expected attempt id 1")
	}
	if resp.RequestID() != RequestID(1) {
		t.Fatal("expected request id 1")
	}
	if resp.CompletedAt() != core.SimTime(150) {
		t.Fatal("expected completed at 150")
	}
}

func TestResponseError(t *testing.T) {
	resp := ResponseError(RequestAttemptID(1), RequestID(1), core.SimTime(150), 500, "Internal Server Error")
	if !resp.IsError() {
		t.Fatal("expected error")
	}
	if resp.IsSuccess() {
		t.Fatal("expected not success")
	}
	if resp.ErrorCode() != 500 {
		t.Fatalf("expected error code 500, got %d", resp.ErrorCode())
	}
	if resp.ErrorMsg() != "Internal Server Error" {
		t.Fatalf("expected matching error msg, got %q", resp.ErrorMsg())
	}
}

func TestRequestStatusExhausted(t *testing.T) {
	r := NewRequest(RequestID(1), core.SimTime(0), []byte{})
	r.Complete(core.SimTime(10), RequestStatusExhausted)
	if r.IsSuccessful() {
		t.Fatal("exhausted should not be success")
	}
	if !r.IsCompleted() {
		t.Fatal("exhausted request should be completed")
	}
}

func TestRequestZeroLatency(t *testing.T) {
	r := NewRequest(RequestID(1), core.SimTime(100), []byte{})
	r.Complete(core.SimTime(100), RequestStatusSuccess)
	lat, ok := r.Latency()
	if !ok {
		t.Fatal("expected latency ok")
	}
	if lat != 0 {
		t.Fatalf("expected zero latency, got %v", lat)
	}
}

func TestRequestDeterminism(t *testing.T) {
	simtest.AssertDeterministic(t, 5, 42, func(seed int64) string {
		var trace []string
		r := NewRequest(RequestID(seed), core.SimTime(seed), []byte("payload"))
		trace = append(trace, fmt.Sprintf("created:%d", r.ID()))

		a := NewRequestAttempt(RequestAttemptID(seed), r.ID(), 1, core.SimTime(seed), []byte("attempt"))
		r.AddAttempt(a)
		trace = append(trace, fmt.Sprintf("attempts:%d", r.AttemptCount()))

		r.Complete(core.SimTime(seed+10), RequestStatusSuccess)
		lat, ok := r.Latency()
		trace = append(trace, fmt.Sprintf("latency:%v:%v", ok, lat))
		trace = append(trace, fmt.Sprintf("success:%v", r.IsSuccessful()))

		return simtest.HashTrace(trace)
	})
}

func TestRequestCompleteGuardPending(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for Complete with Pending status")
		}
	}()
	r := NewRequest(RequestID(1), core.SimTime(0), []byte{})
	r.Complete(core.SimTime(10), RequestStatusPending)
}

func TestRequestCompleteGuardDoubleComplete(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for double Complete")
		}
	}()
	r := NewRequest(RequestID(1), core.SimTime(0), []byte{})
	r.Complete(core.SimTime(10), RequestStatusSuccess)
	r.Complete(core.SimTime(20), RequestStatusFailed)
}
