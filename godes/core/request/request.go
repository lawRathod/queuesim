// Package request defines request/response models with lifecycle tracking
// for the GoDES simulation framework.
//
// Types mirror the DesCartes Rust request/response model for cross-validation
// parity. Requests track their creation time, attempts, completion status,
// and latency. Attempts record individual retry lifecycle with per-attempt
// status outcomes. Responses carry either success payloads or typed error
// information.
package request

import "github.com/lawRathod/queuesim/godes/core"

// RequestID is a unique identifier for a request.
type RequestID uint64

// RequestAttemptID is a unique identifier for a request attempt.
type RequestAttemptID uint64

// RequestStatus classifies the terminal state of a Request.
type RequestStatus int

const (
	// RequestStatusPending indicates the request has not yet completed.
	RequestStatusPending RequestStatus = iota
	// RequestStatusSuccess indicates the request completed successfully.
	RequestStatusSuccess
	// RequestStatusFailed indicates the request completed with a failure
	// but may still have remaining retries.
	RequestStatusFailed
	// RequestStatusExhausted indicates the request exhausted all retry
	// attempts without success.
	RequestStatusExhausted
)

// Request represents a logical operation that may be retried.
type Request struct {
	id          RequestID
	createdAt   core.SimTime
	payload     []byte
	attempts    []*RequestAttempt
	completedAt *core.SimTime
	status      RequestStatus
}

// NewRequest creates a new Request with the given id, creation time, and payload.
func NewRequest(id RequestID, createdAt core.SimTime, payload []byte) *Request {
	return &Request{
		id:        id,
		createdAt: createdAt,
		payload:   payload,
		status:    RequestStatusPending,
	}
}

// ID returns the unique identifier of this request.
func (r *Request) ID() RequestID { return r.id }

// CreatedAt returns the simulation time at which the request was created.
func (r *Request) CreatedAt() core.SimTime { return r.createdAt }

// Payload returns the request body.
func (r *Request) Payload() []byte { return r.payload }

// Status returns the current lifecycle status.
func (r *Request) Status() RequestStatus { return r.status }

// CompletedAt returns the completion time, or nil if not yet completed.
func (r *Request) CompletedAt() *core.SimTime { return r.completedAt }

// Attempts returns the list of attempts made for this request.
func (r *Request) Attempts() []*RequestAttempt { return r.attempts }

// AttemptCount returns the number of attempts made for this request.
func (r *Request) AttemptCount() int {
	return len(r.attempts)
}

// IsCompleted returns true if the request has reached a terminal status.
func (r *Request) IsCompleted() bool {
	return r.completedAt != nil
}

// IsSuccessful returns true if the request completed successfully.
func (r *Request) IsSuccessful() bool {
	return r.status == RequestStatusSuccess
}

// Latency returns the time from creation to completion, or (0, false)
// if not yet completed. Uses saturating arithmetic via SimTime.Sub().
func (r *Request) Latency() (core.SimTime, bool) {
	if r.completedAt == nil {
		return 0, false
	}
	return r.completedAt.Sub(r.createdAt.Int64()), true
}

// AddAttempt appends a request attempt to the request's attempt list.
func (r *Request) AddAttempt(attempt *RequestAttempt) {
	r.attempts = append(r.attempts, attempt)
}

// Complete marks the request as completed at the given simulation time
// with the given status. Panics if status is Pending (not terminal) or
// if Complete is called on an already-completed request.
func (r *Request) Complete(at core.SimTime, status RequestStatus) {
	if status == RequestStatusPending {
		panic("request: Complete with Pending status is invalid")
	}
	if r.completedAt != nil {
		panic("request: Complete called on already-completed request")
	}
	r.completedAt = &at
	r.status = status
}

// AttemptStatus classifies the outcome of a single request attempt.
type AttemptStatus int

const (
	// AttemptStatusPending indicates the attempt has not completed.
	AttemptStatusPending AttemptStatus = iota
	// AttemptStatusSuccess indicates the attempt completed successfully.
	AttemptStatusSuccess
	// AttemptStatusTimeout indicates the attempt timed out.
	AttemptStatusTimeout
	// AttemptStatusServerError indicates the attempt failed due to a server-side error.
	AttemptStatusServerError
	// AttemptStatusRejected indicates the attempt was rejected (e.g., rate limited).
	AttemptStatusRejected
)

// RequestAttempt records a single execution attempt for a request.
type RequestAttempt struct {
	id            RequestAttemptID
	requestID     RequestID
	attemptNumber int
	startedAt     core.SimTime
	completedAt   *core.SimTime
	status        AttemptStatus
	payload       []byte
}

// NewRequestAttempt creates a new RequestAttempt with the given parameters.
func NewRequestAttempt(id RequestAttemptID, requestID RequestID, attemptNumber int, startedAt core.SimTime, payload []byte) *RequestAttempt {
	return &RequestAttempt{
		id:            id,
		requestID:     requestID,
		attemptNumber: attemptNumber,
		startedAt:     startedAt,
		status:        AttemptStatusPending,
		payload:       payload,
	}
}

// ID returns the unique identifier of this attempt.
func (a *RequestAttempt) ID() RequestAttemptID { return a.id }

// RequestID returns the ID of the parent request.
func (a *RequestAttempt) RequestID() RequestID { return a.requestID }

// AttemptNumber returns the 1-based attempt index.
func (a *RequestAttempt) AttemptNumber() int { return a.attemptNumber }

// StartedAt returns the simulation time at which the attempt began.
func (a *RequestAttempt) StartedAt() core.SimTime { return a.startedAt }

// CompletedAt returns the completion time, or nil if not yet completed.
func (a *RequestAttempt) CompletedAt() *core.SimTime { return a.completedAt }

// Status returns the current lifecycle status.
func (a *RequestAttempt) Status() AttemptStatus { return a.status }

// Payload returns the attempt-level payload.
func (a *RequestAttempt) Payload() []byte { return a.payload }

// IsCompleted returns true if the attempt has reached a terminal status.
func (a *RequestAttempt) IsCompleted() bool {
	return a.completedAt != nil
}

// IsSuccessful returns true if the attempt completed successfully.
func (a *RequestAttempt) IsSuccessful() bool {
	return a.status == AttemptStatusSuccess
}

// Duration returns the time from start to completion, or (0, false)
// if not yet completed. Uses saturating arithmetic via SimTime.Sub().
func (a *RequestAttempt) Duration() (core.SimTime, bool) {
	if a.completedAt == nil {
		return 0, false
	}
	return a.completedAt.Sub(a.startedAt.Int64()), true
}

// Complete marks the attempt as completed at the given simulation time
// with the given status.
func (a *RequestAttempt) Complete(at core.SimTime, status AttemptStatus) {
	a.completedAt = &at
	a.status = status
}

// ResponseStatus classifies a response as success or error.
type ResponseStatus int

const (
	// ResponseStatusOk indicates a successful response.
	ResponseStatusOk ResponseStatus = iota
	// ResponseStatusError indicates an error response.
	ResponseStatusError
)

// Response carries the result of a completed request attempt.
type Response struct {
	attemptID   RequestAttemptID
	requestID   RequestID
	completedAt core.SimTime
	payload     []byte
	status      ResponseStatus
	errorCode   uint32
	errorMsg    string
}

// ResponseSuccess creates a successful Response with the given payload.
func ResponseSuccess(attemptID RequestAttemptID, requestID RequestID, at core.SimTime, payload []byte) Response {
	return Response{
		attemptID:   attemptID,
		requestID:   requestID,
		completedAt: at,
		payload:     payload,
		status:      ResponseStatusOk,
	}
}

// ResponseError creates an error Response with the given code and message.
func ResponseError(attemptID RequestAttemptID, requestID RequestID, at core.SimTime, code uint32, msg string) Response {
	return Response{
		attemptID:   attemptID,
		requestID:   requestID,
		completedAt: at,
		status:      ResponseStatusError,
		errorCode:   code,
		errorMsg:    msg,
	}
}

// AttemptID returns the identifier of the attempt that produced this response.
func (r *Response) AttemptID() RequestAttemptID { return r.attemptID }

// RequestID returns the identifier of the parent request.
func (r *Response) RequestID() RequestID { return r.requestID }

// CompletedAt returns the simulation time at which the response was produced.
func (r *Response) CompletedAt() core.SimTime { return r.completedAt }

// Payload returns the response body (only set for success responses).
func (r *Response) Payload() []byte { return r.payload }

// Status indicates success or error.
func (r *Response) Status() ResponseStatus { return r.status }

// ErrorCode is a numeric error code (only set for error responses).
func (r *Response) ErrorCode() uint32 { return r.errorCode }

// ErrorMsg is a human-readable error message (only set for error responses).
func (r *Response) ErrorMsg() string { return r.errorMsg }

// IsSuccess returns true if the response indicates success.
func (r *Response) IsSuccess() bool {
	return r.status == ResponseStatusOk
}

// IsError returns true if the response indicates an error.
func (r *Response) IsError() bool {
	return r.status == ResponseStatusError
}
