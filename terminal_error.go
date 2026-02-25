package job

import "fmt"

// TerminalErrorCode identifies non-retryable error classes.
type TerminalErrorCode string

const (
	// TerminalErrorCodeStaleStateMismatch marks stale/state-mismatch execution paths.
	TerminalErrorCodeStaleStateMismatch TerminalErrorCode = "stale_state_mismatch"
)

// NonRetryableError is implemented by errors that should not be retried by worker policy.
type NonRetryableError interface {
	error
	NonRetryable() bool
	NonRetryableReason() string
}

// TerminalError represents a non-retryable execution failure.
type TerminalError struct {
	Code   TerminalErrorCode
	Reason string
	Err    error
}

// NewTerminalError constructs a non-retryable error marker.
func NewTerminalError(code TerminalErrorCode, reason string, err error) error {
	return &TerminalError{
		Code:   code,
		Reason: reason,
		Err:    err,
	}
}

func (e *TerminalError) Error() string {
	if e == nil {
		return ""
	}
	if e.Reason != "" {
		return e.Reason
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	if e.Code != "" {
		return string(e.Code)
	}
	return "terminal error"
}

func (e *TerminalError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (e *TerminalError) NonRetryable() bool {
	return true
}

func (e *TerminalError) NonRetryableReason() string {
	if e == nil {
		return ""
	}
	if e.Reason != "" {
		return e.Reason
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	if e.Code != "" {
		return fmt.Sprintf("terminal:%s", e.Code)
	}
	return "terminal error"
}
