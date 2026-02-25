package worker

import (
	"errors"
	"testing"

	job "github.com/goliatone/go-job"
	"github.com/stretchr/testify/assert"
)

func TestDefaultRetryPolicyDeadLettersTerminalErrors(t *testing.T) {
	policy := DefaultRetryPolicy{
		MaxAttempts: 5,
	}

	err := job.NewTerminalError(job.TerminalErrorCodeStaleStateMismatch, "stale execution", errors.New("version mismatch"))
	opts := policy.Decide(1, err)

	assert.True(t, opts.DeadLetter)
	assert.False(t, opts.Requeue)
	assert.Equal(t, "stale execution", opts.Reason)
}
