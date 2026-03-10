package worker

import (
	"errors"
	"testing"
	"time"

	job "github.com/goliatone/go-job"
	"github.com/goliatone/go-job/queue"
	"github.com/stretchr/testify/assert"
)

func TestDefaultRetryPolicyDeadLettersTerminalErrors(t *testing.T) {
	policy := DefaultRetryPolicy{
		MaxAttempts: 5,
	}

	err := job.NewTerminalError(job.TerminalErrorCodeStaleStateMismatch, "stale execution", errors.New("version mismatch"))
	opts := policy.Decide(1, err)

	assert.Equal(t, queue.NackDispositionDeadLetter, opts.Disposition)
	assert.Equal(t, "stale execution", opts.Reason)
}

func TestComputeBackoffDelayAppliesJitter(t *testing.T) {
	prev := jitterRandFloat64
	t.Cleanup(func() {
		jitterRandFloat64 = prev
	})

	jitterRandFloat64 = func() float64 { return 1.0 } // 1.5x
	delay := computeBackoffDelay(1, BackoffConfig{
		Strategy: BackoffFixed,
		Interval: 100 * time.Millisecond,
		Jitter:   true,
	})
	assert.Equal(t, 150*time.Millisecond, delay)

	jitterRandFloat64 = func() float64 { return 0.0 } // 0.5x
	delay = computeBackoffDelay(1, BackoffConfig{
		Strategy: BackoffFixed,
		Interval: 100 * time.Millisecond,
		Jitter:   true,
	})
	assert.Equal(t, 50*time.Millisecond, delay)
}
