package worker

import (
	"time"

	"github.com/goliatone/go-job/queue"
)

// RetryPolicy decides how the worker should nack a failed delivery.
type RetryPolicy interface {
	Decide(attempt int, err error) queue.NackOptions
}

// BackoffStrategy controls the retry delay profile.
type BackoffStrategy string

const (
	BackoffNone        BackoffStrategy = "none"
	BackoffFixed       BackoffStrategy = "fixed"
	BackoffExponential BackoffStrategy = "exponential"
)

// BackoffConfig configures retry delays.
type BackoffConfig struct {
	Strategy    BackoffStrategy
	Interval    time.Duration
	MaxInterval time.Duration
	Jitter      bool
}

const (
	defaultBackoffInterval    = 100 * time.Millisecond
	defaultBackoffMaxInterval = 5 * time.Second
)

// DefaultRetryPolicy provides simple max-attempts with backoff scheduling.
// MaxAttempts <= 0 means no retries (dead-letter on first failure).
type DefaultRetryPolicy struct {
	MaxAttempts int
	Backoff     BackoffConfig
}

// Decide returns nack options based on attempts and policy.
func (p DefaultRetryPolicy) Decide(attempt int, err error) queue.NackOptions {
	if attempt <= 0 {
		attempt = 1
	}
	maxAttempts := p.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	reason := ""
	if err != nil {
		reason = err.Error()
	}

	if attempt >= maxAttempts {
		return queue.NackOptions{
			DeadLetter: true,
			Reason:     reason,
		}
	}

	delay := computeBackoffDelay(attempt, p.Backoff)
	return queue.NackOptions{
		Delay:   delay,
		Requeue: true,
		Reason:  reason,
	}
}

func computeBackoffDelay(attempt int, cfg BackoffConfig) time.Duration {
	if attempt <= 0 {
		return 0
	}

	interval := cfg.Interval
	if interval <= 0 {
		interval = defaultBackoffInterval
	}

	maxInterval := cfg.MaxInterval
	if maxInterval <= 0 {
		maxInterval = defaultBackoffMaxInterval
	}

	switch cfg.Strategy {
	case BackoffFixed:
		return interval
	case BackoffExponential:
		delay := interval
		for i := 1; i < attempt; i++ {
			delay *= 2
			if delay > maxInterval {
				return maxInterval
			}
		}
		return delay
	default:
		return 0
	}
}
