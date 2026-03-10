package worker

import (
	"errors"
	"math/rand"
	"time"

	job "github.com/goliatone/go-job"
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

var jitterRandFloat64 = rand.Float64

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
	var terminal job.NonRetryableError
	if errors.As(err, &terminal) && terminal.NonRetryable() {
		reason = terminal.NonRetryableReason()
		return queue.NackOptions{
			Disposition: queue.NackDispositionDeadLetter,
			Reason:      reason,
		}
	}

	if attempt >= maxAttempts {
		return queue.NackOptions{
			Disposition: queue.NackDispositionDeadLetter,
			Reason:      reason,
		}
	}

	delay := computeBackoffDelay(attempt, p.Backoff)
	return queue.NackOptions{
		Disposition: queue.NackDispositionRetry,
		Delay:       delay,
		Reason:      reason,
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
		delay := interval
		if cfg.Jitter {
			delay = applyJitter(delay)
		}
		return delay
	case BackoffExponential:
		delay := interval
		for i := 1; i < attempt; i++ {
			delay *= 2
			if delay > maxInterval {
				delay = maxInterval
				break
			}
		}
		if cfg.Jitter {
			delay = applyJitter(delay)
		}
		return delay
	default:
		return 0
	}
}

func applyJitter(delay time.Duration) time.Duration {
	if delay <= 0 {
		return 0
	}
	// Symmetric jitter in [0.5x, 1.5x] of base delay.
	factor := 0.5 + jitterRandFloat64()
	out := time.Duration(float64(delay) * factor)
	if out <= 0 {
		return time.Nanosecond
	}
	return out
}
