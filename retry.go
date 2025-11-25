package job

import (
	"context"
	"math/rand"
	"time"
)

type BackoffStrategy string

const (
	BackoffNone        BackoffStrategy = "none"
	BackoffFixed       BackoffStrategy = "fixed"
	BackoffExponential BackoffStrategy = "exponential"
)

// BackoffConfig configures retry timing.
type BackoffConfig struct {
	Strategy    BackoffStrategy `json:"strategy" yaml:"strategy"`
	Interval    time.Duration   `json:"interval" yaml:"interval"`
	MaxInterval time.Duration   `json:"max_interval" yaml:"max_interval"`
	Jitter      bool            `json:"jitter" yaml:"jitter"`
}

const (
	defaultBackoffInterval    = 100 * time.Millisecond
	defaultBackoffMaxInterval = 5 * time.Second
)

var (
	backoffSleep = sleepWithContext
	backoffRand  = rand.New(rand.NewSource(time.Now().UnixNano()))
)

var (
	defaultConcurrencyLimiter = NewConcurrencyLimiter()
)

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
		return applyJitter(interval, cfg.Jitter)
	case BackoffExponential:
		delay := interval
		for i := 1; i < attempt; i++ {
			delay *= 2
			if delay > maxInterval {
				delay = maxInterval
				break
			}
		}
		return applyJitter(delay, cfg.Jitter)
	default:
		return 0
	}
}

func applyJitter(delay time.Duration, jitter bool) time.Duration {
	if !jitter || delay <= 0 {
		return delay
	}

	// +/-50% jitter
	half := float64(delay) * 0.5
	offset := (backoffRand.Float64()*2 - 1) * half
	jittered := float64(delay) + offset
	if jittered < 0 {
		return 0
	}
	return time.Duration(jittered)
}

func sleepWithContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
