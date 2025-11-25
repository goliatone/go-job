package job

import (
	"context"
	"math/rand"
	"time"
)

// TestSetBackoffSleep replaces the sleeper used during retries. Returns a restore func.
func TestSetBackoffSleep(sleeper func(context.Context, time.Duration) error) func() {
	old := backoffSleep
	if sleeper != nil {
		backoffSleep = sleeper
	}
	return func() { backoffSleep = old }
}

// TestSetBackoffRand replaces the jitter random source. Returns a restore func.
func TestSetBackoffRand(r *rand.Rand) func() {
	old := backoffRand
	if r != nil {
		backoffRand = r
	}
	return func() { backoffRand = old }
}

// TestComputeBackoffDelay exposes backoff calculation for tests.
func TestComputeBackoffDelay(attempt int, cfg BackoffConfig) time.Duration {
	return computeBackoffDelay(attempt, cfg)
}
