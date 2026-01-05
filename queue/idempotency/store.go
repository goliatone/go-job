package idempotency

import (
	"context"
	"errors"
	"time"
)

// Status represents the lifecycle state of an idempotency record.
type Status string

const (
	StatusPending   Status = "pending"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
)

var (
	// ErrNotFound is returned when an idempotency record is missing or expired.
	ErrNotFound = errors.New("idempotency record not found")
)

// Record captures the stored idempotency metadata.
type Record struct {
	Key       string
	Status    Status
	Payload   []byte
	ExpiresAt time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Update describes partial changes to a record.
type Update struct {
	Status    *Status
	Payload   *[]byte
	ExpiresAt *time.Time
}

// Store provides shared idempotency operations.
type Store interface {
	Acquire(ctx context.Context, key string, ttl time.Duration) (Record, bool, error)
	Get(ctx context.Context, key string) (Record, bool, error)
	Update(ctx context.Context, key string, update Update) error
	Delete(ctx context.Context, key string) error
}

// DefaultStatus ensures a non-empty status value.
func DefaultStatus(status Status) Status {
	if status == "" {
		return StatusPending
	}
	return status
}

// IsExpired returns true when the record expiry is set and elapsed.
func IsExpired(record Record, now time.Time) bool {
	if record.ExpiresAt.IsZero() {
		return false
	}
	return !record.ExpiresAt.After(now)
}

func cloneBytes(value []byte) []byte {
	if len(value) == 0 {
		return nil
	}
	out := make([]byte, len(value))
	copy(out, value)
	return out
}
