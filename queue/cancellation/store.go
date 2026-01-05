package cancellation

import (
	"context"
	"time"
)

// Request represents a distributed cancellation signal.
type Request struct {
	Key         string
	Reason      string
	RequestedAt time.Time
}

// Store publishes and retrieves cancellation requests.
type Store interface {
	Request(ctx context.Context, req Request) error
	Get(ctx context.Context, key string) (Request, bool, error)
	Subscribe(ctx context.Context) (<-chan Request, error)
}
