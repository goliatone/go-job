package queue

import (
	"context"
	"time"

	job "github.com/goliatone/go-job"
)

// Receipt identifies a leased delivery for ack/nack operations.
type Receipt struct {
	ID       string
	Token    string
	Attempts int
	LeasedAt time.Time
}

// Storage is the backend contract for durable queue adapters.
type Storage interface {
	Enqueue(ctx context.Context, msg *job.ExecutionMessage) error
	Dequeue(ctx context.Context) (*job.ExecutionMessage, Receipt, error)
	Ack(ctx context.Context, receipt Receipt) error
	Nack(ctx context.Context, receipt Receipt, opts NackOptions) error
}
