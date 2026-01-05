package queue

import (
	"context"
	"time"

	job "github.com/goliatone/go-job"
)

// Enqueuer accepts execution messages for durable delivery.
type Enqueuer interface {
	Enqueue(ctx context.Context, msg *job.ExecutionMessage) error
}

// Dequeuer returns the next delivery when available.
type Dequeuer interface {
	Dequeue(ctx context.Context) (Delivery, error)
}

// Delivery represents a single queue message and its ack/nack handlers.
type Delivery interface {
	Message() *job.ExecutionMessage
	Ack(ctx context.Context) error
	Nack(ctx context.Context, opts NackOptions) error
}

// NackOptions controls retry and DLQ behavior when nacking a message.
type NackOptions struct {
	Delay      time.Duration
	Requeue    bool
	DeadLetter bool
	Reason     string
}
