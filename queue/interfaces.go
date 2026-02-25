package queue

import (
	"context"
	"fmt"
	"time"

	job "github.com/goliatone/go-job"
)

var (
	// ErrScheduledEnqueueUnsupported is returned when an adapter cannot schedule messages.
	ErrScheduledEnqueueUnsupported = fmt.Errorf("scheduled enqueue not supported")
	// ErrLeaseExtensionUnsupported is returned when an adapter cannot extend a lease.
	ErrLeaseExtensionUnsupported = fmt.Errorf("lease extension not supported")
)

// Enqueuer accepts execution messages for durable delivery.
type Enqueuer interface {
	Enqueue(ctx context.Context, msg *job.ExecutionMessage) error
}

// ScheduledEnqueuer supports delayed/scheduled enqueue semantics.
type ScheduledEnqueuer interface {
	Enqueuer
	EnqueueAt(ctx context.Context, msg *job.ExecutionMessage, at time.Time) error
	EnqueueAfter(ctx context.Context, msg *job.ExecutionMessage, delay time.Duration) error
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

// LeaseExtender allows long-running handlers to renew queue leases.
type LeaseExtender interface {
	ExtendLease(ctx context.Context, ttl time.Duration) error
}

// NackOptions controls retry and DLQ behavior when nacking a message.
type NackOptions struct {
	Delay      time.Duration
	Requeue    bool
	DeadLetter bool
	Reason     string
}
