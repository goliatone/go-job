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

// EnqueueReceipt contains queue acceptance metadata for a dispatched message.
type EnqueueReceipt struct {
	DispatchID string
	EnqueuedAt time.Time
}

// ReceiptEnqueuer accepts execution messages and returns acceptance metadata.
type ReceiptEnqueuer interface {
	EnqueueWithReceipt(ctx context.Context, msg *job.ExecutionMessage) (EnqueueReceipt, error)
}

// ScheduledEnqueuer supports delayed/scheduled enqueue semantics.
type ScheduledEnqueuer interface {
	Enqueuer
	EnqueueAt(ctx context.Context, msg *job.ExecutionMessage, at time.Time) error
	EnqueueAfter(ctx context.Context, msg *job.ExecutionMessage, delay time.Duration) error
}

// ReceiptScheduledEnqueuer supports delayed/scheduled enqueue semantics with receipts.
type ReceiptScheduledEnqueuer interface {
	ReceiptEnqueuer
	EnqueueAtWithReceipt(ctx context.Context, msg *job.ExecutionMessage, at time.Time) (EnqueueReceipt, error)
	EnqueueAfterWithReceipt(ctx context.Context, msg *job.ExecutionMessage, delay time.Duration) (EnqueueReceipt, error)
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

// DispatchState defines queue dispatch lifecycle states exposed by status readers.
type DispatchState string

const (
	DispatchStateAccepted   DispatchState = "accepted"
	DispatchStateRunning    DispatchState = "running"
	DispatchStateRetrying   DispatchState = "retrying"
	DispatchStateDeadLetter DispatchState = "dead_letter"
	DispatchStateSucceeded  DispatchState = "succeeded"
	DispatchStateUnknown    DispatchState = "unknown"
)

// DispatchStatus represents the current inferred queue lifecycle state.
type DispatchStatus struct {
	DispatchID     string
	State          DispatchState
	Attempt        int
	EnqueuedAt     *time.Time
	UpdatedAt      *time.Time
	NextRunAt      *time.Time
	TerminalReason string
	Inferred       bool
}

// DispatchStatusReader resolves queue lifecycle state by dispatch id.
type DispatchStatusReader interface {
	GetDispatchStatus(ctx context.Context, dispatchID string) (DispatchStatus, error)
}
