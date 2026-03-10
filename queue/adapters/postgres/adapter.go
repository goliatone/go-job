package postgres

import (
	"context"
	"fmt"
	"time"

	job "github.com/goliatone/go-job"
	"github.com/goliatone/go-job/queue"
)

// Adapter bridges the storage implementation to queue interfaces.
type Adapter struct {
	storage queue.Storage
}

// NewAdapter builds a queue adapter from a storage implementation.
func NewAdapter(storage queue.Storage) *Adapter {
	return &Adapter{storage: storage}
}

// Enqueue forwards to the underlying storage.
func (a *Adapter) Enqueue(ctx context.Context, msg *job.ExecutionMessage) (queue.EnqueueReceipt, error) {
	if a == nil || a.storage == nil {
		return queue.EnqueueReceipt{}, fmt.Errorf("queue adapter not configured")
	}
	return a.storage.Enqueue(ctx, msg)
}

// EnqueueAt forwards scheduled enqueue requests to storage when supported.
func (a *Adapter) EnqueueAt(ctx context.Context, msg *job.ExecutionMessage, at time.Time) (queue.EnqueueReceipt, error) {
	if a == nil || a.storage == nil {
		return queue.EnqueueReceipt{}, fmt.Errorf("queue adapter not configured")
	}
	scheduler, ok := a.storage.(queue.ScheduledStorage)
	if !ok {
		return queue.EnqueueReceipt{}, queue.ErrScheduledEnqueueUnsupported
	}
	return scheduler.EnqueueAt(ctx, msg, at)
}

// EnqueueAfter forwards delayed enqueue requests to storage when supported.
func (a *Adapter) EnqueueAfter(ctx context.Context, msg *job.ExecutionMessage, delay time.Duration) (queue.EnqueueReceipt, error) {
	if a == nil || a.storage == nil {
		return queue.EnqueueReceipt{}, fmt.Errorf("queue adapter not configured")
	}
	scheduler, ok := a.storage.(queue.ScheduledStorage)
	if !ok {
		return queue.EnqueueReceipt{}, queue.ErrScheduledEnqueueUnsupported
	}
	return scheduler.EnqueueAfter(ctx, msg, delay)
}

// Dequeue returns a delivery wrapper when available.
func (a *Adapter) Dequeue(ctx context.Context) (queue.Delivery, error) {
	if a == nil || a.storage == nil {
		return nil, fmt.Errorf("queue adapter not configured")
	}
	msg, receipt, err := a.storage.Dequeue(ctx)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, nil
	}
	return &delivery{storage: a.storage, msg: msg, receipt: receipt}, nil
}

type delivery struct {
	storage queue.Storage
	msg     *job.ExecutionMessage
	receipt queue.Receipt
}

func (d *delivery) Message() *job.ExecutionMessage {
	return d.msg
}

func (d *delivery) Ack(ctx context.Context) error {
	if d == nil || d.storage == nil {
		return fmt.Errorf("queue delivery not configured")
	}
	return d.storage.Ack(ctx, d.receipt)
}

func (d *delivery) Nack(ctx context.Context, opts queue.NackOptions) error {
	if d == nil || d.storage == nil {
		return fmt.Errorf("queue delivery not configured")
	}
	return d.storage.Nack(ctx, d.receipt, opts)
}

func (d *delivery) ExtendLease(ctx context.Context, ttl time.Duration) error {
	if d == nil || d.storage == nil {
		return fmt.Errorf("queue delivery not configured")
	}
	leaseStore, ok := d.storage.(queue.LeaseStorage)
	if !ok {
		return queue.ErrLeaseExtensionUnsupported
	}
	return leaseStore.ExtendLease(ctx, d.receipt, ttl)
}

func (d *delivery) Attempts() int {
	if d == nil {
		return 0
	}
	return d.receipt.Attempts
}

// GetDispatchStatus probes the underlying storage for dispatch lifecycle state.
func (a *Adapter) GetDispatchStatus(ctx context.Context, dispatchID string) (queue.DispatchStatus, error) {
	if a == nil || a.storage == nil {
		return queue.DispatchStatus{}, fmt.Errorf("queue adapter not configured")
	}
	reader, ok := a.storage.(queue.DispatchStatusReader)
	if !ok {
		return queue.DispatchStatus{}, queue.ErrDispatchStatusUnsupported
	}
	return reader.GetDispatchStatus(ctx, dispatchID)
}
