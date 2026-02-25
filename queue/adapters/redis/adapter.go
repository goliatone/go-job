package redis

import (
	"context"
	"fmt"
	"time"

	job "github.com/goliatone/go-job"
	"github.com/goliatone/go-job/queue"
)

// Adapter bridges a Storage implementation to queue interfaces.
type Adapter struct {
	storage queue.Storage
}

// NewAdapter builds an adapter from a storage instance.
func NewAdapter(storage queue.Storage) *Adapter {
	return &Adapter{storage: storage}
}

// Enqueue forwards to the underlying storage.
func (a *Adapter) Enqueue(ctx context.Context, msg *job.ExecutionMessage) error {
	if a == nil || a.storage == nil {
		return fmt.Errorf("queue adapter not configured")
	}
	return a.storage.Enqueue(ctx, msg)
}

// EnqueueAt forwards scheduled enqueue requests to storage when supported.
func (a *Adapter) EnqueueAt(ctx context.Context, msg *job.ExecutionMessage, at time.Time) error {
	if a == nil || a.storage == nil {
		return fmt.Errorf("queue adapter not configured")
	}
	scheduler, ok := a.storage.(queue.ScheduledStorage)
	if !ok {
		return queue.ErrScheduledEnqueueUnsupported
	}
	return scheduler.EnqueueAt(ctx, msg, at)
}

// EnqueueAfter forwards delayed enqueue requests to storage when supported.
func (a *Adapter) EnqueueAfter(ctx context.Context, msg *job.ExecutionMessage, delay time.Duration) error {
	if a == nil || a.storage == nil {
		return fmt.Errorf("queue adapter not configured")
	}
	scheduler, ok := a.storage.(queue.ScheduledStorage)
	if !ok {
		return queue.ErrScheduledEnqueueUnsupported
	}
	return scheduler.EnqueueAfter(ctx, msg, delay)
}

// Dequeue returns a delivery wrapper when a message is available.
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
