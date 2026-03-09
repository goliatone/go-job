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
	_, err := a.EnqueueWithReceipt(ctx, msg)
	return err
}

// EnqueueWithReceipt forwards to the underlying storage and returns enqueue metadata.
func (a *Adapter) EnqueueWithReceipt(ctx context.Context, msg *job.ExecutionMessage) (queue.EnqueueReceipt, error) {
	if a == nil || a.storage == nil {
		return queue.EnqueueReceipt{}, fmt.Errorf("queue adapter not configured")
	}
	if receiver, ok := a.storage.(queue.ReceiptStorage); ok {
		return receiver.EnqueueWithReceipt(ctx, msg)
	}
	if err := a.storage.Enqueue(ctx, msg); err != nil {
		return queue.EnqueueReceipt{}, err
	}
	return queue.EnqueueReceipt{}, nil
}

// EnqueueAt forwards scheduled enqueue requests to storage when supported.
func (a *Adapter) EnqueueAt(ctx context.Context, msg *job.ExecutionMessage, at time.Time) error {
	_, err := a.EnqueueAtWithReceipt(ctx, msg, at)
	return err
}

// EnqueueAtWithReceipt forwards scheduled enqueue requests to storage when supported.
func (a *Adapter) EnqueueAtWithReceipt(ctx context.Context, msg *job.ExecutionMessage, at time.Time) (queue.EnqueueReceipt, error) {
	if a == nil || a.storage == nil {
		return queue.EnqueueReceipt{}, fmt.Errorf("queue adapter not configured")
	}
	if scheduler, ok := a.storage.(queue.ReceiptScheduledStorage); ok {
		return scheduler.EnqueueAtWithReceipt(ctx, msg, at)
	}
	scheduler, ok := a.storage.(queue.ScheduledStorage)
	if !ok {
		return queue.EnqueueReceipt{}, queue.ErrScheduledEnqueueUnsupported
	}
	if err := scheduler.EnqueueAt(ctx, msg, at); err != nil {
		return queue.EnqueueReceipt{}, err
	}
	return queue.EnqueueReceipt{}, nil
}

// EnqueueAfter forwards delayed enqueue requests to storage when supported.
func (a *Adapter) EnqueueAfter(ctx context.Context, msg *job.ExecutionMessage, delay time.Duration) error {
	_, err := a.EnqueueAfterWithReceipt(ctx, msg, delay)
	return err
}

// EnqueueAfterWithReceipt forwards delayed enqueue requests to storage when supported.
func (a *Adapter) EnqueueAfterWithReceipt(ctx context.Context, msg *job.ExecutionMessage, delay time.Duration) (queue.EnqueueReceipt, error) {
	if a == nil || a.storage == nil {
		return queue.EnqueueReceipt{}, fmt.Errorf("queue adapter not configured")
	}
	if scheduler, ok := a.storage.(queue.ReceiptScheduledStorage); ok {
		return scheduler.EnqueueAfterWithReceipt(ctx, msg, delay)
	}
	scheduler, ok := a.storage.(queue.ScheduledStorage)
	if !ok {
		return queue.EnqueueReceipt{}, queue.ErrScheduledEnqueueUnsupported
	}
	if err := scheduler.EnqueueAfter(ctx, msg, delay); err != nil {
		return queue.EnqueueReceipt{}, err
	}
	return queue.EnqueueReceipt{}, nil
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

// GetDispatchStatus probes the underlying storage for dispatch lifecycle state.
func (a *Adapter) GetDispatchStatus(ctx context.Context, dispatchID string) (queue.DispatchStatus, error) {
	if a == nil || a.storage == nil {
		return queue.DispatchStatus{}, fmt.Errorf("queue adapter not configured")
	}
	reader, ok := a.storage.(queue.DispatchStatusReader)
	if !ok {
		return queue.DispatchStatus{
			DispatchID: dispatchID,
			State:      queue.DispatchStateUnknown,
		}, nil
	}
	return reader.GetDispatchStatus(ctx, dispatchID)
}
