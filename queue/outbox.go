package queue

import (
	"context"
	"fmt"
	"strings"
	"time"

	job "github.com/goliatone/go-job"
)

const (
	// DefaultOutboxClaimLimit is used when ClaimPending receives a non-positive limit.
	DefaultOutboxClaimLimit = 100
	// DefaultOutboxLeaseTTL is used when ClaimPending receives a non-positive lease ttl.
	DefaultOutboxLeaseTTL = 30 * time.Second
)

// ClaimedOutboxEntry is a claimed queue delivery represented with outbox semantics.
type ClaimedOutboxEntry struct {
	ID         string
	Message    *job.ExecutionMessage
	Attempts   int
	Status     string
	LeaseOwner string
	LeaseToken string
	LeaseUntil time.Time
	RetryAt    time.Time
	CreatedAt  time.Time
	LastError  string
}

// OutboxStore mirrors durable orchestrator dispatch contracts.
type OutboxStore interface {
	ClaimPending(ctx context.Context, workerID string, limit int, leaseTTL time.Duration) ([]ClaimedOutboxEntry, error)
	MarkCompleted(ctx context.Context, id, leaseToken string) error
	MarkFailed(ctx context.Context, id, leaseToken string, retryAt time.Time, reason string) error
	ExtendLease(ctx context.Context, id, leaseToken string, leaseTTL time.Duration) error
}

// StorageOutboxOption customizes StorageOutboxAdapter behavior.
type StorageOutboxOption func(*StorageOutboxAdapter)

// WithOutboxClock overrides time source used for retry scheduling calculations.
func WithOutboxClock(now func() time.Time) StorageOutboxOption {
	return func(adapter *StorageOutboxAdapter) {
		if now != nil {
			adapter.now = now
		}
	}
}

// StorageOutboxAdapter maps queue.Storage semantics to an OutboxStore contract.
type StorageOutboxAdapter struct {
	storage Storage
	now     func() time.Time
}

// NewStorageOutboxAdapter builds an outbox adapter backed by queue storage.
func NewStorageOutboxAdapter(storage Storage, opts ...StorageOutboxOption) *StorageOutboxAdapter {
	adapter := &StorageOutboxAdapter{
		storage: storage,
		now:     time.Now,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(adapter)
		}
	}
	if adapter.now == nil {
		adapter.now = time.Now
	}
	return adapter
}

// ClaimPending claims pending queue messages as leased outbox entries.
func (a *StorageOutboxAdapter) ClaimPending(ctx context.Context, workerID string, limit int, leaseTTL time.Duration) ([]ClaimedOutboxEntry, error) {
	if a == nil || a.storage == nil {
		return nil, fmt.Errorf("outbox adapter not configured")
	}
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return nil, fmt.Errorf("worker id required")
	}
	if limit <= 0 {
		limit = DefaultOutboxClaimLimit
	}
	if leaseTTL <= 0 {
		leaseTTL = DefaultOutboxLeaseTTL
	}

	var claimed []ClaimedOutboxEntry
	var leaseStore LeaseStorage
	if store, ok := a.storage.(LeaseStorage); ok {
		leaseStore = store
	}

	for len(claimed) < limit {
		msg, receipt, err := a.storage.Dequeue(ctx)
		if err != nil {
			return claimed, err
		}
		if msg == nil {
			break
		}

		if leaseStore != nil {
			if err := leaseStore.ExtendLease(ctx, receipt, leaseTTL); err != nil {
				return claimed, err
			}
		}

		claimed = append(claimed, ClaimedOutboxEntry{
			ID:         receipt.ID,
			Message:    msg,
			Attempts:   receipt.Attempts,
			Status:     "leased",
			LeaseOwner: workerID,
			LeaseToken: receipt.Token,
			LeaseUntil: receipt.LeasedAt.Add(leaseTTL),
			RetryAt:    receipt.AvailableAt,
			CreatedAt:  receipt.CreatedAt,
			LastError:  receipt.LastError,
		})
	}

	return claimed, nil
}

// MarkCompleted marks a claimed entry as completed (ack).
func (a *StorageOutboxAdapter) MarkCompleted(ctx context.Context, id, leaseToken string) error {
	if a == nil || a.storage == nil {
		return fmt.Errorf("outbox adapter not configured")
	}
	id = strings.TrimSpace(id)
	leaseToken = strings.TrimSpace(leaseToken)
	if id == "" || leaseToken == "" {
		return fmt.Errorf("id and lease token required")
	}
	return a.storage.Ack(ctx, Receipt{
		ID:    id,
		Token: leaseToken,
	})
}

// MarkFailed marks a claimed entry as failed and requeues it for retry scheduling.
func (a *StorageOutboxAdapter) MarkFailed(ctx context.Context, id, leaseToken string, retryAt time.Time, reason string) error {
	if a == nil || a.storage == nil {
		return fmt.Errorf("outbox adapter not configured")
	}
	id = strings.TrimSpace(id)
	leaseToken = strings.TrimSpace(leaseToken)
	if id == "" || leaseToken == "" {
		return fmt.Errorf("id and lease token required")
	}

	retryAt = retryAt.UTC()
	delay := time.Duration(0)
	if !retryAt.IsZero() {
		now := a.now().UTC()
		if retryAt.After(now) {
			delay = retryAt.Sub(now)
		}
	}

	return a.storage.Nack(ctx, Receipt{
		ID:    id,
		Token: leaseToken,
	}, NackOptions{
		Disposition: NackDispositionRetry,
		Delay:       delay,
		Reason:      strings.TrimSpace(reason),
	})
}

// ExtendLease extends one claimed entry lease when supported by storage.
func (a *StorageOutboxAdapter) ExtendLease(ctx context.Context, id, leaseToken string, leaseTTL time.Duration) error {
	if a == nil || a.storage == nil {
		return fmt.Errorf("outbox adapter not configured")
	}
	id = strings.TrimSpace(id)
	leaseToken = strings.TrimSpace(leaseToken)
	if id == "" || leaseToken == "" {
		return fmt.Errorf("id and lease token required")
	}
	leaseStore, ok := a.storage.(LeaseStorage)
	if !ok {
		return ErrLeaseExtensionUnsupported
	}
	return leaseStore.ExtendLease(ctx, Receipt{
		ID:    id,
		Token: leaseToken,
	}, leaseTTL)
}

var _ OutboxStore = (*StorageOutboxAdapter)(nil)
