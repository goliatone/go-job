package queue

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	job "github.com/goliatone/go-job"
	"github.com/stretchr/testify/require"
)

func TestStorageOutboxAdapterClaimsAcrossWorkers(t *testing.T) {
	clock := newOutboxClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	storage := newOutboxMemoryStorage(clock.Now)
	adapter := NewStorageOutboxAdapter(storage, WithOutboxClock(clock.Now))

	_, err := storage.Enqueue(context.Background(), &job.ExecutionMessage{JobID: "job-1", ScriptPath: "/tmp/job-1"})
	require.NoError(t, err)
	_, err = storage.Enqueue(context.Background(), &job.ExecutionMessage{JobID: "job-2", ScriptPath: "/tmp/job-2"})
	require.NoError(t, err)

	claimedA, err := adapter.ClaimPending(context.Background(), "worker-a", 1, 30*time.Second)
	require.NoError(t, err)
	require.Len(t, claimedA, 1)
	require.Equal(t, "worker-a", claimedA[0].LeaseOwner)

	claimedB, err := adapter.ClaimPending(context.Background(), "worker-b", 1, 30*time.Second)
	require.NoError(t, err)
	require.Len(t, claimedB, 1)
	require.Equal(t, "worker-b", claimedB[0].LeaseOwner)
	require.NotEqual(t, claimedA[0].ID, claimedB[0].ID)

	require.NoError(t, adapter.MarkCompleted(context.Background(), claimedA[0].ID, claimedA[0].LeaseToken))
	require.NoError(t, adapter.MarkCompleted(context.Background(), claimedB[0].ID, claimedB[0].LeaseToken))

	none, err := adapter.ClaimPending(context.Background(), "worker-a", 10, 30*time.Second)
	require.NoError(t, err)
	require.Len(t, none, 0)
}

func TestStorageOutboxAdapterMarkFailedSchedulesRetryMetadata(t *testing.T) {
	clock := newOutboxClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	storage := newOutboxMemoryStorage(clock.Now)
	adapter := NewStorageOutboxAdapter(storage, WithOutboxClock(clock.Now))

	_, err := storage.Enqueue(context.Background(), &job.ExecutionMessage{JobID: "job", ScriptPath: "/tmp/job"})
	require.NoError(t, err)

	claimed, err := adapter.ClaimPending(context.Background(), "worker-a", 1, 30*time.Second)
	require.NoError(t, err)
	require.Len(t, claimed, 1)
	require.Equal(t, 1, claimed[0].Attempts)

	retryAt := clock.Now().Add(5 * time.Second)
	require.NoError(t, adapter.MarkFailed(context.Background(), claimed[0].ID, claimed[0].LeaseToken, retryAt, "boom"))

	none, err := adapter.ClaimPending(context.Background(), "worker-b", 1, 30*time.Second)
	require.NoError(t, err)
	require.Len(t, none, 0)

	clock.Advance(5 * time.Second)

	reclaimed, err := adapter.ClaimPending(context.Background(), "worker-c", 1, 30*time.Second)
	require.NoError(t, err)
	require.Len(t, reclaimed, 1)
	require.Equal(t, 2, reclaimed[0].Attempts)
	require.Equal(t, retryAt, reclaimed[0].RetryAt)
	require.Equal(t, "boom", reclaimed[0].LastError)
}

type outboxClock struct {
	mu  sync.Mutex
	now time.Time
}

func newOutboxClock(now time.Time) *outboxClock {
	return &outboxClock{now: now}
}

func (c *outboxClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *outboxClock) Advance(delay time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(delay)
}

type outboxMemoryStorage struct {
	mu                sync.Mutex
	now               func() time.Time
	defaultVisibility time.Duration
	sequence          int
	entries           []*outboxMemoryEntry
}

type outboxMemoryEntry struct {
	id         string
	msg        *job.ExecutionMessage
	token      string
	attempts   int
	leasedAt   time.Time
	leaseUntil time.Time
	available  time.Time
	createdAt  time.Time
	lastError  string
	done       bool
}

func newOutboxMemoryStorage(now func() time.Time) *outboxMemoryStorage {
	return &outboxMemoryStorage{
		now:               now,
		defaultVisibility: 30 * time.Second,
	}
}

func (s *outboxMemoryStorage) Enqueue(_ context.Context, msg *job.ExecutionMessage) (EnqueueReceipt, error) {
	if msg == nil {
		return EnqueueReceipt{}, fmt.Errorf("message required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	id := s.nextID("msg")
	s.entries = append(s.entries, &outboxMemoryEntry{
		id:        id,
		msg:       cloneOutboxMessage(msg),
		available: now,
		createdAt: now,
	})
	return EnqueueReceipt{
		DispatchID: id,
		EnqueuedAt: now,
	}, nil
}

func (s *outboxMemoryStorage) Dequeue(_ context.Context) (*job.ExecutionMessage, Receipt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now().UTC()
	for _, entry := range s.entries {
		if entry == nil || entry.done {
			continue
		}
		if !entry.available.IsZero() && entry.available.After(now) {
			continue
		}
		if entry.token != "" && entry.leaseUntil.After(now) {
			continue
		}
		entry.attempts++
		entry.token = s.nextID("token")
		entry.leasedAt = now
		entry.leaseUntil = now.Add(s.defaultVisibility)

		return cloneOutboxMessage(entry.msg), Receipt{
			ID:          entry.id,
			Token:       entry.token,
			Attempts:    entry.attempts,
			LeasedAt:    entry.leasedAt,
			AvailableAt: entry.available,
			CreatedAt:   entry.createdAt,
			LastError:   entry.lastError,
		}, nil
	}
	return nil, Receipt{}, nil
}

func (s *outboxMemoryStorage) Ack(_ context.Context, receipt Receipt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, err := s.findByReceipt(receipt)
	if err != nil {
		return err
	}
	entry.done = true
	entry.token = ""
	entry.leaseUntil = time.Time{}
	return nil
}

func (s *outboxMemoryStorage) Nack(_ context.Context, receipt Receipt, opts NackOptions) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, err := s.findByReceipt(receipt)
	if err != nil {
		return err
	}
	now := s.now().UTC()
	entry.token = ""
	entry.leaseUntil = time.Time{}
	if opts.Reason != "" {
		entry.lastError = opts.Reason
	}
	if opts.Disposition == NackDispositionDeadLetter {
		entry.done = true
		return nil
	}
	if opts.Disposition == NackDispositionRetry {
		entry.available = now.Add(opts.Delay)
		return nil
	}
	entry.done = true
	return nil
}

func (s *outboxMemoryStorage) ExtendLease(_ context.Context, receipt Receipt, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, err := s.findByReceipt(receipt)
	if err != nil {
		return err
	}
	if ttl <= 0 {
		ttl = s.defaultVisibility
	}
	entry.leaseUntil = s.now().UTC().Add(ttl)
	return nil
}

func (s *outboxMemoryStorage) findByReceipt(receipt Receipt) (*outboxMemoryEntry, error) {
	if receipt.ID == "" || receipt.Token == "" {
		return nil, fmt.Errorf("receipt id and token required")
	}
	for _, entry := range s.entries {
		if entry == nil || entry.id != receipt.ID {
			continue
		}
		if entry.token != receipt.Token {
			return nil, fmt.Errorf("receipt token mismatch for %q", receipt.ID)
		}
		return entry, nil
	}
	return nil, fmt.Errorf("receipt %q not found", receipt.ID)
}

func (s *outboxMemoryStorage) nextID(prefix string) string {
	s.sequence++
	return fmt.Sprintf("%s-%d", prefix, s.sequence)
}

func cloneOutboxMessage(msg *job.ExecutionMessage) *job.ExecutionMessage {
	if msg == nil {
		return nil
	}
	copyMsg := *msg
	if msg.Parameters != nil {
		copyMsg.Parameters = make(map[string]any, len(msg.Parameters))
		for key, value := range msg.Parameters {
			copyMsg.Parameters[key] = value
		}
	}
	return &copyMsg
}

var (
	_ Storage      = (*outboxMemoryStorage)(nil)
	_ LeaseStorage = (*outboxMemoryStorage)(nil)
)
