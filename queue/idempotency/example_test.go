package idempotency_test

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/goliatone/go-job/queue/idempotency"
)

func ExampleStore_usage() {
	store := newMemoryStore()
	ctx := context.Background()

	// API path: acquire before enqueueing.
	record, acquired, _ := store.Acquire(ctx, "export:123", 24*time.Hour)
	if acquired {
		_ = record
	}

	// Worker path: update status after processing.
	status := idempotency.StatusCompleted
	payload := []byte("export-id")
	_ = store.Update(ctx, "export:123", idempotency.Update{
		Status:  &status,
		Payload: &payload,
	})

	updated, _, _ := store.Get(ctx, "export:123")
	fmt.Println(updated.Key, updated.Status)

	// Output: export:123 completed
}

type memoryStore struct {
	mu      sync.Mutex
	records map[string]idempotency.Record
}

func newMemoryStore() *memoryStore {
	return &memoryStore{records: make(map[string]idempotency.Record)}
}

func (m *memoryStore) Acquire(_ context.Context, key string, ttl time.Duration) (idempotency.Record, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	record, ok := m.records[key]
	if ok && !idempotency.IsExpired(record, now) {
		return record, false, nil
	}
	record = idempotency.Record{Key: key, Status: idempotency.StatusPending, CreatedAt: now, UpdatedAt: now}
	if ttl > 0 {
		record.ExpiresAt = now.Add(ttl)
	}
	m.records[key] = record
	return record, true, nil
}

func (m *memoryStore) Get(_ context.Context, key string) (idempotency.Record, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, ok := m.records[key]
	if !ok || idempotency.IsExpired(record, time.Now()) {
		delete(m.records, key)
		return idempotency.Record{}, false, nil
	}
	return record, true, nil
}

func (m *memoryStore) Update(_ context.Context, key string, update idempotency.Update) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	record, ok := m.records[key]
	if !ok {
		return idempotency.ErrNotFound
	}
	if update.Status != nil {
		record.Status = *update.Status
	}
	if update.Payload != nil {
		record.Payload = append([]byte(nil), (*update.Payload)...)
	}
	if update.ExpiresAt != nil {
		record.ExpiresAt = update.ExpiresAt.UTC()
	}
	record.UpdatedAt = time.Now()
	m.records[key] = record
	return nil
}

func (m *memoryStore) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.records, key)
	return nil
}

var _ idempotency.Store = (*memoryStore)(nil)
