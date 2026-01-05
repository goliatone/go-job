package redis

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/goliatone/go-job/queue/idempotency"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreAcquireGetUpdateDelete(t *testing.T) {
	clock := newManualClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	client := newFakeClient(clock.Now)
	store := NewStore(client, WithClock(clock.Now))

	record, acquired, err := store.Acquire(context.Background(), "key-1", 10*time.Second)
	require.NoError(t, err)
	assert.True(t, acquired)
	assert.Equal(t, idempotency.StatusPending, record.Status)

	record, acquired, err = store.Acquire(context.Background(), "key-1", 10*time.Second)
	require.NoError(t, err)
	assert.False(t, acquired)

	payload := []byte("result")
	status := idempotency.StatusCompleted
	expiresAt := clock.Now().Add(20 * time.Second)
	require.NoError(t, store.Update(context.Background(), "key-1", idempotency.Update{
		Status:    &status,
		Payload:   &payload,
		ExpiresAt: &expiresAt,
	}))

	fetched, found, err := store.Get(context.Background(), "key-1")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, status, fetched.Status)
	assert.Equal(t, payload, fetched.Payload)

	require.NoError(t, store.Delete(context.Background(), "key-1"))
	_, found, err = store.Get(context.Background(), "key-1")
	require.NoError(t, err)
	assert.False(t, found)
}

func TestStoreExpiresRecords(t *testing.T) {
	clock := newManualClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	client := newFakeClient(clock.Now)
	store := NewStore(client, WithClock(clock.Now))

	_, acquired, err := store.Acquire(context.Background(), "key-1", 5*time.Second)
	require.NoError(t, err)
	assert.True(t, acquired)

	clock.Advance(6 * time.Second)
	_, found, err := store.Get(context.Background(), "key-1")
	require.NoError(t, err)
	assert.False(t, found)
}

type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func newManualClock(start time.Time) *manualClock {
	return &manualClock{now: start}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

type fakeClient struct {
	mu    sync.Mutex
	store map[string]entry
	now   func() time.Time
}

type entry struct {
	value     string
	expiresAt time.Time
}

func newFakeClient(now func() time.Time) *fakeClient {
	return &fakeClient{
		store: make(map[string]entry),
		now:   now,
	}
}

func (c *fakeClient) Get(_ context.Context, key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.store[key]
	if !ok {
		return "", nil
	}
	if !entry.expiresAt.IsZero() && !entry.expiresAt.After(c.now()) {
		delete(c.store, key)
		return "", nil
	}
	return entry.value, nil
}

func (c *fakeClient) Set(_ context.Context, key, value string, ttl time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := entry{value: value}
	if ttl > 0 {
		entry.expiresAt = c.now().Add(ttl)
	}
	c.store[key] = entry
	return nil
}

func (c *fakeClient) SetNX(_ context.Context, key, value string, ttl time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	stored, ok := c.store[key]
	if ok && !stored.expiresAt.IsZero() && !stored.expiresAt.After(c.now()) {
		delete(c.store, key)
		ok = false
	}
	if ok {
		return false, nil
	}
	newEntry := entry{value: value}
	if ttl > 0 {
		newEntry.expiresAt = c.now().Add(ttl)
	}
	c.store[key] = newEntry
	return true, nil
}

func (c *fakeClient) Del(_ context.Context, key string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.store, key)
	return nil
}
