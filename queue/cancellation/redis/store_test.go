package redis

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/goliatone/go-job/queue/cancellation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreRequestGet(t *testing.T) {
	client := newFakeClient()
	clock := newManualClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	store := NewStore(client, WithClock(clock.Now))

	req := cancellation.Request{Key: "export-1", Reason: "user"}
	require.NoError(t, store.Request(context.Background(), req))

	got, found, err := store.Get(context.Background(), "export-1")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "export-1", got.Key)
	assert.Equal(t, "user", got.Reason)
	assert.Equal(t, clock.Now().UTC(), got.RequestedAt)
}

func TestStoreSubscribe(t *testing.T) {
	client := newFakeClient()
	store := NewStore(client, WithPollInterval(5*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, err := store.Subscribe(ctx)
	require.NoError(t, err)

	req := cancellation.Request{Key: "export-2", Reason: "timeout"}
	require.NoError(t, store.Request(context.Background(), req))

	select {
	case out := <-sub:
		assert.Equal(t, "export-2", out.Key)
		assert.Equal(t, "timeout", out.Reason)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for cancellation request")
	}
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

type fakeClient struct {
	mu     sync.Mutex
	hashes map[string]map[string]string
	lists  map[string][]string
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		hashes: make(map[string]map[string]string),
		lists:  make(map[string][]string),
	}
}

func (c *fakeClient) HSet(_ context.Context, key string, values map[string]string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.hashes[key]; !ok {
		c.hashes[key] = make(map[string]string)
	}
	for field, value := range values {
		c.hashes[key][field] = value
	}
	return nil
}

func (c *fakeClient) HGetAll(_ context.Context, key string) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]string)
	for field, value := range c.hashes[key] {
		out[field] = value
	}
	return out, nil
}

func (c *fakeClient) LPush(_ context.Context, key string, values ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, value := range values {
		c.lists[key] = append([]string{value}, c.lists[key]...)
	}
	return nil
}

func (c *fakeClient) RPop(_ context.Context, key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	values := c.lists[key]
	if len(values) == 0 {
		return "", nil
	}
	value := values[len(values)-1]
	c.lists[key] = values[:len(values)-1]
	return value, nil
}

func (c *fakeClient) Del(_ context.Context, keys ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, key := range keys {
		delete(c.hashes, key)
		delete(c.lists, key)
	}
	return nil
}
