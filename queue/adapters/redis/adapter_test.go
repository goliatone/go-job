package redis

import (
	"context"
	"sync"
	"testing"
	"time"

	job "github.com/goliatone/go-job"
	"github.com/goliatone/go-job/queue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageEnqueueDequeueAck(t *testing.T) {
	client := newFakeClient()
	clock := newManualClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	storage := NewStorage(client,
		WithClock(clock.Now),
		WithVisibilityTimeout(10*time.Second),
		WithIDFunc(sequence("msg-1")),
		WithTokenFunc(sequence("token-1")),
	)

	msg := &job.ExecutionMessage{JobID: "export", ScriptPath: "/tmp/export"}
	require.NoError(t, storage.Enqueue(context.Background(), msg))

	out, receipt, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, "export", out.JobID)
	assert.Equal(t, "token-1", receipt.Token)
	assert.Equal(t, 1, receipt.Attempts)

	require.NoError(t, storage.Ack(context.Background(), receipt))
	assert.False(t, client.HasKey(storage.keys.message(receipt.ID)))
}

func TestAdapterEnqueueReceiptsAndDispatchStatusLifecycle(t *testing.T) {
	client := newFakeClient()
	clock := newManualClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	storage := NewStorage(client,
		WithClock(clock.Now),
		WithVisibilityTimeout(10*time.Second),
		WithIDFunc(sequence("msg-1", "msg-2", "msg-3")),
		WithTokenFunc(sequence("token-1", "token-2", "token-3")),
	)
	adapter := NewAdapter(storage)
	ctx := context.Background()

	msg := &job.ExecutionMessage{JobID: "export", ScriptPath: "/tmp/export"}
	receipt, err := adapter.EnqueueWithReceipt(ctx, msg)
	require.NoError(t, err)
	require.NotEmpty(t, receipt.DispatchID)
	require.False(t, receipt.EnqueuedAt.IsZero())

	status, err := adapter.GetDispatchStatus(ctx, receipt.DispatchID)
	require.NoError(t, err)
	assert.Equal(t, queue.DispatchStateAccepted, status.State)

	delivery, err := adapter.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, delivery)

	status, err = adapter.GetDispatchStatus(ctx, receipt.DispatchID)
	require.NoError(t, err)
	assert.Equal(t, queue.DispatchStateRunning, status.State)

	require.NoError(t, delivery.Nack(ctx, queue.NackOptions{
		Delay:   5 * time.Second,
		Requeue: true,
		Reason:  "retry",
	}))
	status, err = adapter.GetDispatchStatus(ctx, receipt.DispatchID)
	require.NoError(t, err)
	assert.Equal(t, queue.DispatchStateRetrying, status.State)
	assert.Equal(t, "retry", status.TerminalReason)

	clock.Advance(5 * time.Second)
	delivery, err = adapter.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, delivery)
	require.NoError(t, delivery.Nack(ctx, queue.NackOptions{
		DeadLetter: true,
		Reason:     "fatal",
	}))

	status, err = adapter.GetDispatchStatus(ctx, receipt.DispatchID)
	require.NoError(t, err)
	assert.Equal(t, queue.DispatchStateDeadLetter, status.State)
	assert.Equal(t, "fatal", status.TerminalReason)

	receipt2, err := adapter.EnqueueWithReceipt(ctx, msg)
	require.NoError(t, err)
	delivery2, err := adapter.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, delivery2)
	require.NoError(t, delivery2.Ack(ctx))

	status2, err := adapter.GetDispatchStatus(ctx, receipt2.DispatchID)
	require.NoError(t, err)
	assert.Equal(t, queue.DispatchStateSucceeded, status2.State)
	assert.True(t, status2.Inferred)
}

func TestAdapterScheduledReceiptVariants(t *testing.T) {
	client := newFakeClient()
	clock := newManualClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	storage := NewStorage(client,
		WithClock(clock.Now),
		WithVisibilityTimeout(10*time.Second),
		WithIDFunc(sequence("msg-1", "msg-2")),
		WithTokenFunc(sequence("token-1", "token-2")),
	)
	adapter := NewAdapter(storage)
	msg := &job.ExecutionMessage{JobID: "export", ScriptPath: "/tmp/export"}

	r1, err := adapter.EnqueueAtWithReceipt(context.Background(), msg, clock.Now().Add(1*time.Minute))
	require.NoError(t, err)
	require.NotEmpty(t, r1.DispatchID)

	r2, err := adapter.EnqueueAfterWithReceipt(context.Background(), msg, 5*time.Second)
	require.NoError(t, err)
	require.NotEmpty(t, r2.DispatchID)
}

func TestStorageNackDelayedRetry(t *testing.T) {
	client := newFakeClient()
	clock := newManualClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	storage := NewStorage(client,
		WithClock(clock.Now),
		WithVisibilityTimeout(10*time.Second),
		WithIDFunc(sequence("msg-1")),
		WithTokenFunc(sequence("token-1", "token-2")),
	)

	msg := &job.ExecutionMessage{JobID: "export", ScriptPath: "/tmp/export"}
	require.NoError(t, storage.Enqueue(context.Background(), msg))

	_, receipt, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	require.False(t, receipt.CreatedAt.IsZero())

	require.NoError(t, storage.Nack(context.Background(), receipt, queue.NackOptions{
		Delay:   5 * time.Second,
		Requeue: true,
		Reason:  "retry",
	}))

	none, _, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	assert.Nil(t, none)

	clock.Advance(5 * time.Second)
	out, receipt, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, 2, receipt.Attempts)
	assert.Equal(t, "token-2", receipt.Token)
	assert.Equal(t, "retry", receipt.LastError)
	assert.Equal(t, clock.Now(), receipt.AvailableAt)
}

func TestStorageVisibilityTimeoutRequeues(t *testing.T) {
	client := newFakeClient()
	clock := newManualClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	storage := NewStorage(client,
		WithClock(clock.Now),
		WithVisibilityTimeout(2*time.Second),
		WithIDFunc(sequence("msg-1")),
		WithTokenFunc(sequence("token-1", "token-2")),
	)

	msg := &job.ExecutionMessage{JobID: "export", ScriptPath: "/tmp/export"}
	require.NoError(t, storage.Enqueue(context.Background(), msg))

	_, receipt, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "token-1", receipt.Token)

	clock.Advance(3 * time.Second)
	out, receipt, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, 2, receipt.Attempts)
	assert.Equal(t, "token-2", receipt.Token)
}

func TestStorageDeadLetters(t *testing.T) {
	client := newFakeClient()
	clock := newManualClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	storage := NewStorage(client,
		WithClock(clock.Now),
		WithVisibilityTimeout(2*time.Second),
		WithIDFunc(sequence("msg-1")),
		WithTokenFunc(sequence("token-1")),
	)

	msg := &job.ExecutionMessage{JobID: "export", ScriptPath: "/tmp/export"}
	require.NoError(t, storage.Enqueue(context.Background(), msg))

	_, receipt, err := storage.Dequeue(context.Background())
	require.NoError(t, err)

	require.NoError(t, storage.Nack(context.Background(), receipt, queue.NackOptions{
		DeadLetter: true,
		Reason:     "fatal",
	}))

	dlq := client.List(storage.keys.dlq())
	require.Len(t, dlq, 1)
	assert.Equal(t, receipt.ID, dlq[0])
}

func TestStorageEnqueueAt(t *testing.T) {
	client := newFakeClient()
	clock := newManualClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	storage := NewStorage(client,
		WithClock(clock.Now),
		WithVisibilityTimeout(10*time.Second),
		WithIDFunc(sequence("msg-1")),
		WithTokenFunc(sequence("token-1")),
	)

	msg := &job.ExecutionMessage{JobID: "export", ScriptPath: "/tmp/export"}
	require.NoError(t, storage.EnqueueAt(context.Background(), msg, clock.Now().Add(10*time.Second)))

	none, _, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	assert.Nil(t, none)

	clock.Advance(10 * time.Second)
	out, _, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, "export", out.JobID)
}

func TestStorageExtendLease(t *testing.T) {
	client := newFakeClient()
	clock := newManualClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	storage := NewStorage(client,
		WithClock(clock.Now),
		WithVisibilityTimeout(2*time.Second),
		WithIDFunc(sequence("msg-1")),
		WithTokenFunc(sequence("token-1", "token-2")),
	)

	msg := &job.ExecutionMessage{JobID: "export", ScriptPath: "/tmp/export"}
	require.NoError(t, storage.Enqueue(context.Background(), msg))

	_, receipt, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	require.NoError(t, storage.ExtendLease(context.Background(), receipt, 10*time.Second))

	clock.Advance(3 * time.Second)
	none, _, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	assert.Nil(t, none)

	clock.Advance(8 * time.Second)
	out, next, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, 2, next.Attempts)
	assert.Equal(t, "token-2", next.Token)
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
	mu     sync.Mutex
	hashes map[string]map[string]string
	lists  map[string][]string
	zsets  map[string]map[string]float64
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		hashes: make(map[string]map[string]string),
		lists:  make(map[string][]string),
		zsets:  make(map[string]map[string]float64),
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

func (c *fakeClient) HGet(_ context.Context, key, field string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if fields, ok := c.hashes[key]; ok {
		return fields[field], nil
	}
	return "", nil
}

func (c *fakeClient) HDel(_ context.Context, key string, fields ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.hashes[key]; !ok {
		return nil
	}
	for _, field := range fields {
		delete(c.hashes[key], field)
	}
	if len(c.hashes[key]) == 0 {
		delete(c.hashes, key)
	}
	return nil
}

func (c *fakeClient) LPush(_ context.Context, key string, values ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	list := c.lists[key]
	for _, value := range values {
		list = append([]string{value}, list...)
	}
	c.lists[key] = list
	return nil
}

func (c *fakeClient) RPop(_ context.Context, key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	list := c.lists[key]
	if len(list) == 0 {
		return "", nil
	}
	value := list[len(list)-1]
	c.lists[key] = list[:len(list)-1]
	return value, nil
}

func (c *fakeClient) ZAdd(_ context.Context, key string, score float64, member string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.zsets[key]; !ok {
		c.zsets[key] = make(map[string]float64)
	}
	c.zsets[key][member] = score
	return nil
}

func (c *fakeClient) ZRem(_ context.Context, key string, members ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, member := range members {
		delete(c.zsets[key], member)
	}
	if len(c.zsets[key]) == 0 {
		delete(c.zsets, key)
	}
	return nil
}

func (c *fakeClient) ZRangeByScore(_ context.Context, key string, max float64, limit int64) ([]ZItem, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var items []ZItem
	for member, score := range c.zsets[key] {
		if score <= max {
			items = append(items, ZItem{Member: member, Score: score})
		}
	}
	if len(items) == 0 {
		return nil, nil
	}
	sortZItems(items)
	if limit > 0 && int64(len(items)) > limit {
		items = items[:limit]
	}
	return items, nil
}

func (c *fakeClient) Del(_ context.Context, keys ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, key := range keys {
		delete(c.hashes, key)
		delete(c.lists, key)
		delete(c.zsets, key)
	}
	return nil
}

func (c *fakeClient) HasKey(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.hashes[key]; ok {
		return true
	}
	if _, ok := c.lists[key]; ok {
		return true
	}
	if _, ok := c.zsets[key]; ok {
		return true
	}
	return false
}

func (c *fakeClient) List(key string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	list := c.lists[key]
	out := make([]string, len(list))
	copy(out, list)
	return out
}

func sortZItems(items []ZItem) {
	for i := 1; i < len(items); i++ {
		for j := i; j > 0 && items[j-1].Score > items[j].Score; j-- {
			items[j], items[j-1] = items[j-1], items[j]
		}
	}
}

func sequence(values ...string) func() string {
	index := 0
	return func() string {
		if index >= len(values) {
			return ""
		}
		value := values[index]
		index++
		return value
	}
}
