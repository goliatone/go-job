package redis

import (
	"context"
	"fmt"
	"strconv"
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
	_, err := storage.Enqueue(context.Background(), msg)
	require.NoError(t, err)

	out, receipt, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, "export", out.JobID)
	assert.NotEmpty(t, receipt.Token)
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
	receipt, err := adapter.Enqueue(ctx, msg)
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
		Disposition: queue.NackDispositionRetry,
		Delay:       5 * time.Second,
		Reason:      "retry",
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
		Disposition: queue.NackDispositionDeadLetter,
		Reason:      "fatal",
	}))

	status, err = adapter.GetDispatchStatus(ctx, receipt.DispatchID)
	require.NoError(t, err)
	assert.Equal(t, queue.DispatchStateDeadLetter, status.State)
	assert.Equal(t, "fatal", status.TerminalReason)

	receipt2, err := adapter.Enqueue(ctx, msg)
	require.NoError(t, err)
	delivery2, err := adapter.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, delivery2)
	require.NoError(t, delivery2.Ack(ctx))

	status2, err := adapter.GetDispatchStatus(ctx, receipt2.DispatchID)
	require.NoError(t, err)
	assert.Equal(t, queue.DispatchStateSucceeded, status2.State)
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

	r1, err := adapter.EnqueueAt(context.Background(), msg, clock.Now().Add(1*time.Minute))
	require.NoError(t, err)
	require.NotEmpty(t, r1.DispatchID)

	r2, err := adapter.EnqueueAfter(context.Background(), msg, 5*time.Second)
	require.NoError(t, err)
	require.NotEmpty(t, r2.DispatchID)
}

func TestAdapterDispatchStatusTerminalFailedAndCanceled(t *testing.T) {
	client := newFakeClient()
	clock := newManualClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	storage := NewStorage(client,
		WithClock(clock.Now),
		WithVisibilityTimeout(10*time.Second),
		WithIDFunc(sequence("msg-1", "msg-2")),
		WithTokenFunc(sequence("token-1", "token-2")),
	)
	adapter := NewAdapter(storage)
	ctx := context.Background()
	msg := &job.ExecutionMessage{JobID: "export", ScriptPath: "/tmp/export"}

	failedReceipt, err := adapter.Enqueue(ctx, msg)
	require.NoError(t, err)
	failedDelivery, err := adapter.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, failedDelivery)
	require.NoError(t, failedDelivery.Nack(ctx, queue.NackOptions{
		Disposition: queue.NackDispositionFailed,
		Reason:      "failed-terminal",
	}))

	status, err := adapter.GetDispatchStatus(ctx, failedReceipt.DispatchID)
	require.NoError(t, err)
	assert.Equal(t, queue.DispatchStateFailed, status.State)
	assert.Equal(t, "failed-terminal", status.TerminalReason)

	canceledReceipt, err := adapter.Enqueue(ctx, msg)
	require.NoError(t, err)
	canceledDelivery, err := adapter.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, canceledDelivery)
	require.NoError(t, canceledDelivery.Nack(ctx, queue.NackOptions{
		Disposition: queue.NackDispositionCanceled,
		Reason:      "canceled-terminal",
	}))

	status, err = adapter.GetDispatchStatus(ctx, canceledReceipt.DispatchID)
	require.NoError(t, err)
	assert.Equal(t, queue.DispatchStateCanceled, status.State)
	assert.Equal(t, "canceled-terminal", status.TerminalReason)
}

func TestAdapterGetDispatchStatusUnsupported(t *testing.T) {
	adapter := NewAdapter(stubStorageNoStatus{})
	_, err := adapter.GetDispatchStatus(context.Background(), "dispatch-1")
	require.ErrorIs(t, err, queue.ErrDispatchStatusUnsupported)
}

func TestAdapterGetDispatchStatusNotFound(t *testing.T) {
	client := newFakeClient()
	storage := NewStorage(client)
	adapter := NewAdapter(storage)

	_, err := adapter.GetDispatchStatus(context.Background(), "missing-dispatch")
	require.ErrorIs(t, err, queue.ErrDispatchNotFound)
}

func TestStorageLegacyMessageBecomesStatusTrackedOnTransition(t *testing.T) {
	client := newFakeClient()
	clock := newManualClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	storage := NewStorage(client,
		WithClock(clock.Now),
		WithVisibilityTimeout(10*time.Second),
		WithIDFunc(sequence("legacy-1")),
		WithTokenFunc(sequence("token-legacy")),
	)
	ctx := context.Background()

	msg := &job.ExecutionMessage{JobID: "legacy", ScriptPath: "/tmp/legacy"}
	payload, err := queue.EncodeExecutionMessage(msg)
	require.NoError(t, err)

	now := clock.Now().UTC().UnixNano()
	require.NoError(t, client.HSet(ctx, storage.keys.message("legacy-1"), map[string]string{
		fieldPayload:   string(payload),
		fieldAttempts:  "0",
		fieldToken:     "",
		fieldAvailable: strconv.FormatInt(now, 10),
		fieldCreatedAt: strconv.FormatInt(now, 10),
		fieldUpdatedAt: strconv.FormatInt(now, 10),
	}))
	require.NoError(t, client.LPush(ctx, storage.keys.ready(), "legacy-1"))

	adapter := NewAdapter(storage)
	delivery, err := adapter.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, delivery)

	status, err := adapter.GetDispatchStatus(ctx, "legacy-1")
	require.NoError(t, err)
	assert.Equal(t, queue.DispatchStateRunning, status.State)

	require.NoError(t, delivery.Ack(ctx))
	status, err = adapter.GetDispatchStatus(ctx, "legacy-1")
	require.NoError(t, err)
	assert.Equal(t, queue.DispatchStateSucceeded, status.State)
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
	_, err := storage.Enqueue(context.Background(), msg)
	require.NoError(t, err)

	_, receipt, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	require.False(t, receipt.CreatedAt.IsZero())
	firstToken := receipt.Token

	require.NoError(t, storage.Nack(context.Background(), receipt, queue.NackOptions{
		Disposition: queue.NackDispositionRetry,
		Delay:       5 * time.Second,
		Reason:      "retry",
	}))

	none, _, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	assert.Nil(t, none)

	clock.Advance(5 * time.Second)
	out, receipt, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, 2, receipt.Attempts)
	assert.NotEmpty(t, receipt.Token)
	assert.NotEqual(t, firstToken, receipt.Token)
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
	_, err := storage.Enqueue(context.Background(), msg)
	require.NoError(t, err)

	_, receipt, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	firstToken := receipt.Token
	assert.NotEmpty(t, firstToken)

	clock.Advance(3 * time.Second)
	out, receipt, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, 2, receipt.Attempts)
	assert.NotEmpty(t, receipt.Token)
	assert.NotEqual(t, firstToken, receipt.Token)
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
	_, err := storage.Enqueue(context.Background(), msg)
	require.NoError(t, err)

	_, receipt, err := storage.Dequeue(context.Background())
	require.NoError(t, err)

	require.NoError(t, storage.Nack(context.Background(), receipt, queue.NackOptions{
		Disposition: queue.NackDispositionDeadLetter,
		Reason:      "fatal",
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
	_, err := storage.EnqueueAt(context.Background(), msg, clock.Now().Add(10*time.Second))
	require.NoError(t, err)

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
	_, err := storage.Enqueue(context.Background(), msg)
	require.NoError(t, err)

	_, receipt, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	firstToken := receipt.Token
	assert.NotEmpty(t, firstToken)
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
	assert.NotEmpty(t, next.Token)
	assert.NotEqual(t, firstToken, next.Token)
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

func (c *fakeClient) Eval(_ context.Context, script string, keys []string, args ...any) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch script {
	case DequeueScript:
		return c.evalDequeue(keys, args...)
	case AckScript:
		return c.evalAck(keys, args...)
	case NackScript:
		return c.evalNack(keys, args...)
	case ExtendLeaseScript:
		return c.evalExtendLease(keys, args...)
	default:
		return nil, fmt.Errorf("unsupported script")
	}
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

func (c *fakeClient) Expire(_ context.Context, _ string, _ time.Duration) error {
	return nil
}

func (c *fakeClient) evalDequeue(keys []string, args ...any) (any, error) {
	if len(keys) != 5 || len(args) < 3 {
		return nil, fmt.Errorf("invalid dequeue eval arguments")
	}
	now, err := evalArgInt64(args[0])
	if err != nil {
		return nil, err
	}
	batch, err := evalArgInt64(args[1])
	if err != nil {
		return nil, err
	}
	leaseUntil, err := evalArgInt64(args[2])
	if err != nil {
		return nil, err
	}

	for _, id := range c.zrangeByScoreNoLock(keys[1], float64(now), batch) {
		c.zremNoLock(keys[1], id)
		c.lpushNoLock(keys[0], id)
	}
	for _, id := range c.zrangeByScoreNoLock(keys[2], float64(now), batch) {
		c.zremNoLock(keys[2], id)
		msgKey := keys[3] + id
		c.hsetNoLock(msgKey, map[string]string{
			fieldToken:     "",
			fieldLeasedAt:  "0",
			fieldUpdatedAt: strconv.FormatInt(now, 10),
		})
		c.lpushNoLock(keys[0], id)
	}

	id := c.rpopNoLock(keys[0])
	if id == "" {
		return []any{}, nil
	}
	msgKey := keys[3] + id
	payload := c.hashes[msgKey][fieldPayload]
	if payload == "" {
		return []any{scriptErrMissingPayload, id}, nil
	}
	token := c.nextLeaseTokenNoLock(keys[4], now, id)
	attempts := parseInt(c.hashes[msgKey][fieldAttempts]) + 1
	c.hsetNoLock(msgKey, map[string]string{
		fieldAttempts:  strconv.Itoa(attempts),
		fieldToken:     token,
		fieldLeasedAt:  strconv.FormatInt(now, 10),
		fieldUpdatedAt: strconv.FormatInt(now, 10),
	})
	c.zaddNoLock(keys[2], float64(leaseUntil), id)
	return []any{
		id,
		payload,
		strconv.Itoa(attempts),
		token,
		c.hashes[msgKey][fieldAvailable],
		c.hashes[msgKey][fieldCreatedAt],
		c.hashes[msgKey][fieldLastError],
	}, nil
}

func (c *fakeClient) evalAck(keys []string, args ...any) (any, error) {
	if len(keys) != 3 || len(args) < 2 {
		return nil, fmt.Errorf("invalid ack eval arguments")
	}
	id := fmt.Sprint(args[0])
	token := fmt.Sprint(args[1])
	msgKey := keys[0]
	current := c.hashes[msgKey][fieldToken]
	if current == "" {
		return []any{scriptErrTokenMissing, id}, nil
	}
	if current != token {
		return []any{scriptErrTokenMismatch, id}, nil
	}

	attempts := c.hashes[msgKey][fieldAttempts]
	createdAt := c.hashes[msgKey][fieldCreatedAt]
	c.zremNoLock(keys[1], id)
	c.zremNoLock(keys[2], id)
	delete(c.hashes, msgKey)
	return []any{attempts, createdAt}, nil
}

func (c *fakeClient) evalNack(keys []string, args ...any) (any, error) {
	if len(keys) != 5 || len(args) < 6 {
		return nil, fmt.Errorf("invalid nack eval arguments")
	}
	id := fmt.Sprint(args[0])
	token := fmt.Sprint(args[1])
	now, err := evalArgInt64(args[2])
	if err != nil {
		return nil, err
	}
	disposition := fmt.Sprint(args[3])
	delay, err := evalArgInt64(args[4])
	if err != nil {
		return nil, err
	}
	reason := fmt.Sprint(args[5])

	msgKey := keys[0]
	current := c.hashes[msgKey][fieldToken]
	if current == "" {
		return []any{scriptErrTokenMissing, id}, nil
	}
	if current != token {
		return []any{scriptErrTokenMismatch, id}, nil
	}
	attempts := c.hashes[msgKey][fieldAttempts]
	createdAt := c.hashes[msgKey][fieldCreatedAt]

	c.zremNoLock(keys[3], id)
	c.zremNoLock(keys[2], id)

	switch disposition {
	case string(queue.NackDispositionDeadLetter):
		c.hsetNoLock(msgKey, map[string]string{
			fieldToken:     "",
			fieldLeasedAt:  "0",
			fieldUpdatedAt: strconv.FormatInt(now, 10),
			fieldLastError: reason,
			fieldDeadAt:    strconv.FormatInt(now, 10),
		})
		c.lpushNoLock(keys[4], id)
		return []any{attempts, createdAt, "0"}, nil
	case string(queue.NackDispositionRetry):
		availableAt := now + delay
		c.hsetNoLock(msgKey, map[string]string{
			fieldToken:     "",
			fieldLeasedAt:  "0",
			fieldUpdatedAt: strconv.FormatInt(now, 10),
			fieldLastError: reason,
			fieldAvailable: strconv.FormatInt(availableAt, 10),
		})
		if delay > 0 {
			c.zaddNoLock(keys[2], float64(availableAt), id)
		} else {
			c.lpushNoLock(keys[1], id)
		}
		return []any{attempts, createdAt, strconv.FormatInt(availableAt, 10)}, nil
	default:
		delete(c.hashes, msgKey)
		return []any{attempts, createdAt, "0"}, nil
	}
}

func (c *fakeClient) evalExtendLease(keys []string, args ...any) (any, error) {
	if len(keys) != 2 || len(args) < 4 {
		return nil, fmt.Errorf("invalid extend lease eval arguments")
	}
	id := fmt.Sprint(args[0])
	token := fmt.Sprint(args[1])
	now, err := evalArgInt64(args[2])
	if err != nil {
		return nil, err
	}
	leaseUntil, err := evalArgInt64(args[3])
	if err != nil {
		return nil, err
	}

	msgKey := keys[0]
	current := c.hashes[msgKey][fieldToken]
	if current == "" {
		return []any{scriptErrTokenMissing, id}, nil
	}
	if current != token {
		return []any{scriptErrTokenMismatch, id}, nil
	}
	attempts := c.hashes[msgKey][fieldAttempts]
	createdAt := c.hashes[msgKey][fieldCreatedAt]

	c.hsetNoLock(msgKey, map[string]string{
		fieldLeasedAt:  strconv.FormatInt(now, 10),
		fieldUpdatedAt: strconv.FormatInt(now, 10),
	})
	c.zaddNoLock(keys[1], float64(leaseUntil), id)
	return []any{attempts, createdAt}, nil
}

func (c *fakeClient) hsetNoLock(key string, values map[string]string) {
	if _, ok := c.hashes[key]; !ok {
		c.hashes[key] = make(map[string]string)
	}
	for field, value := range values {
		c.hashes[key][field] = value
	}
}

func (c *fakeClient) lpushNoLock(key string, values ...string) {
	list := c.lists[key]
	for _, value := range values {
		list = append([]string{value}, list...)
	}
	c.lists[key] = list
}

func (c *fakeClient) rpopNoLock(key string) string {
	list := c.lists[key]
	if len(list) == 0 {
		return ""
	}
	value := list[len(list)-1]
	c.lists[key] = list[:len(list)-1]
	return value
}

func (c *fakeClient) zaddNoLock(key string, score float64, member string) {
	if _, ok := c.zsets[key]; !ok {
		c.zsets[key] = make(map[string]float64)
	}
	c.zsets[key][member] = score
}

func (c *fakeClient) zremNoLock(key string, members ...string) {
	for _, member := range members {
		delete(c.zsets[key], member)
	}
	if len(c.zsets[key]) == 0 {
		delete(c.zsets, key)
	}
}

func (c *fakeClient) zrangeByScoreNoLock(key string, max float64, limit int64) []string {
	var items []ZItem
	for member, score := range c.zsets[key] {
		if score <= max {
			items = append(items, ZItem{Member: member, Score: score})
		}
	}
	if len(items) == 0 {
		return nil
	}
	sortZItems(items)
	if limit > 0 && int64(len(items)) > limit {
		items = items[:limit]
	}
	out := make([]string, len(items))
	for idx := range items {
		out[idx] = items[idx].Member
	}
	return out
}

func (c *fakeClient) nextLeaseTokenNoLock(counterKey string, now int64, id string) string {
	key := "__lease_counter__:" + counterKey
	seq := parseInt(c.hashes[key]["seq"]) + 1
	c.hsetNoLock(key, map[string]string{
		"seq": strconv.Itoa(seq),
	})
	return fmt.Sprintf("%s:%d:%d", id, now, seq)
}

func evalArgInt64(raw any) (int64, error) {
	switch value := raw.(type) {
	case int64:
		return value, nil
	case int:
		return int64(value), nil
	case float64:
		return int64(value), nil
	case string:
		if value == "" {
			return 0, nil
		}
		return strconv.ParseInt(value, 10, 64)
	default:
		return 0, fmt.Errorf("unsupported argument type %T", raw)
	}
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

type stubStorageNoStatus struct{}

func (stubStorageNoStatus) Enqueue(context.Context, *job.ExecutionMessage) (queue.EnqueueReceipt, error) {
	return queue.EnqueueReceipt{}, nil
}

func (stubStorageNoStatus) Dequeue(context.Context) (*job.ExecutionMessage, queue.Receipt, error) {
	return nil, queue.Receipt{}, nil
}

func (stubStorageNoStatus) Ack(context.Context, queue.Receipt) error {
	return nil
}

func (stubStorageNoStatus) Nack(context.Context, queue.Receipt, queue.NackOptions) error {
	return nil
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
