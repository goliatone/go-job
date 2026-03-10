package worker

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	job "github.com/goliatone/go-job"
	"github.com/goliatone/go-job/queue"
	queueRedis "github.com/goliatone/go-job/queue/adapters/redis"
	qidempotency "github.com/goliatone/go-job/queue/idempotency"
	"github.com/stretchr/testify/require"
)

func TestQueueIntegrationStatusAndDownload(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	clock := newManualClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	client := newFakeRedisClient()
	storage := queueRedis.NewStorage(client,
		queueRedis.WithClock(clock.Now),
		queueRedis.WithVisibilityTimeout(5*time.Second),
		queueRedis.WithIDFunc(sequence("msg-1")),
		queueRedis.WithTokenFunc(sequence("token-1")),
	)
	adapter := queueRedis.NewAdapter(storage)

	store := newExportStore()
	done := make(chan struct{})
	var doneOnce sync.Once
	task := &trackingTask{id: "export", path: "/tmp/export", store: store}
	hook := HookFuncs{
		OnStartFunc: func(_ context.Context, event Event) {
			id := exportIDFromMessage(event.Message)
			if id != "" {
				store.SetStatus(id, "running")
			}
		},
		OnSuccessFunc: func(_ context.Context, event Event) {
			id := exportIDFromMessage(event.Message)
			if id != "" {
				store.SetStatus(id, "completed")
			}
			doneOnce.Do(func() {
				close(done)
			})
		},
	}

	worker := NewWorker(adapter, WithConcurrency(1), WithIdleDelay(0), WithHooks(hook))
	require.NoError(t, worker.Register(task))
	require.NoError(t, worker.Start(ctx))
	t.Cleanup(func() {
		_ = worker.Stop(context.Background())
	})

	exportID := "exp-123"
	store.SetStatus(exportID, "queued")
	msg := &job.ExecutionMessage{
		JobID:      task.id,
		ScriptPath: task.path,
		Parameters: map[string]any{"export_id": exportID},
	}
	_, err := adapter.Enqueue(ctx, msg)
	require.NoError(t, err)

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timeout waiting for export completion")
	}

	require.Equal(t, "completed", store.Status(exportID))
	require.Equal(t, "downloads/"+exportID+".csv", store.Download(exportID))
	require.False(t, client.HasKey("queue:msg:msg-1"))
}

func TestQueueIntegrationIdempotencyDropAcks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	clock := newManualClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	client := newFakeRedisClient()
	storage := queueRedis.NewStorage(client,
		queueRedis.WithClock(clock.Now),
		queueRedis.WithVisibilityTimeout(5*time.Second),
		queueRedis.WithIDFunc(sequence("msg-1", "msg-2")),
		queueRedis.WithTokenFunc(sequence("token-1", "token-2")),
	)
	adapter := queueRedis.NewAdapter(storage)

	tracker := job.NewIdempotencyTracker()
	factory := func(task job.Task) *job.TaskCommander {
		return job.NewTaskCommander(task).WithIdempotencyTracker(tracker)
	}

	var execCount int32
	task := &testTask{
		id:   "export",
		path: "/tmp/export",
		exec: func(context.Context, *job.ExecutionMessage) error {
			atomic.AddInt32(&execCount, 1)
			return nil
		},
	}

	var successes int32
	hook := HookFuncs{
		OnSuccessFunc: func(context.Context, Event) {
			atomic.AddInt32(&successes, 1)
		},
	}

	worker := NewWorker(adapter,
		WithConcurrency(1),
		WithIdleDelay(0),
		WithCommanderFactory(factory),
		WithHooks(hook),
	)
	require.NoError(t, worker.Register(task))
	require.NoError(t, worker.Start(ctx))
	t.Cleanup(func() {
		_ = worker.Stop(context.Background())
	})

	msg := &job.ExecutionMessage{
		JobID:          task.id,
		ScriptPath:     task.path,
		IdempotencyKey: "export-dup",
		DedupPolicy:    job.DedupPolicyDrop,
	}
	_, err := adapter.Enqueue(ctx, msg)
	require.NoError(t, err)
	_, err = adapter.Enqueue(ctx, msg)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&successes) == 2
	}, 2*time.Second, 10*time.Millisecond)

	require.Equal(t, int32(1), atomic.LoadInt32(&execCount))
	require.False(t, client.HasKey("queue:msg:msg-1"))
	require.False(t, client.HasKey("queue:msg:msg-2"))
	require.Equal(t, 0, client.ListLen("queue:dlq"))
}

func TestQueueIntegrationSharedIdempotencyDropsDuplicateAcrossWorkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	clock := newManualClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	client := newFakeRedisClient()
	storage := queueRedis.NewStorage(client,
		queueRedis.WithClock(clock.Now),
		queueRedis.WithVisibilityTimeout(5*time.Second),
		queueRedis.WithIDFunc(sequence("msg-1", "msg-2")),
		queueRedis.WithTokenFunc(sequence("token-1", "token-2")),
	)
	adapter := queueRedis.NewAdapter(storage)

	store := newIntegrationIdempotencyStore()
	var execCount int32
	var successCount int32
	task := &testTask{
		id:   "export",
		path: "/tmp/export",
		exec: func(context.Context, *job.ExecutionMessage) error {
			atomic.AddInt32(&execCount, 1)
			return nil
		},
	}
	hook := HookFuncs{
		OnSuccessFunc: func(context.Context, Event) {
			atomic.AddInt32(&successCount, 1)
		},
	}

	workerA := NewWorker(adapter,
		WithConcurrency(1),
		WithIdleDelay(0),
		WithHooks(hook),
		WithIdempotencyStore(store, time.Minute),
	)
	workerB := NewWorker(adapter,
		WithConcurrency(1),
		WithIdleDelay(0),
		WithHooks(hook),
		WithIdempotencyStore(store, time.Minute),
	)
	require.NoError(t, workerA.Register(task))
	require.NoError(t, workerB.Register(task))
	require.NoError(t, workerA.Start(ctx))
	require.NoError(t, workerB.Start(ctx))
	t.Cleanup(func() {
		_ = workerA.Stop(context.Background())
		_ = workerB.Stop(context.Background())
	})

	msg := &job.ExecutionMessage{
		JobID:          task.id,
		ScriptPath:     task.path,
		IdempotencyKey: "shared-dup",
		DedupPolicy:    job.DedupPolicyDrop,
	}
	_, err := adapter.Enqueue(ctx, msg)
	require.NoError(t, err)
	_, err = adapter.Enqueue(ctx, msg)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&successCount) == 2
	}, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, int32(1), atomic.LoadInt32(&execCount))
}

func TestQueueIntegrationManualReplayDropsWithSharedIdempotency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	clock := newManualClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	client := newFakeRedisClient()
	storage := queueRedis.NewStorage(client,
		queueRedis.WithClock(clock.Now),
		queueRedis.WithVisibilityTimeout(5*time.Second),
		queueRedis.WithIDFunc(sequence("msg-1", "msg-2")),
		queueRedis.WithTokenFunc(sequence("token-1", "token-2")),
	)
	adapter := queueRedis.NewAdapter(storage)

	store := newIntegrationIdempotencyStore()
	var execCount int32
	var successCount int32
	task := &testTask{
		id:   "export",
		path: "/tmp/export",
		exec: func(context.Context, *job.ExecutionMessage) error {
			atomic.AddInt32(&execCount, 1)
			return nil
		},
	}
	hook := HookFuncs{
		OnSuccessFunc: func(context.Context, Event) {
			atomic.AddInt32(&successCount, 1)
		},
	}

	worker := NewWorker(adapter,
		WithConcurrency(1),
		WithIdleDelay(0),
		WithHooks(hook),
		WithIdempotencyStore(store, time.Minute),
	)
	require.NoError(t, worker.Register(task))
	require.NoError(t, worker.Start(ctx))
	t.Cleanup(func() {
		_ = worker.Stop(context.Background())
	})

	msg := &job.ExecutionMessage{
		JobID:          task.id,
		ScriptPath:     task.path,
		IdempotencyKey: "manual-replay",
		DedupPolicy:    job.DedupPolicyDrop,
	}
	_, err := adapter.Enqueue(ctx, msg)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&successCount) == 1
	}, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, int32(1), atomic.LoadInt32(&execCount))

	_, err = adapter.Enqueue(ctx, msg)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&successCount) == 2
	}, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, int32(1), atomic.LoadInt32(&execCount))
}

func TestQueueIntegrationRetryAndDLQ(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	clock := newManualClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	client := newFakeRedisClient()
	storage := queueRedis.NewStorage(client,
		queueRedis.WithClock(clock.Now),
		queueRedis.WithVisibilityTimeout(5*time.Second),
		queueRedis.WithIDFunc(sequence("msg-1")),
		queueRedis.WithTokenFunc(sequence("token-1", "token-2")),
	)
	adapter := queueRedis.NewAdapter(storage)

	attempts := make(chan int, 2)
	task := &testTask{
		id:   "export",
		path: "/tmp/export",
		exec: func(context.Context, *job.ExecutionMessage) error {
			attempts <- 1
			return fmt.Errorf("boom")
		},
	}

	policy := DefaultRetryPolicy{
		MaxAttempts: 2,
		Backoff: BackoffConfig{
			Strategy: BackoffFixed,
			Interval: 5 * time.Second,
		},
	}

	worker := NewWorker(adapter,
		WithConcurrency(1),
		WithIdleDelay(0),
		WithRetryPolicy(policy),
	)
	require.NoError(t, worker.Register(task))
	require.NoError(t, worker.Start(ctx))
	t.Cleanup(func() {
		_ = worker.Stop(context.Background())
	})

	msg := &job.ExecutionMessage{JobID: task.id, ScriptPath: task.path}
	_, err := adapter.Enqueue(ctx, msg)
	require.NoError(t, err)

	select {
	case <-attempts:
	case <-ctx.Done():
		t.Fatal("timeout waiting for first attempt")
	}

	require.Eventually(t, func() bool {
		return client.ZSetLen("queue:delayed") == 1
	}, 2*time.Second, 10*time.Millisecond)

	clock.Advance(5 * time.Second)

	select {
	case <-attempts:
	case <-ctx.Done():
		t.Fatal("timeout waiting for second attempt")
	}

	require.Eventually(t, func() bool {
		return client.ListLen("queue:dlq") == 1
	}, 2*time.Second, 10*time.Millisecond)
}

func TestQueueIntegrationStaleResumeTerminalErrorGoesToDLQWithoutRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	clock := newManualClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	client := newFakeRedisClient()
	storage := queueRedis.NewStorage(client,
		queueRedis.WithClock(clock.Now),
		queueRedis.WithVisibilityTimeout(5*time.Second),
		queueRedis.WithIDFunc(sequence("msg-1")),
		queueRedis.WithTokenFunc(sequence("token-1")),
	)
	adapter := queueRedis.NewAdapter(storage)

	var attempts int32
	task := &testTask{
		id:   "export",
		path: "/tmp/export",
		exec: func(context.Context, *job.ExecutionMessage) error {
			atomic.AddInt32(&attempts, 1)
			return job.NewTerminalError(
				job.TerminalErrorCodeStaleStateMismatch,
				"stale resume expected version mismatch",
				errors.New("version mismatch"),
			)
		},
	}

	policy := DefaultRetryPolicy{
		MaxAttempts: 5,
		Backoff: BackoffConfig{
			Strategy: BackoffFixed,
			Interval: 5 * time.Second,
		},
	}

	worker := NewWorker(adapter,
		WithConcurrency(1),
		WithIdleDelay(0),
		WithRetryPolicy(policy),
	)
	require.NoError(t, worker.Register(task))
	require.NoError(t, worker.Start(ctx))
	t.Cleanup(func() {
		_ = worker.Stop(context.Background())
	})

	_, err := adapter.Enqueue(ctx, &job.ExecutionMessage{JobID: task.id, ScriptPath: task.path})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return client.ListLen("queue:dlq") == 1
	}, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, int32(1), atomic.LoadInt32(&attempts))
	require.Equal(t, 0, client.ZSetLen("queue:delayed"))
}

type trackingTask struct {
	id    string
	path  string
	store *exportStore
}

func (t *trackingTask) GetID() string                        { return t.id }
func (t *trackingTask) GetHandler() func() error             { return func() error { return nil } }
func (t *trackingTask) GetHandlerConfig() job.HandlerOptions { return job.HandlerOptions{} }
func (t *trackingTask) GetConfig() job.Config                { return job.Config{} }
func (t *trackingTask) GetPath() string                      { return t.path }
func (t *trackingTask) GetEngine() job.Engine                { return nil }
func (t *trackingTask) Execute(_ context.Context, msg *job.ExecutionMessage) error {
	if t == nil || t.store == nil {
		return fmt.Errorf("export store required")
	}
	exportID := exportIDFromMessage(msg)
	if exportID == "" {
		return fmt.Errorf("export_id required")
	}
	t.store.SetDownload(exportID, "downloads/"+exportID+".csv")
	return nil
}

type exportStore struct {
	mu       sync.Mutex
	status   map[string]string
	download map[string]string
}

func newExportStore() *exportStore {
	return &exportStore{
		status:   make(map[string]string),
		download: make(map[string]string),
	}
}

func (s *exportStore) SetStatus(id, status string) {
	if s == nil || id == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status[id] = status
}

func (s *exportStore) Status(id string) string {
	if s == nil || id == "" {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status[id]
}

func (s *exportStore) SetDownload(id, value string) {
	if s == nil || id == "" {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.download[id] = value
}

func (s *exportStore) Download(id string) string {
	if s == nil || id == "" {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.download[id]
}

func exportIDFromMessage(msg *job.ExecutionMessage) string {
	if msg == nil || msg.Parameters == nil {
		return ""
	}
	if value, ok := msg.Parameters["export_id"]; ok {
		if id, ok := value.(string); ok {
			return id
		}
	}
	return ""
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

type fakeRedisClient struct {
	mu     sync.Mutex
	hashes map[string]map[string]string
	lists  map[string][]string
	zsets  map[string]map[string]float64
}

func newFakeRedisClient() *fakeRedisClient {
	return &fakeRedisClient{
		hashes: make(map[string]map[string]string),
		lists:  make(map[string][]string),
		zsets:  make(map[string]map[string]float64),
	}
}

func (c *fakeRedisClient) HSet(_ context.Context, key string, values map[string]string) error {
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

func (c *fakeRedisClient) HGetAll(_ context.Context, key string) (map[string]string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]string)
	for field, value := range c.hashes[key] {
		out[field] = value
	}
	return out, nil
}

func (c *fakeRedisClient) HGet(_ context.Context, key, field string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if fields, ok := c.hashes[key]; ok {
		return fields[field], nil
	}
	return "", nil
}

func (c *fakeRedisClient) HDel(_ context.Context, key string, fields ...string) error {
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

func (c *fakeRedisClient) LPush(_ context.Context, key string, values ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	list := c.lists[key]
	for _, value := range values {
		list = append([]string{value}, list...)
	}
	c.lists[key] = list
	return nil
}

func (c *fakeRedisClient) RPop(_ context.Context, key string) (string, error) {
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

func (c *fakeRedisClient) ZAdd(_ context.Context, key string, score float64, member string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.zsets[key]; !ok {
		c.zsets[key] = make(map[string]float64)
	}
	c.zsets[key][member] = score
	return nil
}

func (c *fakeRedisClient) ZRem(_ context.Context, key string, members ...string) error {
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

func (c *fakeRedisClient) ZRangeByScore(_ context.Context, key string, max float64, limit int64) ([]queueRedis.ZItem, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var items []queueRedis.ZItem
	for member, score := range c.zsets[key] {
		if score <= max {
			items = append(items, queueRedis.ZItem{Member: member, Score: score})
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

func (c *fakeRedisClient) Eval(_ context.Context, script string, keys []string, args ...any) (any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch script {
	case queueRedis.DequeueScript:
		return c.evalDequeue(keys, args...)
	case queueRedis.AckScript:
		return c.evalAck(keys, args...)
	case queueRedis.NackScript:
		return c.evalNack(keys, args...)
	case queueRedis.ExtendLeaseScript:
		return c.evalExtendLease(keys, args...)
	default:
		return nil, fmt.Errorf("unsupported script")
	}
}

func (c *fakeRedisClient) Del(_ context.Context, keys ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, key := range keys {
		delete(c.hashes, key)
		delete(c.lists, key)
		delete(c.zsets, key)
	}
	return nil
}

func (c *fakeRedisClient) Expire(_ context.Context, _ string, _ time.Duration) error {
	return nil
}

func (c *fakeRedisClient) evalDequeue(keys []string, args ...any) (any, error) {
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
			"token":      "",
			"leased_at":  "0",
			"updated_at": strconv.FormatInt(now, 10),
		})
		c.lpushNoLock(keys[0], id)
	}

	id := c.rpopNoLock(keys[0])
	if id == "" {
		return []any{}, nil
	}
	msgKey := keys[3] + id
	payload := c.hashes[msgKey]["payload"]
	if payload == "" {
		return []any{"ERR_MISSING_PAYLOAD", id}, nil
	}
	token := c.nextLeaseTokenNoLock(keys[4], now, id)
	attempts := parseStringInt(c.hashes[msgKey]["attempts"]) + 1
	c.hsetNoLock(msgKey, map[string]string{
		"attempts":   strconv.Itoa(attempts),
		"token":      token,
		"leased_at":  strconv.FormatInt(now, 10),
		"updated_at": strconv.FormatInt(now, 10),
	})
	c.zaddNoLock(keys[2], float64(leaseUntil), id)
	return []any{
		id,
		payload,
		strconv.Itoa(attempts),
		token,
		c.hashes[msgKey]["available_at"],
		c.hashes[msgKey]["created_at"],
		c.hashes[msgKey]["last_error"],
	}, nil
}

func (c *fakeRedisClient) evalAck(keys []string, args ...any) (any, error) {
	if len(keys) != 3 || len(args) < 2 {
		return nil, fmt.Errorf("invalid ack eval arguments")
	}
	id := fmt.Sprint(args[0])
	token := fmt.Sprint(args[1])
	msgKey := keys[0]
	current := c.hashes[msgKey]["token"]
	if current == "" {
		return []any{"ERR_TOKEN_MISSING", id}, nil
	}
	if current != token {
		return []any{"ERR_TOKEN_MISMATCH", id}, nil
	}
	attempts := c.hashes[msgKey]["attempts"]
	createdAt := c.hashes[msgKey]["created_at"]
	c.zremNoLock(keys[1], id)
	c.zremNoLock(keys[2], id)
	delete(c.hashes, msgKey)
	return []any{attempts, createdAt}, nil
}

func (c *fakeRedisClient) evalNack(keys []string, args ...any) (any, error) {
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
	current := c.hashes[msgKey]["token"]
	if current == "" {
		return []any{"ERR_TOKEN_MISSING", id}, nil
	}
	if current != token {
		return []any{"ERR_TOKEN_MISMATCH", id}, nil
	}
	attempts := c.hashes[msgKey]["attempts"]
	createdAt := c.hashes[msgKey]["created_at"]

	c.zremNoLock(keys[3], id)
	c.zremNoLock(keys[2], id)

	switch disposition {
	case string(queue.NackDispositionDeadLetter):
		c.hsetNoLock(msgKey, map[string]string{
			"token":            "",
			"leased_at":        "0",
			"updated_at":       strconv.FormatInt(now, 10),
			"last_error":       reason,
			"dead_lettered_at": strconv.FormatInt(now, 10),
		})
		c.lpushNoLock(keys[4], id)
		return []any{attempts, createdAt, "0"}, nil
	case string(queue.NackDispositionRetry):
		availableAt := now + delay
		c.hsetNoLock(msgKey, map[string]string{
			"token":        "",
			"leased_at":    "0",
			"updated_at":   strconv.FormatInt(now, 10),
			"last_error":   reason,
			"available_at": strconv.FormatInt(availableAt, 10),
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

func (c *fakeRedisClient) evalExtendLease(keys []string, args ...any) (any, error) {
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
	current := c.hashes[msgKey]["token"]
	if current == "" {
		return []any{"ERR_TOKEN_MISSING", id}, nil
	}
	if current != token {
		return []any{"ERR_TOKEN_MISMATCH", id}, nil
	}
	attempts := c.hashes[msgKey]["attempts"]
	createdAt := c.hashes[msgKey]["created_at"]
	c.hsetNoLock(msgKey, map[string]string{
		"leased_at":  strconv.FormatInt(now, 10),
		"updated_at": strconv.FormatInt(now, 10),
	})
	c.zaddNoLock(keys[1], float64(leaseUntil), id)
	return []any{attempts, createdAt}, nil
}

func (c *fakeRedisClient) hsetNoLock(key string, values map[string]string) {
	if _, ok := c.hashes[key]; !ok {
		c.hashes[key] = make(map[string]string)
	}
	for field, value := range values {
		c.hashes[key][field] = value
	}
}

func (c *fakeRedisClient) lpushNoLock(key string, values ...string) {
	list := c.lists[key]
	for _, value := range values {
		list = append([]string{value}, list...)
	}
	c.lists[key] = list
}

func (c *fakeRedisClient) rpopNoLock(key string) string {
	list := c.lists[key]
	if len(list) == 0 {
		return ""
	}
	value := list[len(list)-1]
	c.lists[key] = list[:len(list)-1]
	return value
}

func (c *fakeRedisClient) zaddNoLock(key string, score float64, member string) {
	if _, ok := c.zsets[key]; !ok {
		c.zsets[key] = make(map[string]float64)
	}
	c.zsets[key][member] = score
}

func (c *fakeRedisClient) zremNoLock(key string, members ...string) {
	for _, member := range members {
		delete(c.zsets[key], member)
	}
	if len(c.zsets[key]) == 0 {
		delete(c.zsets, key)
	}
}

func (c *fakeRedisClient) zrangeByScoreNoLock(key string, max float64, limit int64) []string {
	var items []queueRedis.ZItem
	for member, score := range c.zsets[key] {
		if score <= max {
			items = append(items, queueRedis.ZItem{Member: member, Score: score})
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

func (c *fakeRedisClient) nextLeaseTokenNoLock(counterKey string, now int64, id string) string {
	key := "__lease_counter__:" + counterKey
	seq := parseStringInt(c.hashes[key]["seq"]) + 1
	c.hsetNoLock(key, map[string]string{
		"seq": strconv.Itoa(seq),
	})
	return fmt.Sprintf("%s:%d:%d", id, now, seq)
}

func parseStringInt(value string) int {
	if value == "" {
		return 0
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return parsed
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

func (c *fakeRedisClient) HasKey(key string) bool {
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

func (c *fakeRedisClient) ListLen(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.lists[key])
}

func (c *fakeRedisClient) ZSetLen(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.zsets[key])
}

func sortZItems(items []queueRedis.ZItem) {
	for i := 1; i < len(items); i++ {
		for j := i; j > 0 && items[j-1].Score > items[j].Score; j-- {
			items[j], items[j-1] = items[j-1], items[j]
		}
	}
}

func sequence(values ...string) func() string {
	var index atomic.Int64
	return func() string {
		i := int(index.Add(1) - 1)
		if i >= len(values) {
			return ""
		}
		value := values[i]
		return value
	}
}

type integrationIdempotencyStore struct {
	mu      sync.Mutex
	records map[string]qidempotency.Record
}

func newIntegrationIdempotencyStore() *integrationIdempotencyStore {
	return &integrationIdempotencyStore{
		records: make(map[string]qidempotency.Record),
	}
}

func (s *integrationIdempotencyStore) Acquire(_ context.Context, key string, ttl time.Duration) (qidempotency.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	if record, ok := s.records[key]; ok && !qidempotency.IsExpired(record, now) {
		return record, false, nil
	}
	record := qidempotency.Record{
		Key:       key,
		Status:    qidempotency.StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if ttl > 0 {
		record.ExpiresAt = now.Add(ttl)
	}
	s.records[key] = record
	return record, true, nil
}

func (s *integrationIdempotencyStore) Get(_ context.Context, key string) (qidempotency.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[key]
	if !ok || qidempotency.IsExpired(record, time.Now().UTC()) {
		return qidempotency.Record{}, false, nil
	}
	return record, true, nil
}

func (s *integrationIdempotencyStore) Update(_ context.Context, key string, update qidempotency.Update) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[key]
	if !ok {
		return qidempotency.ErrNotFound
	}
	if update.Status != nil {
		record.Status = qidempotency.DefaultStatus(*update.Status)
	}
	if update.Payload != nil {
		payload := *update.Payload
		if len(payload) == 0 {
			record.Payload = nil
		} else {
			record.Payload = append([]byte(nil), payload...)
		}
	}
	if update.ExpiresAt != nil {
		record.ExpiresAt = *update.ExpiresAt
	}
	record.UpdatedAt = time.Now().UTC()
	s.records[key] = record
	return nil
}

func (s *integrationIdempotencyStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, key)
	return nil
}

var _ queueRedis.Client = (*fakeRedisClient)(nil)
var _ queue.Dequeuer = (*queueRedis.Adapter)(nil)
var _ qidempotency.Store = (*integrationIdempotencyStore)(nil)
