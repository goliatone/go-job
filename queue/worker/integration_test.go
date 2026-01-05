package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	job "github.com/goliatone/go-job"
	"github.com/goliatone/go-job/queue"
	queueRedis "github.com/goliatone/go-job/queue/adapters/redis"
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
	require.NoError(t, adapter.Enqueue(ctx, msg))

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
	require.NoError(t, adapter.Enqueue(ctx, msg))
	require.NoError(t, adapter.Enqueue(ctx, msg))

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&successes) == 2
	}, 2*time.Second, 10*time.Millisecond)

	require.Equal(t, int32(1), atomic.LoadInt32(&execCount))
	require.False(t, client.HasKey("queue:msg:msg-1"))
	require.False(t, client.HasKey("queue:msg:msg-2"))
	require.Equal(t, 0, client.ListLen("queue:dlq"))
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
	require.NoError(t, adapter.Enqueue(ctx, msg))

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

var _ queueRedis.Client = (*fakeRedisClient)(nil)
var _ queue.Dequeuer = (*queueRedis.Adapter)(nil)
