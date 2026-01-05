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
	"github.com/goliatone/go-job/queue/cancellation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegistryValidation(t *testing.T) {
	registry := NewRegistry()

	err := registry.Add(nil, nil)
	assert.Error(t, err)

	missingID := &testTask{id: "", path: "/tmp/task"}
	err = registry.Add(missingID, nil)
	assert.Error(t, err)

	missingPath := &testTask{id: "task", path: ""}
	err = registry.Add(missingPath, nil)
	assert.Error(t, err)

	valid := &testTask{id: "task", path: "/tmp/task"}
	require.NoError(t, registry.Add(valid, nil))
	assert.Error(t, registry.Add(valid, nil))
}

func TestWorkerAcksOnSuccess(t *testing.T) {
	dequeuer := &fakeDequeuer{deliveries: make(chan queue.Delivery, 1)}
	ackCh := make(chan struct{}, 1)
	delivery := &fakeDelivery{
		msg:      &job.ExecutionMessage{JobID: "job", ScriptPath: "/tmp/job"},
		attempts: 1,
		ackCh:    ackCh,
	}
	dequeuer.deliveries <- delivery

	var starts int32
	var successes int32
	hook := HookFuncs{
		OnStartFunc: func(context.Context, Event) {
			atomic.AddInt32(&starts, 1)
		},
		OnSuccessFunc: func(context.Context, Event) {
			atomic.AddInt32(&successes, 1)
		},
	}

	worker := NewWorker(dequeuer, WithConcurrency(1), WithIdleDelay(0), WithHooks(hook))
	require.NoError(t, worker.Register(&testTask{id: "job", path: "/tmp/job"}))
	require.NoError(t, worker.Start(context.Background()))

	select {
	case <-ackCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ack")
	}

	require.NoError(t, worker.Stop(context.Background()))
	assert.Equal(t, int32(1), atomic.LoadInt32(&delivery.acked))
	assert.Equal(t, int32(0), atomic.LoadInt32(&delivery.nacked))
	assert.Equal(t, int32(1), atomic.LoadInt32(&starts))
	assert.Equal(t, int32(1), atomic.LoadInt32(&successes))
}

func TestWorkerNacksWithRetry(t *testing.T) {
	dequeuer := &fakeDequeuer{deliveries: make(chan queue.Delivery, 1)}
	nackCh := make(chan queue.NackOptions, 1)
	delivery := &fakeDelivery{
		msg:      &job.ExecutionMessage{JobID: "job", ScriptPath: "/tmp/job"},
		attempts: 1,
		nackCh:   nackCh,
	}
	dequeuer.deliveries <- delivery

	var retries int32
	hook := HookFuncs{
		OnRetryFunc: func(context.Context, Event) {
			atomic.AddInt32(&retries, 1)
		},
	}

	policy := DefaultRetryPolicy{
		MaxAttempts: 3,
		Backoff: BackoffConfig{
			Strategy: BackoffFixed,
			Interval: 10 * time.Millisecond,
		},
	}

	worker := NewWorker(dequeuer, WithConcurrency(1), WithIdleDelay(0), WithRetryPolicy(policy), WithHooks(hook))
	require.NoError(t, worker.Register(&testTask{id: "job", path: "/tmp/job", err: assert.AnError}))
	require.NoError(t, worker.Start(context.Background()))

	var opts queue.NackOptions
	select {
	case opts = <-nackCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for nack")
	}

	require.NoError(t, worker.Stop(context.Background()))
	assert.True(t, opts.Requeue)
	assert.False(t, opts.DeadLetter)
	assert.Equal(t, 10*time.Millisecond, opts.Delay)
	assert.Equal(t, int32(1), atomic.LoadInt32(&retries))
}

func TestWorkerRoutesToDLQOnFinalAttempt(t *testing.T) {
	dequeuer := &fakeDequeuer{deliveries: make(chan queue.Delivery, 1)}
	nackCh := make(chan queue.NackOptions, 1)
	delivery := &fakeDelivery{
		msg:      &job.ExecutionMessage{JobID: "job", ScriptPath: "/tmp/job"},
		attempts: 2,
		nackCh:   nackCh,
	}
	dequeuer.deliveries <- delivery

	var failures int32
	hook := HookFuncs{
		OnFailureFunc: func(context.Context, Event) {
			atomic.AddInt32(&failures, 1)
		},
	}

	policy := DefaultRetryPolicy{MaxAttempts: 2}

	worker := NewWorker(dequeuer, WithConcurrency(1), WithIdleDelay(0), WithRetryPolicy(policy), WithHooks(hook))
	require.NoError(t, worker.Register(&testTask{id: "job", path: "/tmp/job", err: assert.AnError}))
	require.NoError(t, worker.Start(context.Background()))

	var opts queue.NackOptions
	select {
	case opts = <-nackCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for nack")
	}

	require.NoError(t, worker.Stop(context.Background()))
	assert.True(t, opts.DeadLetter)
	assert.False(t, opts.Requeue)
	assert.Equal(t, int32(1), atomic.LoadInt32(&failures))
}

func TestWorkerDisablesTaskCommanderRetries(t *testing.T) {
	dequeuer := &fakeDequeuer{deliveries: make(chan queue.Delivery, 1)}
	nackCh := make(chan queue.NackOptions, 1)
	delivery := &fakeDelivery{
		msg:      &job.ExecutionMessage{JobID: "job", ScriptPath: "/tmp/job"},
		attempts: 1,
		nackCh:   nackCh,
	}
	dequeuer.deliveries <- delivery

	var execCount int32
	task := &testTask{
		id:   "job",
		path: "/tmp/job",
		cfg:  job.Config{Retries: 2},
		exec: func(context.Context, *job.ExecutionMessage) error {
			atomic.AddInt32(&execCount, 1)
			return assert.AnError
		},
	}

	worker := NewWorker(dequeuer, WithConcurrency(1), WithIdleDelay(0))
	require.NoError(t, worker.Register(task))
	require.NoError(t, worker.Start(context.Background()))

	select {
	case <-nackCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for nack")
	}

	require.NoError(t, worker.Stop(context.Background()))
	assert.Equal(t, int32(1), atomic.LoadInt32(&execCount))
}

func TestWorkerCancelsBeforeExecute(t *testing.T) {
	dequeuer := &fakeDequeuer{deliveries: make(chan queue.Delivery, 1)}
	nackCh := make(chan queue.NackOptions, 1)
	delivery := &fakeDelivery{
		msg:      &job.ExecutionMessage{JobID: "job", ScriptPath: "/tmp/job", IdempotencyKey: "cancel-key"},
		attempts: 1,
		nackCh:   nackCh,
	}
	dequeuer.deliveries <- delivery

	store := newFakeCancelStore()
	require.NoError(t, store.Request(context.Background(), cancellation.Request{Key: "cancel-key", Reason: "user"}))

	var execCount int32
	task := &testTask{
		id:   "job",
		path: "/tmp/job",
		exec: func(context.Context, *job.ExecutionMessage) error {
			atomic.AddInt32(&execCount, 1)
			return nil
		},
	}

	worker := NewWorker(dequeuer,
		WithConcurrency(1),
		WithIdleDelay(0),
		WithCancellationStore(store),
		WithCancelPollInterval(5*time.Millisecond),
	)
	require.NoError(t, worker.Register(task))
	require.NoError(t, worker.Start(context.Background()))

	var opts queue.NackOptions
	select {
	case opts = <-nackCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for nack")
	}

	require.NoError(t, worker.Stop(context.Background()))
	assert.Equal(t, int32(0), atomic.LoadInt32(&execCount))
	assert.False(t, opts.Requeue)
	assert.False(t, opts.DeadLetter)
	assert.Equal(t, "user", opts.Reason)
}

func TestWorkerCancelsDuringExecution(t *testing.T) {
	dequeuer := &fakeDequeuer{deliveries: make(chan queue.Delivery, 1)}
	nackCh := make(chan queue.NackOptions, 1)
	delivery := &fakeDelivery{
		msg:      &job.ExecutionMessage{JobID: "job", ScriptPath: "/tmp/job", IdempotencyKey: "cancel-key"},
		attempts: 1,
		nackCh:   nackCh,
	}
	dequeuer.deliveries <- delivery

	store := newFakeCancelStore()
	started := make(chan struct{})

	task := &testTask{
		id:   "job",
		path: "/tmp/job",
		exec: func(ctx context.Context, _ *job.ExecutionMessage) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		},
	}

	worker := NewWorker(dequeuer,
		WithConcurrency(1),
		WithIdleDelay(0),
		WithCancellationStore(store),
		WithCancelPollInterval(5*time.Millisecond),
	)
	require.NoError(t, worker.Register(task))
	require.NoError(t, worker.Start(context.Background()))

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for task start")
	}

	require.NoError(t, store.Request(context.Background(), cancellation.Request{Key: "cancel-key", Reason: "timeout"}))

	var opts queue.NackOptions
	select {
	case opts = <-nackCh:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for nack")
	}

	require.NoError(t, worker.Stop(context.Background()))
	assert.False(t, opts.Requeue)
	assert.False(t, opts.DeadLetter)
	assert.Equal(t, "timeout", opts.Reason)
}

type fakeDequeuer struct {
	deliveries chan queue.Delivery
}

func (d *fakeDequeuer) Dequeue(ctx context.Context) (queue.Delivery, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case delivery, ok := <-d.deliveries:
		if !ok {
			return nil, nil
		}
		return delivery, nil
	}
}

type fakeDelivery struct {
	msg      *job.ExecutionMessage
	attempts int
	acked    int32
	nacked   int32
	ackCh    chan struct{}
	nackCh   chan queue.NackOptions
}

func (d *fakeDelivery) Message() *job.ExecutionMessage {
	return d.msg
}

func (d *fakeDelivery) Ack(context.Context) error {
	atomic.AddInt32(&d.acked, 1)
	if d.ackCh != nil {
		select {
		case d.ackCh <- struct{}{}:
		default:
		}
	}
	return nil
}

func (d *fakeDelivery) Nack(_ context.Context, opts queue.NackOptions) error {
	atomic.AddInt32(&d.nacked, 1)
	if d.nackCh != nil {
		select {
		case d.nackCh <- opts:
		default:
		}
	}
	return nil
}

func (d *fakeDelivery) Attempts() int {
	return d.attempts
}

type fakeCancelStore struct {
	mu       sync.Mutex
	requests map[string]cancellation.Request
}

func newFakeCancelStore() *fakeCancelStore {
	return &fakeCancelStore{requests: make(map[string]cancellation.Request)}
}

func (s *fakeCancelStore) Request(_ context.Context, req cancellation.Request) error {
	if req.Key == "" {
		return fmt.Errorf("key required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.RequestedAt.IsZero() {
		req.RequestedAt = time.Now().UTC()
	}
	s.requests[req.Key] = req
	return nil
}

func (s *fakeCancelStore) Get(_ context.Context, key string) (cancellation.Request, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	req, ok := s.requests[key]
	return req, ok, nil
}

func (s *fakeCancelStore) Subscribe(ctx context.Context) (<-chan cancellation.Request, error) {
	ch := make(chan cancellation.Request)
	go func() {
		<-ctx.Done()
		close(ch)
	}()
	return ch, nil
}

type testTask struct {
	id   string
	path string
	cfg  job.Config
	err  error
	exec func(context.Context, *job.ExecutionMessage) error
}

func (t *testTask) GetID() string                        { return t.id }
func (t *testTask) GetHandler() func() error             { return func() error { return nil } }
func (t *testTask) GetHandlerConfig() job.HandlerOptions { return job.HandlerOptions{} }
func (t *testTask) GetConfig() job.Config                { return t.cfg }
func (t *testTask) GetPath() string                      { return t.path }
func (t *testTask) GetEngine() job.Engine                { return nil }
func (t *testTask) Execute(ctx context.Context, msg *job.ExecutionMessage) error {
	if t.exec != nil {
		return t.exec(ctx, msg)
	}
	return t.err
}
