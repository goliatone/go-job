package job_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/goliatone/go-job"
	qidempotency "github.com/goliatone/go-job/queue/idempotency"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type countingTask struct {
	id    string
	path  string
	cfg   job.Config
	count int
	err   error
}

func (t *countingTask) GetID() string                        { return t.id }
func (t *countingTask) GetHandler() func() error             { return func() error { return nil } }
func (t *countingTask) GetHandlerConfig() job.HandlerOptions { return job.HandlerOptions{} }
func (t *countingTask) GetConfig() job.Config                { return t.cfg }
func (t *countingTask) GetPath() string                      { return t.path }
func (t *countingTask) GetEngine() job.Engine                { return nil }
func (t *countingTask) Execute(_ context.Context, _ *job.ExecutionMessage) error {
	t.count++
	return t.err
}

func TestIdempotencyDropSkipsDuplicate(t *testing.T) {
	task := &countingTask{id: "drop-task", path: "/tmp/drop", cfg: job.Config{}}
	tracker := job.NewIdempotencyTracker()
	cmd := job.NewTaskCommander(task).WithIdempotencyTracker(tracker)

	msg := &job.ExecutionMessage{JobID: task.id, ScriptPath: task.path, IdempotencyKey: "key-1", DedupPolicy: job.DedupPolicyDrop}

	require.NoError(t, cmd.Execute(context.Background(), msg))
	err := cmd.Execute(context.Background(), msg)
	require.ErrorIs(t, err, job.ErrIdempotentDrop)

	assert.Equal(t, 1, task.count, "task should execute only once under drop policy")
}

func TestIdempotencyMergeReusesResult(t *testing.T) {
	task := &countingTask{id: "merge-task", path: "/tmp/merge", cfg: job.Config{}}
	tracker := job.NewIdempotencyTracker()
	cmd := job.NewTaskCommander(task).WithIdempotencyTracker(tracker)

	msg := &job.ExecutionMessage{JobID: task.id, ScriptPath: task.path, IdempotencyKey: "key-merge", DedupPolicy: job.DedupPolicyMerge}

	require.NoError(t, cmd.Execute(context.Background(), msg))
	err := cmd.Execute(context.Background(), msg)
	require.NoError(t, err, "merge should return prior result (nil)")
	assert.Equal(t, 1, task.count, "merge should not re-execute task")
}

func TestIdempotencyMergePropagatesPreviousError(t *testing.T) {
	task := &countingTask{id: "merge-error-task", path: "/tmp/merge-error", cfg: job.Config{}, err: errors.New("boom")}
	tracker := job.NewIdempotencyTracker()
	cmd := job.NewTaskCommander(task).WithIdempotencyTracker(tracker)

	msg := &job.ExecutionMessage{JobID: task.id, ScriptPath: task.path, IdempotencyKey: "key-merge-error", DedupPolicy: job.DedupPolicyMerge}

	err := cmd.Execute(context.Background(), msg)
	require.EqualError(t, err, "boom")
	task.err = nil // second attempt should not run, but preserve previous error

	err = cmd.Execute(context.Background(), msg)
	require.EqualError(t, err, "boom")
	assert.Equal(t, 1, task.count, "merge should not execute again after error")
}

func TestIdempotencyReplaceAllowsSubsequentRun(t *testing.T) {
	task := &countingTask{id: "replace-task", path: "/tmp/replace", cfg: job.Config{}}
	tracker := job.NewIdempotencyTracker()
	cmd := job.NewTaskCommander(task).WithIdempotencyTracker(tracker)

	msg := &job.ExecutionMessage{JobID: task.id, ScriptPath: task.path, IdempotencyKey: "key-replace", DedupPolicy: job.DedupPolicyReplace}

	require.NoError(t, cmd.Execute(context.Background(), msg))
	require.NoError(t, cmd.Execute(context.Background(), msg))
	assert.Equal(t, 2, task.count, "replace should allow new execution and override previous entry")
}

func TestSharedIdempotencyStoreDropAcrossCommanders(t *testing.T) {
	task := &countingTask{id: "shared-drop-task", path: "/tmp/shared-drop", cfg: job.Config{}}
	store := newSharedMemoryStore()

	cmdA := job.NewTaskCommander(task).WithSharedIdempotencyStore(store, time.Minute)
	cmdB := job.NewTaskCommander(task).WithSharedIdempotencyStore(store, time.Minute)

	msg := &job.ExecutionMessage{
		JobID:          task.id,
		ScriptPath:     task.path,
		IdempotencyKey: "shared-key",
		DedupPolicy:    job.DedupPolicyDrop,
	}

	require.NoError(t, cmdA.Execute(context.Background(), msg))
	err := cmdB.Execute(context.Background(), msg)
	require.ErrorIs(t, err, job.ErrIdempotentDrop)
	assert.Equal(t, 1, task.count, "task should execute once across commanders sharing a store")
}

func TestSharedIdempotencyStoreMergePropagatesError(t *testing.T) {
	task := &countingTask{id: "shared-merge-task", path: "/tmp/shared-merge", cfg: job.Config{}, err: errors.New("boom")}
	store := newSharedMemoryStore()

	cmdA := job.NewTaskCommander(task).WithSharedIdempotencyStore(store, time.Minute)
	cmdB := job.NewTaskCommander(task).WithSharedIdempotencyStore(store, time.Minute)

	msg := &job.ExecutionMessage{
		JobID:          task.id,
		ScriptPath:     task.path,
		IdempotencyKey: "shared-merge-key",
		DedupPolicy:    job.DedupPolicyMerge,
	}

	err := cmdA.Execute(context.Background(), msg)
	require.EqualError(t, err, "boom")
	task.err = nil

	err = cmdB.Execute(context.Background(), msg)
	require.EqualError(t, err, "boom")
	assert.Equal(t, 1, task.count, "merge should not re-execute across commanders")
}

type sharedMemoryStore struct {
	mu      sync.Mutex
	records map[string]qidempotency.Record
}

func newSharedMemoryStore() *sharedMemoryStore {
	return &sharedMemoryStore{
		records: make(map[string]qidempotency.Record),
	}
}

func (s *sharedMemoryStore) Acquire(_ context.Context, key string, ttl time.Duration) (qidempotency.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	record, ok := s.records[key]
	if ok && !qidempotency.IsExpired(record, now) {
		return record, false, nil
	}
	record = qidempotency.Record{
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

func (s *sharedMemoryStore) Get(_ context.Context, key string) (qidempotency.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[key]
	if !ok || qidempotency.IsExpired(record, time.Now().UTC()) {
		return qidempotency.Record{}, false, nil
	}
	return record, true, nil
}

func (s *sharedMemoryStore) Update(_ context.Context, key string, update qidempotency.Update) error {
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

func (s *sharedMemoryStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, key)
	return nil
}

var _ qidempotency.Store = (*sharedMemoryStore)(nil)
