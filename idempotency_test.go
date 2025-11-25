package job_test

import (
	"context"
	"errors"
	"testing"

	"github.com/goliatone/go-job"
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
