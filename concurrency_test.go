package job_test

import (
	"context"
	"testing"
	"time"

	"github.com/goliatone/go-job"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConcurrencyLimiterBlocksWhenLimitReached(t *testing.T) {
	limiter := job.NewConcurrencyLimiter()
	task := &blockingTask{
		id:   "conc-task",
		done: make(chan struct{}),
	}
	cmd := job.NewTaskCommander(task).WithConcurrencyLimiter(limiter)

	msg := &job.ExecutionMessage{JobID: task.id, ScriptPath: task.path(), Config: job.Config{MaxConcurrency: 1}}

	// First call acquires slot and blocks until released.
	go func() { _ = cmd.Execute(context.Background(), msg) }()
	task.waitStarted()

	err := cmd.Execute(context.Background(), msg)
	require.ErrorIs(t, err, job.ErrConcurrencyLimit)

	close(task.done)
}

func TestConcurrencyLimiterScopeSplitsCapacity(t *testing.T) {
	limiter := job.NewConcurrencyLimiter()
	scopeFn := func(msg *job.ExecutionMessage) string {
		if msg.Parameters == nil {
			return ""
		}
		if v, ok := msg.Parameters["scope"].(string); ok {
			return v
		}
		return ""
	}

	task := &countingTask{id: "scoped-task", path: "/tmp/path"}
	cmd := job.NewTaskCommander(task).WithConcurrencyLimiter(limiter).WithScopeExtractor(scopeFn)

	msgA := &job.ExecutionMessage{JobID: task.id, ScriptPath: task.path, Config: job.Config{MaxConcurrency: 1}, Parameters: map[string]any{"scope": "A"}}
	msgB := &job.ExecutionMessage{JobID: task.id, ScriptPath: task.path, Config: job.Config{MaxConcurrency: 1}, Parameters: map[string]any{"scope": "B"}}

	require.NoError(t, cmd.Execute(context.Background(), msgA))
	require.NoError(t, cmd.Execute(context.Background(), msgB))
}

func TestQuotaCheckerBlocksOversizedPayload(t *testing.T) {
	qc := job.BasicQuotaChecker{PayloadSizeLimit: 8}
	task := &countingTask{id: "quota-task", path: "/tmp/quota"}
	cmd := job.NewTaskCommander(task).WithQuotaChecker(qc)

	msg := &job.ExecutionMessage{
		JobID:      task.id,
		ScriptPath: task.path,
		Parameters: map[string]any{"data": "0123456789"},
	}

	err := cmd.Execute(context.Background(), msg)
	require.ErrorIs(t, err, job.ErrQuotaExceeded)
	assert.Equal(t, 0, task.count)
}

type blockingTask struct {
	id    string
	start chan struct{}
	done  chan struct{}
}

func (b *blockingTask) waitStarted() {
	select {
	case <-b.start:
	case <-time.After(time.Second):
	}
}

func (b *blockingTask) path() string { return "/tmp/blocking" }

func (b *blockingTask) GetID() string                        { return b.id }
func (b *blockingTask) GetHandler() func() error             { return func() error { return nil } }
func (b *blockingTask) GetHandlerConfig() job.HandlerOptions { return job.HandlerOptions{} }
func (b *blockingTask) GetConfig() job.Config                { return job.Config{MaxConcurrency: 1} }
func (b *blockingTask) GetPath() string                      { return b.path() }
func (b *blockingTask) GetEngine() job.Engine                { return nil }
func (b *blockingTask) Execute(context.Context, *job.ExecutionMessage) error {
	if b.start != nil {
		close(b.start)
	}
	if b.done != nil {
		<-b.done
	}
	return nil
}
