package job_test

import (
	"context"
	"testing"
	"time"

	"github.com/goliatone/go-command"
	"github.com/goliatone/go-command/router"
	"github.com/goliatone/go-job"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type recordingEngine struct {
	lastCtx context.Context
	lastMsg *job.ExecutionMessage
}

func (r *recordingEngine) Name() string                              { return "recording" }
func (r *recordingEngine) ParseJob(string, []byte) (job.Task, error) { return nil, nil }
func (r *recordingEngine) CanHandle(string) bool                     { return true }
func (r *recordingEngine) Execute(ctx context.Context, msg *job.ExecutionMessage) error {
	r.lastCtx = ctx
	r.lastMsg = msg
	return nil
}

type noopEngine struct{}

func (noopEngine) Name() string                                         { return "noop" }
func (noopEngine) ParseJob(string, []byte) (job.Task, error)            { return nil, nil }
func (noopEngine) CanHandle(string) bool                                { return true }
func (noopEngine) Execute(context.Context, *job.ExecutionMessage) error { return nil }

func TestTaskExecuteUsesProvidedContext(t *testing.T) {
	engine := &recordingEngine{}
	task := job.NewBaseTask("ctx-task", "/tmp/script.js", "js", job.Config{}, "console.log('ok')", engine)

	ctx := context.WithValue(context.Background(), "req_id", "123")
	err := task.Execute(ctx, &job.ExecutionMessage{Parameters: map[string]any{}})
	require.NoError(t, err)

	assert.Equal(t, "123", engine.lastCtx.Value("req_id"))
	require.NotNil(t, engine.lastMsg)
	assert.Equal(t, "ctx-task", engine.lastMsg.JobID)
}

func TestBaseEngineRespectsInboundDeadline(t *testing.T) {
	be := job.NewBaseEngine(noopEngine{}, "noop")
	deadline := time.Now().Add(75 * time.Millisecond)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	execCtx, execCancel := be.GetExecutionContext(ctx)
	defer execCancel()

	d1, ok1 := ctx.Deadline()
	d2, ok2 := execCtx.Deadline()
	require.True(t, ok1)
	require.True(t, ok2)
	assert.Equal(t, d1, d2)
}

func TestHandlerOptionsMappingFromConfig(t *testing.T) {
	deadline := time.Now().Add(time.Hour).UTC()
	cfg := job.Config{
		Schedule:    "@hourly",
		Retries:     3,
		Timeout:     5 * time.Second,
		Deadline:    deadline,
		MaxRuns:     2,
		RunOnce:     true,
		ExitOnError: true,
	}

	task := job.NewBaseTask("handler-task", "/tmp/h.txt", "shell", cfg, "echo", noopEngine{})
	opts := task.GetHandlerConfig()

	assert.Equal(t, cfg.Retries, opts.MaxRetries)
	assert.Equal(t, cfg.MaxRuns, opts.MaxRuns)
	assert.Equal(t, cfg.Timeout, opts.Timeout)
	assert.Equal(t, cfg.Deadline, opts.Deadline)
	assert.True(t, opts.RunOnce)
	assert.True(t, opts.ExitOnError)
	assert.Equal(t, cfg.Schedule, opts.Expression)
}

func TestBuildExecutionMessageUsesCachedScript(t *testing.T) {
	task := job.NewBaseTask("cached", "/tmp/script.js", "js", job.Config{}, "cached-script", noopEngine{})

	msg, err := job.BuildExecutionMessageForTask(task, map[string]any{"foo": "bar"})
	require.NoError(t, err)

	assert.Equal(t, "cached", msg.JobID)
	assert.Equal(t, "cached-script", msg.Parameters["script"])
	assert.Equal(t, "bar", msg.Parameters["foo"])
}

func TestRegisterTasksWithMuxCreatesCommander(t *testing.T) {
	engine := &recordingEngine{}
	task := job.NewBaseTask("mux-task", "/tmp/script.js", "js", job.Config{}, "console.log('hi')", engine)

	mux := router.NewMux()
	subs := job.RegisterTasksWithMux(mux, []job.Task{task})
	require.Len(t, subs, 1)

	pattern := job.TaskCommandPattern(task)
	entries := mux.Get(pattern)
	require.Len(t, entries, 1)

	cmd, ok := entries[0].Handler.(command.Commander[*job.ExecutionMessage])
	require.True(t, ok)

	err := cmd.Execute(context.Background(), &job.ExecutionMessage{Parameters: map[string]any{}})
	require.NoError(t, err)
	require.NotNil(t, engine.lastMsg)
	assert.Equal(t, "mux-task", engine.lastMsg.JobID)
	assert.Equal(t, "console.log('hi')", engine.lastMsg.Parameters["script"])
}
