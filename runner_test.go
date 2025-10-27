package job_test

import (
	"context"
	"errors"
	"testing"

	"github.com/goliatone/go-command"
	"github.com/goliatone/go-job"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type staticSourceProvider struct {
	scripts []job.ScriptInfo
}

func (s *staticSourceProvider) GetScript(path string) ([]byte, error) {
	for _, script := range s.scripts {
		if script.Path == path {
			return script.Content, nil
		}
	}
	return nil, errors.New("script not found")
}

func (s *staticSourceProvider) ListScripts(ctx context.Context) ([]job.ScriptInfo, error) {
	return s.scripts, nil
}

type stubTask struct {
	id string
}

func (s stubTask) GetID() string {
	return s.id
}

func (s stubTask) GetHandler() func() error {
	return func() error { return nil }
}

func (s stubTask) GetHandlerConfig() command.HandlerConfig {
	return command.HandlerConfig{}
}

func (s stubTask) GetConfig() job.Config {
	return job.Config{}
}

type stubTaskCreator struct {
	tasks    []job.Task
	err      error
	onCreate func()
	calls    int
}

func (s *stubTaskCreator) CreateTasks(ctx context.Context) ([]job.Task, error) {
	s.calls++
	if s.onCreate != nil {
		s.onCreate()
	}
	return s.tasks, s.err
}

func TestRunnerRejectsDuplicateTaskIDsFromCustomProvider(t *testing.T) {
	provider := &staticSourceProvider{
		scripts: []job.ScriptInfo{
			{
				Path:    "jobs/email/welcome.sh",
				Content: []byte("echo hello"),
			},
			{
				Path:    "jobs/notifications/welcome.sh",
				Content: []byte("echo hello again"),
			},
		},
	}

	engine := job.NewShellRunner()
	creator := job.NewTaskCreator(provider, []job.Engine{engine})

	var events []job.TaskEvent
	runner := job.NewRunner(
		job.WithTaskIDProvider(func(string) string { return "welcome" }),
		job.WithTaskEventHandler(func(event job.TaskEvent) {
			events = append(events, event)
		}),
		job.WithTaskCreator(creator),
	)

	err := runner.Start(context.Background())
	require.NoError(t, err)

	tasks := runner.RegisteredTasks()
	require.Len(t, tasks, 1)
	assert.Equal(t, "welcome", tasks[0].GetID())

	require.Len(t, events, 2)

	assert.Equal(t, job.TaskEventRegistered, events[0].Type)
	assert.Equal(t, "welcome", events[0].TaskID)
	assert.Equal(t, "jobs/email/welcome.sh", events[0].ScriptPath)
	assert.NoError(t, events[0].Err)

	assert.Equal(t, job.TaskEventRegistrationFailed, events[1].Type)
	assert.Equal(t, "welcome", events[1].TaskID)
	assert.Equal(t, "jobs/notifications/welcome.sh", events[1].ScriptPath)
	assert.Error(t, events[1].Err)
}

func TestRunnerHonoursContextCancellationBetweenTaskCreators(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	firstCreator := &stubTaskCreator{
		tasks: []job.Task{stubTask{id: "first"}},
		onCreate: func() {
			cancel()
		},
	}

	secondCreator := &stubTaskCreator{}

	var events []job.TaskEvent
	runner := job.NewRunner(
		job.WithTaskEventHandler(func(event job.TaskEvent) {
			events = append(events, event)
		}),
		job.WithTaskCreator(firstCreator),
		job.WithTaskCreator(secondCreator),
	)

	err := runner.Start(ctx)
	assert.ErrorIs(t, err, context.Canceled)

	assert.Equal(t, 1, firstCreator.calls)
	assert.Equal(t, 0, secondCreator.calls)

	require.Len(t, events, 1)
	assert.Equal(t, job.TaskEventRegistrationFailed, events[0].Type)
	assert.ErrorIs(t, events[0].Err, context.Canceled)
}

func TestRunnerTaskEventHookProvidesAccuratePayloads(t *testing.T) {
	provider := &staticSourceProvider{
		scripts: []job.ScriptInfo{
			{
				Path:    "jobs/js/send_email.js",
				Content: []byte("console.log('ok')"),
			},
			{
				Path:    "jobs/unsupported/cleanup.txt",
				Content: []byte("noop"),
			},
		},
	}

	engine := job.NewJSRunner()
	creator := job.NewTaskCreator(provider, []job.Engine{engine})

	var events []job.TaskEvent
	runner := job.NewRunner(
		job.WithTaskIDProvider(func(path string) string { return path }),
		job.WithTaskEventHandler(func(event job.TaskEvent) {
			events = append(events, event)
		}),
		job.WithTaskCreator(creator),
	)

	err := runner.Start(context.Background())
	require.NoError(t, err)

	require.Len(t, events, 2)

	var successEvent, failureEvent *job.TaskEvent
	for i := range events {
		switch events[i].Type {
		case job.TaskEventRegistered:
			successEvent = &events[i]
		case job.TaskEventRegistrationFailed:
			failureEvent = &events[i]
		}
	}

	require.NotNil(t, successEvent)
	require.NotNil(t, failureEvent)

	assert.Equal(t, "jobs/js/send_email.js", successEvent.TaskID)
	assert.Equal(t, "jobs/js/send_email.js", successEvent.ScriptPath)
	assert.NotNil(t, successEvent.Task)
	assert.NoError(t, successEvent.Err)

	assert.Equal(t, "jobs/unsupported/cleanup.txt", failureEvent.TaskID)
	assert.Equal(t, "jobs/unsupported/cleanup.txt", failureEvent.ScriptPath)
	assert.Nil(t, failureEvent.Task)
	assert.Error(t, failureEvent.Err)
}
