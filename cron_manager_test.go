package job

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/goliatone/go-command"
	gocron "github.com/goliatone/go-command/cron"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestScheduleDefinitionValidate(t *testing.T) {
	err := (ScheduleDefinition{}).Validate()
	require.Error(t, err)

	valid := ScheduleDefinition{
		ID:         "daily-report",
		Expression: "0 7 * * *",
		Message:    ExecutionMessage{JobID: "report"},
	}
	require.NoError(t, valid.Validate())
}

func TestCronManagerRegisterUpdateDelete(t *testing.T) {
	reg := newStubRegistry()
	task := newStubTask("job-1", Config{Schedule: "@hourly", Retries: 2})
	require.NoError(t, reg.Add(task))

	scheduler := newStubScheduler()
	manager := NewCronManager(reg, scheduler)

	def := ScheduleDefinition{
		ID:         "nightly",
		Expression: "0 0 * * *",
		Message: ExecutionMessage{
			JobID:          task.GetID(),
			Parameters:     map[string]any{"scope": "alpha"},
			IdempotencyKey: "job-1-nightly",
			DedupPolicy:    DedupPolicyDrop,
			Config:         Config{Retries: 1, MaxConcurrency: 2},
		},
	}

	require.NoError(t, manager.Register(context.Background(), def))

	schedules := manager.List()
	require.Len(t, schedules, 1)
	assert.Equal(t, def.Expression, schedules[0].Expression)
	assert.Equal(t, def.Message.JobID, schedules[0].Message.JobID)
	assert.Equal(t, def.Message.Config.Retries, schedules[0].Message.Config.Retries)
	assert.Equal(t, def.Message.DedupPolicy, schedules[0].Message.DedupPolicy)
	assert.Equal(t, def.Message.IdempotencyKey, schedules[0].Message.IdempotencyKey)
	assert.Equal(t, def.Expression, schedules[0].Message.Config.Schedule)
	assert.Equal(t, 1, scheduler.count())

	// Ensure clones are returned
	schedules[0].Message.Parameters["scope"] = "beta"
	assert.Equal(t, "alpha", manager.List()[0].Message.Parameters["scope"])

	update := def
	update.Expression = "15 3 * * *"
	update.Message.Config.Retries = 3
	require.NoError(t, manager.Update(context.Background(), update))
	updated := manager.List()
	require.Len(t, updated, 1)
	assert.Equal(t, "15 3 * * *", updated[0].Expression)
	assert.Equal(t, 3, updated[0].Message.Config.Retries)
	assert.Equal(t, 1, scheduler.count())

	require.NoError(t, manager.Delete(context.Background(), def.ID))
	assert.Empty(t, manager.List())
	assert.Zero(t, scheduler.count())
}

func TestCronManagerReconcile(t *testing.T) {
	reg := newStubRegistry()
	task := newStubTask("job-1", Config{Schedule: "@hourly"})
	taskTwo := newStubTask("job-2", Config{Schedule: "@hourly"})
	require.NoError(t, reg.Add(task))
	require.NoError(t, reg.Add(taskTwo))

	scheduler := newStubScheduler()
	manager := NewCronManager(reg, scheduler)

	initial := ScheduleDefinition{
		ID:         "job-1-hourly",
		Expression: "0 * * * *",
		Message:    ExecutionMessage{JobID: task.GetID()},
	}
	require.NoError(t, manager.Register(context.Background(), initial))

	desired := []ScheduleDefinition{
		{
			ID:         "job-1-hourly",
			Expression: "*/30 * * * *",
			Message:    ExecutionMessage{JobID: task.GetID(), Config: Config{Retries: 2}},
		},
		{
			ID:         "job-2-nightly",
			Expression: "30 1 * * *",
			Message:    ExecutionMessage{JobID: taskTwo.GetID()},
		},
	}

	result, err := manager.Reconcile(context.Background(), desired)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"job-2-nightly"}, result.Added)
	assert.ElementsMatch(t, []string{"job-1-hourly"}, result.Updated)
	assert.Empty(t, result.Removed)

	schedules := manager.List()
	require.Len(t, schedules, 2)
	assert.Equal(t, "*/30 * * * *", findSchedule(t, schedules, "job-1-hourly").Expression)
	assert.Equal(t, "30 1 * * *", findSchedule(t, schedules, "job-2-nightly").Expression)

	desired = desired[1:] // drop job-1-hourly
	result, err = manager.Reconcile(context.Background(), desired)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"job-1-hourly"}, result.Removed)
	assert.Len(t, manager.List(), 1)
	assert.Equal(t, "job-2-nightly", manager.List()[0].ID)
}

func TestScheduleSyncCommandCronAndCLI(t *testing.T) {
	reg := newStubRegistry()
	task := newStubTask("job-1", Config{Schedule: "@hourly"})
	require.NoError(t, reg.Add(task))

	scheduler := newStubScheduler()
	manager := NewCronManager(reg, scheduler)

	loaderCalls := 0
	loader := func(ctx context.Context) ([]ScheduleDefinition, error) {
		loaderCalls++
		return []ScheduleDefinition{
			{ID: "sync-1", Expression: "*/10 * * * *", Message: ExecutionMessage{JobID: task.GetID()}},
		}, nil
	}

	cmd := NewScheduleSyncCommand(
		manager,
		loader,
		WithScheduleSyncCron("@daily"),
		WithScheduleSyncCLIName("sync-from-settings"),
		WithScheduleSyncCLIDescription("sync schedules from go-settings"),
		WithScheduleSyncCLIGroup("jobs"),
	)

	require.NoError(t, cmd.CronHandler()())
	assert.Equal(t, 1, loaderCalls)
	assert.Equal(t, "@daily", cmd.CronOptions().Expression)
	assert.Len(t, manager.List(), 1)

	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "schedules.yaml")
	fileContent := `
- id: file-sync
  expression: "0 5 * * *"
  message:
    job_id: job-1
    config:
      retries: 4
`
	require.NoError(t, os.WriteFile(filePath, []byte(fileContent), 0o600))

	cliHandler, ok := cmd.CLIHandler().(*scheduleSyncCLI)
	require.True(t, ok, "CLI handler should be scheduleSyncCLI")
	cliHandler.From = filePath
	require.NoError(t, cliHandler.Run())
	assert.Equal(t, 1, loaderCalls, "CLI path should not call loader when file provided")

	schedules := manager.List()
	require.Len(t, schedules, 1)
	assert.Equal(t, "file-sync", schedules[0].ID)
	assert.Equal(t, "0 5 * * *", schedules[0].Expression)
	assert.Equal(t, 4, schedules[0].Message.Config.Retries)
}

func findSchedule(t *testing.T, schedules []ScheduleDefinition, id string) ScheduleDefinition {
	t.Helper()
	for _, s := range schedules {
		if s.ID == id {
			return s
		}
	}
	t.Fatalf("schedule %s not found", id)
	return ScheduleDefinition{}
}

type stubScheduler struct {
	configs map[int]command.HandlerConfig
	jobs    map[int]func() error
	nextID  int
}

func newStubScheduler() *stubScheduler {
	return &stubScheduler{
		configs: make(map[int]command.HandlerConfig),
		jobs:    make(map[int]func() error),
		nextID:  1,
	}
}

func (s *stubScheduler) AddHandler(cfg command.HandlerConfig, handler any) (gocron.Subscription, error) {
	fn, ok := handler.(func() error)
	if !ok {
		return nil, fmt.Errorf("unexpected handler type %T", handler)
	}
	id := s.nextID
	s.nextID++
	s.configs[id] = cfg
	s.jobs[id] = fn
	return &stubSubscription{scheduler: s, id: id}, nil
}

func (s *stubScheduler) count() int {
	return len(s.jobs)
}

type stubSubscription struct {
	scheduler *stubScheduler
	id        int
}

func (s *stubSubscription) Unsubscribe() {
	delete(s.scheduler.jobs, s.id)
	delete(s.scheduler.configs, s.id)
}

type stubRegistry struct {
	tasks map[string]Task
}

func newStubRegistry() *stubRegistry {
	return &stubRegistry{tasks: make(map[string]Task)}
}

func (r *stubRegistry) List() []Task {
	out := make([]Task, 0, len(r.tasks))
	for _, t := range r.tasks {
		out = append(out, t)
	}
	return out
}

func (r *stubRegistry) Add(job Task) error {
	if job == nil {
		return fmt.Errorf("task is nil")
	}
	r.tasks[job.GetID()] = job
	return nil
}

func (r *stubRegistry) Get(id string) (Task, bool) {
	task, ok := r.tasks[id]
	return task, ok
}

func (r *stubRegistry) SetResult(id string, result Result) error { return nil }
func (r *stubRegistry) GetResult(id string) (Result, bool)       { return Result{}, false }

type stubTask struct {
	id     string
	path   string
	config Config
}

func newStubTask(id string, cfg Config) *stubTask {
	if cfg.Schedule == "" {
		cfg.Schedule = DefaultSchedule
	}
	return &stubTask{
		id:     id,
		path:   "/tmp/" + id,
		config: cfg,
	}
}

func (t *stubTask) GetID() string            { return t.id }
func (t *stubTask) GetHandler() func() error { return func() error { return nil } }
func (t *stubTask) GetHandlerConfig() HandlerOptions {
	return HandlerOptions{HandlerConfig: command.HandlerConfig{Expression: t.config.Schedule}}
}
func (t *stubTask) GetConfig() Config                                { return t.config }
func (t *stubTask) GetPath() string                                  { return t.path }
func (t *stubTask) GetEngine() Engine                                { return nil }
func (t *stubTask) Execute(context.Context, *ExecutionMessage) error { return nil }
