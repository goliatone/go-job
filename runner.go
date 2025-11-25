package job

import (
	"context"
	"fmt"
	"sync"
)

type Runner struct {
	mx       sync.RWMutex
	registry Registry

	parser            MetadataParser
	errorHandler      func(Task, error)
	taskCreators      []TaskCreator
	logger            Logger
	loggerProvider    LoggerProvider
	taskIDProvider    TaskIDProvider
	taskEventHandlers []TaskEventHandler
}

func NewRunner(opts ...Option) *Runner {
	loggerProvider := newStdLoggerProvider()
	rn := &Runner{
		registry:       NewMemoryRegistry(),
		parser:         NewYAMLMetadataParser(),
		loggerProvider: loggerProvider,
		logger:         loggerProvider.GetLogger("job:runner"),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(rn)
		}
	}

	if rn.errorHandler == nil {
		rn.errorHandler = func(task Task, err error) {
			if task != nil {
				rn.logger.Error("task registration error", "task_id", task.GetID(), "error", err)
				return
			}
			rn.logger.Error("runner error", "error", err)
		}
	}

	return rn
}

func (r *Runner) Start(ctx context.Context) error {
	for _, make := range r.taskCreators {
		if err := ctx.Err(); err != nil {
			r.handleContextCancellation(err)
			return err
		}

		tasks, err := make.CreateTasks(ctx)
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				r.handleContextCancellation(ctxErr)
				return ctxErr
			}

			r.errorHandler(nil, err)
			r.emitTaskEvent(TaskEvent{
				Type: TaskEventRegistrationFailed,
				Err:  err,
			})
			continue
		}

		for _, task := range tasks {
			if err := ctx.Err(); err != nil {
				r.handleContextCancellation(err)
				return err
			}

			if err := r.registry.Add(task); err != nil {
				r.errorHandler(task, err)
				r.emitTaskEvent(TaskEvent{
					Type:       TaskEventRegistrationFailed,
					TaskID:     task.GetID(),
					ScriptPath: taskScriptPath(task),
					Task:       task,
					Err:        err,
				})
				continue
			}

			r.emitTaskEvent(TaskEvent{
				Type:       TaskEventRegistered,
				TaskID:     task.GetID(),
				ScriptPath: taskScriptPath(task),
				Task:       task,
			})
		}
	}

	if err := ctx.Err(); err != nil {
		r.handleContextCancellation(err)
		return err
	}

	return nil
}

func (r *Runner) Stop(_ context.Context) error {
	return nil
}

func (r *Runner) RegisteredTasks() []Task {
	return r.registry.List()
}

// SetResult stores result metadata for a given job ID.
func (r *Runner) SetResult(jobID string, result Result) error {
	if r == nil || r.registry == nil {
		return fmt.Errorf("runner registry not configured")
	}
	return r.registry.SetResult(jobID, result)
}

// GetResult retrieves result metadata for a given job ID.
func (r *Runner) GetResult(jobID string) (Result, bool) {
	if r == nil || r.registry == nil {
		return Result{}, false
	}
	return r.registry.GetResult(jobID)
}

func (r *Runner) emitTaskEvent(event TaskEvent) {
	if event.Type == "" {
		event.Type = TaskEventRegistrationFailed
	}

	switch event.Type {
	case TaskEventRegistered:
		args := []any{
			"task_id", event.TaskID,
			"script_path", event.ScriptPath,
		}
		r.logger.Info("task registered", args...)
	case TaskEventRegistrationFailed:
		args := []any{
			"task_id", event.TaskID,
			"script_path", event.ScriptPath,
		}
		if event.Err != nil {
			args = append(args, "error", event.Err)
		}
		r.logger.Warn("task registration failed", args...)
	}

	for _, handler := range r.taskEventHandlers {
		handler(event)
	}
}

func (r *Runner) handleContextCancellation(err error) {
	if err == nil {
		return
	}
	r.logger.Warn("task discovery cancelled", "error", err)
	r.errorHandler(nil, err)
	r.emitTaskEvent(TaskEvent{
		Type: TaskEventRegistrationFailed,
		Err:  err,
	})
}

func (r *Runner) attachTaskCreatorOptions(creator TaskCreator) {
	if creator == nil {
		return
	}

	if r.loggerProvider != nil {
		switch tc := creator.(type) {
		case LoggerProviderAware:
			tc.SetLoggerProvider(r.loggerProvider)
		case LoggerAware:
			tc.SetLogger(r.loggerProvider.GetLogger("job:task_creator"))
		}
	}

	if r.taskIDProvider != nil {
		if consumer, ok := creator.(TaskIDProviderAware); ok {
			consumer.SetTaskIDProvider(r.taskIDProvider)
		}
	}

	if emitter, ok := creator.(TaskEventEmitter); ok {
		for _, handler := range r.taskEventHandlers {
			emitter.AddTaskEventHandler(handler)
		}
	}
}

func (r *Runner) propagateTaskEventHandler(handler TaskEventHandler) {
	if handler == nil {
		return
	}

	for _, creator := range r.taskCreators {
		if emitter, ok := creator.(TaskEventEmitter); ok {
			emitter.AddTaskEventHandler(handler)
		}
	}
}

func (r *Runner) propagateTaskIDProvider() {
	if r.taskIDProvider == nil {
		return
	}

	for _, creator := range r.taskCreators {
		if consumer, ok := creator.(TaskIDProviderAware); ok {
			consumer.SetTaskIDProvider(r.taskIDProvider)
		}
	}
}

func (r *Runner) propagateLoggerProvider() {
	if r.loggerProvider == nil {
		return
	}

	r.logger = r.loggerProvider.GetLogger("job:runner")

	for _, creator := range r.taskCreators {
		switch tc := creator.(type) {
		case LoggerProviderAware:
			tc.SetLoggerProvider(r.loggerProvider)
		}
	}
}

func taskScriptPath(task Task) string {
	if task == nil {
		return ""
	}
	type pathAware interface {
		GetPath() string
	}
	if v, ok := task.(pathAware); ok {
		return v.GetPath()
	}
	return ""
}
