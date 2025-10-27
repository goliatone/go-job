package job

import (
	"context"
	"fmt"
)

type taskCreator struct {
	engines        []Engine
	errorHandler   func(Task, error)
	sourceProvider SourceProvider
	logger         Logger
	taskIDProvider TaskIDProvider
	eventHandlers  []TaskEventHandler
}

func NewTaskCreator(provider SourceProvider, engines []Engine) *taskCreator {
	tc := &taskCreator{
		sourceProvider: provider,
		engines:        engines,
		logger:         &defaultLogger{},
	}

	tc.errorHandler = func(task Task, err error) {
		if task != nil {
			tc.logger.Error("task creator error", "id", task.GetID(), err)
		} else {
			tc.logger.Error("task creator error", err)
		}
	}

	return tc
}

func (f *taskCreator) WithLogger(logger Logger) *taskCreator {
	f.logger = logger
	return f
}

// WithTaskIDProvider sets the strategy used to derive task IDs for scripts discovered by this creator.
func (f *taskCreator) WithTaskIDProvider(provider TaskIDProvider) *taskCreator {
	f.SetTaskIDProvider(provider)
	return f
}

// SetTaskIDProvider satisfies TaskIDProvider consumers by configuring the strategy.
func (f *taskCreator) SetTaskIDProvider(provider TaskIDProvider) {
	f.taskIDProvider = provider
	f.applyTaskIDProvider()
}

// AddTaskEventHandler registers an observer for task registration events.
func (f *taskCreator) AddTaskEventHandler(handler TaskEventHandler) {
	if handler != nil {
		f.eventHandlers = append(f.eventHandlers, handler)
	}
}

// WithErrorHandler sets a custom error handler
func (f *taskCreator) WithErrorHandler(handler func(Task, error)) *taskCreator {
	f.errorHandler = handler
	return f
}

func (r *taskCreator) CreateTasks(ctx context.Context) ([]Task, error) {
	r.applyTaskIDProvider()

	scripts, err := r.sourceProvider.ListScripts(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list scripts: %w", err)
	}

	var tasks []Task

	for _, script := range scripts {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		scriptID := script.ID
		if r.taskIDProvider != nil {
			scriptID = r.taskIDProvider(script.Path)
		} else if scriptID == "" {
			scriptID = DefaultTaskIDProvider(script.Path)
		}

		var compatibleEngine Engine
		for _, engine := range r.engines {
			if engine.CanHandle(script.Path) {
				compatibleEngine = engine
				break
			}
		}

		if compatibleEngine == nil {
			r.logger.Warn("task had no compatible engine", "path", script.Path)
			r.emitTaskEvent(TaskEvent{
				Type:       TaskEventRegistrationFailed,
				TaskID:     scriptID,
				ScriptPath: script.Path,
				Err:        fmt.Errorf("no compatible engine for script %s", script.Path),
			})
			continue
		}

		task, err := compatibleEngine.ParseJob(script.Path, script.Content)
		if err != nil {
			regErr := fmt.Errorf("failed to parse task %s: %w", script.Path, err)
			r.errorHandler(task, regErr)
			r.emitTaskEvent(TaskEvent{
				Type:       TaskEventRegistrationFailed,
				TaskID:     scriptID,
				ScriptPath: script.Path,
				Task:       task,
				Err:        regErr,
			})
			continue
		}

		tasks = append(tasks, task)
	}
	return tasks, nil
}

func (r *taskCreator) applyTaskIDProvider() {
	if r.taskIDProvider == nil {
		return
	}
	for _, engine := range r.engines {
		if aware, ok := engine.(TaskIDProviderAware); ok {
			aware.SetTaskIDProvider(r.taskIDProvider)
		}
	}
}

func (r *taskCreator) emitTaskEvent(event TaskEvent) {
	for _, handler := range r.eventHandlers {
		handler(event)
	}
}
