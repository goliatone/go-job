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
	loggerProvider LoggerProvider
	taskIDProvider TaskIDProvider
	eventHandlers  []TaskEventHandler
}

func NewTaskCreator(provider SourceProvider, engines []Engine) *taskCreator {
	loggerProvider := newStdLoggerProvider()
	tc := &taskCreator{
		sourceProvider: provider,
		engines:        engines,
		loggerProvider: loggerProvider,
		logger:         loggerProvider.GetLogger("job:task_creator"),
	}

	tc.errorHandler = func(task Task, err error) {
		if task != nil {
			tc.logger.Error("task creator error", "task_id", task.GetID(), "error", err)
		} else {
			tc.logger.Error("task creator error", "error", err)
		}
	}

	tc.applyLoggerProvider()

	return tc
}

func (f *taskCreator) WithLogger(logger Logger) *taskCreator {
	f.SetLogger(logger)
	return f
}

func (f *taskCreator) SetLogger(logger Logger) {
	if logger == nil {
		if f.loggerProvider == nil {
			f.loggerProvider = newStdLoggerProvider()
		}
		f.logger = f.loggerProvider.GetLogger("job:task_creator")
		return
	}
	f.logger = logger
}

func (f *taskCreator) SetLoggerProvider(provider LoggerProvider) {
	if provider == nil {
		provider = newStdLoggerProvider()
	}
	f.loggerProvider = provider
	f.logger = provider.GetLogger("job:task_creator")
	f.applyLoggerProvider()
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
			r.logger.Warn("task skipped: no compatible engine", "script_path", script.Path, "task_id", scriptID)
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

		r.logger.Debug("task parsed", "task_id", task.GetID(), "script_path", script.Path, "engine", compatibleEngine.Name())
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

func (r *taskCreator) applyLoggerProvider() {
	for _, engine := range r.engines {
		switch eng := engine.(type) {
		case LoggerProviderAware:
			eng.SetLoggerProvider(r.loggerProvider)
		case LoggerAware:
			eng.SetLogger(r.loggerProvider.GetLogger(engine.Name()))
		}
	}
}
