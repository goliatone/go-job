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
}

func NewTaskCreator(provider SourceProvider, engines []Engine) *taskCreator {
	tc := &taskCreator{
		sourceProvider: provider,
		engines:        engines,
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

// WithErrorHandler sets a custom error handler
func (f *taskCreator) WithErrorHandler(handler func(Task, error)) *taskCreator {
	f.errorHandler = handler
	return f
}

func (r *taskCreator) CreateTasks(ctx context.Context) ([]Task, error) {
	scripts, err := r.sourceProvider.ListScripts(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list scripts: %w", err)
	}

	var tasks []Task

	for _, script := range scripts {
		var compatibleEngine Engine
		for _, engine := range r.engines {
			if engine.CanHandle(script.Path) {
				compatibleEngine = engine
				break
			}
		}

		if compatibleEngine == nil {
			r.logger.Warn("task had no compatible engine", "path", script.Path)
			continue
		}

		task, err := compatibleEngine.ParseJob(script.Path, script.Content)
		if err != nil {
			r.errorHandler(task, fmt.Errorf("failed to parse task %s: %w", script.Path, err))
			continue
		}

		tasks = append(tasks, task)
	}
	return tasks, nil
}
