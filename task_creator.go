package job

import (
	"context"
	"fmt"
)

type taskCreator struct {
	engines        []Engine
	errorHandler   func(error)
	sourceProvider SourceProvider
	logger         Logger
}

func NewTaskCreator(provider SourceProvider, engines []Engine) *taskCreator {
	return &taskCreator{
		sourceProvider: provider,
		engines:        engines,
		errorHandler: func(err error) {
			fmt.Printf(">> task creator error: %v <<\n", err)
		},
	}
}

func (f *taskCreator) WithLogger(logger Logger) *taskCreator {
	f.logger = logger
	return f
}

// WithErrorHandler sets a custom error handler
func (f *taskCreator) WithErrorHandler(handler func(error)) *taskCreator {
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
			fmt.Printf("[WARN] task '%s' had no compatible engine", script.Path)
			continue
		}

		task, err := compatibleEngine.ParseJob(script.Path, script.Content)
		if err != nil {
			r.errorHandler(fmt.Errorf("failed to parse task %s: %w", script.Path, err))
			continue
		}

		tasks = append(tasks, task)
	}
	return tasks, nil
}
