package job

import (
	"context"
	"fmt"
)

type FileSystemTaskCreator struct {
	engines        []Engine
	errorHandler   func(error)
	sourceProvider SourceProvider
}

func NewFileSystemTaskCreator(provider SourceProvider, engines []Engine) *FileSystemTaskCreator {
	return &FileSystemTaskCreator{
		sourceProvider: provider,
		engines:        engines,
		errorHandler: func(err error) {
			fmt.Printf("task creator error: %v\n", err)
		},
	}
}

// WithErrorHandler sets a custom error handler
func (f *FileSystemTaskCreator) WithErrorHandler(handler func(error)) *FileSystemTaskCreator {
	f.errorHandler = handler
	return f
}

func (r *FileSystemTaskCreator) CreateTasks(ctx context.Context) ([]Task, error) {
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
