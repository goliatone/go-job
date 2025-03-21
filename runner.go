package job

import (
	"context"
	"fmt"
	"sync"
)

type Runner struct {
	mx       sync.RWMutex
	registry Registry

	parser       MetadataParser
	errorHandler func(error)
	taskCreators []TaskCreator
}

func NewRunner(opts ...Option) *Runner {
	runner := &Runner{
		registry: NewMemoryRegistry(),
		parser:   NewYAMLMetadataParser(),
		errorHandler: func(err error) {
			fmt.Printf("job error: %v\n", err)
		},
	}

	for _, opt := range opts {
		if opt != nil {
			opt(runner)
		}
	}
	return runner
}

func (r *Runner) Start(ctx context.Context) error {
	for _, make := range r.taskCreators {
		tasks, err := make.CreateTasks(ctx)
		if err != nil {
			r.errorHandler(err)
			continue
		}
		for _, task := range tasks {
			if err := r.registry.Add(task); err != nil {
				r.errorHandler(err)
			}
		}
	}
	return nil
}

func (r *Runner) Stop(_ context.Context) error {
	return nil
}

func (r *Runner) RegisteredTasks() []Task {
	return r.registry.List()
}
