package job

import (
	"context"
	"sync"
)

type Runner struct {
	mx       sync.RWMutex
	registry Registry

	parser       MetadataParser
	errorHandler func(Task, error)
	taskCreators []TaskCreator
	logger       Logger
}

func NewRunner(opts ...Option) *Runner {
	rn := &Runner{
		registry: NewMemoryRegistry(),
		parser:   NewYAMLMetadataParser(),
		logger:   &defaultLogger{},
	}

	for _, opt := range opts {
		if opt != nil {
			opt(rn)
		}
	}

	if rn.errorHandler == nil {
		rn.errorHandler = func(task Task, err error) {
			if task != nil {
				rn.logger.Error("job error", err, "id", task.GetID())
				return
			}
			rn.logger.Error("job error", err)
		}
	}

	return rn
}

func (r *Runner) Start(ctx context.Context) error {
	for _, make := range r.taskCreators {
		tasks, err := make.CreateTasks(ctx)
		if err != nil {
			r.errorHandler(nil, err)
			continue
		}
		for _, task := range tasks {
			if err := r.registry.Add(task); err != nil {
				r.errorHandler(task, err)
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
