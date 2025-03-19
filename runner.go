package job

import (
	"context"
	"fmt"
	"sync"

	"github.com/goliatone/go-command"
)

type Runner struct {
	mx       sync.RWMutex
	engines  []Engine
	registry Registry

	parser       MetadataParser
	errorHandler func(error)
	taskCreators []TaskCreator
}

func NewRunner(opts ...Option) *Runner {
	runner := &Runner{
		registry: &memoryRegistry{
			jobs: make(map[string]Task),
		},
		parser: &yamlMetadataParser{},
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
	if len(r.engines) == 0 {
		return command.WrapError("FileSystemjob", "no engines registered", nil)
	}

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

func (r *Runner) AddEngine(engine Engine) {
	r.mx.Lock()
	defer r.mx.Unlock()
	for _, existing := range r.engines {
		if existing.Name() == engine.Name() {
			return
		}
	}
	r.engines = append(r.engines, engine)
}
