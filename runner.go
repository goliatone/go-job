package job

import (
	"context"
	"sync"
)

type Runner struct {
	mx       sync.RWMutex
	registry Registry

	parser            MetadataParser
	errorHandler      func(Task, error)
	taskCreators      []TaskCreator
	logger            Logger
	taskIDProvider    TaskIDProvider
	taskEventHandlers []TaskEventHandler
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

func (r *Runner) emitTaskEvent(event TaskEvent) {
	for _, handler := range r.taskEventHandlers {
		handler(event)
	}
}

func (r *Runner) handleContextCancellation(err error) {
	if err == nil {
		return
	}
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
