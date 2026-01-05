package command

import (
	"context"
	"fmt"

	job "github.com/goliatone/go-job"
)

// Task adapts a command registry entry into a job.Task.
type Task struct {
	reg    *Registry
	id     string
	config job.Config
}

// NewTask builds a task for a registered command id.
func NewTask(reg *Registry, id string) *Task {
	task := &Task{reg: reg, id: id}
	if reg != nil {
		if entry, ok := reg.Get(id); ok {
			task.config = entry.Config
		}
	}
	return task
}

func (t *Task) GetID() string                        { return t.id }
func (t *Task) GetPath() string                      { return t.id }
func (t *Task) GetConfig() job.Config                { return t.config }
func (t *Task) GetHandler() func() error             { return func() error { return nil } }
func (t *Task) GetHandlerConfig() job.HandlerOptions { return job.HandlerOptions{} }
func (t *Task) GetEngine() job.Engine                { return nil }

func (t *Task) Execute(ctx context.Context, msg *job.ExecutionMessage) error {
	if t == nil || t.reg == nil {
		return fmt.Errorf("command task not configured")
	}
	entry, ok := t.reg.Get(t.id)
	if !ok || entry.Handler == nil {
		return fmt.Errorf("command %q not registered", t.id)
	}
	if msg == nil {
		msg = &job.ExecutionMessage{}
	}
	return entry.Handler(ctx, msg.Parameters)
}
