package job

import (
	"context"

	"github.com/goliatone/go-command"
	"github.com/goliatone/go-command/dispatcher"
	"github.com/goliatone/go-errors"
)

// TaskCommander wraps a job Task so it can be dispatched via go-command.
type TaskCommander struct {
	Task Task
}

// Execute runs the underlying Task handler with validation.
func (t TaskCommander) Execute(ctx context.Context, msg *ExecutionMessage) error {
	if msg == nil {
		return errors.New("execution message required", errors.CategoryBadInput).
			WithTextCode("JOB_EXEC_MSG_NIL")
	}
	if err := msg.Validate(); err != nil {
		return errors.Wrap(err, errors.CategoryBadInput, "invalid execution message").
			WithTextCode("JOB_EXEC_MSG_INVALID")
	}
	if t.Task == nil {
		return errors.New("task not configured", errors.CategoryInternal).
			WithTextCode("JOB_TASK_MISSING")
	}
	return t.Task.GetHandler()()
}

// RegisterTasksWithDispatcher subscribes tasks into the go-command dispatcher.
// Returns subscriptions so callers can manage lifecycle.
func RegisterTasksWithDispatcher(tasks []Task) []dispatcher.Subscription {
	var subs []dispatcher.Subscription
	for _, task := range tasks {
		if task == nil {
			continue
		}
		cmd := TaskCommander{Task: task}
		sub := dispatcher.SubscribeCommand[*ExecutionMessage](cmd)
		subs = append(subs, sub)
	}
	return subs
}
