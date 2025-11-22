package job

import (
	"context"
	"fmt"

	"github.com/goliatone/go-command/router"
)

type messageComposer interface {
	buildExecutionMessage(*ExecutionMessage) (*ExecutionMessage, error)
}

// ExecutionMessageBuilder builds a message with cached script content.
type ExecutionMessageBuilder interface {
	BuildExecutionMessage(params map[string]any) (*ExecutionMessage, error)
}

// BuildExecutionMessageForTask returns an ExecutionMessage populated with task
// metadata and cached script content, avoiding re-reading from providers.
func BuildExecutionMessageForTask(task Task, params map[string]any) (*ExecutionMessage, error) {
	if task == nil {
		return nil, fmt.Errorf("task is nil")
	}

	if builder, ok := task.(ExecutionMessageBuilder); ok {
		return builder.BuildExecutionMessage(params)
	}

	msg := &ExecutionMessage{
		JobID:      task.GetID(),
		ScriptPath: task.GetPath(),
		Config:     task.GetConfig(),
		Parameters: cloneParams(params),
	}
	if msg.Parameters == nil {
		msg.Parameters = make(map[string]any)
	}
	return msg, nil
}

// CompleteExecutionMessage merges the provided message (which may already have
// overrides) with task defaults and cached script content.
func CompleteExecutionMessage(task Task, msg *ExecutionMessage) (*ExecutionMessage, error) {
	if task == nil {
		return nil, fmt.Errorf("task is nil")
	}

	if composer, ok := task.(messageComposer); ok {
		return composer.buildExecutionMessage(msg)
	}

	// Fallback path when the task does not expose the composer.
	base, err := BuildExecutionMessageForTask(task, nil)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return base, nil
	}

	if msg.JobID != "" {
		base.JobID = msg.JobID
	}
	if msg.ScriptPath != "" {
		base.ScriptPath = msg.ScriptPath
	}

	base.Config = mergeConfigDefaults(task.GetConfig(), msg.Config)
	if msg.Parameters != nil {
		base.Parameters = cloneParams(msg.Parameters)
	}

	return base, nil
}

// TaskCommander adapts a Task to the command.Commander interface.
type TaskCommander struct {
	task Task
}

func NewTaskCommander(task Task) *TaskCommander {
	return &TaskCommander{task: task}
}

func (c *TaskCommander) Execute(ctx context.Context, msg *ExecutionMessage) error {
	if c == nil || c.task == nil {
		return fmt.Errorf("task commander has no task")
	}
	finalMsg, err := CompleteExecutionMessage(c.task, msg)
	if err != nil {
		return err
	}
	return c.task.Execute(ctx, finalMsg)
}

// TaskCommandPattern builds a mux pattern for the task commander.
func TaskCommandPattern(task Task) string {
	return fmt.Sprintf("%s/%s", ExecutionMessage{}.Type(), task.GetID())
}

// RegisterTasksWithMux registers tasks as commanders on the provided mux and
// returns subscriptions for later teardown.
func RegisterTasksWithMux(mux *router.Mux, tasks []Task) []router.Subscription {
	if mux == nil {
		return nil
	}
	entries := make([]router.Subscription, 0, len(tasks))
	for _, task := range tasks {
		if task == nil {
			continue
		}
		entry := mux.Add(TaskCommandPattern(task), NewTaskCommander(task))
		entries = append(entries, entry)
	}
	return entries
}

// cloneParams defensively copies the parameters map to avoid caller mutation.
func cloneParams(params map[string]any) map[string]any {
	if len(params) == 0 {
		return nil
	}
	out := make(map[string]any, len(params))
	for k, v := range params {
		out[k] = v
	}
	return out
}
