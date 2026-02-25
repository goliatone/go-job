package command

import (
	"context"
	"fmt"
	"time"

	job "github.com/goliatone/go-job"
	"github.com/goliatone/go-job/queue"
)

// Enqueue validates the command id and enqueues a background job.
func Enqueue(ctx context.Context, enqueuer queue.Enqueuer, reg *Registry, id string, params map[string]any) error {
	if enqueuer == nil {
		return fmt.Errorf("enqueuer not configured")
	}
	if id == "" {
		return fmt.Errorf("command id required")
	}
	if reg != nil {
		if _, ok := reg.Get(id); !ok {
			return fmt.Errorf("command %q not registered", id)
		}
	}

	msg := &job.ExecutionMessage{
		JobID:      id,
		ScriptPath: id,
		Parameters: params,
	}
	return enqueuer.Enqueue(ctx, msg)
}

// EnqueueAt validates the command id and schedules a background job.
func EnqueueAt(ctx context.Context, enqueuer queue.ScheduledEnqueuer, reg *Registry, id string, params map[string]any, at time.Time) error {
	if enqueuer == nil {
		return fmt.Errorf("enqueuer not configured")
	}
	if id == "" {
		return fmt.Errorf("command id required")
	}
	if reg != nil {
		if _, ok := reg.Get(id); !ok {
			return fmt.Errorf("command %q not registered", id)
		}
	}

	msg := &job.ExecutionMessage{
		JobID:      id,
		ScriptPath: id,
		Parameters: params,
	}
	return enqueuer.EnqueueAt(ctx, msg, at)
}

// EnqueueAfter validates the command id and schedules a delayed background job.
func EnqueueAfter(ctx context.Context, enqueuer queue.ScheduledEnqueuer, reg *Registry, id string, params map[string]any, delay time.Duration) error {
	if enqueuer == nil {
		return fmt.Errorf("enqueuer not configured")
	}
	if id == "" {
		return fmt.Errorf("command id required")
	}
	if reg != nil {
		if _, ok := reg.Get(id); !ok {
			return fmt.Errorf("command %q not registered", id)
		}
	}

	msg := &job.ExecutionMessage{
		JobID:      id,
		ScriptPath: id,
		Parameters: params,
	}
	return enqueuer.EnqueueAfter(ctx, msg, delay)
}
