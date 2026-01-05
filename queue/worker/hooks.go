package worker

import (
	"context"
	"time"

	job "github.com/goliatone/go-job"
	"github.com/goliatone/go-job/queue"
)

// Event captures worker lifecycle details for hooks.
type Event struct {
	Delivery  queue.Delivery
	Message   *job.ExecutionMessage
	Task      job.Task
	Attempt   int
	Delay     time.Duration
	Err       error
	StartedAt time.Time
	Duration  time.Duration
}

// Hook exposes lifecycle callbacks for worker execution.
type Hook interface {
	OnStart(ctx context.Context, event Event)
	OnSuccess(ctx context.Context, event Event)
	OnFailure(ctx context.Context, event Event)
	OnRetry(ctx context.Context, event Event)
}

// HookFuncs provides a function-based hook implementation.
type HookFuncs struct {
	OnStartFunc   func(context.Context, Event)
	OnSuccessFunc func(context.Context, Event)
	OnFailureFunc func(context.Context, Event)
	OnRetryFunc   func(context.Context, Event)
}

func (h HookFuncs) OnStart(ctx context.Context, event Event) {
	if h.OnStartFunc != nil {
		h.OnStartFunc(ctx, event)
	}
}

func (h HookFuncs) OnSuccess(ctx context.Context, event Event) {
	if h.OnSuccessFunc != nil {
		h.OnSuccessFunc(ctx, event)
	}
}

func (h HookFuncs) OnFailure(ctx context.Context, event Event) {
	if h.OnFailureFunc != nil {
		h.OnFailureFunc(ctx, event)
	}
}

func (h HookFuncs) OnRetry(ctx context.Context, event Event) {
	if h.OnRetryFunc != nil {
		h.OnRetryFunc(ctx, event)
	}
}
