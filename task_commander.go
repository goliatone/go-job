package job

import (
	"context"
	"fmt"

	"github.com/goliatone/go-command/router"
	"github.com/goliatone/go-errors"
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
	if msg.IdempotencyKey != "" {
		base.IdempotencyKey = msg.IdempotencyKey
	}
	if msg.DedupPolicy != "" {
		base.DedupPolicy = msg.DedupPolicy
	}
	if msg.OutputCallback != nil {
		base.OutputCallback = msg.OutputCallback
	}
	if msg.Result != nil {
		base.Result = msg.Result
	}

	base.Config = mergeConfigDefaults(task.GetConfig(), msg.Config)
	if msg.Parameters != nil {
		if base.Parameters == nil {
			base.Parameters = make(map[string]any, len(msg.Parameters))
		}
		for k, v := range msg.Parameters {
			base.Parameters[k] = v
		}
	}

	return base, nil
}

// TaskCommander adapts a Task to the command.Commander interface.
type TaskCommander struct {
	Task    Task
	tracker *IdempotencyTracker
	limiter *ConcurrencyLimiter
	quotas  QuotaChecker
	scope   func(*ExecutionMessage) string
}

func NewTaskCommander(task Task) *TaskCommander {
	return &TaskCommander{
		Task:    task,
		tracker: defaultIdempotencyTracker,
		limiter: defaultConcurrencyLimiter,
		quotas:  defaultQuotaChecker,
	}
}

// WithIdempotencyTracker overrides the tracker used for deduplication checks.
func (c *TaskCommander) WithIdempotencyTracker(tracker *IdempotencyTracker) *TaskCommander {
	if c == nil {
		return nil
	}
	c.tracker = tracker
	return c
}

// WithConcurrencyLimiter overrides the limiter used for concurrency control.
func (c *TaskCommander) WithConcurrencyLimiter(limiter *ConcurrencyLimiter) *TaskCommander {
	if c == nil {
		return nil
	}
	c.limiter = limiter
	return c
}

// WithQuotaChecker overrides quota enforcement.
func (c *TaskCommander) WithQuotaChecker(qc QuotaChecker) *TaskCommander {
	if c == nil {
		return nil
	}
	if qc != nil {
		c.quotas = qc
	}
	return c
}

// WithScopeExtractor sets a scope extractor for concurrency keys.
func (c *TaskCommander) WithScopeExtractor(fn func(*ExecutionMessage) string) *TaskCommander {
	if c == nil {
		return nil
	}
	c.scope = fn
	return c
}

func (c *TaskCommander) Execute(ctx context.Context, msg *ExecutionMessage) error {
	if ctx == nil {
		ctx = context.Background()
	}

	if msg == nil {
		return errors.New("execution message required", errors.CategoryBadInput).
			WithTextCode("JOB_EXEC_MSG_NIL")
	}

	if c == nil || c.Task == nil {
		return errors.New("task not configured", errors.CategoryInternal).
			WithTextCode("JOB_TASK_MISSING")
	}

	finalMsg, err := CompleteExecutionMessage(c.Task, msg)
	if err != nil {
		return err
	}

	if err := finalMsg.Validate(); err != nil {
		return errors.Wrap(err, errors.CategoryBadInput, "invalid execution message").
			WithTextCode("JOB_EXEC_MSG_INVALID")
	}

	decision, prevErr := dedupBeforeExecute(c.tracker, finalMsg)
	switch decision {
	case dedupDrop:
		return ErrIdempotentDrop
	case dedupMerge:
		return prevErr
	}

	if err := c.quotas.Check(finalMsg); err != nil {
		return err
	}

	release, err := c.acquireConcurrency(finalMsg)
	if err != nil {
		return err
	}
	defer release()

	defer dedupAfterExecute(c.tracker, finalMsg, &err)

	maxRetries := finalMsg.Config.Retries
	backoffCfg := finalMsg.Config.Backoff

	for attempt := 0; ; attempt++ {
		err = c.Task.Execute(ctx, finalMsg)
		if err == nil {
			return nil
		}

		if attempt >= maxRetries {
			return err
		}

		delay := computeBackoffDelay(attempt+1, backoffCfg)
		if sleepErr := backoffSleep(ctx, delay); sleepErr != nil {
			return sleepErr
		}
	}
}

func (c *TaskCommander) acquireConcurrency(msg *ExecutionMessage) (func(), error) {
	if c == nil || c.limiter == nil || msg == nil || msg.Config.MaxConcurrency <= 0 {
		return func() {}, nil
	}

	limiter := c.limiter
	if c.scope != nil {
		limiter = limiter.WithScopeExtractor(c.scope)
	}
	return limiter.Acquire(msg, msg.Config.MaxConcurrency)
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
