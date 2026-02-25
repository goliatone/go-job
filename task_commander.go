package job

import (
	"context"
	"fmt"
	"time"

	"github.com/goliatone/go-command/router"
	"github.com/goliatone/go-errors"
	qidempotency "github.com/goliatone/go-job/queue/idempotency"
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
	msg.normalize()
	return msg, nil
}

// CompleteExecutionMessage merges the provided message (which may already have
// overrides) with task defaults and cached script content.
func CompleteExecutionMessage(task Task, msg *ExecutionMessage) (*ExecutionMessage, error) {
	if task == nil {
		return nil, fmt.Errorf("task is nil")
	}

	if composer, ok := task.(messageComposer); ok {
		built, err := composer.buildExecutionMessage(msg)
		if err != nil {
			return nil, err
		}
		if built != nil {
			built.normalize()
		}
		return built, nil
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
	base.MachineID = msg.MachineID
	base.EntityID = msg.EntityID
	base.ExecutionID = msg.ExecutionID
	base.ExpectedState = msg.ExpectedState
	base.ExpectedVersion = msg.ExpectedVersion
	base.ResumeEvent = msg.ResumeEvent
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

	base.normalize()
	return base, nil
}

// TaskCommander adapts a Task to the command.Commander interface.
type TaskCommander struct {
	Task     Task
	tracker  *IdempotencyTracker
	store    qidempotency.Store
	storeTTL time.Duration
	limiter  *ConcurrencyLimiter
	quotas   QuotaChecker
	scope    func(*ExecutionMessage) string
	retries  *int
}

func NewTaskCommander(task Task) *TaskCommander {
	return &TaskCommander{
		Task:     task,
		tracker:  defaultIdempotencyTracker,
		storeTTL: 24 * time.Hour,
		limiter:  defaultConcurrencyLimiter,
		quotas:   defaultQuotaChecker,
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

// WithSharedIdempotencyStore enables distributed idempotency checks across workers.
func (c *TaskCommander) WithSharedIdempotencyStore(store qidempotency.Store, ttl time.Duration) *TaskCommander {
	if c == nil {
		return nil
	}
	c.store = store
	if ttl > 0 {
		c.storeTTL = ttl
	}
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

// WithRetryOverride forces TaskCommander to use the provided retry count.
func (c *TaskCommander) WithRetryOverride(maxRetries int) *TaskCommander {
	if c == nil {
		return nil
	}
	if maxRetries < 0 {
		maxRetries = 0
	}
	c.retries = &maxRetries
	return c
}

func (c *TaskCommander) Execute(ctx context.Context, msg *ExecutionMessage) error {
	if ctx == nil {
		ctx = context.Background()
	}

	if msg == nil {
		return errors.NewValidation("execution message required",
			errors.FieldError{
				Field:   "execution_message",
				Message: "cannot be nil; provide an ExecutionMessage with job_id and script_path",
			},
		).WithTextCode("JOB_EXEC_MSG_NIL")
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

	decision, prevErr, dedupErr := c.dedupBeforeExecute(ctx, finalMsg)
	if dedupErr != nil {
		return dedupErr
	}
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

	defer c.dedupAfterExecute(ctx, finalMsg, &err)

	maxRetries := finalMsg.Config.Retries
	if c.retries != nil {
		maxRetries = *c.retries
	}
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

func (c *TaskCommander) dedupBeforeExecute(ctx context.Context, msg *ExecutionMessage) (dedupDecision, error, error) {
	if c == nil || c.store == nil {
		decision, prevErr := dedupBeforeExecute(c.tracker, msg)
		return decision, prevErr, nil
	}
	if msg == nil || msg.IdempotencyKey == "" || msg.DedupPolicy == "" || msg.DedupPolicy == DedupPolicyIgnore {
		return dedupProceed, nil, nil
	}

	record, created, err := c.store.Acquire(ctx, msg.IdempotencyKey, c.idempotencyTTL())
	if err != nil {
		return dedupProceed, nil, err
	}
	if created {
		return dedupProceed, nil, nil
	}

	switch msg.DedupPolicy {
	case DedupPolicyDrop:
		return dedupDrop, nil, nil
	case DedupPolicyMerge:
		if record.Status == qidempotency.StatusFailed && len(record.Payload) > 0 {
			return dedupMerge, fmt.Errorf("%s", string(record.Payload)), nil
		}
		return dedupMerge, nil, nil
	case DedupPolicyReplace:
		status := qidempotency.StatusPending
		emptyPayload := []byte(nil)
		expiresAt := time.Now().UTC().Add(c.idempotencyTTL())
		if err := c.store.Update(ctx, msg.IdempotencyKey, qidempotency.Update{
			Status:    &status,
			Payload:   &emptyPayload,
			ExpiresAt: &expiresAt,
		}); err != nil {
			return dedupProceed, nil, err
		}
		return dedupProceed, nil, nil
	default:
		return dedupProceed, nil, nil
	}
}

func (c *TaskCommander) dedupAfterExecute(ctx context.Context, msg *ExecutionMessage, execErr *error) {
	if c == nil || c.store == nil {
		dedupAfterExecute(c.tracker, msg, execErr)
		return
	}
	if msg == nil || msg.IdempotencyKey == "" || msg.DedupPolicy == "" || msg.DedupPolicy == DedupPolicyIgnore {
		return
	}

	status := qidempotency.StatusCompleted
	payload := []byte(nil)
	if execErr != nil && *execErr != nil {
		status = qidempotency.StatusFailed
		payload = []byte((*execErr).Error())
	}
	expiresAt := time.Now().UTC().Add(c.idempotencyTTL())
	_ = c.store.Update(ctx, msg.IdempotencyKey, qidempotency.Update{
		Status:    &status,
		Payload:   &payload,
		ExpiresAt: &expiresAt,
	})
}

func (c *TaskCommander) idempotencyTTL() time.Duration {
	if c == nil || c.storeTTL <= 0 {
		return 24 * time.Hour
	}
	return c.storeTTL
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
