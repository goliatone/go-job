package job

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/goliatone/go-command"
	gocron "github.com/goliatone/go-command/cron"
	"github.com/goliatone/go-errors"
)

// ScheduleDefinition describes a cron-driven job execution.
// Expression defines the cron spec, and Message carries the payload and
// execution options (including retries, backoff, idempotency, and limits).
type ScheduleDefinition struct {
	ID         string           `json:"id" yaml:"id"`
	Expression string           `json:"expression" yaml:"expression"`
	Message    ExecutionMessage `json:"message" yaml:"message"`
}

// ReconcileResult captures the diff outcome when aligning schedules.
type ReconcileResult struct {
	Added   []string
	Updated []string
	Removed []string
}

type cronScheduler interface {
	AddHandler(command.HandlerConfig, any) (gocron.Subscription, error)
}

type scheduledEntry struct {
	definition   ScheduleDefinition
	subscription gocron.Subscription
}

// CronManager provides runtime CRUD and reconciliation for cron schedules.
type CronManager struct {
	registry  Registry
	scheduler cronScheduler

	tracker *IdempotencyTracker
	limiter *ConcurrencyLimiter
	quotas  QuotaChecker

	mu        sync.RWMutex
	schedules map[string]*scheduledEntry
}

// NewCronManager wires schedule management against a task registry and a cron scheduler.
func NewCronManager(registry Registry, scheduler cronScheduler) *CronManager {
	return &CronManager{
		registry:  registry,
		scheduler: scheduler,
		tracker:   defaultIdempotencyTracker,
		limiter:   defaultConcurrencyLimiter,
		quotas:    defaultQuotaChecker,
		schedules: make(map[string]*scheduledEntry),
	}
}

// WithIdempotencyTracker overrides the tracker used for scheduled runs.
func (m *CronManager) WithIdempotencyTracker(tracker *IdempotencyTracker) *CronManager {
	if tracker != nil {
		m.tracker = tracker
	}
	return m
}

// WithConcurrencyLimiter overrides the limiter used for scheduled runs.
func (m *CronManager) WithConcurrencyLimiter(limiter *ConcurrencyLimiter) *CronManager {
	if limiter != nil {
		m.limiter = limiter
	}
	return m
}

// WithQuotaChecker overrides quota enforcement for scheduled runs.
func (m *CronManager) WithQuotaChecker(qc QuotaChecker) *CronManager {
	if qc != nil {
		m.quotas = qc
	}
	return m
}

// Register registers a new cron schedule; returns an error if the ID already exists.
func (m *CronManager) Register(ctx context.Context, def ScheduleDefinition) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := def.Validate(); err != nil {
		return err
	}
	if m.registry == nil {
		return fmt.Errorf("registry is not configured")
	}
	if m.scheduler == nil {
		return fmt.Errorf("scheduler is not configured")
	}

	m.mu.Lock()
	if _, exists := m.schedules[def.ID]; exists {
		m.mu.Unlock()
		return fmt.Errorf("schedule with ID %q already exists", def.ID)
	}
	m.mu.Unlock()

	resolved, handlerOpts, msg, err := m.resolve(def)
	if err != nil {
		return err
	}

	cmd := m.buildCommander(resolved.Message.JobID)
	if cmd == nil {
		return fmt.Errorf("task %q not found for schedule %q", resolved.Message.JobID, resolved.ID)
	}

	job := func() error {
		return cmd.Execute(context.Background(), cloneExecutionMessage(msg))
	}

	sub, err := m.scheduler.AddHandler(handlerOpts.ToCommandConfig(), job)
	if err != nil {
		return fmt.Errorf("failed to register schedule %q: %w", def.ID, err)
	}

	m.mu.Lock()
	m.schedules[resolved.ID] = &scheduledEntry{
		definition:   resolved,
		subscription: sub,
	}
	m.mu.Unlock()

	return nil
}

// Update replaces an existing schedule in-place.
func (m *CronManager) Update(ctx context.Context, def ScheduleDefinition) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := def.Validate(); err != nil {
		return err
	}

	m.mu.RLock()
	existing, ok := m.schedules[def.ID]
	m.mu.RUnlock()
	if !ok {
		return fmt.Errorf("schedule %q not found", def.ID)
	}

	resolved, handlerOpts, msg, err := m.resolve(def)
	if err != nil {
		return err
	}

	cmd := m.buildCommander(resolved.Message.JobID)
	if cmd == nil {
		return fmt.Errorf("task %q not found for schedule %q", resolved.Message.JobID, resolved.ID)
	}

	job := func() error {
		return cmd.Execute(context.Background(), cloneExecutionMessage(msg))
	}

	sub, err := m.scheduler.AddHandler(handlerOpts.ToCommandConfig(), job)
	if err != nil {
		return fmt.Errorf("failed to update schedule %q: %w", def.ID, err)
	}

	m.mu.Lock()
	m.schedules[resolved.ID] = &scheduledEntry{
		definition:   resolved,
		subscription: sub,
	}
	m.mu.Unlock()

	if existing.subscription != nil {
		existing.subscription.Unsubscribe()
	}

	return nil
}

// Delete removes a schedule and unsubscribes it from the scheduler.
func (m *CronManager) Delete(ctx context.Context, id string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if id == "" {
		return fmt.Errorf("schedule id is required")
	}

	m.mu.Lock()
	entry, ok := m.schedules[id]
	if ok {
		delete(m.schedules, id)
	}
	m.mu.Unlock()

	if !ok {
		return fmt.Errorf("schedule %q not found", id)
	}

	if entry.subscription != nil {
		entry.subscription.Unsubscribe()
	}
	return nil
}

// List returns a copy of registered schedules.
func (m *CronManager) List() []ScheduleDefinition {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make([]ScheduleDefinition, 0, len(m.schedules))
	for _, entry := range m.schedules {
		out = append(out, cloneScheduleDefinition(entry.definition))
	}
	return out
}

// Reconcile aligns current schedules with the desired set, adding, updating, and removing as needed.
func (m *CronManager) Reconcile(ctx context.Context, desired []ScheduleDefinition) (ReconcileResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var result ReconcileResult
	targets := make(map[string]ScheduleDefinition, len(desired))
	for _, def := range desired {
		targets[def.ID] = def
	}

	for id, def := range targets {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		m.mu.RLock()
		existing, ok := m.schedules[id]
		m.mu.RUnlock()

		if !ok {
			if err := m.Register(ctx, def); err != nil {
				return result, err
			}
			result.Added = append(result.Added, id)
			continue
		}

		resolved, _, _, err := m.resolve(def)
		if err != nil {
			return result, err
		}

		if !definitionsEqual(resolved, existing.definition) {
			if err := m.Update(ctx, def); err != nil {
				return result, err
			}
			result.Updated = append(result.Updated, id)
		}
	}

	m.mu.RLock()
	currentIDs := make([]string, 0, len(m.schedules))
	for id := range m.schedules {
		currentIDs = append(currentIDs, id)
	}
	m.mu.RUnlock()

	for _, id := range currentIDs {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if _, ok := targets[id]; !ok {
			if err := m.Delete(ctx, id); err != nil {
				return result, err
			}
			result.Removed = append(result.Removed, id)
		}
	}

	return result, nil
}

// Validate ensures the schedule definition contains required fields.
func (d ScheduleDefinition) Validate() error {
	var fieldErrors []errors.FieldError

	if d.ID == "" {
		fieldErrors = append(fieldErrors, errors.FieldError{
			Field:   "id",
			Message: "cannot be empty",
		})
	}
	if d.Expression == "" {
		fieldErrors = append(fieldErrors, errors.FieldError{
			Field:   "expression",
			Message: "cannot be empty",
		})
	}
	if d.Message.JobID == "" {
		fieldErrors = append(fieldErrors, errors.FieldError{
			Field:   "message.job_id",
			Message: "cannot be empty",
		})
	}

	if len(fieldErrors) > 0 {
		return errors.NewValidation("schedule validation failed", fieldErrors...)
	}
	return nil
}

func (m *CronManager) resolve(def ScheduleDefinition) (ScheduleDefinition, HandlerOptions, *ExecutionMessage, error) {
	task, ok := m.registry.Get(def.Message.JobID)
	if !ok || task == nil {
		return ScheduleDefinition{}, HandlerOptions{}, nil, fmt.Errorf("task %q not found", def.Message.JobID)
	}

	mergedConfig := mergeConfigDefaults(task.GetConfig(), def.Message.Config)
	if def.Expression != "" {
		mergedConfig.Schedule = def.Expression
	}

	msg := def.Message
	msg.Config = mergedConfig

	execMsg, err := CompleteExecutionMessage(task, &msg)
	if err != nil {
		return ScheduleDefinition{}, HandlerOptions{}, nil, err
	}

	if err := execMsg.Validate(); err != nil {
		return ScheduleDefinition{}, HandlerOptions{}, nil, err
	}

	handlerOpts := applyConfigToHandlerOptions(task.GetHandlerConfig(), mergedConfig)
	if handlerOpts.Expression == "" {
		handlerOpts.Expression = DefaultSchedule
	}

	resolved := ScheduleDefinition{
		ID:         def.ID,
		Expression: handlerOpts.Expression,
		Message:    *cloneExecutionMessage(execMsg),
	}

	return resolved, handlerOpts, execMsg, nil
}

func (m *CronManager) buildCommander(taskID string) *TaskCommander {
	if m.registry == nil {
		return nil
	}
	task, ok := m.registry.Get(taskID)
	if !ok || task == nil {
		return nil
	}
	cmd := NewTaskCommander(task).
		WithIdempotencyTracker(m.tracker).
		WithConcurrencyLimiter(m.limiter).
		WithQuotaChecker(m.quotas)
	return cmd
}

func applyConfigToHandlerOptions(base HandlerOptions, cfg Config) HandlerOptions {
	if cfg.Schedule != "" {
		base.Expression = cfg.Schedule
	}
	// Keep retries handled by TaskCommander to avoid double-application in the runner layer.
	base.MaxRetries = 0
	if cfg.Timeout != 0 {
		base.Timeout = cfg.Timeout
	}
	if cfg.NoTimeout {
		base.NoTimeout = true
	}
	if !cfg.Deadline.IsZero() {
		base.Deadline = cfg.Deadline
	}
	if cfg.MaxRuns != 0 {
		base.MaxRuns = cfg.MaxRuns
	}
	if cfg.RunOnce {
		base.RunOnce = true
	}
	if cfg.ExitOnError {
		base.ExitOnError = true
	}
	return base
}

func definitionsEqual(a, b ScheduleDefinition) bool {
	return reflect.DeepEqual(a, b)
}

func cloneExecutionMessage(msg *ExecutionMessage) *ExecutionMessage {
	if msg == nil {
		return nil
	}
	cloned := *msg
	cloned.Parameters = cloneParams(msg.Parameters)
	if msg.Result != nil {
		resultCopy := *msg.Result
		cloned.Result = &resultCopy
	}
	return &cloned
}

func cloneScheduleDefinition(def ScheduleDefinition) ScheduleDefinition {
	return ScheduleDefinition{
		ID:         def.ID,
		Expression: def.Expression,
		Message:    *cloneExecutionMessage(&def.Message),
	}
}
