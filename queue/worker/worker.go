package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	job "github.com/goliatone/go-job"
	"github.com/goliatone/go-job/queue"
	"github.com/goliatone/go-job/queue/cancellation"
)

const (
	defaultConcurrency = 1
	defaultIdleDelay   = 100 * time.Millisecond
	defaultCancelPoll  = 250 * time.Millisecond
)

// ShutdownHook runs during worker shutdown.
type ShutdownHook func(context.Context) error

// CommanderFactory builds a TaskCommander for a task.
type CommanderFactory func(job.Task) *job.TaskCommander

// Option configures a Worker instance.
type Option func(*Worker)

// WithConcurrency sets the number of worker goroutines.
func WithConcurrency(concurrency int) Option {
	return func(w *Worker) {
		if concurrency > 0 {
			w.concurrency = concurrency
		}
	}
}

// WithIdleDelay controls the delay between dequeue attempts when idle.
func WithIdleDelay(delay time.Duration) Option {
	return func(w *Worker) {
		if delay >= 0 {
			w.idleDelay = delay
		}
	}
}

// WithLogger injects a logger for worker events.
func WithLogger(logger job.Logger) Option {
	return func(w *Worker) {
		if logger != nil {
			w.logger = logger
		}
	}
}

// WithHooks adds lifecycle hooks.
func WithHooks(hooks ...Hook) Option {
	return func(w *Worker) {
		for _, hook := range hooks {
			if hook != nil {
				w.hooks = append(w.hooks, hook)
			}
		}
	}
}

// WithRetryPolicy sets the retry policy for nacks.
func WithRetryPolicy(policy RetryPolicy) Option {
	return func(w *Worker) {
		if policy != nil {
			w.retryPolicy = policy
		}
	}
}

// WithRegistry supplies a task registry.
func WithRegistry(registry *Registry) Option {
	return func(w *Worker) {
		if registry != nil {
			w.registry = registry
		}
	}
}

// WithShutdownHook registers a shutdown hook.
func WithShutdownHook(hook ShutdownHook) Option {
	return func(w *Worker) {
		if hook != nil {
			w.shutdownHooks = append(w.shutdownHooks, hook)
		}
	}
}

// WithTaskCommanderRetries enables or disables TaskCommander retries.
func WithTaskCommanderRetries(enabled bool) Option {
	return func(w *Worker) {
		w.commanderRetries = enabled
	}
}

// WithCommanderFactory overrides the TaskCommander construction.
func WithCommanderFactory(factory CommanderFactory) Option {
	return func(w *Worker) {
		if factory != nil {
			w.commanderFactory = factory
		}
	}
}

// WithCancellationStore configures a distributed cancellation store.
func WithCancellationStore(store cancellation.Store) Option {
	return func(w *Worker) {
		w.cancelStore = store
	}
}

// WithCancelKeyExtractor sets the key extractor for cancellation lookups.
func WithCancelKeyExtractor(fn func(*job.ExecutionMessage) string) Option {
	return func(w *Worker) {
		if fn != nil {
			w.cancelKeyFn = fn
		}
	}
}

// WithCancelPollInterval sets the polling interval for cancellation checks.
func WithCancelPollInterval(interval time.Duration) Option {
	return func(w *Worker) {
		if interval >= 0 {
			w.cancelPoll = interval
		}
	}
}

// Worker consumes queue deliveries and dispatches tasks.
type Worker struct {
	dequeuer         queue.Dequeuer
	registry         *Registry
	concurrency      int
	idleDelay        time.Duration
	logger           job.Logger
	hooks            []Hook
	retryPolicy      RetryPolicy
	commanderFactory CommanderFactory
	commanderRetries bool
	shutdownHooks    []ShutdownHook
	cancelStore      cancellation.Store
	cancelKeyFn      func(*job.ExecutionMessage) string
	cancelPoll       time.Duration
	mu               sync.Mutex
	wg               sync.WaitGroup
	running          bool
	cancel           context.CancelFunc
}

// NewWorker builds a worker with default settings.
func NewWorker(dequeuer queue.Dequeuer, opts ...Option) *Worker {
	loggerProvider := job.NewStdLoggerProvider()
	w := &Worker{
		dequeuer:    dequeuer,
		registry:    NewRegistry(),
		concurrency: defaultConcurrency,
		idleDelay:   defaultIdleDelay,
		cancelPoll:  defaultCancelPoll,
		logger:      loggerProvider.GetLogger("queue:worker"),
		retryPolicy: DefaultRetryPolicy{MaxAttempts: 1},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(w)
		}
	}
	if w.registry == nil {
		w.registry = NewRegistry()
	}
	if w.logger == nil {
		w.logger = loggerProvider.GetLogger("queue:worker")
	}
	if w.retryPolicy == nil {
		w.retryPolicy = DefaultRetryPolicy{MaxAttempts: 1}
	}
	return w
}

// Register adds a task to the worker registry.
func (w *Worker) Register(task job.Task) error {
	if w == nil {
		return fmt.Errorf("worker not configured")
	}
	if w.registry == nil {
		w.registry = NewRegistry()
	}
	return w.registry.Add(task, w.buildCommander(task))
}

// RegisterAll adds multiple tasks.
func (w *Worker) RegisterAll(tasks []job.Task) error {
	for _, task := range tasks {
		if task == nil {
			continue
		}
		if err := w.Register(task); err != nil {
			return err
		}
	}
	return nil
}

// RegisteredTasks returns the current task list.
func (w *Worker) RegisteredTasks() []job.Task {
	if w == nil || w.registry == nil {
		return nil
	}
	return w.registry.List()
}

// Start begins consuming messages using configured concurrency.
func (w *Worker) Start(ctx context.Context) error {
	if w == nil {
		return fmt.Errorf("worker not configured")
	}
	if w.dequeuer == nil {
		return fmt.Errorf("worker dequeuer not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return fmt.Errorf("worker already started")
	}
	runCtx, cancel := context.WithCancel(ctx)
	w.running = true
	w.cancel = cancel
	w.mu.Unlock()

	for i := 0; i < w.concurrency; i++ {
		w.wg.Add(1)
		go w.run(runCtx)
	}
	return nil
}

// Stop cancels active workers and waits for completion.
func (w *Worker) Stop(ctx context.Context) error {
	if w == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	w.mu.Lock()
	if !w.running {
		w.mu.Unlock()
		return nil
	}
	cancel := w.cancel
	w.running = false
	w.cancel = nil
	w.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	return w.runShutdownHooks(ctx)
}

func (w *Worker) run(ctx context.Context) {
	defer w.wg.Done()

	for {
		if ctx.Err() != nil {
			return
		}

		delivery, err := w.dequeuer.Dequeue(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logDequeuerError(err)
			w.waitIdle(ctx)
			continue
		}

		if delivery == nil {
			w.waitIdle(ctx)
			continue
		}

		w.handleDelivery(ctx, delivery)
	}
}

func (w *Worker) handleDelivery(ctx context.Context, delivery queue.Delivery) {
	started := time.Now()
	msg := delivery.Message()
	event := Event{
		Delivery:  delivery,
		Message:   msg,
		Attempt:   deliveryAttempts(delivery),
		StartedAt: started,
	}

	w.logStart(event)
	w.emitStart(ctx, event)

	if msg == nil {
		w.failDelivery(ctx, event, fmt.Errorf("delivery message required"), queue.NackOptions{
			DeadLetter: true,
			Reason:     "delivery message required",
		})
		return
	}

	if err := queue.ValidateRequiredMessage(msg); err != nil {
		w.failDelivery(ctx, event, err, queue.NackOptions{
			DeadLetter: true,
			Reason:     err.Error(),
		})
		return
	}

	entry, ok := w.registry.Get(msg.JobID)
	if !ok || entry.Task == nil {
		w.failDelivery(ctx, event, fmt.Errorf("task %q not registered", msg.JobID), queue.NackOptions{
			DeadLetter: true,
			Reason:     "task not registered",
		})
		return
	}
	event.Task = entry.Task

	if msg.ScriptPath != entry.Task.GetPath() {
		w.failDelivery(ctx, event, fmt.Errorf("script path mismatch for task %q", msg.JobID), queue.NackOptions{
			DeadLetter: true,
			Reason:     "script path mismatch",
		})
		return
	}

	cancelState := &cancelState{}
	cancelKey := w.cancellationKey(msg)
	if cancelKey != "" && w.checkCancellation(ctx, cancelKey, cancelState) {
		opts := w.cancelNackOptions(cancelState.reason())
		w.failDelivery(ctx, event, context.Canceled, opts)
		return
	}

	commander := entry.Commander
	if commander == nil {
		commander = w.buildCommander(entry.Task)
	}

	execCtx := ctx
	if cancelKey != "" && w.cancelStore != nil {
		var cancel context.CancelFunc
		execCtx, cancel = context.WithCancel(ctx)
		defer cancel()
		go w.monitorCancellation(execCtx, cancelKey, cancelState, cancel)
	}

	execErr := commander.Execute(execCtx, msg)
	event.Duration = time.Since(started)
	if execErr == nil {
		if err := delivery.Ack(ctx); err != nil {
			w.logAckError(event, err)
		}
		w.logSuccess(event)
		w.emitSuccess(ctx, event)
		return
	}
	if errors.Is(execErr, job.ErrIdempotentDrop) {
		if err := delivery.Ack(ctx); err != nil {
			w.logAckError(event, err)
		}
		w.logSuccess(event)
		w.emitSuccess(ctx, event)
		return
	}

	if cancelState.requested() {
		opts := w.cancelNackOptions(cancelState.reason())
		w.failDelivery(ctx, event, execErr, opts)
		return
	}

	event.Err = execErr
	opts := w.retryPolicy.Decide(event.Attempt, execErr)
	w.failDelivery(ctx, event, execErr, opts)
}

func (w *Worker) failDelivery(ctx context.Context, event Event, err error, opts queue.NackOptions) {
	event.Err = err
	event.Duration = time.Since(event.StartedAt)
	event.Delay = opts.Delay

	if opts.Requeue {
		w.logRetry(event)
		w.emitRetry(ctx, event)
	} else {
		w.logFailure(event)
		w.emitFailure(ctx, event)
	}

	if event.Delivery == nil {
		return
	}
	if nackErr := event.Delivery.Nack(ctx, opts); nackErr != nil {
		w.logNackError(event, nackErr)
	}
}

func (w *Worker) runShutdownHooks(ctx context.Context) error {
	if len(w.shutdownHooks) == 0 {
		return nil
	}
	var errs []error
	for _, hook := range w.shutdownHooks {
		if hook == nil {
			continue
		}
		if err := hook(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (w *Worker) buildCommander(task job.Task) *job.TaskCommander {
	if w.commanderFactory != nil {
		return w.commanderFactory(task)
	}
	commander := job.NewTaskCommander(task)
	if !w.commanderRetries {
		commander.WithRetryOverride(0)
	}
	return commander
}

func (w *Worker) waitIdle(ctx context.Context) {
	if w.idleDelay <= 0 {
		return
	}
	timer := time.NewTimer(w.idleDelay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func (w *Worker) emitStart(ctx context.Context, event Event) {
	for _, hook := range w.hooks {
		hook.OnStart(ctx, event)
	}
}

func (w *Worker) emitSuccess(ctx context.Context, event Event) {
	for _, hook := range w.hooks {
		hook.OnSuccess(ctx, event)
	}
}

func (w *Worker) emitFailure(ctx context.Context, event Event) {
	for _, hook := range w.hooks {
		hook.OnFailure(ctx, event)
	}
}

func (w *Worker) emitRetry(ctx context.Context, event Event) {
	for _, hook := range w.hooks {
		hook.OnRetry(ctx, event)
	}
}

func (w *Worker) logStart(event Event) {
	if w.logger == nil {
		return
	}
	jobID, scriptPath := eventMessageFields(event)
	w.logger.Debug("queue delivery started", "job_id", jobID, "script_path", scriptPath, "attempt", event.Attempt)
}

func (w *Worker) logSuccess(event Event) {
	if w.logger == nil {
		return
	}
	jobID, scriptPath := eventMessageFields(event)
	w.logger.Info("queue delivery succeeded", "job_id", jobID, "script_path", scriptPath, "attempt", event.Attempt, "duration", event.Duration)
}

func (w *Worker) logFailure(event Event) {
	if w.logger == nil {
		return
	}
	jobID, scriptPath := eventMessageFields(event)
	w.logger.Error("queue delivery failed", "job_id", jobID, "script_path", scriptPath, "attempt", event.Attempt, "duration", event.Duration, "error", event.Err)
}

func (w *Worker) logRetry(event Event) {
	if w.logger == nil {
		return
	}
	jobID, scriptPath := eventMessageFields(event)
	w.logger.Warn("queue delivery retry scheduled", "job_id", jobID, "script_path", scriptPath, "attempt", event.Attempt, "delay", event.Delay, "error", event.Err)
}

func (w *Worker) logDequeuerError(err error) {
	if w.logger == nil {
		return
	}
	w.logger.Warn("queue dequeue error", "error", err)
}

func (w *Worker) logAckError(event Event, err error) {
	if w.logger == nil {
		return
	}
	jobID, scriptPath := eventMessageFields(event)
	w.logger.Error("queue delivery ack failed", "job_id", jobID, "script_path", scriptPath, "error", err)
}

func (w *Worker) logNackError(event Event, err error) {
	if w.logger == nil {
		return
	}
	jobID, scriptPath := eventMessageFields(event)
	w.logger.Error("queue delivery nack failed", "job_id", jobID, "script_path", scriptPath, "error", err)
}

func (w *Worker) logCancelCheckError(key string, err error) {
	if w.logger == nil {
		return
	}
	w.logger.Warn("queue cancellation check failed", "cancel_key", key, "error", err)
}

func eventMessageFields(event Event) (string, string) {
	if event.Message != nil {
		return event.Message.JobID, event.Message.ScriptPath
	}
	if event.Task != nil {
		return event.Task.GetID(), event.Task.GetPath()
	}
	return "", ""
}

type deliveryAttemptsReader interface {
	Attempts() int
}

func deliveryAttempts(delivery queue.Delivery) int {
	if delivery == nil {
		return 1
	}
	if reader, ok := delivery.(deliveryAttemptsReader); ok {
		if attempts := reader.Attempts(); attempts > 0 {
			return attempts
		}
	}
	return 1
}
