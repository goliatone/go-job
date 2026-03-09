package command

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	job "github.com/goliatone/go-job"
	"github.com/goliatone/go-job/queue"
	qidempotency "github.com/goliatone/go-job/queue/idempotency"
)

const (
	// DispatchMetadataKey is the reserved execution parameter key for enqueue metadata.
	DispatchMetadataKey = "_dispatch_metadata"
	defaultScopeKey     = "global"
	defaultDedupTTL     = 24 * time.Hour
)

// EnqueueOptions defines dispatch metadata used by queue-backed command enqueue paths.
type EnqueueOptions struct {
	Delay          time.Duration
	RunAt          *time.Time
	IdempotencyKey string
	DedupPolicy    job.DeduplicationPolicy
	CorrelationID  string
	Metadata       map[string]any

	// IdempotencyStore enables durable deduplication for drop|merge|replace policies.
	IdempotencyStore qidempotency.Store
	// IdempotencyTTL controls dedup record expiration. Defaults to 24h when unset.
	IdempotencyTTL time.Duration
}

type dedupResolution struct {
	enabled       bool
	key           string
	ttl           time.Duration
	store         qidempotency.Store
	policy        job.DeduplicationPolicy
	createdRecord bool
}

type dedupReceiptPayload struct {
	DispatchID    string    `json:"dispatch_id"`
	EnqueuedAt    time.Time `json:"enqueued_at"`
	CommandID     string    `json:"command_id,omitempty"`
	CorrelationID string    `json:"correlation_id,omitempty"`
}

// Enqueue validates the command id and enqueues a background job.
func Enqueue(ctx context.Context, enqueuer queue.Enqueuer, reg *Registry, id string, params map[string]any) error {
	_, err := EnqueueWithOptions(ctx, enqueuer, reg, id, params, EnqueueOptions{})
	return err
}

// EnqueueAt validates the command id and schedules a background job.
func EnqueueAt(ctx context.Context, enqueuer queue.ScheduledEnqueuer, reg *Registry, id string, params map[string]any, at time.Time) error {
	_, err := EnqueueAtWithOptions(ctx, enqueuer, reg, id, params, at, EnqueueOptions{})
	return err
}

// EnqueueAfter validates the command id and schedules a delayed background job.
func EnqueueAfter(ctx context.Context, enqueuer queue.ScheduledEnqueuer, reg *Registry, id string, params map[string]any, delay time.Duration) error {
	_, err := EnqueueAfterWithOptions(ctx, enqueuer, reg, id, params, delay, EnqueueOptions{})
	return err
}

// EnqueueWithOptions validates command enqueue inputs and returns queue acceptance metadata.
func EnqueueWithOptions(ctx context.Context, enqueuer queue.Enqueuer, reg *Registry, id string, params map[string]any, opts EnqueueOptions) (queue.EnqueueReceipt, error) {
	if err := validateEnqueueInputs(enqueuer, reg, id); err != nil {
		return queue.EnqueueReceipt{}, err
	}
	if err := validateOptions(opts); err != nil {
		return queue.EnqueueReceipt{}, err
	}

	msg, err := buildExecutionMessage(id, params, opts)
	if err != nil {
		return queue.EnqueueReceipt{}, err
	}

	dedup, shortCircuit, err := prepareDedup(ctx, id, opts)
	if err != nil {
		return queue.EnqueueReceipt{}, err
	}
	if shortCircuit != nil {
		return *shortCircuit, nil
	}

	receipt, err := enqueueWithSchedule(ctx, enqueuer, msg, opts.RunAt, opts.Delay)
	if err != nil {
		rollbackDedup(ctx, dedup)
		return queue.EnqueueReceipt{}, err
	}

	if err := persistDedupReceipt(ctx, dedup, receipt, id, opts.CorrelationID); err != nil {
		return receipt, err
	}
	return receipt, nil
}

// EnqueueAtWithOptions validates command enqueue inputs for explicit scheduled execution.
func EnqueueAtWithOptions(ctx context.Context, enqueuer queue.ScheduledEnqueuer, reg *Registry, id string, params map[string]any, at time.Time, opts EnqueueOptions) (queue.EnqueueReceipt, error) {
	opts.RunAt = nil
	opts.Delay = 0
	if err := validateEnqueueInputs(enqueuer, reg, id); err != nil {
		return queue.EnqueueReceipt{}, err
	}
	if err := validateOptions(opts); err != nil {
		return queue.EnqueueReceipt{}, err
	}

	msg, err := buildExecutionMessage(id, params, opts)
	if err != nil {
		return queue.EnqueueReceipt{}, err
	}

	dedup, shortCircuit, err := prepareDedup(ctx, id, opts)
	if err != nil {
		return queue.EnqueueReceipt{}, err
	}
	if shortCircuit != nil {
		return *shortCircuit, nil
	}

	receipt, err := enqueueAtWithReceipt(ctx, enqueuer, msg, at)
	if err != nil {
		rollbackDedup(ctx, dedup)
		return queue.EnqueueReceipt{}, err
	}

	if err := persistDedupReceipt(ctx, dedup, receipt, id, opts.CorrelationID); err != nil {
		return receipt, err
	}
	return receipt, nil
}

// EnqueueAfterWithOptions validates command enqueue inputs for explicit delayed execution.
func EnqueueAfterWithOptions(ctx context.Context, enqueuer queue.ScheduledEnqueuer, reg *Registry, id string, params map[string]any, delay time.Duration, opts EnqueueOptions) (queue.EnqueueReceipt, error) {
	opts.RunAt = nil
	opts.Delay = 0
	if err := validateEnqueueInputs(enqueuer, reg, id); err != nil {
		return queue.EnqueueReceipt{}, err
	}
	if err := validateOptions(opts); err != nil {
		return queue.EnqueueReceipt{}, err
	}

	msg, err := buildExecutionMessage(id, params, opts)
	if err != nil {
		return queue.EnqueueReceipt{}, err
	}

	dedup, shortCircuit, err := prepareDedup(ctx, id, opts)
	if err != nil {
		return queue.EnqueueReceipt{}, err
	}
	if shortCircuit != nil {
		return *shortCircuit, nil
	}

	receipt, err := enqueueAfterWithReceipt(ctx, enqueuer, msg, delay)
	if err != nil {
		rollbackDedup(ctx, dedup)
		return queue.EnqueueReceipt{}, err
	}

	if err := persistDedupReceipt(ctx, dedup, receipt, id, opts.CorrelationID); err != nil {
		return receipt, err
	}
	return receipt, nil
}

// EnqueuePayloadWithOptions converts a typed payload into queue parameters and enqueues it.
func EnqueuePayloadWithOptions[T any](ctx context.Context, enqueuer queue.Enqueuer, reg *Registry, id string, payload T, opts EnqueueOptions) (queue.EnqueueReceipt, error) {
	params, err := ParametersFromPayload(payload)
	if err != nil {
		return queue.EnqueueReceipt{}, err
	}
	return EnqueueWithOptions(ctx, enqueuer, reg, id, params, opts)
}

// EnqueuePayloadAtWithOptions converts a typed payload into queue parameters and schedules it at a specific time.
func EnqueuePayloadAtWithOptions[T any](ctx context.Context, enqueuer queue.ScheduledEnqueuer, reg *Registry, id string, payload T, at time.Time, opts EnqueueOptions) (queue.EnqueueReceipt, error) {
	params, err := ParametersFromPayload(payload)
	if err != nil {
		return queue.EnqueueReceipt{}, err
	}
	return EnqueueAtWithOptions(ctx, enqueuer, reg, id, params, at, opts)
}

// EnqueuePayloadAfterWithOptions converts a typed payload into queue parameters and schedules it after a delay.
func EnqueuePayloadAfterWithOptions[T any](ctx context.Context, enqueuer queue.ScheduledEnqueuer, reg *Registry, id string, payload T, delay time.Duration, opts EnqueueOptions) (queue.EnqueueReceipt, error) {
	params, err := ParametersFromPayload(payload)
	if err != nil {
		return queue.EnqueueReceipt{}, err
	}
	return EnqueueAfterWithOptions(ctx, enqueuer, reg, id, params, delay, opts)
}

// ParametersFromPayload maps a typed payload to queue parameters via JSON tags.
func ParametersFromPayload[T any](payload T) (map[string]any, error) {
	value := any(payload)
	if value == nil {
		return nil, nil
	}
	if params, ok := value.(map[string]any); ok {
		return cloneAnyMap(params), nil
	}

	rv := reflect.ValueOf(value)
	if rv.Kind() == reflect.Ptr && rv.IsNil() {
		return nil, nil
	}

	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	if string(raw) == "null" {
		return nil, nil
	}

	var params map[string]any
	if err := json.Unmarshal(raw, &params); err != nil {
		return nil, fmt.Errorf("typed payload must encode to a JSON object: %w", err)
	}
	return cloneAnyMap(params), nil
}

func validateEnqueueInputs(enqueuer queue.Enqueuer, reg *Registry, id string) error {
	if enqueuer == nil {
		return fmt.Errorf("enqueuer not configured")
	}
	commandID := strings.TrimSpace(id)
	if commandID == "" {
		return fmt.Errorf("command id required")
	}
	if reg != nil {
		if _, ok := reg.Get(commandID); !ok {
			return fmt.Errorf("command %q not registered", commandID)
		}
	}
	return nil
}

func validateOptions(opts EnqueueOptions) error {
	if opts.Delay != 0 && opts.RunAt != nil {
		return fmt.Errorf("delay and run_at are mutually exclusive")
	}
	if opts.Delay < 0 {
		return fmt.Errorf("delay must be >= 0")
	}
	if opts.RunAt != nil && !opts.RunAt.After(time.Now()) {
		return fmt.Errorf("run_at must be in the future")
	}

	policy := normalizeDedupPolicy(opts.DedupPolicy)
	if policy == "" {
		policy = job.DedupPolicyIgnore
	}
	if !isValidDedupPolicy(policy) {
		return fmt.Errorf("invalid dedup policy %q", opts.DedupPolicy)
	}
	if requiresIdempotencyKey(policy) && strings.TrimSpace(opts.IdempotencyKey) == "" {
		return fmt.Errorf("idempotency_key is required for dedup policy %q", policy)
	}
	if policy != job.DedupPolicyIgnore && opts.IdempotencyStore == nil {
		return fmt.Errorf("shared idempotency store is required for dedup policy %q", policy)
	}
	return nil
}

func buildExecutionMessage(id string, params map[string]any, opts EnqueueOptions) (*job.ExecutionMessage, error) {
	cloned := cloneAnyMap(params)
	if len(opts.Metadata) > 0 {
		if cloned == nil {
			cloned = make(map[string]any)
		}
		cloned[DispatchMetadataKey] = cloneAnyMap(opts.Metadata)
	}
	msg := &job.ExecutionMessage{
		JobID:          strings.TrimSpace(id),
		ScriptPath:     strings.TrimSpace(id),
		Parameters:     cloned,
		IdempotencyKey: strings.TrimSpace(opts.IdempotencyKey),
		DedupPolicy:    normalizeDedupPolicy(opts.DedupPolicy),
		ExecutionID:    strings.TrimSpace(opts.CorrelationID),
	}
	if msg.DedupPolicy == "" {
		msg.DedupPolicy = job.DedupPolicyIgnore
	}
	return msg, nil
}

func enqueueWithSchedule(ctx context.Context, enqueuer queue.Enqueuer, msg *job.ExecutionMessage, runAt *time.Time, delay time.Duration) (queue.EnqueueReceipt, error) {
	if runAt != nil {
		scheduler, ok := enqueuer.(queue.ScheduledEnqueuer)
		if !ok {
			return queue.EnqueueReceipt{}, queue.ErrScheduledEnqueueUnsupported
		}
		return enqueueAtWithReceipt(ctx, scheduler, msg, *runAt)
	}
	if delay != 0 {
		scheduler, ok := enqueuer.(queue.ScheduledEnqueuer)
		if !ok {
			return queue.EnqueueReceipt{}, queue.ErrScheduledEnqueueUnsupported
		}
		return enqueueAfterWithReceipt(ctx, scheduler, msg, delay)
	}
	return enqueueWithReceipt(ctx, enqueuer, msg)
}

func enqueueWithReceipt(ctx context.Context, enqueuer queue.Enqueuer, msg *job.ExecutionMessage) (queue.EnqueueReceipt, error) {
	if receiver, ok := enqueuer.(queue.ReceiptEnqueuer); ok {
		return receiver.EnqueueWithReceipt(ctx, msg)
	}
	if err := enqueuer.Enqueue(ctx, msg); err != nil {
		return queue.EnqueueReceipt{}, err
	}
	return queue.EnqueueReceipt{}, nil
}

func enqueueAtWithReceipt(ctx context.Context, enqueuer queue.ScheduledEnqueuer, msg *job.ExecutionMessage, at time.Time) (queue.EnqueueReceipt, error) {
	if receiver, ok := enqueuer.(queue.ReceiptScheduledEnqueuer); ok {
		return receiver.EnqueueAtWithReceipt(ctx, msg, at)
	}
	if err := enqueuer.EnqueueAt(ctx, msg, at); err != nil {
		return queue.EnqueueReceipt{}, err
	}
	return queue.EnqueueReceipt{}, nil
}

func enqueueAfterWithReceipt(ctx context.Context, enqueuer queue.ScheduledEnqueuer, msg *job.ExecutionMessage, delay time.Duration) (queue.EnqueueReceipt, error) {
	if receiver, ok := enqueuer.(queue.ReceiptScheduledEnqueuer); ok {
		return receiver.EnqueueAfterWithReceipt(ctx, msg, delay)
	}
	if err := enqueuer.EnqueueAfter(ctx, msg, delay); err != nil {
		return queue.EnqueueReceipt{}, err
	}
	return queue.EnqueueReceipt{}, nil
}

func prepareDedup(ctx context.Context, commandID string, opts EnqueueOptions) (dedupResolution, *queue.EnqueueReceipt, error) {
	policy := normalizeDedupPolicy(opts.DedupPolicy)
	if policy == "" || policy == job.DedupPolicyIgnore {
		return dedupResolution{}, nil, nil
	}

	idempotencyKey := strings.TrimSpace(opts.IdempotencyKey)
	scope := extractTenantScope(opts.Metadata)
	dedupKey := canonicalDedupKey(scope, strings.TrimSpace(commandID), idempotencyKey)
	ttl := opts.IdempotencyTTL
	if ttl <= 0 {
		ttl = defaultDedupTTL
	}

	store := opts.IdempotencyStore
	record, created, err := store.Acquire(ctx, dedupKey, ttl)
	if err != nil {
		return dedupResolution{}, nil, err
	}

	switch policy {
	case job.DedupPolicyDrop:
		if !created {
			return dedupResolution{}, nil, fmt.Errorf("duplicate dispatch for %q with dedup policy %q", commandID, policy)
		}
	case job.DedupPolicyMerge:
		if !created {
			receipt, ok := decodeDedupReceipt(record.Payload)
			if ok {
				return dedupResolution{}, &receipt, nil
			}
			return dedupResolution{}, nil, fmt.Errorf("duplicate dispatch for %q with dedup policy %q", commandID, policy)
		}
	case job.DedupPolicyReplace:
		if !created {
			status := qidempotency.StatusPending
			var empty []byte
			expiresAt := time.Now().UTC().Add(ttl)
			if err := store.Update(ctx, dedupKey, qidempotency.Update{
				Status:    &status,
				Payload:   &empty,
				ExpiresAt: &expiresAt,
			}); err != nil {
				return dedupResolution{}, nil, err
			}
		}
	default:
		return dedupResolution{}, nil, fmt.Errorf("invalid dedup policy %q", policy)
	}

	return dedupResolution{
		enabled:       true,
		key:           dedupKey,
		ttl:           ttl,
		store:         store,
		policy:        policy,
		createdRecord: created,
	}, nil, nil
}

func persistDedupReceipt(ctx context.Context, res dedupResolution, receipt queue.EnqueueReceipt, commandID, correlationID string) error {
	if !res.enabled {
		return nil
	}
	status := qidempotency.StatusCompleted
	expiresAt := time.Now().UTC().Add(res.ttl)
	payload, err := json.Marshal(dedupReceiptPayload{
		DispatchID:    receipt.DispatchID,
		EnqueuedAt:    receipt.EnqueuedAt.UTC(),
		CommandID:     strings.TrimSpace(commandID),
		CorrelationID: strings.TrimSpace(correlationID),
	})
	if err != nil {
		return err
	}
	return res.store.Update(ctx, res.key, qidempotency.Update{
		Status:    &status,
		Payload:   &payload,
		ExpiresAt: &expiresAt,
	})
}

func rollbackDedup(ctx context.Context, res dedupResolution) {
	if !res.enabled {
		return
	}
	if res.createdRecord || res.policy == job.DedupPolicyReplace {
		_ = res.store.Delete(ctx, res.key)
	}
}

func decodeDedupReceipt(payload []byte) (queue.EnqueueReceipt, bool) {
	if len(payload) == 0 {
		return queue.EnqueueReceipt{}, false
	}
	var stored dedupReceiptPayload
	if err := json.Unmarshal(payload, &stored); err != nil {
		return queue.EnqueueReceipt{}, false
	}
	if strings.TrimSpace(stored.DispatchID) == "" || stored.EnqueuedAt.IsZero() {
		return queue.EnqueueReceipt{}, false
	}
	return queue.EnqueueReceipt{
		DispatchID: strings.TrimSpace(stored.DispatchID),
		EnqueuedAt: stored.EnqueuedAt.UTC(),
	}, true
}

func normalizeDedupPolicy(policy job.DeduplicationPolicy) job.DeduplicationPolicy {
	return job.DeduplicationPolicy(strings.ToLower(strings.TrimSpace(string(policy))))
}

func isValidDedupPolicy(policy job.DeduplicationPolicy) bool {
	switch policy {
	case "", job.DedupPolicyIgnore, job.DedupPolicyDrop, job.DedupPolicyMerge, job.DedupPolicyReplace:
		return true
	default:
		return false
	}
}

func requiresIdempotencyKey(policy job.DeduplicationPolicy) bool {
	switch policy {
	case job.DedupPolicyDrop, job.DedupPolicyMerge, job.DedupPolicyReplace:
		return true
	default:
		return false
	}
}

func canonicalDedupKey(scope, commandID, idempotencyKey string) string {
	scope = strings.TrimSpace(scope)
	if scope == "" {
		scope = defaultScopeKey
	}
	commandID = strings.TrimSpace(commandID)
	idempotencyKey = strings.TrimSpace(idempotencyKey)
	return fmt.Sprintf("%s:%s:%s", scope, commandID, idempotencyKey)
}

func extractTenantScope(metadata map[string]any) string {
	if len(metadata) == 0 {
		return defaultScopeKey
	}

	if scope := stringFromMap(metadata, "tenant_scope"); scope != "" {
		return scope
	}

	if tenantScope := tenantOrgScope(metadata); tenantScope != "" {
		return tenantScope
	}

	scopeRaw, ok := metadata["scope"]
	if !ok {
		return defaultScopeKey
	}
	scopeMap, ok := toMap(scopeRaw)
	if !ok {
		return defaultScopeKey
	}
	if tenantScope := tenantOrgScope(scopeMap); tenantScope != "" {
		return tenantScope
	}

	return defaultScopeKey
}

func tenantOrgScope(values map[string]any) string {
	tenant := stringFromMap(values, "tenant_id")
	if tenant == "" {
		return ""
	}
	org := stringFromMap(values, "organization_id")
	if org == "" {
		return tenant
	}
	return tenant + "/" + org
}

func stringFromMap(values map[string]any, key string) string {
	raw, ok := values[key]
	if !ok || raw == nil {
		return ""
	}
	switch value := raw.(type) {
	case string:
		return strings.TrimSpace(value)
	case fmt.Stringer:
		return strings.TrimSpace(value.String())
	default:
		return strings.TrimSpace(fmt.Sprint(value))
	}
}

func toMap(value any) (map[string]any, bool) {
	switch typed := value.(type) {
	case map[string]any:
		return typed, true
	case map[string]string:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = item
		}
		return out, true
	default:
		return nil, false
	}
}

func cloneAnyMap(values map[string]any) map[string]any {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]any, len(values))
	for key, value := range values {
		out[key] = cloneAny(value)
	}
	return out
}

func cloneAny(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return cloneAnyMap(typed)
	case map[string]string:
		out := make(map[string]string, len(typed))
		for key, item := range typed {
			out[key] = item
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for idx := range typed {
			out[idx] = cloneAny(typed[idx])
		}
		return out
	case []byte:
		if len(typed) == 0 {
			return []byte{}
		}
		out := make([]byte, len(typed))
		copy(out, typed)
		return out
	case json.RawMessage:
		out := make(json.RawMessage, len(typed))
		copy(out, typed)
		return out
	default:
		return typed
	}
}
