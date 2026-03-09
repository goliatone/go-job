package command

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	job "github.com/goliatone/go-job"
	"github.com/goliatone/go-job/queue"
	qidempotency "github.com/goliatone/go-job/queue/idempotency"
	"github.com/stretchr/testify/require"
)

func TestEnqueueWithOptionsMapsCorrelationMetadataAndReceipt(t *testing.T) {
	enqueuer := newRecordingEnqueuer()
	params := map[string]any{"id": "123"}
	opts := EnqueueOptions{
		CorrelationID:  "corr-1",
		Metadata:       map[string]any{"tenant_id": "tenant-1", "organization_id": "org-1"},
		IdempotencyKey: "idem-1",
		DedupPolicy:    job.DedupPolicyIgnore,
	}

	receipt, err := EnqueueWithOptions(context.Background(), enqueuer, nil, "cmd.export", params, opts)
	require.NoError(t, err)
	require.Equal(t, "dispatch-1", receipt.DispatchID)
	require.False(t, receipt.EnqueuedAt.IsZero())

	require.Equal(t, "corr-1", enqueuer.msg.ExecutionID)
	require.Equal(t, "idem-1", enqueuer.msg.IdempotencyKey)
	require.Equal(t, job.DedupPolicyIgnore, enqueuer.msg.DedupPolicy)
	metadata, ok := enqueuer.msg.Parameters[DispatchMetadataKey].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "tenant-1", metadata["tenant_id"])
	require.Equal(t, "org-1", metadata["organization_id"])
}

func TestEnqueuePayloadWithOptionsUsesJSONTags(t *testing.T) {
	type payload struct {
		ExportID string `json:"export_id"`
		Count    int    `json:"count"`
	}
	enqueuer := newRecordingEnqueuer()

	_, err := EnqueuePayloadWithOptions(context.Background(), enqueuer, nil, "cmd.export", payload{
		ExportID: "exp-1",
		Count:    2,
	}, EnqueueOptions{})
	require.NoError(t, err)
	require.Equal(t, "exp-1", enqueuer.msg.Parameters["export_id"])
	require.Equal(t, float64(2), enqueuer.msg.Parameters["count"])
}

func TestEnqueueWithOptionsClonesInputParameters(t *testing.T) {
	enqueuer := newRecordingEnqueuer()
	params := map[string]any{
		"nested": map[string]any{
			"value": "alpha",
		},
	}

	_, err := EnqueueWithOptions(context.Background(), enqueuer, nil, "cmd.export", params, EnqueueOptions{})
	require.NoError(t, err)

	nested := params["nested"].(map[string]any)
	nested["value"] = "beta"
	captured := enqueuer.msg.Parameters["nested"].(map[string]any)
	require.Equal(t, "alpha", captured["value"])
}

func TestEnqueueWithOptionsScheduledParity(t *testing.T) {
	enqueuer := newRecordingEnqueuer()

	_, err := EnqueueWithOptions(context.Background(), enqueuer, nil, "cmd.export", map[string]any{"id": "1"}, EnqueueOptions{
		Delay: 5 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, 1, enqueuer.enqueueAfterCalls)

	runAt := time.Now().UTC().Add(30 * time.Second)
	_, err = EnqueueWithOptions(context.Background(), enqueuer, nil, "cmd.export", map[string]any{"id": "1"}, EnqueueOptions{
		RunAt: &runAt,
	})
	require.NoError(t, err)
	require.Equal(t, 1, enqueuer.enqueueAtCalls)
}

func TestEnqueueAtAndAfterWithOptionsReturnReceipts(t *testing.T) {
	enqueuer := newRecordingEnqueuer()
	at := time.Now().UTC().Add(1 * time.Minute)

	r1, err := EnqueueAtWithOptions(context.Background(), enqueuer, nil, "cmd.export", map[string]any{"id": "1"}, at, EnqueueOptions{})
	require.NoError(t, err)
	require.Equal(t, "dispatch-1", r1.DispatchID)

	r2, err := EnqueueAfterWithOptions(context.Background(), enqueuer, nil, "cmd.export", map[string]any{"id": "2"}, 10*time.Second, EnqueueOptions{})
	require.NoError(t, err)
	require.Equal(t, "dispatch-2", r2.DispatchID)
	require.Equal(t, 1, enqueuer.enqueueAtCalls)
	require.Equal(t, 1, enqueuer.enqueueAfterCalls)
}

func TestEnqueueWithOptionsDedupDrop(t *testing.T) {
	enqueuer := newRecordingEnqueuer()
	store := newMemoryDedupStore()
	opts := EnqueueOptions{
		DedupPolicy:      job.DedupPolicyDrop,
		IdempotencyKey:   "dup-key",
		IdempotencyStore: store,
		Metadata:         map[string]any{"tenant_scope": "scope-a"},
	}

	_, err := EnqueueWithOptions(context.Background(), enqueuer, nil, "cmd.export", map[string]any{"id": "1"}, opts)
	require.NoError(t, err)

	_, err = EnqueueWithOptions(context.Background(), enqueuer, nil, "cmd.export", map[string]any{"id": "1"}, opts)
	require.Error(t, err)
	require.Equal(t, 1, enqueuer.enqueueCalls)
	require.Equal(t, "scope-a:cmd.export:dup-key", store.lastAcquireKey())
}

func TestEnqueueWithOptionsDedupMergeReturnsPriorReceipt(t *testing.T) {
	enqueuer := newRecordingEnqueuer()
	store := newMemoryDedupStore()
	opts := EnqueueOptions{
		DedupPolicy:      job.DedupPolicyMerge,
		IdempotencyKey:   "merge-key",
		IdempotencyStore: store,
	}

	first, err := EnqueueWithOptions(context.Background(), enqueuer, nil, "cmd.export", map[string]any{"id": "1"}, opts)
	require.NoError(t, err)

	second, err := EnqueueWithOptions(context.Background(), enqueuer, nil, "cmd.export", map[string]any{"id": "1"}, opts)
	require.NoError(t, err)
	require.Equal(t, first.DispatchID, second.DispatchID)
	require.Equal(t, 1, enqueuer.enqueueCalls)
}

func TestEnqueueWithOptionsDedupReplaceEnqueuesAgain(t *testing.T) {
	enqueuer := newRecordingEnqueuer()
	store := newMemoryDedupStore()
	opts := EnqueueOptions{
		DedupPolicy:      job.DedupPolicyReplace,
		IdempotencyKey:   "replace-key",
		IdempotencyStore: store,
	}

	first, err := EnqueueWithOptions(context.Background(), enqueuer, nil, "cmd.export", map[string]any{"id": "1"}, opts)
	require.NoError(t, err)
	second, err := EnqueueWithOptions(context.Background(), enqueuer, nil, "cmd.export", map[string]any{"id": "2"}, opts)
	require.NoError(t, err)
	require.NotEqual(t, first.DispatchID, second.DispatchID)
	require.Equal(t, 2, enqueuer.enqueueCalls)
}

func TestEnqueueWithOptionsRequiresStoreForNonIgnorePolicies(t *testing.T) {
	_, err := EnqueueWithOptions(context.Background(), newRecordingEnqueuer(), nil, "cmd.export", map[string]any{"id": "1"}, EnqueueOptions{
		DedupPolicy:    job.DedupPolicyDrop,
		IdempotencyKey: "key",
	})
	require.Error(t, err)
}

func TestExtractTenantScopePriority(t *testing.T) {
	require.Equal(t, "tenant-scope", extractTenantScope(map[string]any{
		"tenant_scope": "tenant-scope",
		"tenant_id":    "tenant-1",
	}))
	require.Equal(t, "tenant-1/org-1", extractTenantScope(map[string]any{
		"tenant_id":       "tenant-1",
		"organization_id": "org-1",
	}))
	require.Equal(t, "tenant-2/org-2", extractTenantScope(map[string]any{
		"scope": map[string]any{
			"tenant_id":       "tenant-2",
			"organization_id": "org-2",
		},
	}))
	require.Equal(t, "global", extractTenantScope(nil))
}

type recordingEnqueuer struct {
	mu                sync.Mutex
	msg               *job.ExecutionMessage
	enqueueCalls      int
	enqueueAtCalls    int
	enqueueAfterCalls int
	counter           int
}

func newRecordingEnqueuer() *recordingEnqueuer {
	return &recordingEnqueuer{}
}

func (e *recordingEnqueuer) Enqueue(_ context.Context, msg *job.ExecutionMessage) error {
	_, err := e.EnqueueWithReceipt(context.Background(), msg)
	return err
}

func (e *recordingEnqueuer) EnqueueWithReceipt(_ context.Context, msg *job.ExecutionMessage) (queue.EnqueueReceipt, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.enqueueCalls++
	e.counter++
	e.msg = cloneExecutionMessage(msg)
	return queue.EnqueueReceipt{
		DispatchID: fmt.Sprintf("dispatch-%d", e.counter),
		EnqueuedAt: time.Unix(int64(e.counter), 0).UTC(),
	}, nil
}

func (e *recordingEnqueuer) EnqueueAt(_ context.Context, msg *job.ExecutionMessage, at time.Time) error {
	_, err := e.EnqueueAtWithReceipt(context.Background(), msg, at)
	return err
}

func (e *recordingEnqueuer) EnqueueAtWithReceipt(_ context.Context, msg *job.ExecutionMessage, at time.Time) (queue.EnqueueReceipt, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.enqueueAtCalls++
	e.counter++
	e.msg = cloneExecutionMessage(msg)
	return queue.EnqueueReceipt{
		DispatchID: fmt.Sprintf("dispatch-%d", e.counter),
		EnqueuedAt: at.UTC(),
	}, nil
}

func (e *recordingEnqueuer) EnqueueAfter(_ context.Context, msg *job.ExecutionMessage, delay time.Duration) error {
	_, err := e.EnqueueAfterWithReceipt(context.Background(), msg, delay)
	return err
}

func (e *recordingEnqueuer) EnqueueAfterWithReceipt(_ context.Context, msg *job.ExecutionMessage, _ time.Duration) (queue.EnqueueReceipt, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.enqueueAfterCalls++
	e.counter++
	e.msg = cloneExecutionMessage(msg)
	return queue.EnqueueReceipt{
		DispatchID: fmt.Sprintf("dispatch-%d", e.counter),
		EnqueuedAt: time.Unix(int64(e.counter), 0).UTC(),
	}, nil
}

func cloneExecutionMessage(msg *job.ExecutionMessage) *job.ExecutionMessage {
	if msg == nil {
		return nil
	}
	cp := *msg
	if msg.Parameters != nil {
		cp.Parameters = cloneAnyMap(msg.Parameters)
	}
	return &cp
}

type memoryDedupStore struct {
	mu          sync.Mutex
	records     map[string]qidempotency.Record
	acquireKeys []string
}

func newMemoryDedupStore() *memoryDedupStore {
	return &memoryDedupStore{
		records: make(map[string]qidempotency.Record),
	}
}

func (s *memoryDedupStore) Acquire(_ context.Context, key string, ttl time.Duration) (qidempotency.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.acquireKeys = append(s.acquireKeys, key)
	now := time.Now().UTC()
	if record, ok := s.records[key]; ok && !qidempotency.IsExpired(record, now) {
		return cloneRecord(record), false, nil
	}

	record := qidempotency.Record{
		Key:       key,
		Status:    qidempotency.StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
		ExpiresAt: now.Add(ttl),
	}
	s.records[key] = record
	return cloneRecord(record), true, nil
}

func (s *memoryDedupStore) Get(_ context.Context, key string) (qidempotency.Record, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[key]
	if !ok || qidempotency.IsExpired(record, time.Now().UTC()) {
		return qidempotency.Record{}, false, nil
	}
	return cloneRecord(record), true, nil
}

func (s *memoryDedupStore) Update(_ context.Context, key string, update qidempotency.Update) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.records[key]
	if !ok {
		return qidempotency.ErrNotFound
	}
	if update.Status != nil {
		record.Status = qidempotency.DefaultStatus(*update.Status)
	}
	if update.Payload != nil {
		if *update.Payload == nil {
			record.Payload = nil
		} else {
			record.Payload = append([]byte(nil), (*update.Payload)...)
		}
	}
	if update.ExpiresAt != nil {
		record.ExpiresAt = update.ExpiresAt.UTC()
	}
	record.UpdatedAt = time.Now().UTC()
	s.records[key] = record
	return nil
}

func (s *memoryDedupStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, key)
	return nil
}

func (s *memoryDedupStore) lastAcquireKey() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.acquireKeys) == 0 {
		return ""
	}
	return s.acquireKeys[len(s.acquireKeys)-1]
}

func cloneRecord(record qidempotency.Record) qidempotency.Record {
	record.Payload = append([]byte(nil), record.Payload...)
	return record
}

var _ qidempotency.Store = (*memoryDedupStore)(nil)
