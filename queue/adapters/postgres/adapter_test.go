package postgres

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	job "github.com/goliatone/go-job"
	"github.com/goliatone/go-job/queue"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStorageEnqueueDequeueAck(t *testing.T) {
	storage, cleanup := setupStorage(t)
	defer cleanup()

	msg := &job.ExecutionMessage{JobID: "export", ScriptPath: "/tmp/export"}
	_, err := storage.Enqueue(context.Background(), msg)
	require.NoError(t, err)

	out, receipt, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, "export", out.JobID)
	assert.Equal(t, "token-1", receipt.Token)
	assert.Equal(t, 1, receipt.Attempts)

	require.NoError(t, storage.Ack(context.Background(), receipt))
	assert.Equal(t, 0, countRows(t, storage.db, storage.table))
}

func TestAdapterEnqueueReceiptsAndDispatchStatusLifecycle(t *testing.T) {
	storage, cleanup := setupStorage(t)
	defer cleanup()
	adapter := NewAdapter(storage)
	ctx := context.Background()

	msg := &job.ExecutionMessage{JobID: "export", ScriptPath: "/tmp/export"}
	receipt, err := adapter.Enqueue(ctx, msg)
	require.NoError(t, err)
	require.NotEmpty(t, receipt.DispatchID)
	require.False(t, receipt.EnqueuedAt.IsZero())

	status, err := adapter.GetDispatchStatus(ctx, receipt.DispatchID)
	require.NoError(t, err)
	assert.Equal(t, queue.DispatchStateAccepted, status.State)

	delivery, err := adapter.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, delivery)

	status, err = adapter.GetDispatchStatus(ctx, receipt.DispatchID)
	require.NoError(t, err)
	assert.Equal(t, queue.DispatchStateRunning, status.State)

	require.NoError(t, delivery.Nack(ctx, queue.NackOptions{
		Disposition: queue.NackDispositionRetry,
		Delay:       5 * time.Second,
		Reason:      "retry",
	}))
	status, err = adapter.GetDispatchStatus(ctx, receipt.DispatchID)
	require.NoError(t, err)
	assert.Equal(t, queue.DispatchStateRetrying, status.State)
	assert.Equal(t, "retry", status.TerminalReason)

	storage.clock.Advance(5 * time.Second)
	delivery, err = adapter.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, delivery)
	require.NoError(t, delivery.Nack(ctx, queue.NackOptions{
		Disposition: queue.NackDispositionDeadLetter,
		Reason:      "fatal",
	}))

	status, err = adapter.GetDispatchStatus(ctx, receipt.DispatchID)
	require.NoError(t, err)
	assert.Equal(t, queue.DispatchStateDeadLetter, status.State)
	assert.Equal(t, "fatal", status.TerminalReason)

	receipt2, err := adapter.Enqueue(ctx, msg)
	require.NoError(t, err)
	delivery2, err := adapter.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, delivery2)
	require.NoError(t, delivery2.Ack(ctx))

	status2, err := adapter.GetDispatchStatus(ctx, receipt2.DispatchID)
	require.NoError(t, err)
	assert.Equal(t, queue.DispatchStateSucceeded, status2.State)
}

func TestAdapterScheduledReceiptVariants(t *testing.T) {
	storage, cleanup := setupStorage(t)
	defer cleanup()
	adapter := NewAdapter(storage)

	msg := &job.ExecutionMessage{JobID: "export", ScriptPath: "/tmp/export"}
	at := storage.clock.Now().Add(1 * time.Minute)

	r1, err := adapter.EnqueueAt(context.Background(), msg, at)
	require.NoError(t, err)
	require.NotEmpty(t, r1.DispatchID)

	r2, err := adapter.EnqueueAfter(context.Background(), msg, 5*time.Second)
	require.NoError(t, err)
	require.NotEmpty(t, r2.DispatchID)
}

func TestAdapterDispatchStatusTerminalFailedAndCanceled(t *testing.T) {
	storage, cleanup := setupStorage(t)
	defer cleanup()
	adapter := NewAdapter(storage)
	ctx := context.Background()

	msg := &job.ExecutionMessage{JobID: "export", ScriptPath: "/tmp/export"}
	failedReceipt, err := adapter.Enqueue(ctx, msg)
	require.NoError(t, err)
	failedDelivery, err := adapter.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, failedDelivery)
	require.NoError(t, failedDelivery.Nack(ctx, queue.NackOptions{
		Disposition: queue.NackDispositionFailed,
		Reason:      "failed-terminal",
	}))

	status, err := adapter.GetDispatchStatus(ctx, failedReceipt.DispatchID)
	require.NoError(t, err)
	assert.Equal(t, queue.DispatchStateFailed, status.State)
	assert.Equal(t, "failed-terminal", status.TerminalReason)

	canceledReceipt, err := adapter.Enqueue(ctx, msg)
	require.NoError(t, err)
	canceledDelivery, err := adapter.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, canceledDelivery)
	require.NoError(t, canceledDelivery.Nack(ctx, queue.NackOptions{
		Disposition: queue.NackDispositionCanceled,
		Reason:      "canceled-terminal",
	}))

	status, err = adapter.GetDispatchStatus(ctx, canceledReceipt.DispatchID)
	require.NoError(t, err)
	assert.Equal(t, queue.DispatchStateCanceled, status.State)
	assert.Equal(t, "canceled-terminal", status.TerminalReason)
}

func TestAdapterGetDispatchStatusUnsupported(t *testing.T) {
	adapter := NewAdapter(stubStorageNoStatus{})
	_, err := adapter.GetDispatchStatus(context.Background(), "dispatch-1")
	require.ErrorIs(t, err, queue.ErrDispatchStatusUnsupported)
}

func TestAdapterGetDispatchStatusNotFound(t *testing.T) {
	storage, cleanup := setupStorage(t)
	defer cleanup()

	adapter := NewAdapter(storage)
	_, err := adapter.GetDispatchStatus(context.Background(), "missing-dispatch")
	require.ErrorIs(t, err, queue.ErrDispatchNotFound)
}

func TestStorageLegacyMessageBecomesStatusTrackedOnTransition(t *testing.T) {
	storage, cleanup := setupStorage(t)
	defer cleanup()

	ctx := context.Background()
	msg := &job.ExecutionMessage{JobID: "legacy", ScriptPath: "/tmp/legacy"}
	payload, err := queue.EncodeExecutionMessage(msg)
	require.NoError(t, err)

	now := storage.clock.Now().UTC().UnixNano()
	_, err = storage.db.ExecContext(ctx, "INSERT INTO "+storage.table+" (id, payload, attempts, available_at, leased_until, token, created_at, updated_at) VALUES (?, ?, 0, ?, 0, '', ?, ?)",
		"legacy-1", string(payload), now, now, now)
	require.NoError(t, err)

	adapter := NewAdapter(storage)
	delivery, err := adapter.Dequeue(ctx)
	require.NoError(t, err)
	require.NotNil(t, delivery)

	status, err := adapter.GetDispatchStatus(ctx, "legacy-1")
	require.NoError(t, err)
	assert.Equal(t, queue.DispatchStateRunning, status.State)

	require.NoError(t, delivery.Ack(ctx))
	status, err = adapter.GetDispatchStatus(ctx, "legacy-1")
	require.NoError(t, err)
	assert.Equal(t, queue.DispatchStateSucceeded, status.State)
}

func TestStorageNackDelayedRetry(t *testing.T) {
	storage, cleanup := setupStorage(t)
	defer cleanup()

	msg := &job.ExecutionMessage{JobID: "export", ScriptPath: "/tmp/export"}
	_, err := storage.Enqueue(context.Background(), msg)
	require.NoError(t, err)

	_, receipt, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	require.False(t, receipt.CreatedAt.IsZero())

	require.NoError(t, storage.Nack(context.Background(), receipt, queue.NackOptions{
		Disposition: queue.NackDispositionRetry,
		Delay:       5 * time.Second,
		Reason:      "retry",
	}))

	none, _, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	assert.Nil(t, none)

	storage.clock.Advance(5 * time.Second)
	out, receipt, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, 2, receipt.Attempts)
	assert.Equal(t, "token-2", receipt.Token)
	assert.Equal(t, "retry", receipt.LastError)
	assert.Equal(t, storage.clock.Now(), receipt.AvailableAt)
}

func TestStorageVisibilityTimeoutRequeues(t *testing.T) {
	storage, cleanup := setupStorage(t)
	defer cleanup()

	msg := &job.ExecutionMessage{JobID: "export", ScriptPath: "/tmp/export"}
	_, err := storage.Enqueue(context.Background(), msg)
	require.NoError(t, err)

	_, receipt, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "token-1", receipt.Token)

	storage.clock.Advance(3 * time.Second)
	out, receipt, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, 2, receipt.Attempts)
	assert.Equal(t, "token-2", receipt.Token)
}

func TestStorageDeadLetters(t *testing.T) {
	storage, cleanup := setupStorage(t)
	defer cleanup()

	msg := &job.ExecutionMessage{JobID: "export", ScriptPath: "/tmp/export"}
	_, err := storage.Enqueue(context.Background(), msg)
	require.NoError(t, err)

	_, receipt, err := storage.Dequeue(context.Background())
	require.NoError(t, err)

	require.NoError(t, storage.Nack(context.Background(), receipt, queue.NackOptions{
		Disposition: queue.NackDispositionDeadLetter,
		Reason:      "fatal",
	}))

	assert.Equal(t, 1, countRows(t, storage.db, storage.dlqTable))
}

func TestStorageEnqueueAt(t *testing.T) {
	storage, cleanup := setupStorage(t)
	defer cleanup()

	msg := &job.ExecutionMessage{JobID: "export", ScriptPath: "/tmp/export"}
	_, err := storage.EnqueueAt(context.Background(), msg, storage.clock.Now().Add(10*time.Second))
	require.NoError(t, err)

	none, _, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	assert.Nil(t, none)

	storage.clock.Advance(10 * time.Second)
	out, _, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, "export", out.JobID)
}

func TestStorageExtendLease(t *testing.T) {
	storage, cleanup := setupStorage(t)
	defer cleanup()

	msg := &job.ExecutionMessage{JobID: "export", ScriptPath: "/tmp/export"}
	_, err := storage.Enqueue(context.Background(), msg)
	require.NoError(t, err)

	_, receipt, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	require.NoError(t, storage.ExtendLease(context.Background(), receipt, 10*time.Second))

	storage.clock.Advance(3 * time.Second)
	none, _, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	assert.Nil(t, none)

	storage.clock.Advance(8 * time.Second)
	out, next, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, 2, next.Attempts)
	assert.Equal(t, "token-2", next.Token)
}

func TestStorageRejectsInvalidTableIdentifiers(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	storage := NewStorage(db,
		WithDialect(DialectSQLite),
		WithTableName("queue_messages;DROP TABLE queue_messages"),
	)
	_, err = storage.Enqueue(context.Background(), &job.ExecutionMessage{JobID: "export"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid queue table identifier")
}

type testStorage struct {
	*Storage
	clock *manualClock
}

func setupStorage(t *testing.T) (*testStorage, func()) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)

	clock := newManualClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

	storage := NewStorage(db,
		WithDialect(DialectSQLite),
		WithUseSkipLocked(false),
		WithClock(clock.Now),
		WithVisibilityTimeout(2*time.Second),
		WithIDFunc(sequence("msg-1", "msg-2", "msg-3", "msg-4", "msg-5")),
		WithTokenFunc(sequence("token-1", "token-2", "token-3", "token-4", "token-5")),
	)
	require.NoError(t, storage.Migrate(context.Background()))

	cleanup := func() {
		_ = storage.Cleanup(context.Background())
		_ = db.Close()
	}

	return &testStorage{Storage: storage, clock: clock}, cleanup
}

func countRows(t *testing.T, db *sql.DB, table string) int {
	var count int
	row := db.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM "+table)
	require.NoError(t, row.Scan(&count))
	return count
}

type manualClock struct {
	mu  sync.Mutex
	now time.Time
}

func newManualClock(start time.Time) *manualClock {
	return &manualClock{now: start}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func sequence(values ...string) func() string {
	index := 0
	return func() string {
		if index >= len(values) {
			return ""
		}
		value := values[index]
		index++
		return value
	}
}

type stubStorageNoStatus struct{}

func (stubStorageNoStatus) Enqueue(context.Context, *job.ExecutionMessage) (queue.EnqueueReceipt, error) {
	return queue.EnqueueReceipt{}, nil
}

func (stubStorageNoStatus) Dequeue(context.Context) (*job.ExecutionMessage, queue.Receipt, error) {
	return nil, queue.Receipt{}, nil
}

func (stubStorageNoStatus) Ack(context.Context, queue.Receipt) error {
	return nil
}

func (stubStorageNoStatus) Nack(context.Context, queue.Receipt, queue.NackOptions) error {
	return nil
}
