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
	require.NoError(t, storage.Enqueue(context.Background(), msg))

	out, receipt, err := storage.Dequeue(context.Background())
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, "export", out.JobID)
	assert.Equal(t, "token-1", receipt.Token)
	assert.Equal(t, 1, receipt.Attempts)

	require.NoError(t, storage.Ack(context.Background(), receipt))
	assert.Equal(t, 0, countRows(t, storage.db, storage.table))
}

func TestStorageNackDelayedRetry(t *testing.T) {
	storage, cleanup := setupStorage(t)
	defer cleanup()

	msg := &job.ExecutionMessage{JobID: "export", ScriptPath: "/tmp/export"}
	require.NoError(t, storage.Enqueue(context.Background(), msg))

	_, receipt, err := storage.Dequeue(context.Background())
	require.NoError(t, err)

	require.NoError(t, storage.Nack(context.Background(), receipt, queue.NackOptions{
		Delay:   5 * time.Second,
		Requeue: true,
		Reason:  "retry",
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
}

func TestStorageVisibilityTimeoutRequeues(t *testing.T) {
	storage, cleanup := setupStorage(t)
	defer cleanup()

	msg := &job.ExecutionMessage{JobID: "export", ScriptPath: "/tmp/export"}
	require.NoError(t, storage.Enqueue(context.Background(), msg))

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
	require.NoError(t, storage.Enqueue(context.Background(), msg))

	_, receipt, err := storage.Dequeue(context.Background())
	require.NoError(t, err)

	require.NoError(t, storage.Nack(context.Background(), receipt, queue.NackOptions{
		DeadLetter: true,
		Reason:     "fatal",
	}))

	assert.Equal(t, 1, countRows(t, storage.db, storage.dlqTable))
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
		WithIDFunc(sequence("msg-1")),
		WithTokenFunc(sequence("token-1", "token-2")),
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
