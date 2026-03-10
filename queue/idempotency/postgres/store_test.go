package postgres

import (
	"context"
	"database/sql"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goliatone/go-job/queue/idempotency"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreAcquireGetUpdateDelete(t *testing.T) {
	store, cleanup := setupStore(t)
	defer cleanup()

	record, acquired, err := store.Acquire(context.Background(), "key-1", 10*time.Second)
	require.NoError(t, err)
	assert.True(t, acquired)
	assert.Equal(t, idempotency.StatusPending, record.Status)

	record, acquired, err = store.Acquire(context.Background(), "key-1", 10*time.Second)
	require.NoError(t, err)
	assert.False(t, acquired)

	payload := []byte("result")
	status := idempotency.StatusCompleted
	expiresAt := store.clock.Now().Add(20 * time.Second)
	require.NoError(t, store.Update(context.Background(), "key-1", idempotency.Update{
		Status:    &status,
		Payload:   &payload,
		ExpiresAt: &expiresAt,
	}))

	fetched, found, err := store.Get(context.Background(), "key-1")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, status, fetched.Status)
	assert.Equal(t, payload, fetched.Payload)

	require.NoError(t, store.Delete(context.Background(), "key-1"))
	_, found, err = store.Get(context.Background(), "key-1")
	require.NoError(t, err)
	assert.False(t, found)
}

func TestStoreExpiresRecords(t *testing.T) {
	store, cleanup := setupStore(t)
	defer cleanup()

	_, acquired, err := store.Acquire(context.Background(), "key-1", 5*time.Second)
	require.NoError(t, err)
	assert.True(t, acquired)

	store.clock.Advance(6 * time.Second)
	_, found, err := store.Get(context.Background(), "key-1")
	require.NoError(t, err)
	assert.False(t, found)

	_, acquired, err = store.Acquire(context.Background(), "key-1", 5*time.Second)
	require.NoError(t, err)
	assert.True(t, acquired)
}

func TestStoreAcquireConcurrentSameKey(t *testing.T) {
	store, cleanup := setupStore(t)
	defer cleanup()

	const workers = 12
	var acquiredCount atomic.Int64
	errCh := make(chan error, workers)

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			_, acquired, err := store.Acquire(context.Background(), "same-key", 30*time.Second)
			if err != nil {
				errCh <- err
				return
			}
			if acquired {
				acquiredCount.Add(1)
			}
		}()
	}
	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}
	assert.Equal(t, int64(1), acquiredCount.Load())
}

func TestStoreRejectsInvalidTableIdentifier(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	store := NewStore(db,
		WithDialect(DialectSQLite),
		WithTableName("idempotency_records;DROP TABLE idempotency_records"),
	)
	_, _, err = store.Acquire(context.Background(), "key-1", 10*time.Second)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid idempotency table identifier")
}

type testStore struct {
	*Store
	clock *manualClock
}

func setupStore(t *testing.T) (*testStore, func()) {
	db, err := sql.Open("sqlite3", "file:idempotency_store_test?mode=memory&cache=shared")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)

	clock := newManualClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

	store := NewStore(db,
		WithDialect(DialectSQLite),
		WithUseForUpdate(false),
		WithClock(clock.Now),
	)
	require.NoError(t, store.Migrate(context.Background()))

	cleanup := func() {
		_ = store.Cleanup(context.Background())
		_ = db.Close()
	}

	return &testStore{Store: store, clock: clock}, cleanup
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
