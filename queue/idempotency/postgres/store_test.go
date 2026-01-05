package postgres

import (
	"context"
	"database/sql"
	"sync"
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

type testStore struct {
	*Store
	clock *manualClock
}

func setupStore(t *testing.T) (*testStore, func()) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)

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
