package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/goliatone/go-job/queue/cancellation"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreRequestGet(t *testing.T) {
	store, cleanup := setupStore(t)
	defer cleanup()

	req := cancellation.Request{Key: "export-1", Reason: "user"}
	require.NoError(t, store.Request(context.Background(), req))

	got, found, err := store.Get(context.Background(), "export-1")
	require.NoError(t, err)
	assert.True(t, found)
	assert.Equal(t, "export-1", got.Key)
	assert.Equal(t, "user", got.Reason)
	assert.Equal(t, store.clock.Now().UTC(), got.RequestedAt)
}

func TestStoreSubscribe(t *testing.T) {
	store, cleanup := setupStore(t)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, err := store.Subscribe(ctx)
	require.NoError(t, err)

	req := cancellation.Request{Key: "export-2", Reason: "timeout"}
	require.NoError(t, store.Request(context.Background(), req))

	select {
	case out := <-sub:
		assert.Equal(t, "export-2", out.Key)
		assert.Equal(t, "timeout", out.Reason)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timeout waiting for cancellation request")
	}
}

func TestStoreSubscribeHandlesBatchWithSameTimestamps(t *testing.T) {
	store, cleanup := setupStoreWithBatch(t, 2)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, err := store.Subscribe(ctx)
	require.NoError(t, err)

	for idx := 1; idx <= 5; idx++ {
		key := fmt.Sprintf("export-%d", idx)
		require.NoError(t, store.Request(context.Background(), cancellation.Request{
			Key:         key,
			Reason:      "batch",
			RequestedAt: store.clock.Now(),
		}))
	}

	received := make(map[string]struct{})
	deadline := time.After(1500 * time.Millisecond)
	for len(received) < 5 {
		select {
		case req := <-sub:
			received[req.Key] = struct{}{}
		case <-deadline:
			t.Fatalf("timed out waiting for subscription batch pagination, received=%d", len(received))
		}
	}
}

func TestStoreRejectsInvalidTableIdentifier(t *testing.T) {
	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	defer db.Close()

	store := NewStore(db,
		WithDialect(DialectSQLite),
		WithTableName("queue_cancellations;DROP TABLE queue_cancellations"),
	)
	err = store.Request(context.Background(), cancellation.Request{Key: "export-1"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid cancellation table identifier")
}

type testStore struct {
	*Store
	clock *manualClock
}

func setupStore(t *testing.T) (*testStore, func()) {
	return setupStoreWithBatch(t, 50)
}

func setupStoreWithBatch(t *testing.T, batch int) (*testStore, func()) {
	db, err := sql.Open("sqlite3", "file:cancellation_store_test?mode=memory&cache=shared")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)

	clock := newManualClock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))

	store := NewStore(db,
		WithDialect(DialectSQLite),
		WithClock(clock.Now),
		WithPollInterval(5*time.Millisecond),
		WithBatchSize(batch),
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
