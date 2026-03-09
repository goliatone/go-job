package command

import (
	"context"
	"database/sql"
	"sync/atomic"
	"testing"
	"time"

	"github.com/goliatone/go-command"
	"github.com/goliatone/go-job/queue"
	queuePostgres "github.com/goliatone/go-job/queue/adapters/postgres"
	"github.com/goliatone/go-job/queue/worker"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"
)

type integrationMessage struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

type integrationCommand struct {
	calls *atomic.Int32
	done  chan struct{}
}

func (c *integrationCommand) Execute(_ context.Context, msg integrationMessage) error {
	if c.calls != nil {
		c.calls.Add(1)
	}
	if c.done != nil {
		select {
		case c.done <- struct{}{}:
		default:
		}
	}
	_ = msg
	return nil
}

func TestCommandBridgeIntegrationEnqueueWorkerConsumeSuccess(t *testing.T) {
	adapter, cleanup := setupIntegrationAdapter(t)
	defer cleanup()

	reg := NewRegistry()
	var calls atomic.Int32
	done := make(chan struct{}, 1)
	cmd := &integrationCommand{calls: &calls, done: done}
	require.NoError(t, RegisterCommand(reg, cmd))
	id := command.GetMessageType(integrationMessage{})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	w, err := StartLocalWorker(ctx, adapter, reg, LocalWorkerConfig{
		WorkerOptions: []worker.Option{
			worker.WithConcurrency(1),
			worker.WithIdleDelay(0),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = w.Stop(context.Background())
	})

	receipt, err := EnqueueWithOptions(ctx, adapter, reg, id, map[string]any{
		"name":  "alpha",
		"count": 1,
	}, EnqueueOptions{})
	require.NoError(t, err)
	require.NotEmpty(t, receipt.DispatchID)

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("timeout waiting for command execution")
	}
	require.Equal(t, int32(1), calls.Load())

	require.Eventually(t, func() bool {
		status, err := adapter.GetDispatchStatus(context.Background(), receipt.DispatchID)
		return err == nil && status.State == queue.DispatchStateSucceeded && status.Inferred
	}, 2*time.Second, 20*time.Millisecond)
}

func TestCommandBridgeIntegrationExplicitIDsSubset(t *testing.T) {
	adapter, cleanup := setupIntegrationAdapter(t)
	defer cleanup()

	reg := NewRegistry()
	var callsA atomic.Int32
	doneA := make(chan struct{}, 1)
	require.NoError(t, RegisterCommand(reg, &integrationCommand{calls: &callsA, done: doneA}))

	var callsB atomic.Int32
	doneB := make(chan struct{}, 1)
	type secondMessage struct {
		Value string `json:"value"`
	}
	type secondCommand struct {
		calls *atomic.Int32
		done  chan struct{}
	}
	execSecond := secondCommand{calls: &callsB, done: doneB}
	require.NoError(t, RegisterCommand(reg, command.CommandFunc[secondMessage](func(_ context.Context, _ secondMessage) error {
		execSecond.calls.Add(1)
		select {
		case execSecond.done <- struct{}{}:
		default:
		}
		return nil
	})))

	idA := command.GetMessageType(integrationMessage{})
	idB := command.GetMessageType(secondMessage{})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	w, err := StartLocalWorker(ctx, adapter, reg, LocalWorkerConfig{
		IDs: []string{idA},
		WorkerOptions: []worker.Option{
			worker.WithConcurrency(1),
			worker.WithIdleDelay(0),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = w.Stop(context.Background())
	})
	require.Equal(t, 1, w.Status().Registered)

	_, err = EnqueueWithOptions(ctx, adapter, reg, idA, map[string]any{"name": "alpha", "count": 1}, EnqueueOptions{})
	require.NoError(t, err)
	select {
	case <-doneA:
	case <-ctx.Done():
		t.Fatal("timeout waiting for subset command execution")
	}
	require.Equal(t, int32(1), callsA.Load())

	receiptB, err := EnqueueWithOptions(ctx, adapter, reg, idB, map[string]any{"value": "beta"}, EnqueueOptions{})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		status, err := adapter.GetDispatchStatus(context.Background(), receiptB.DispatchID)
		return err == nil && status.State == queue.DispatchStateDeadLetter
	}, 2*time.Second, 20*time.Millisecond)
	require.Equal(t, int32(0), callsB.Load())
}

func TestCommandBridgeIntegrationInvalidCommandIDFailsFast(t *testing.T) {
	adapter, cleanup := setupIntegrationAdapter(t)
	defer cleanup()

	reg := NewRegistry()
	require.NoError(t, RegisterCommand(reg, &integrationCommand{}))

	_, err := EnqueueWithOptions(context.Background(), adapter, reg, "unknown.command", map[string]any{"name": "x"}, EnqueueOptions{})
	require.Error(t, err)
}

func TestCommandBridgeIntegrationStartLocalWorkerUnknownIDFails(t *testing.T) {
	adapter, cleanup := setupIntegrationAdapter(t)
	defer cleanup()

	reg := NewRegistry()
	require.NoError(t, RegisterCommand(reg, &integrationCommand{}))

	_, err := NewLocalWorker(adapter, reg, LocalWorkerConfig{
		IDs: []string{"unknown.command"},
	})
	require.Error(t, err)
}

func TestCommandBridgeIntegrationMalformedParametersDeadLetter(t *testing.T) {
	adapter, cleanup := setupIntegrationAdapter(t)
	defer cleanup()

	reg := NewRegistry()
	var calls atomic.Int32
	require.NoError(t, RegisterCommand(reg, &integrationCommand{calls: &calls}))
	id := command.GetMessageType(integrationMessage{})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	w, err := StartLocalWorker(ctx, adapter, reg, LocalWorkerConfig{
		WorkerOptions: []worker.Option{
			worker.WithConcurrency(1),
			worker.WithIdleDelay(0),
			worker.WithRetryPolicy(worker.DefaultRetryPolicy{MaxAttempts: 1}),
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = w.Stop(context.Background())
	})

	receipt, err := EnqueueWithOptions(ctx, adapter, reg, id, map[string]any{
		"name":  "alpha",
		"count": "not-an-int",
	}, EnqueueOptions{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		status, err := adapter.GetDispatchStatus(context.Background(), receipt.DispatchID)
		return err == nil && status.State == queue.DispatchStateDeadLetter
	}, 2*time.Second, 20*time.Millisecond)
	require.Equal(t, int32(0), calls.Load())
}

func setupIntegrationAdapter(t *testing.T) (*queuePostgres.Adapter, func()) {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	require.NoError(t, err)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	storage := queuePostgres.NewStorage(db,
		queuePostgres.WithDialect(queuePostgres.DialectSQLite),
		queuePostgres.WithUseSkipLocked(false),
		queuePostgres.WithVisibilityTimeout(2*time.Second),
		queuePostgres.WithIDFunc(sequence("msg-1", "msg-2", "msg-3", "msg-4", "msg-5", "msg-6", "msg-7")),
		queuePostgres.WithTokenFunc(sequence("token-1", "token-2", "token-3", "token-4", "token-5", "token-6", "token-7")),
	)
	require.NoError(t, storage.Migrate(context.Background()))

	cleanup := func() {
		_ = storage.Cleanup(context.Background())
		_ = db.Close()
	}

	return queuePostgres.NewAdapter(storage), cleanup
}

func sequence(values ...string) func() string {
	var next atomic.Int64
	return func() string {
		idx := int(next.Add(1) - 1)
		if idx >= len(values) {
			return ""
		}
		return values[idx]
	}
}
