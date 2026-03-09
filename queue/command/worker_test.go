package command

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/goliatone/go-command"
	"github.com/goliatone/go-job/queue"
	"github.com/goliatone/go-job/queue/worker"
	"github.com/stretchr/testify/require"
)

type registerMessageA struct{}
type registerMessageB struct{}

type registerCommandA struct{}
type registerCommandB struct{}

func (registerCommandA) Execute(context.Context, registerMessageA) error { return nil }
func (registerCommandB) Execute(context.Context, registerMessageB) error { return nil }

func TestRegisterAllValidatesWorkerAndRegistry(t *testing.T) {
	reg := NewRegistry()
	require.Error(t, RegisterAll(nil, reg, nil))

	w := worker.NewWorker(noopDequeuer{})
	require.Error(t, RegisterAll(w, nil, nil))
}

func TestRegisterAllRegistersAllWhenIDsEmpty(t *testing.T) {
	reg := newRegisterAllTestRegistry(t)
	w := worker.NewWorker(noopDequeuer{})

	require.NoError(t, RegisterAll(w, reg, nil))
	ids := registeredTaskIDs(w)
	require.Len(t, ids, 2)
	require.Contains(t, ids, command.GetMessageType(registerMessageA{}))
	require.Contains(t, ids, command.GetMessageType(registerMessageB{}))
}

func TestRegisterAllRegistersExplicitSubset(t *testing.T) {
	reg := newRegisterAllTestRegistry(t)
	w := worker.NewWorker(noopDequeuer{})
	idA := command.GetMessageType(registerMessageA{})

	require.NoError(t, RegisterAll(w, reg, []string{idA}))
	ids := registeredTaskIDs(w)
	require.Equal(t, []string{idA}, ids)
}

func TestRegisterAllRejectsUnknownAndBlankIDs(t *testing.T) {
	reg := newRegisterAllTestRegistry(t)
	w := worker.NewWorker(noopDequeuer{})

	require.Error(t, RegisterAll(w, reg, []string{""}))
	require.Error(t, RegisterAll(w, reg, []string{"unknown.command"}))
}

func TestRegisterAllDeduplicatesExplicitIDs(t *testing.T) {
	reg := newRegisterAllTestRegistry(t)
	w := worker.NewWorker(noopDequeuer{})
	idA := command.GetMessageType(registerMessageA{})

	require.NoError(t, RegisterAll(w, reg, []string{idA, idA, idA}))
	ids := registeredTaskIDs(w)
	require.Equal(t, []string{idA}, ids)
}

func TestNewLocalWorkerRegistersCommandsAndAppliesOptions(t *testing.T) {
	reg := newRegisterAllTestRegistry(t)
	idA := command.GetMessageType(registerMessageA{})

	w, err := NewLocalWorker(noopDequeuer{}, reg, LocalWorkerConfig{
		IDs:           []string{idA},
		WorkerOptions: []worker.Option{worker.WithConcurrency(3)},
	})
	require.NoError(t, err)

	status := w.Status()
	require.Equal(t, 3, status.Concurrency)
	require.Equal(t, 1, status.Registered)
}

func TestNewLocalWorkerRejectsUnknownIDs(t *testing.T) {
	reg := newRegisterAllTestRegistry(t)
	_, err := NewLocalWorker(noopDequeuer{}, reg, LocalWorkerConfig{
		IDs: []string{"unknown.command"},
	})
	require.Error(t, err)
}

func TestStartLocalWorkerStartsAndStops(t *testing.T) {
	reg := newRegisterAllTestRegistry(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	w, err := StartLocalWorker(ctx, noopDequeuer{}, reg, LocalWorkerConfig{
		WorkerOptions: []worker.Option{worker.WithIdleDelay(0)},
	})
	require.NoError(t, err)
	require.True(t, w.Status().Running)
	require.NoError(t, w.Stop(context.Background()))
}

func newRegisterAllTestRegistry(t *testing.T) *Registry {
	t.Helper()
	reg := NewRegistry()
	require.NoError(t, RegisterCommand(reg, registerCommandA{}))
	require.NoError(t, RegisterCommand(reg, registerCommandB{}))
	return reg
}

func registeredTaskIDs(w *worker.Worker) []string {
	tasks := w.RegisteredTasks()
	out := make([]string, 0, len(tasks))
	for _, task := range tasks {
		if task == nil {
			continue
		}
		out = append(out, task.GetID())
	}
	slices.Sort(out)
	return out
}

type noopDequeuer struct{}

func (noopDequeuer) Dequeue(context.Context) (queue.Delivery, error) {
	return nil, nil
}
