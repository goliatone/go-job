package worker

import (
	"context"
	"fmt"
	"time"
)

const (
	// WorkerStatusRunning means the worker is actively consuming deliveries.
	WorkerStatusRunning = "running"
	// WorkerStatusPaused means consumption is paused but worker goroutines remain active.
	WorkerStatusPaused = "paused"
	// WorkerStatusStopped means the worker has been stopped.
	WorkerStatusStopped = "stopped"
)

// Status reports worker control/runtime state for orchestration surfaces.
type Status struct {
	Status      string
	Running     bool
	Paused      bool
	Concurrency int
	Registered  int
	StartedAt   time.Time
	StoppedAt   time.Time
}

// Pause temporarily halts delivery consumption without stopping worker goroutines.
func (w *Worker) Pause() error {
	if w == nil {
		return fmt.Errorf("worker not configured")
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.running || w.paused {
		return nil
	}
	w.paused = true
	w.resumeCh = make(chan struct{})
	return nil
}

// Resume restarts delivery consumption after Pause.
func (w *Worker) Resume() error {
	if w == nil {
		return fmt.Errorf("worker not configured")
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.running || !w.paused {
		return nil
	}
	w.paused = false
	if w.resumeCh != nil {
		close(w.resumeCh)
		w.resumeCh = nil
	}
	return nil
}

// Status returns current worker execution control state.
func (w *Worker) Status() Status {
	if w == nil {
		return Status{Status: WorkerStatusStopped}
	}

	w.mu.Lock()
	running := w.running
	paused := w.paused
	concurrency := w.concurrency
	startedAt := w.startedAt
	stoppedAt := w.stoppedAt
	registry := w.registry
	w.mu.Unlock()

	status := WorkerStatusStopped
	if running {
		status = WorkerStatusRunning
		if paused {
			status = WorkerStatusPaused
		}
	}

	registered := 0
	if registry != nil {
		registered = len(registry.List())
	}

	return Status{
		Status:      status,
		Running:     running,
		Paused:      paused,
		Concurrency: concurrency,
		Registered:  registered,
		StartedAt:   startedAt,
		StoppedAt:   stoppedAt,
	}
}

func (w *Worker) waitIfPaused(ctx context.Context) error {
	for {
		paused, resume := w.pauseSnapshot()
		if !paused {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-resume:
		}
	}
}

func (w *Worker) pauseSnapshot() (bool, <-chan struct{}) {
	if w == nil {
		return false, nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if !w.paused {
		return false, nil
	}
	if w.resumeCh == nil {
		w.resumeCh = make(chan struct{})
	}
	return true, w.resumeCh
}
