package command

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/goliatone/go-job/queue"
	"github.com/goliatone/go-job/queue/worker"
)

// RegisterAll registers command tasks with a worker.
// When ids is empty, all registered commands are added.
func RegisterAll(w *worker.Worker, reg *Registry, ids []string) error {
	if w == nil {
		return fmt.Errorf("worker not configured")
	}
	if reg == nil {
		return fmt.Errorf("command registry not configured")
	}

	resolvedIDs, err := resolveRegistrationIDs(reg, ids)
	if err != nil {
		return err
	}

	for _, id := range resolvedIDs {
		if err := w.Register(NewTask(reg, id)); err != nil {
			return err
		}
	}
	return nil
}

// LocalWorkerConfig configures local same-machine queue worker bootstrap.
type LocalWorkerConfig struct {
	IDs           []string
	WorkerOptions []worker.Option
}

// NewLocalWorker builds a worker and registers command tasks from the queue command registry.
func NewLocalWorker(dequeuer queue.Dequeuer, reg *Registry, cfg LocalWorkerConfig) (*worker.Worker, error) {
	if dequeuer == nil {
		return nil, fmt.Errorf("dequeuer not configured")
	}
	if reg == nil {
		return nil, fmt.Errorf("command registry not configured")
	}

	w := worker.NewWorker(dequeuer, cfg.WorkerOptions...)
	if err := RegisterAll(w, reg, cfg.IDs); err != nil {
		return nil, err
	}
	return w, nil
}

// StartLocalWorker builds and starts a local same-machine queue worker.
func StartLocalWorker(ctx context.Context, dequeuer queue.Dequeuer, reg *Registry, cfg LocalWorkerConfig) (*worker.Worker, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	w, err := NewLocalWorker(dequeuer, reg, cfg)
	if err != nil {
		return nil, err
	}
	if err := w.Start(ctx); err != nil {
		return nil, err
	}
	return w, nil
}

func resolveRegistrationIDs(reg *Registry, ids []string) ([]string, error) {
	if reg == nil {
		return nil, fmt.Errorf("command registry not configured")
	}

	if len(ids) == 0 {
		entries := reg.List()
		out := make([]string, 0, len(entries))
		for _, entry := range entries {
			if strings.TrimSpace(entry.ID) == "" {
				continue
			}
			out = append(out, entry.ID)
		}
		sort.Strings(out)
		return out, nil
	}

	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		commandID := strings.TrimSpace(id)
		if commandID == "" {
			return nil, fmt.Errorf("command id cannot be empty")
		}
		if _, ok := reg.Get(commandID); !ok {
			return nil, fmt.Errorf("command %q not registered", commandID)
		}
		if _, ok := seen[commandID]; ok {
			continue
		}
		seen[commandID] = struct{}{}
		out = append(out, commandID)
	}
	return out, nil
}
