package command

import (
	"fmt"

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

	if len(ids) == 0 {
		for _, entry := range reg.List() {
			if err := w.Register(NewTask(reg, entry.ID)); err != nil {
				return err
			}
		}
		return nil
	}

	for _, id := range ids {
		if id == "" {
			continue
		}
		if err := w.Register(NewTask(reg, id)); err != nil {
			return err
		}
	}
	return nil
}
