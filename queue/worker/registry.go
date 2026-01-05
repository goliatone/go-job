package worker

import (
	"fmt"
	"sync"

	job "github.com/goliatone/go-job"
)

// Entry stores a registered task and its commander.
type Entry struct {
	Task      job.Task
	Commander *job.TaskCommander
}

// Registry stores tasks by job_id and validates registrations.
type Registry struct {
	mu    sync.RWMutex
	tasks map[string]Entry
}

// NewRegistry creates an empty task registry.
func NewRegistry() *Registry {
	return &Registry{
		tasks: make(map[string]Entry),
	}
}

// Add registers a task in the registry.
func (r *Registry) Add(task job.Task, commander *job.TaskCommander) error {
	if r == nil {
		return fmt.Errorf("registry not configured")
	}
	if task == nil {
		return fmt.Errorf("task required")
	}

	taskID := task.GetID()
	if taskID == "" {
		return fmt.Errorf("task id required")
	}

	if task.GetPath() == "" {
		return fmt.Errorf("task script path required")
	}

	if commander == nil {
		commander = job.NewTaskCommander(task)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.tasks[taskID]; exists {
		return fmt.Errorf("task %q already registered", taskID)
	}

	r.tasks[taskID] = Entry{
		Task:      task,
		Commander: commander,
	}
	return nil
}

// Get retrieves a registered task entry.
func (r *Registry) Get(id string) (Entry, bool) {
	if r == nil {
		return Entry{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.tasks[id]
	return entry, ok
}

// List returns registered tasks.
func (r *Registry) List() []job.Task {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	tasks := make([]job.Task, 0, len(r.tasks))
	for _, entry := range r.tasks {
		tasks = append(tasks, entry.Task)
	}
	return tasks
}
