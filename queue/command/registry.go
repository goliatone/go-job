package command

import (
	"context"
	"fmt"
	"sync"

	job "github.com/goliatone/go-job"
)

// HandlerFunc executes a command using decoded parameters.
type HandlerFunc func(ctx context.Context, params map[string]any) error

// Entry tracks a registered command handler.
type Entry struct {
	ID          string
	MessageType string
	Config      job.Config
	Handler     HandlerFunc
}

// Registry stores command handlers by id.
type Registry struct {
	mu      sync.RWMutex
	entries map[string]Entry
}

// NewRegistry creates an empty command registry.
func NewRegistry() *Registry {
	return &Registry{entries: make(map[string]Entry)}
}

// Register adds a command entry to the registry.
func (r *Registry) Register(entry Entry) error {
	if r == nil {
		return fmt.Errorf("command registry not configured")
	}
	if entry.ID == "" {
		return fmt.Errorf("command id required")
	}
	if entry.Handler == nil {
		return fmt.Errorf("command handler required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.entries[entry.ID]; exists {
		return fmt.Errorf("command %q already registered", entry.ID)
	}
	r.entries[entry.ID] = entry
	return nil
}

// Get returns a registered entry by id.
func (r *Registry) Get(id string) (Entry, bool) {
	if r == nil {
		return Entry{}, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	entry, ok := r.entries[id]
	return entry, ok
}

// List returns all registered entries.
func (r *Registry) List() []Entry {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	entries := make([]Entry, 0, len(r.entries))
	for _, entry := range r.entries {
		entries = append(entries, entry)
	}
	return entries
}
