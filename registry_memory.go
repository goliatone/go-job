package job

import (
	"fmt"
	"sync"
)

type memoryRegistry struct {
	mx      sync.RWMutex
	jobs    map[string]Task
	results map[string]Result
}

func NewMemoryRegistry() *memoryRegistry {
	return &memoryRegistry{
		jobs:    make(map[string]Task),
		results: make(map[string]Result),
	}
}

func (r *memoryRegistry) Add(job Task) error {
	r.mx.Lock()
	defer r.mx.Unlock()

	id := job.GetID()
	if _, exists := r.jobs[id]; exists {
		return fmt.Errorf("job with ID %s already exists", id)
	}

	r.jobs[id] = job
	return nil
}

func (r *memoryRegistry) Get(id string) (Task, bool) {
	r.mx.Lock()
	defer r.mx.Unlock()

	job, ok := r.jobs[id]
	return job, ok
}

func (r *memoryRegistry) List() []Task {
	r.mx.Lock()
	defer r.mx.Unlock()

	jobs := make([]Task, 0, len(r.jobs))
	for _, job := range r.jobs {
		jobs = append(jobs, job)
	}
	return jobs
}

func (r *memoryRegistry) SetResult(id string, result Result) error {
	r.mx.Lock()
	defer r.mx.Unlock()
	if id == "" {
		return fmt.Errorf("job id required")
	}
	r.results[id] = result
	return nil
}

func (r *memoryRegistry) GetResult(id string) (Result, bool) {
	r.mx.Lock()
	defer r.mx.Unlock()
	result, ok := r.results[id]
	return result, ok
}
