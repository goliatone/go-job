package job

import (
	"fmt"
	"sync"
)

type memoryRegistry struct {
	mx   sync.RWMutex
	jobs map[string]Task
}

func NewMemoryRegistry() *memoryRegistry {
	return &memoryRegistry{
		jobs: make(map[string]Task),
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
