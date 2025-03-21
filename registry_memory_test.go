package job_test

import (
	"sync"
	"testing"

	"github.com/goliatone/go-job"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemoryRegistry_AddAndGet(t *testing.T) {
	registry := job.NewMemoryRegistry()
	mockTask := new(MockTask)
	mockTask.On("GetID").Return("task-1")

	err := registry.Add(mockTask)
	require.NoError(t, err)

	retrieved, found := registry.Get("task-1")
	assert.True(t, found)
	assert.Equal(t, mockTask, retrieved)
}

func TestMemoryRegistry_Add_Duplicate(t *testing.T) {
	registry := job.NewMemoryRegistry()
	mockTask := new(MockTask)
	mockTask.On("GetID").Return("task-1")

	err := registry.Add(mockTask)
	require.NoError(t, err)

	err = registry.Add(mockTask)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already exists")
}

func TestMemoryRegistry_List(t *testing.T) {
	registry := job.NewMemoryRegistry()
	mockTask1 := new(MockTask)
	mockTask2 := new(MockTask)
	mockTask1.On("GetID").Return("task-1")
	mockTask2.On("GetID").Return("task-2")

	_ = registry.Add(mockTask1)
	_ = registry.Add(mockTask2)

	jobs := registry.List()
	assert.Len(t, jobs, 2)
}

func TestMemoryRegistry_Concurrency(t *testing.T) {
	registry := job.NewMemoryRegistry()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			mockTask := new(MockTask)
			mockTask.On("GetID").Return("task-%d", id)
			_ = registry.Add(mockTask)
		}(i)
	}

	wg.Wait()
	jobs := registry.List()
	assert.GreaterOrEqual(t, len(jobs), 1)
}
