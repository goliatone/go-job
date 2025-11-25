package job_test

import (
	"context"
	"math/rand"
	"testing"
	"time"

	"github.com/goliatone/go-job"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeBackoffDelayProfiles(t *testing.T) {
	restoreRand := job.TestSetBackoffRand(rand.New(rand.NewSource(1)))
	defer restoreRand()

	none := job.BackoffConfig{Strategy: job.BackoffNone}
	assert.Equal(t, time.Duration(0), job.TestComputeBackoffDelay(1, none))

	fixed := job.BackoffConfig{Strategy: job.BackoffFixed, Interval: 100 * time.Millisecond}
	assert.Equal(t, 100*time.Millisecond, job.TestComputeBackoffDelay(1, fixed))
	assert.Equal(t, 100*time.Millisecond, job.TestComputeBackoffDelay(2, fixed))

	exp := job.BackoffConfig{Strategy: job.BackoffExponential, Interval: 50 * time.Millisecond, MaxInterval: 150 * time.Millisecond, Jitter: false}
	assert.Equal(t, 50*time.Millisecond, job.TestComputeBackoffDelay(1, exp))
	assert.Equal(t, 100*time.Millisecond, job.TestComputeBackoffDelay(2, exp))
	assert.Equal(t, 150*time.Millisecond, job.TestComputeBackoffDelay(3, exp)) // capped
}

func TestRetryExecutorUsesBackoffAndStopsOnSuccess(t *testing.T) {
	restoreRand := job.TestSetBackoffRand(rand.New(rand.NewSource(2)))
	defer restoreRand()

	task := &flakyRetryTask{
		cfg: job.Config{
			Retries: 2,
			Backoff: job.BackoffConfig{Strategy: job.BackoffFixed, Interval: 10 * time.Millisecond},
		},
	}

	var delays []time.Duration
	restoreSleep := job.TestSetBackoffSleep(func(_ context.Context, d time.Duration) error {
		delays = append(delays, d)
		return nil
	})
	defer restoreSleep()

	cmd := job.NewTaskCommander(task)
	msg := &job.ExecutionMessage{JobID: "retry-task", ScriptPath: "/tmp/retry"}

	require.NoError(t, cmd.Execute(context.Background(), msg))
	assert.Equal(t, 2, task.count, "should retry once then succeed")
	assert.Contains(t, delays, 10*time.Millisecond)
}

type flakyRetryTask struct {
	count int
	cfg   job.Config
}

var _ job.Task = (*flakyRetryTask)(nil)

func (f *flakyRetryTask) GetID() string                        { return "retry-task" }
func (f *flakyRetryTask) GetHandler() func() error             { return func() error { return nil } }
func (f *flakyRetryTask) GetHandlerConfig() job.HandlerOptions { return job.HandlerOptions{} }
func (f *flakyRetryTask) GetConfig() job.Config                { return f.cfg }
func (f *flakyRetryTask) GetPath() string                      { return "/tmp/retry" }
func (f *flakyRetryTask) GetEngine() job.Engine                { return nil }
func (f *flakyRetryTask) Execute(_ context.Context, _ *job.ExecutionMessage) error {
	f.count++
	if f.count == 1 {
		return assert.AnError
	}
	return nil
}
