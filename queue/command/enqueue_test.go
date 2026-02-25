package command

import (
	"context"
	"testing"
	"time"

	job "github.com/goliatone/go-job"
	"github.com/stretchr/testify/require"
)

func TestEnqueueAtUsesScheduledEnqueuer(t *testing.T) {
	enqueuer := &captureScheduledEnqueuer{}
	at := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	err := EnqueueAt(context.Background(), enqueuer, nil, "cmd.export", map[string]any{"id": "123"}, at)
	require.NoError(t, err)
	require.Equal(t, "cmd.export", enqueuer.msg.JobID)
	require.Equal(t, "cmd.export", enqueuer.msg.ScriptPath)
	require.Equal(t, at, enqueuer.at)
}

func TestEnqueueAfterUsesScheduledEnqueuer(t *testing.T) {
	enqueuer := &captureScheduledEnqueuer{}

	err := EnqueueAfter(context.Background(), enqueuer, nil, "cmd.export", map[string]any{"id": "123"}, 5*time.Second)
	require.NoError(t, err)
	require.Equal(t, "cmd.export", enqueuer.msg.JobID)
	require.Equal(t, "cmd.export", enqueuer.msg.ScriptPath)
	require.Equal(t, 5*time.Second, enqueuer.delay)
}

type captureScheduledEnqueuer struct {
	msg   *job.ExecutionMessage
	at    time.Time
	delay time.Duration
}

func (e *captureScheduledEnqueuer) Enqueue(_ context.Context, msg *job.ExecutionMessage) error {
	e.msg = msg
	return nil
}

func (e *captureScheduledEnqueuer) EnqueueAt(_ context.Context, msg *job.ExecutionMessage, at time.Time) error {
	e.msg = msg
	e.at = at
	return nil
}

func (e *captureScheduledEnqueuer) EnqueueAfter(_ context.Context, msg *job.ExecutionMessage, delay time.Duration) error {
	e.msg = msg
	e.delay = delay
	return nil
}
