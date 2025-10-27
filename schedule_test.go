package job

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNextRunStandardParser(t *testing.T) {
	base := time.Date(2025, 3, 14, 15, 9, 0, 0, time.UTC)
	next, err := NextRun("*/5 * * * *", base)
	require.NoError(t, err)
	assert.Equal(t, time.Date(2025, 3, 14, 15, 10, 0, 0, time.UTC), next)
}

func TestNextRunSecondsParser(t *testing.T) {
	base := time.Date(2025, 3, 14, 15, 9, 30, 0, time.UTC)
	next, err := NextRun("*/10 * * * * *", base, WithSecondsPrecision())
	require.NoError(t, err)
	assert.Equal(t, time.Date(2025, 3, 14, 15, 9, 40, 0, time.UTC), next)
}

func TestNextRunInvalidExpression(t *testing.T) {
	_, err := NextRun("invalid", time.Now())
	require.Error(t, err)
}

func TestTaskScheduleExposure(t *testing.T) {
	config := Config{
		Schedule: "0 12 * * *",
		RunOnce:  true,
		Retries:  3,
		Timeout:  time.Minute,
	}

	schedule := NewTaskSchedule(config)
	assert.Equal(t, "0 12 * * *", schedule.Expression)
	assert.True(t, schedule.RunOnce)
	assert.Equal(t, 3, schedule.MaxRetries)
	assert.Equal(t, time.Minute, schedule.Timeout)
}

func TestTaskScheduleDefaultTimeout(t *testing.T) {
	config := Config{}
	schedule := NewTaskSchedule(config)
	assert.Equal(t, DefaultSchedule, schedule.Expression)
	assert.Equal(t, DefaultTimeout, schedule.Timeout)
}

func TestTaskScheduleFromTask(t *testing.T) {
	task := NewBaseTask("example", "path", "shell", Config{Schedule: "@daily"}, "echo", nil)
	schedule := TaskScheduleFromTask(task)
	assert.Equal(t, "@daily", schedule.Expression)
}
