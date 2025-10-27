package job

import (
	"fmt"
	"time"

	rcron "github.com/robfig/cron/v3"
)

// NextRun returns the next execution time for the provided cron expression using the
// same parser configuration as the embedded scheduler utilities.
func NextRun(expression string, after time.Time, opts ...SchedulerOption) (time.Time, error) {
	if expression == "" {
		return time.Time{}, fmt.Errorf("cron expression cannot be empty")
	}

	schedulerCfg := &schedulerConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(schedulerCfg)
		}
	}

	cronParser := schedulerCfg.parser()
	schedule, err := cronParser.Parse(expression)
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to parse cron expression %q: %w", expression, err)
	}

	base := after
	if base.IsZero() {
		base = time.Now().In(schedulerCfg.location())
	}

	next := schedule.Next(base)
	return next, nil
}

// SchedulerOption allows callers to control the behaviour of the NextRun helper.
type SchedulerOption func(*schedulerConfig)

// WithSecondsPrecision enables second-level cron expressions when calculating NextRun.
func WithSecondsPrecision() SchedulerOption {
	return func(c *schedulerConfig) {
		c.useSeconds = true
	}
}

// WithLocation overrides the time zone used by NextRun when none is supplied.
func WithLocation(loc *time.Location) SchedulerOption {
	return func(c *schedulerConfig) {
		c.locationOverride = loc
	}
}

type schedulerConfig struct {
	useSeconds       bool
	locationOverride *time.Location
}

func (c *schedulerConfig) location() *time.Location {
	if c.locationOverride != nil {
		return c.locationOverride
	}
	return time.Local
}

func (c *schedulerConfig) parser() rcron.Parser {
	fields := rcron.Minute | rcron.Hour | rcron.Dom | rcron.Month | rcron.Dow | rcron.Descriptor
	if c.useSeconds {
		fields |= rcron.Second
	}
	return rcron.NewParser(fields)
}

// TaskSchedule captures scheduling semantics for a task.
type TaskSchedule struct {
	Expression string        `json:"expression"`
	RunOnce    bool          `json:"run_once"`
	MaxRetries int           `json:"max_retries"`
	Timeout    time.Duration `json:"timeout"`
}

// NewTaskSchedule builds a TaskSchedule from a job Config.
func NewTaskSchedule(cfg Config) TaskSchedule {
	expression := cfg.Schedule
	if expression == "" {
		expression = DefaultSchedule
	}

	timeout := cfg.Timeout
	if timeout == 0 && !cfg.NoTimeout {
		timeout = DefaultTimeout
	}

	return TaskSchedule{
		Expression: expression,
		RunOnce:    cfg.RunOnce,
		MaxRetries: cfg.Retries,
		Timeout:    timeout,
	}
}

// TaskScheduleFromTask extracts scheduling semantics from a Task implementation.
func TaskScheduleFromTask(task Task) TaskSchedule {
	if task == nil {
		return NewTaskSchedule(Config{})
	}
	return NewTaskSchedule(task.GetConfig())
}
