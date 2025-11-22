package job

import (
	"time"

	"github.com/goliatone/go-command"
)

// HandlerOptions extends command.HandlerConfig with exit-on-error semantics and
// implements the getter interfaces expected by go-command runner configurators.
type HandlerOptions struct {
	command.HandlerConfig
	ExitOnError bool `json:"exit_on_error" yaml:"exit_on_error"`
}

// ToCommandConfig returns the embedded command.HandlerConfig.
func (h HandlerOptions) ToCommandConfig() command.HandlerConfig {
	return h.HandlerConfig
}

// GetTimeout satisfies runner.TimeoutGetter.
func (h HandlerOptions) GetTimeout() time.Duration {
	return h.Timeout
}

// GetDeadline satisfies runner.DeadlineGetter.
func (h HandlerOptions) GetDeadline() time.Time {
	return h.Deadline
}

// GetRunOnce satisfies runner.RunOnceGetter.
func (h HandlerOptions) GetRunOnce() bool {
	return h.RunOnce
}

// GetMaxRetries satisfies runner.MaxRetriesGetter.
func (h HandlerOptions) GetMaxRetries() int {
	return h.MaxRetries
}

// GetMaxRuns satisfies runner.MaxRunsGetter.
func (h HandlerOptions) GetMaxRuns() int {
	return h.MaxRuns
}

// GetNoTimeout satisfies runner.NoTimeoutGetter when present.
func (h HandlerOptions) GetNoTimeout() bool {
	return h.NoTimeout
}

// GetExpression exposes the cron expression for schedulers.
func (h HandlerOptions) GetExpression() string {
	return h.Expression
}

// GetExitOnError satisfies runner.ExitOnErrorGetter.
func (h HandlerOptions) GetExitOnError() bool {
	return h.ExitOnError
}
