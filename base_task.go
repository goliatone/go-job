package job

import (
	"context"
	"fmt"
	"time"

	"github.com/goliatone/go-command"
)

type baseTask struct {
	id            string
	scriptPath    string
	scriptType    string
	handlerOpts   HandlerOptions
	config        Config
	scriptContent string
	engine        Engine
	logger        Logger
}

var _ Task = &baseTask{}

func (j *baseTask) GetID() string {
	return j.id
}

func (j *baseTask) GetHandler() func() error {
	return func() error {
		return j.Execute(context.Background(), nil)
	}
}

func (j *baseTask) GetHandlerConfig() HandlerOptions {
	return j.handlerOpts
}

func (j *baseTask) GetConfig() Config {
	return j.config
}

func (j *baseTask) GetPath() string {
	return j.scriptPath
}

func (j *baseTask) GetEngine() Engine {
	return j.engine
}

func (j *baseTask) Execute(ctx context.Context, msg *ExecutionMessage) error {
	if ctx == nil {
		ctx = context.Background()
	}

	execMsg, err := j.buildExecutionMessage(msg)
	if err != nil {
		return err
	}

	logger := j.taskLogger()
	baseArgs := []any{"task_id", j.id, "script_path", j.scriptPath}
	if j.engine != nil {
		baseArgs = append(baseArgs, "engine", j.engine.Name())
	}

	logger.Debug("task execution started", baseArgs...)

	start := time.Now()
	err = j.engine.Execute(ctx, execMsg)
	duration := time.Since(start)

	durationArgs := append(append([]any{}, baseArgs...), "duration", duration)

	if err != nil {
		logger.Error("task execution failed", append(durationArgs, "error", err)...)
		return err
	}

	logger.Info("task execution completed", durationArgs...)
	return nil
}

func (j *baseTask) buildExecutionMessage(msg *ExecutionMessage) (*ExecutionMessage, error) {
	if j.engine == nil {
		return nil, fmt.Errorf("task %s has no engine", j.id)
	}

	if msg == nil {
		msg = &ExecutionMessage{}
	}

	if msg.JobID == "" {
		msg.JobID = j.id
	}

	if msg.ScriptPath == "" {
		msg.ScriptPath = j.scriptPath
	}

	msg.Config = mergeConfigDefaults(j.config, msg.Config)

	if msg.Parameters == nil {
		msg.Parameters = make(map[string]any)
	}
	if _, ok := msg.Parameters["script"]; !ok {
		msg.Parameters["script"] = j.scriptContent
	}

	msg.normalize()
	return msg, nil
}

// BuildExecutionMessage exposes a cached-script message builder for external callers.
func (j *baseTask) BuildExecutionMessage(params map[string]any) (*ExecutionMessage, error) {
	var msg *ExecutionMessage
	if params != nil {
		msg = &ExecutionMessage{
			Parameters: cloneParams(params),
		}
	}
	return j.buildExecutionMessage(msg)
}

func NewBaseTask(
	id, path, scriptType string,
	config Config,
	scriptContent string,
	engine Engine,
) Task {
	handlerOpts := &HandlerOptions{
		HandlerConfig: command.HandlerConfig{
			Expression: DefaultSchedule,
			Timeout:    DefaultTimeout,
		},
	}

	if config.Schedule != "" {
		handlerOpts.Expression = config.Schedule
	}

	if !config.Deadline.IsZero() {
		handlerOpts.Deadline = config.Deadline
	}

	if config.Retries != 0 {
		handlerOpts.MaxRetries = config.Retries
	}

	if config.MaxRuns != 0 {
		handlerOpts.MaxRuns = config.MaxRuns
	}

	if !config.NoTimeout {
		handlerOpts.Timeout = config.Timeout
	}

	if config.RunOnce {
		handlerOpts.RunOnce = true
	}

	if config.NoTimeout {
		handlerOpts.NoTimeout = true
	}

	if config.ExitOnError {
		handlerOpts.ExitOnError = true
	}

	return &baseTask{
		id:            id,
		scriptPath:    path,
		scriptType:    scriptType,
		handlerOpts:   *handlerOpts,
		scriptContent: scriptContent,
		engine:        engine,
		config:        config,
		logger:        newStdLoggerProvider().GetLogger("job:task"),
	}
}

func (j *baseTask) taskLogger() Logger {
	logger := j.logger
	if logger == nil {
		logger = newStdLoggerProvider().GetLogger("job:task")
	}

	fields := map[string]any{
		"task_id":     j.id,
		"script_path": j.scriptPath,
	}

	if j.engine != nil {
		fields["engine"] = j.engine.Name()
	}

	if fl, ok := logger.(FieldsLogger); ok {
		return fl.WithFields(fields)
	}

	return logger
}
