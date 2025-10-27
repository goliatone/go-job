package job

import (
	"context"
	"time"

	"github.com/goliatone/go-command"
)

type baseTask struct {
	id            string
	scriptPath    string
	scriptType    string
	handlerOpts   command.HandlerConfig
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
		ctx := context.Background()
		emsg := &ExecutionMessage{
			JobID:      j.id,
			ScriptPath: j.scriptPath,
			Config:     j.config,
			Parameters: make(map[string]any),
		}
		emsg.Parameters["script"] = j.scriptContent

		logger := j.taskLogger()
		baseArgs := []any{"task_id", j.id, "script_path", j.scriptPath}
		if j.engine != nil {
			baseArgs = append(baseArgs, "engine", j.engine.Name())
		}

		logger.Debug("task execution started", baseArgs...)

		start := time.Now()
		err := j.engine.Execute(ctx, emsg)
		duration := time.Since(start)

		durationArgs := append(append([]any{}, baseArgs...), "duration", duration)

		if err != nil {
			logger.Error("task execution failed", append(durationArgs, "error", err)...)
			return err
		}

		logger.Info("task execution completed", durationArgs...)
		return nil
	}
}

func (j *baseTask) GetHandlerConfig() command.HandlerConfig {
	return j.handlerOpts
}

func (j *baseTask) GetConfig() Config {
	return j.config
}

func (j *baseTask) GetPath() string {
	return j.scriptPath
}

func NewBaseTask(
	id, path, scriptType string,
	config Config,
	scriptContent string,
	engine Engine,
) Task {
	handlerOpts := &command.HandlerConfig{
		Expression: DefaultSchedule,
		Timeout:    DefaultTimeout,
	}

	// Map known meta fields to JobConfig
	if config.Schedule != "" {
		handlerOpts.Expression = config.Schedule
	}

	if config.Retries != 0 {
		handlerOpts.MaxRetries = config.Retries
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

	// handlerOpts.Deadline
	// handlerOpts.MaxRetries
	// handlerOpts.MaxRuns

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
