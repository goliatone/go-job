package job

import (
	"context"
	"fmt"

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
		fmt.Println("executing engine " + j.engine.Name())

		return j.engine.Execute(ctx, emsg)
	}
}

func (j *baseTask) GetHandlerConfig() command.HandlerConfig {
	return j.handlerOpts
}

func (j *baseTask) GetConfig() Config {
	return j.config
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
	}
}
