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
	handlerOpts   command.HandlerConfig
	config        Config
	scriptContent string
	engine        Engine
}

var _ Task = &baseTask{}

func (j *baseTask) GetID() string {
	return j.id
}

func (j *baseTask) GetHandler() command.CommandFunc[*ExecutionMessage] {
	return func(ctx context.Context, msg *ExecutionMessage) error {

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
	meta map[string]any,
	scriptContent string,
	engine Engine,
) Task {
	handlerOpts := &command.HandlerConfig{
		Expression: "* * * * *",
		Timeout:    time.Minute,
	}

	config := Config{
		Schedule:   "* * * * *",
		ScriptType: scriptType,
	}

	// Map known meta fields to JobConfig
	if schedule, ok := meta["schedule"].(string); ok && schedule != "" {
		config.Schedule = schedule
		handlerOpts.Expression = schedule
	}

	if retries, ok := meta["retries"].(int); ok {
		config.Retries = retries
		handlerOpts.MaxRetries = retries
	}

	if timeout, ok := meta["timeout"].(time.Duration); ok {
		config.Timeout = timeout
		handlerOpts.Timeout = timeout
	}

	if runOnce, ok := meta["run_once"].(bool); ok {
		config.RunOnce = runOnce
		handlerOpts.RunOnce = runOnce
	}

	if debug, ok := meta["debug"].(bool); ok {
		config.Debug = debug
	}

	if env, ok := meta["env"].(map[string]string); ok {
		config.Env = env
	}

	if transaction, ok := meta["transaction"].(bool); ok {
		config.Transaction = transaction
	}

	// Add remaining metadata to Config.Metadata
	config.Metadata = make(map[string]any)
	for k, v := range meta {
		switch k {
		case "schedule", "retries", "timeout", "run_once", "debug", "env", "transaction", "script_type":
			// These are already handled above
		default:
			config.Metadata[k] = v
		}
	}

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
