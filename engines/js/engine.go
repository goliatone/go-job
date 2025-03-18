package js

import (
	"context"
	"fmt"

	"github.com/dop251/goja"
	"github.com/goliatone/go-command"
	"github.com/goliatone/go-job"
)

type Engine struct {
	*job.BaseEngine
}

func New(opts ...Option) *Engine {
	e := &Engine{
		BaseEngine: job.NewBaseEngine("javascript", ".js"),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(e)
		}
	}

	return e
}

func (e *Engine) Execute(ctx context.Context, msg job.ExecutionMessage) error {
	scriptContent, err := e.GetScriptContent(msg)
	if err != nil {
		return err
	}

	execCtx, cancel := e.GetExecutionContext(ctx)
	defer cancel()

	vm := goja.New()

	if err := e.setupConsole(vm); err != nil {
		return command.WrapError("JSEngineError", "failed to set console object", err)
	}

	if err := e.setupFetch(vm); err != nil {
		return command.WrapError("JSEngineError", "failed to set fetch function", err)
	}

	if msg.Parameters != nil {
		for k, v := range msg.Parameters {
			if k == "script" {
				continue
			}

			if err := vm.Set(k, v); err != nil {
				return command.WrapError(
					"JSEngineError",
					fmt.Sprintf("failed to set parameter %s", k),
					err,
				)
			}
		}
	}

	if msg.Config.Env != nil {
		for k, v := range msg.Config.Env {
			if err := vm.Set(k, v); err != nil {
				return command.WrapError(
					"JSEngineError",
					fmt.Sprintf("failed to set env var %s", k),
					err,
				)
			}
		}
	}

	resultCh := make(chan error, 1)

	go func() {
		_, err := vm.RunString(scriptContent)
		resultCh <- err
	}()

	select {
	case err := <-resultCh:
		if err != nil {
			return command.WrapError("JSExecutionError", "script execution failed", err)
		}
		return nil
	case <-execCtx.Done():
		return command.WrapError("JSExecutionTimeout", "script execution timed out", execCtx.Err())
	}
}
