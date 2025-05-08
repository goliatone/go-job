package job

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/dop251/goja"
	"github.com/dop251/goja_nodejs/buffer"
	"github.com/dop251/goja_nodejs/console"
	"github.com/dop251/goja_nodejs/eventloop"

	"github.com/dop251/goja_nodejs/process"
	"github.com/dop251/goja_nodejs/require"
	"github.com/dop251/goja_nodejs/url"
	"github.com/goliatone/go-command"
)

type JSEngine struct {
	*BaseEngine
	moduleLoader func(path string) ([]byte, error)
	panicHandler func(funcName string, fields ...map[string]any)
	pathResolver func(base, path string) string
}

func NewJSRunner(opts ...JSOption) *JSEngine {
	e := &JSEngine{
		moduleLoader: require.DefaultSourceLoader,
		pathResolver: require.DefaultPathResolver,
	}
	e.BaseEngine = NewBaseEngine(e, "javascript", ".js")

	for _, opt := range opts {
		if opt != nil {
			opt(e)
		}
	}

	if e.panicHandler == nil {
		e.panicHandler = command.MakePanicHandler(command.DefaultPanicLogger)
	}

	return e
}

// Execute runs a JavaScript file in a Node-like environment using goja_nodejs' eventloop.
func (e *JSEngine) Execute(ctx context.Context, msg *ExecutionMessage) error {
	defer e.panicHandler("JSEngine.Execute", map[string]any{
		"scriptPath": msg.ScriptPath,
	})

	scriptContent, err := e.GetScriptContent(msg)
	if err != nil {
		return command.WrapError("JSEngineError", "failed to get script content", err)
	}

	execCtx, cancel := e.GetExecutionContext(ctx)
	defer cancel()

	// Create a custom require registry that knows how to load modules
	registry := require.NewRegistry(
		require.WithLoader(e.moduleLoader),
		// require.WithGlobalFolders(),
	)

	loop := eventloop.NewEventLoop(
		eventloop.WithRegistry(registry),
		// eventloop.EnableConsole(true),
	)

	loop.Start()
	defer loop.StopNoWait()

	configErrCh := make(chan error, 1)
	ok := loop.RunOnLoop(func(vm *goja.Runtime) {
		process.Enable(vm)
		url.Enable(vm)
		buffer.Enable(vm)
		console.Enable(vm)

		if ferr := e.SetupFetch(vm); ferr != nil {
			configErrCh <- ferr
			return
		}

		if ferr := e.configureScriptEnvironment(vm, msg); ferr != nil {
			configErrCh <- ferr
			return
		}

		configErrCh <- nil
	})

	if !ok {
		return command.WrapError(
			"JSEngineError",
			"loop was terminated before configuration",
			nil,
		)
	}

	if err := <-configErrCh; err != nil {
		loop.Terminate()
		return command.WrapError(
			"JSEngineError",
			"failed to configure the VM environment",
			err,
		)
	}

	execErrCh := make(chan error, 1)
	ok = loop.RunOnLoop(func(vm *goja.Runtime) {
		_, runErr := vm.RunScript(msg.ScriptPath, scriptContent)
		execErrCh <- runErr
	})

	if !ok {
		return command.WrapError(
			"JSEngineError",
			"loop was terminated before running script",
			nil,
		)
	}

	select {
	case err := <-execErrCh:
		loop.Stop()
		if err != nil {
			return command.WrapError(
				"JSExecutionError",
				"script execution failed",
				err,
			)
		}
		return nil
	case <-execCtx.Done():
		loop.Terminate()
		return command.WrapError("JSExecutionTimeout", "script execution timed out", execCtx.Err())
	}
}

func (e *JSEngine) configureScriptEnvironment(vm *goja.Runtime, msg *ExecutionMessage) error {
	scriptDir := filepath.Dir(msg.ScriptPath)
	if err := vm.Set("__dirname", scriptDir); err != nil {
		return fmt.Errorf("failed to set __dirname: %w", err)
	}

	if err := vm.Set("__filename", msg.ScriptPath); err != nil {
		return fmt.Errorf("failed to set __filename: %w", err)
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

	return nil
}
