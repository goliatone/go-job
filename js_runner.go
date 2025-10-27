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
	"github.com/goliatone/go-errors"
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

// SetTaskIDProvider overrides the ID derivation strategy for tasks parsed by the JS engine.
func (e *JSEngine) SetTaskIDProvider(provider TaskIDProvider) {
	if e.BaseEngine != nil {
		e.BaseEngine.SetTaskIDProvider(provider)
	}
}

// Execute runs a JavaScript file in a Node-like environment using goja_nodejs' eventloop.
func (e *JSEngine) Execute(ctx context.Context, msg *ExecutionMessage) error {
	defer e.panicHandler("JSEngine.Execute", map[string]any{
		"scriptPath": msg.ScriptPath,
	})

	scriptContent, err := e.GetScriptContent(msg)
	if err != nil {
		return err
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

		if ferr := e.setupFetch(vm); ferr != nil {
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
		return errors.New("loop was terminated before configuration", errors.CategoryInternal).
			WithTextCode("JS_LOOP_TERMINATED").
			WithMetadata(map[string]any{
				"operation":   "configure_loop",
				"script_path": msg.ScriptPath,
				"phase":       "pre_configuration",
			})
	}

	if err := <-configErrCh; err != nil {
		loop.Terminate()
		return errors.Wrap(err, errors.CategoryInternal, "failed to configure the VM environment").
			WithTextCode("JS_VM_CONFIG_ERROR").
			WithMetadata(map[string]any{
				"operation":   "configure_vm",
				"script_path": msg.ScriptPath,
			})
	}

	execErrCh := make(chan error, 1)
	ok = loop.RunOnLoop(func(vm *goja.Runtime) {
		_, runErr := vm.RunScript(msg.ScriptPath, scriptContent)
		execErrCh <- runErr
	})

	if !ok {
		return errors.New("loop was terminated before running script", errors.CategoryInternal).
			WithTextCode("JS_LOOP_TERMINATED").
			WithMetadata(map[string]any{
				"operation":   "execute_script",
				"script_path": msg.ScriptPath,
				"phase":       "pre_execution",
			})
	}

	select {
	case err := <-execErrCh:
		loop.Stop()
		if err != nil {
			return errors.Wrap(err, errors.CategoryInternal, "script execution failed").
				WithTextCode("JS_EXECUTION_ERROR").
				WithMetadata(map[string]any{
					"operation":   "run_script",
					"script_path": msg.ScriptPath,
				})
		}
		return nil
	case <-execCtx.Done():
		loop.Terminate()
		return errors.Wrap(execCtx.Err(), errors.CategoryExternal, "script execution timed out").
			WithTextCode("JS_EXECUTION_TIMEOUT").
			WithMetadata(map[string]any{
				"operation":   "execute_script",
				"script_path": msg.ScriptPath,
				"timeout":     "context_deadline",
			})
	}
}

func (e *JSEngine) configureScriptEnvironment(vm *goja.Runtime, msg *ExecutionMessage) error {
	scriptDir := filepath.Dir(msg.ScriptPath)
	if err := vm.Set("__dirname", scriptDir); err != nil {
		return errors.Wrap(err, errors.CategoryInternal, "failed to set __dirname").
			WithTextCode("JS_SET_DIRNAME_ERROR").
			WithMetadata(map[string]any{
				"operation":   "set_dirname",
				"script_path": msg.ScriptPath,
				"dirname":     scriptDir,
			})
	}

	if err := vm.Set("__filename", msg.ScriptPath); err != nil {
		return errors.Wrap(err, errors.CategoryInternal, "failed to set __filename").
			WithTextCode("JS_SET_FILENAME_ERROR").
			WithMetadata(map[string]any{
				"operation":   "set_filename",
				"script_path": msg.ScriptPath,
			})
	}

	if msg.Parameters != nil {
		for k, v := range msg.Parameters {
			if k == "script" {
				continue
			}

			if err := vm.Set(k, v); err != nil {
				return errors.Wrap(err, errors.CategoryInternal, fmt.Sprintf("failed to set parameter %s", k)).
					WithTextCode("JS_SET_PARAMETER_ERROR").
					WithMetadata(map[string]any{
						"operation":      "set_parameter",
						"script_path":    msg.ScriptPath,
						"parameter_name": k,
						"parameter_type": fmt.Sprintf("%T", v),
					})
			}
		}
	}

	if msg.Config.Env != nil {
		for k, v := range msg.Config.Env {
			if err := vm.Set(k, v); err != nil {
				return errors.Wrap(err, errors.CategoryInternal, fmt.Sprintf("failed to set env var %s", k)).
					WithTextCode("JS_SET_ENV_ERROR").
					WithMetadata(map[string]any{
						"operation":   "set_environment_variable",
						"script_path": msg.ScriptPath,
						"env_name":    k,
					})
			}
		}
	}

	return nil
}
