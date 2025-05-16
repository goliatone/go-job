package job

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"

	"github.com/goliatone/go-errors"
)

type ShellEngine struct {
	*BaseEngine
	shell       string
	shellArgs   []string
	workDir     string
	environment []string
}

func NewShellRunner(opts ...ShellOption) *ShellEngine {
	e := &ShellEngine{
		shell:     "/bin/sh",
		shellArgs: []string{"-c"},
	}
	e.BaseEngine = NewBaseEngine(e, "shell", ".sh", ".bash")

	for _, opt := range opts {
		if opt != nil {
			opt(e)
		}
	}

	return e
}

func (e *ShellEngine) Execute(ctx context.Context, msg *ExecutionMessage) error {
	scriptContent, err := e.GetScriptContent(msg)
	if err != nil {
		return err
	}
	execCtx, cancel := e.GetExecutionContext(ctx)
	defer cancel()

	cmd := exec.CommandContext(execCtx, e.shell, append(e.shellArgs, scriptContent)...)

	if e.workDir != "" {
		cmd.Dir = e.workDir
	}

	// NOTE: Use this if you know what you are doing :)
	if use, ok := msg.Config.Metadata["use_env"].(bool); ok && use {
		cmd.Env = os.Environ()
	}

	if e.environment != nil {
		cmd.Env = append(cmd.Env, e.environment...)
	}

	if msg.Config.Env != nil {
		for k, v := range msg.Config.Env {
			cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", k, v))
		}
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	// Create a map for detailed error info
	// resultDetails := map[string]any{
	// 	"stdout": stdout.String(),
	// 	"stderr": stderr.String(),
	// }

	e.logger.Info("=== EXECUTE SH ====")
	e.logger.Info(msg.ScriptPath)

	if err := cmd.Run(); err != nil {
		e.logger.Info(stderr.String())
		return errors.Wrap(err, errors.CategoryExternal, "script execution failed").
			WithTextCode("SHELL_EXECUTION_ERROR").
			WithMetadata(map[string]any{
				"operation":   "execute_command",
				"script_path": msg.ScriptPath,
				"shell":       e.shell,
				"working_dir": e.workDir,
				"stdout":      stdout.String(),
				"stderr":      stderr.String(),
				"exit_code":   getExitCode(err),
			})
	}

	e.logger.Info(stdout.String())

	if exitCode := cmd.ProcessState.ExitCode(); exitCode != 0 {
		return errors.New(errors.CategoryExternal, "script exited with non-zero status").
			WithTextCode("SHELL_EXECUTION_ERROR").
			WithMetadata(map[string]any{
				"operation":   "execute_command",
				"script_path": msg.ScriptPath,
				"shell":       e.shell,
				"working_dir": e.workDir,
				"stdout":      stdout.String(),
				"stderr":      stderr.String(),
				"exit_code":   exitCode,
			})
	}

	return nil
}

func getExitCode(err error) int {
	if exitError, ok := err.(*exec.ExitError); ok {
		return exitError.ExitCode()
	}
	return -1
}
