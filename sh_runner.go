package job

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"

	"github.com/goliatone/go-command"
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

func (e *ShellEngine) Execute(ctx context.Context, msg ExecutionMessage) error {
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

	// TODO: this should be optional
	cmd.Env = os.Environ()
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

	if err := cmd.Run(); err != nil {
		return command.WrapError(
			"ShellExecutionError",
			fmt.Sprintf("script execution failed: %v\nStderr: %s", err, stderr.String()),
			err,
		)
	}

	if exitCode := cmd.ProcessState.ExitCode(); exitCode != 0 {
		return command.WrapError(
			"ShellExecutionError",
			fmt.Sprintf("script exited with non-zero status: %d\nStderr: %s", exitCode, stderr.String()),
			nil,
		)
	}

	return nil
}
