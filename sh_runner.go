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
	fmt.Println("=== EXECUTE SH ====")
	fmt.Println(msg.ScriptPath)

	if err := cmd.Run(); err != nil {
		fmt.Println(stderr.String())
		return command.WrapError(
			"ShellExecutionError",
			fmt.Sprintf("script execution failed: %v\nStderr: %s", err, stderr.String()),
			err,
		)
	}

	fmt.Println(stdout.String())

	if exitCode := cmd.ProcessState.ExitCode(); exitCode != 0 {
		return command.WrapError(
			"ShellExecutionError",
			fmt.Sprintf("script exited with non-zero status: %d\nStderr: %s", exitCode, stderr.String()),
			nil,
		)
	}

	return nil
}
