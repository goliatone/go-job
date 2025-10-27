package job

import (
	"io/fs"
	"strings"
	"time"
)

type ShellOption func(*ShellEngine)

// WithShellFS sets the default filesystem timeout
func WithShellFS(dirfs fs.FS) ShellOption {
	return func(e *ShellEngine) {
		if dirfs != nil {
			e.FS = dirfs
		}
	}
}

// WithShellExtension adds file extensions that this engine can handle
func WithShellExtension(ext string) ShellOption {
	return func(e *ShellEngine) {
		if ext == "" {
			return
		}

		if !strings.HasPrefix(ext, ".") {
			ext = "." + ext
		}
		e.FileExtensions = append(e.FileExtensions, ext)
	}
}

// WithShellTimeout sets the default execution timeout
func WithShellTimeout(timeout time.Duration) ShellOption {
	return func(e *ShellEngine) {
		if timeout > 0 {
			e.Timeout = timeout
		}
	}
}

// WithShellShell sets the shell executable and arguments
func WithShellShell(shell string, args ...string) ShellOption {
	return func(e *ShellEngine) {
		if shell != "" {
			e.shell = shell
			e.shellArgs = args
		}
	}
}

// WithShellWorkingDirectory sets the working directory for script execution
func WithShellWorkingDirectory(dir string) ShellOption {
	return func(e *ShellEngine) {
		if dir != "" {
			e.workDir = dir
		}
	}
}

// WithShellEnvironment sets additional environment variables
func WithShellEnvironment(env []string) ShellOption {
	return func(e *ShellEngine) {
		if env != nil {
			e.environment = env
		}
	}
}

// WithShellMetadataParser sets a custom metadata parser
func WithShellMetadataParser(parser MetadataParser) ShellOption {
	return func(e *ShellEngine) {
		if parser != nil {
			e.MetadataParser = parser
		}
	}
}

func WithShellLogger(logger Logger) ShellOption {
	return func(se *ShellEngine) {
		if logger != nil {
			se.SetLogger(logger)
		}
	}
}
