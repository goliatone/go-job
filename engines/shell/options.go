package shell

import (
	"io/fs"
	"strings"
	"time"

	"github.com/goliatone/go-job"
)

type Option func(*Engine)

// WithFS sets the default filesystem timeout
func WithFS(dirfs fs.FS) Option {
	return func(e *Engine) {
		if dirfs != nil {
			e.FS = dirfs
		}
	}
}

// WithExtension adds file extensions that this engine can handle
func WithExtension(ext string) Option {
	return func(e *Engine) {
		if ext == "" {
			return
		}

		if !strings.HasPrefix(ext, ".") {
			ext = "." + ext
		}
		e.FileExtensions = append(e.FileExtensions, ext)
	}
}

// WithTimeout sets the default execution timeout
func WithTimeout(timeout time.Duration) Option {
	return func(e *Engine) {
		if timeout > 0 {
			e.Timeout = timeout
		}
	}
}

// WithShell sets the shell executable and arguments
func WithShell(shell string, args ...string) Option {
	return func(e *Engine) {
		if shell != "" {
			e.shell = shell
			e.shellArgs = args
		}
	}
}

// WithWorkingDirectory sets the working directory for script execution
func WithWorkingDirectory(dir string) Option {
	return func(e *Engine) {
		if dir != "" {
			e.workDir = dir
		}
	}
}

// WithEnvironment sets additional environment variables
func WithEnvironment(env []string) Option {
	return func(e *Engine) {
		if env != nil {
			e.environment = env
		}
	}
}

// WithMetadataParser sets a custom metadata parser
func WithMetadataParser(parser job.MetadataParser) Option {
	return func(e *Engine) {
		if parser != nil {
			e.MetadataParser = parser
		}
	}
}
