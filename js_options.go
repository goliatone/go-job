package job

import (
	"io/fs"
	"strings"
	"time"
)

type JSOption func(*JSEngine)

func WithJSExtension(ext string) JSOption {
	return func(e *JSEngine) {
		if ext == "" {
			return
		}

		if !strings.HasPrefix(ext, ".") {
			ext = "." + ext
		}

		e.FileExtensions = append(e.FileExtensions, ext)
	}
}

// WithJSFS sets the default execution timeout
func WithJSFS(dirfs fs.FS) JSOption {
	return func(e *JSEngine) {
		if dirfs != nil {
			e.FS = dirfs
		}
	}
}

// WithJSTimeout sets the default execution timeout
func WithJSTimeout(timeout time.Duration) JSOption {
	return func(e *JSEngine) {
		if timeout > 0 {
			e.Timeout = timeout
		}
	}
}

// WithJSMetadataParser sets a custom metadata parser
func WithJSMetadataParser(parser MetadataParser) JSOption {
	return func(e *JSEngine) {
		if parser != nil {
			e.MetadataParser = parser
		}
	}
}

func WithJSPathResolver(resolver func(base, path string) string) JSOption {
	return func(j *JSEngine) {
		j.pathResolver = resolver
	}
}

func WithJSModuleLoader(loader func(path string) ([]byte, error)) JSOption {
	return func(j *JSEngine) {
		j.moduleLoader = loader
	}
}

func WithJSPanicHandler(handler func(funcName string, fields ...map[string]any)) JSOption {
	return func(j *JSEngine) {
		j.panicHandler = handler
	}
}

func WithJSLogger(logger Logger) JSOption {
	return func(se *JSEngine) {
		if logger != nil {
			se.SetLogger(logger)
		}
	}
}
