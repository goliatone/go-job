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
