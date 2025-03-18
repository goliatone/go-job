package js

import (
	"io/fs"
	"strings"
	"time"

	"github.com/goliatone/go-job"
)

type Option func(*Engine)

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

// WithFS sets the default execution timeout
func WithFS(dirfs fs.FS) Option {
	return func(e *Engine) {
		if dirfs != nil {
			e.FS = dirfs
		}
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

// WithMetadataParser sets a custom metadata parser
func WithMetadataParser(parser job.MetadataParser) Option {
	return func(e *Engine) {
		if parser != nil {
			e.MetadataParser = parser
		}
	}
}
