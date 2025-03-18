package sql

import (
	"io/fs"
	"strings"
	"time"

	"github.com/goliatone/go-job"
)

type Option func(*Engine)

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

// WithDatabase sets the database connection
func WithDatabase(driverName, dataSourceName string) Option {
	return func(e *Engine) {
		e.driverName = driverName
		e.dataSourceName = dataSourceName
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

// WithFS sets the default filesystem timeout
func WithFS(dirfs fs.FS) Option {
	return func(e *Engine) {
		if dirfs != nil {
			e.FS = dirfs
		}
	}
}
