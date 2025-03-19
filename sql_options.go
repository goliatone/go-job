package job

import (
	"io/fs"
	"strings"
	"time"
)

type SQLOption func(*SQLEngine)

// WithSQLExtension adds file extensions that this SQLOption can handle
func WithSQLExtension(ext string) SQLOption {
	return func(e *SQLEngine) {
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
func WithSQLTimeout(timeout time.Duration) SQLOption {
	return func(e *SQLEngine) {
		if timeout > 0 {
			e.Timeout = timeout
		}
	}
}

// WithDatabase sets the database connection
func WithSQLDatabase(driverName, dataSourceName string) SQLOption {
	return func(e *SQLEngine) {
		e.driverName = driverName
		e.dataSourceName = dataSourceName
	}
}

// WithSQLMetadataParser sets a custom metadata parser
func WithSQLMetadataParser(parser MetadataParser) SQLOption {
	return func(e *SQLEngine) {
		if parser != nil {
			e.MetadataParser = parser
		}
	}
}

// WithSQLFS sets the default filesystem timeout
func WithSQLFS(dirfs fs.FS) SQLOption {
	return func(e *SQLEngine) {
		if dirfs != nil {
			e.FS = dirfs
		}
	}
}
