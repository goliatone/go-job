package job

import "io/fs"

type Option func(*FileSystemTask)

func WithRootDir(path string) Option {
	return func(fsjr *FileSystemTask) {
		fsjr.rootDir = path
	}
}

func WithDirFS(dfs fs.FS) Option {
	return func(fsjr *FileSystemTask) {
		fsjr.fs = dfs
	}
}

func WithEngines(engines ...Engine) Option {
	return func(fsjr *FileSystemTask) {
		fsjr.engines = append(fsjr.engines, engines...)
	}
}

func WithErrorHandler(handler func(error)) Option {
	return func(fsjr *FileSystemTask) {
		if handler != nil {
			fsjr.errorHandler = handler
		}
	}
}

func WithRegistry(registry Registry) Option {
	return func(fsjr *FileSystemTask) {
		if registry != nil {
			fsjr.registry = registry
		}
	}
}

func WithMetadataParser(parser MetadataParser) Option {
	return func(fsjr *FileSystemTask) {
		if parser != nil {
			fsjr.parser = parser
		}
	}
}
