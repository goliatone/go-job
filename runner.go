package job

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"

	"github.com/goliatone/go-command"
)

type FileSystemTask struct {
	mx           sync.RWMutex
	engines      []Engine
	registry     Registry
	rootDir      string
	fs           fs.FS
	parser       MetadataParser
	errorHandler func(error)
}

func NewFileSystemJob(opts ...Option) *FileSystemTask {
	runner := &FileSystemTask{
		registry: &memoryRegistry{
			jobs: make(map[string]Task),
		},
		parser: &yamlMetadataParser{},
		errorHandler: func(err error) {
			fmt.Printf("job error: %v\n", err)
		},
		// TODO: have a default root path that makes sense
		fs: os.DirFS("."),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(runner)
		}
	}
	return runner
}

func (r *FileSystemTask) Start(ctx context.Context) error {
	if r.rootDir == "" {
		return command.WrapError("FileSystemjob", "root directory not specified", nil)
	}
	if len(r.engines) == 0 {
		return command.WrapError("FileSystemjob", "no engines registered", nil)
	}
	return r.scanDirectory(ctx, r.rootDir)
}

func (r *FileSystemTask) Stop(_ context.Context) error {
	return nil
}

func (r *FileSystemTask) RegisteredTasks() []Task {
	return r.registry.List()
}

func (r *FileSystemTask) AddEngine(engine Engine) {
	r.mx.Lock()
	defer r.mx.Unlock()
	for _, existing := range r.engines {
		if existing.Name() == engine.Name() {
			return
		}
	}
	r.engines = append(r.engines, engine)
}

func (r *FileSystemTask) scanDirectory(ctx context.Context, dir string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		//continue processing
	}

	return fs.WalkDir(r.fs, dir, func(path string, d fs.DirEntry, err error) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// continue process
		}

		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		var compatibleEngine Engine
		for _, engine := range r.engines {
			if engine.CanHandle(path) {
				compatibleEngine = engine
				break
			}
		}

		if compatibleEngine == nil {
			// TODO: consider if we want to error here
			return nil
		}

		file, err := r.fs.Open(path)
		if err != nil {
			return fmt.Errorf("failed to read file %s: %w", path, err)
		}
		defer file.Close()

		info, err := file.Stat()
		if err != nil {
			return fmt.Errorf("failed to stat file %s: %w", path, err)
		}

		content := make([]byte, info.Size())
		if _, err := file.Read(content); err != nil {
			return fmt.Errorf("failed to read file %s: %w", path, err)
		}

		absPath := path
		if r.rootDir != "" {
			absPath = filepath.Join(r.rootDir, path)
		}

		job, err := compatibleEngine.ParseJob(absPath, content)
		if err != nil {
			//NOTE: ensure this is the behavior that makes the most sense i.e.
			// log but dont faile if a single job fails to parse
			r.errorHandler(fmt.Errorf("failed to parse job %s: %w", path, err))
			return nil
		}

		if err := r.registry.Add(job); err != nil {
			r.errorHandler(fmt.Errorf("failed to register job %s: %w", path, err))
			return nil
		}

		return nil
	})
}
