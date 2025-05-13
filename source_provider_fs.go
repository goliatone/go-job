package job

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

var _ SourceProvider = &FileSystemSourceProvider{}

type FileSystemSourceProvider struct {
	rootDir string
	fs      fs.FS
}

func NewFileSystemSourceProvider(rootDir string, fss ...fs.FS) *FileSystemSourceProvider {
	fsys := os.DirFS(rootDir)
	if len(fss) > 0 {
		fsys = fss[0]
	}
	return &FileSystemSourceProvider{
		rootDir: rootDir,
		fs:      fsys,
	}
}

func (p *FileSystemSourceProvider) GetScript(path string) ([]byte, error) {

	path = filepath.Clean(path)

	file, err := p.fs.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", path, err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat file %s: %w", path, err)
	}

	content := make([]byte, info.Size())
	if _, err := file.Read(content); err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", path, err)
	}

	return content, nil
}

func (p *FileSystemSourceProvider) ListScripts(ctx context.Context) ([]ScriptInfo, error) {
	var scripts []ScriptInfo

	err := fs.WalkDir(p.fs, ".", func(path string, d fs.DirEntry, err error) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("failed ot get file info for %s: %w", path, err)
		}

		content := make([]byte, info.Size())
		file, err := p.fs.Open(path)
		if err != nil {
			return fmt.Errorf("failed to read file %s: %w", path, err)
		}
		defer file.Close()

		if _, err := file.Read(content); err != nil {
			return fmt.Errorf("failed to read file %s: %w", path, err)
		}

		absPath := path
		if p.rootDir != "" {
			absPath = filepath.Join(p.rootDir, path)
		}
		scripts = append(scripts, ScriptInfo{
			ID:      filepath.Base(path),
			Path:    absPath,
			Content: content,
		})

		return nil
	})

	return scripts, err
}
