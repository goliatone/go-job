package job

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

var _ SourceProvider = &FileSystemSourceProvider{}

type FileSystemSourceProvider struct {
	rootDir     string
	fs          fs.FS
	maxFileSize int64
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

var ErrScriptTooLarge = errors.New("script exceeds maximum size limit")

func (p *FileSystemSourceProvider) WithMaxFileSize(limit int64) *FileSystemSourceProvider {
	p.maxFileSize = limit
	return p
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
		if err != nil {
			return err
		}

		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if d.IsDir() {
			return nil
		}

		content, err := p.loadScriptContent(ctx, path)
		if err != nil {
			return err
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

func (p *FileSystemSourceProvider) loadScriptContent(ctx context.Context, path string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	file, err := p.fs.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", path, err)
	}

	content, readErr := p.readFile(ctx, path, file)
	closeErr := file.Close()

	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, fmt.Errorf("failed to close file %s: %w", path, closeErr)
	}

	return content, nil
}

func (p *FileSystemSourceProvider) readFile(ctx context.Context, path string, file fs.File) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var initialSize int
	if info, err := file.Stat(); err == nil {
		if size := info.Size(); size >= 0 {
			if p.maxFileSize > 0 && size > p.maxFileSize {
				return nil, fmt.Errorf("%w: script %s has size %d bytes (limit %d)", ErrScriptTooLarge, path, size, p.maxFileSize)
			}
			if size < int64(^uint(0)>>1) {
				initialSize = int(size)
			}
		}
	}

	buf := bytes.NewBuffer(make([]byte, 0, initialSize))
	chunk := make([]byte, 32*1024)
	var total int64

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		n, err := file.Read(chunk)
		if n > 0 {
			total += int64(n)
			if p.maxFileSize > 0 && total > p.maxFileSize {
				return nil, fmt.Errorf("%w: script %s exceeded limit %d bytes", ErrScriptTooLarge, path, p.maxFileSize)
			}
			if _, werr := buf.Write(chunk[:n]); werr != nil {
				return nil, fmt.Errorf("failed to buffer file %s: %w", path, werr)
			}
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("failed to read file %s: %w", path, err)
		}
	}

	return buf.Bytes(), nil
}
