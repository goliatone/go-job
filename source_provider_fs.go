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
	"runtime"
)

var _ SourceProvider = &FileSystemSourceProvider{}

type FileSystemSourceProvider struct {
	rootDir        string
	fs             fs.FS
	maxFileSize    int64
	ignoreMatchers []func(string, fs.DirEntry) bool
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

// WithIgnoreGlobs skips files or directories matching any glob pattern (filepath.Match semantics).
// Patterns are matched against paths relative to rootDir, using "/" separators.
func (p *FileSystemSourceProvider) WithIgnoreGlobs(patterns ...string) *FileSystemSourceProvider {
	for _, pat := range patterns {
		if pat == "" {
			continue
		}
		p.ignoreMatchers = append(p.ignoreMatchers, func(path string, d fs.DirEntry) bool {
			matched, _ := filepath.Match(pat, path)
			return matched
		})
	}
	return p
}

// WithIgnorePaths skips exact relative paths (files or directories) during discovery.
func (p *FileSystemSourceProvider) WithIgnorePaths(paths ...string) *FileSystemSourceProvider {
	for _, path := range paths {
		if path == "" {
			continue
		}
		clean := filepath.Clean(path)
		p.ignoreMatchers = append(p.ignoreMatchers, func(pth string, _ fs.DirEntry) bool {
			return filepath.Clean(pth) == clean
		})
	}
	return p
}

func (p *FileSystemSourceProvider) GetScript(path string) ([]byte, error) {

	path = filepath.Clean(path)

	file, err := p.fs.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", path, err)
	}

	content, readErr := p.readFile(context.Background(), path, file)
	closeErr := file.Close()
	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, fmt.Errorf("failed to close file %s: %w", path, closeErr)
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

		if p.shouldIgnore(path, d) {
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
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

		runtime.Gosched()

		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, ctxErr
	}

	return scripts, nil
}

func (p *FileSystemSourceProvider) loadScriptContent(ctx context.Context, path string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	file, err := p.fs.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %s: %w", path, err)
	}

	if err := ctx.Err(); err != nil {
		_ = file.Close()
		return nil, err
	}

	content, readErr := p.readFile(ctx, path, file)
	closeErr := file.Close()

	if readErr != nil {
		return nil, readErr
	}
	if closeErr != nil {
		return nil, fmt.Errorf("failed to close file %s: %w", path, closeErr)
	}

	if err := ctx.Err(); err != nil {
		return nil, err
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

func (p *FileSystemSourceProvider) shouldIgnore(path string, d fs.DirEntry) bool {
	for _, matcher := range p.ignoreMatchers {
		if matcher != nil && matcher(path, d) {
			return true
		}
	}
	return false
}
