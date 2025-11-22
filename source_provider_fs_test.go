package job_test

import (
	"bytes"
	"context"
	"io/fs"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/goliatone/go-job"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileSystemSourceProviderCancelsWalkOnContext(t *testing.T) {
	ready := make(chan struct{}, 1)

	fsys := &instrumentedFS{
		data: fstest.MapFS{
			"first.js":  {Data: []byte("console.log('a')")},
			"second.js": {Data: []byte("console.log('b')")},
		},
		onOpen: func(name string) {
			if name == "first.js" {
				select {
				case ready <- struct{}{}:
				default:
				}
			}
		},
	}

	provider := job.NewFileSystemSourceProvider(".", fsys)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	var (
		scripts []job.ScriptInfo
		err     error
	)

	go func() {
		scripts, err = provider.ListScripts(ctx)
		close(done)
	}()

	<-ready
	cancel()
	<-done

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.LessOrEqual(t, len(scripts), 1)
}

func TestFileSystemSourceProviderClosesFilesPerIteration(t *testing.T) {
	counterFS := &countingFS{
		data: fstest.MapFS{
			"first.sql":  {Data: []byte("SELECT 1;")},
			"second.sql": {Data: []byte("SELECT 2;")},
		},
	}

	provider := job.NewFileSystemSourceProvider(".", counterFS)

	scripts, err := provider.ListScripts(context.Background())
	require.NoError(t, err)
	assert.Len(t, scripts, 2)

	counterFS.mu.Lock()
	opens := counterFS.opens
	closes := counterFS.closes
	counterFS.mu.Unlock()

	assert.Equal(t, opens, closes)
}

func TestFileSystemSourceProviderMaxFileSizeGuard(t *testing.T) {
	provider := job.NewFileSystemSourceProvider(".", fstest.MapFS{
		"big.sh": {Data: bytes.Repeat([]byte("x"), 1024)},
	})
	provider.WithMaxFileSize(512)

	scripts, err := provider.ListScripts(context.Background())
	require.Error(t, err)
	assert.ErrorIs(t, err, job.ErrScriptTooLarge)
	assert.Nil(t, scripts)
}

func TestFileSystemSourceProviderIgnoreGlobsAndPaths(t *testing.T) {
	provider := job.NewFileSystemSourceProvider(".", fstest.MapFS{
		"keep/a.js":   {Data: []byte("console.log('a')")},
		"ignore.db":   {Data: []byte("db file")},
		"ignore/b.sh": {Data: []byte("echo b")},
	})

	provider.WithIgnoreGlobs("*.db")
	provider.WithIgnorePaths("ignore")

	scripts, err := provider.ListScripts(context.Background())
	require.NoError(t, err)
	require.Len(t, scripts, 1)
	assert.Equal(t, "keep/a.js", scripts[0].Path)
}

type instrumentedFS struct {
	data   fstest.MapFS
	onOpen func(string)
}

func (i *instrumentedFS) Open(name string) (fs.File, error) {
	if i.onOpen != nil {
		i.onOpen(name)
	}
	return i.data.Open(name)
}

func (i *instrumentedFS) ReadDir(name string) ([]fs.DirEntry, error) {
	return i.data.ReadDir(name)
}

type countingFS struct {
	data   fstest.MapFS
	mu     sync.Mutex
	opens  int
	closes int
}

func (c *countingFS) Open(name string) (fs.File, error) {
	file, err := c.data.Open(name)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	c.opens++
	c.mu.Unlock()

	return &countingFile{
		File: file,
		onClose: func() {
			c.mu.Lock()
			c.closes++
			c.mu.Unlock()
		},
	}, nil
}

func (c *countingFS) ReadDir(name string) ([]fs.DirEntry, error) {
	return c.data.ReadDir(name)
}

type countingFile struct {
	fs.File
	onClose func()
}

func (c *countingFile) Close() error {
	if c.onClose != nil {
		c.onClose()
	}
	return c.File.Close()
}
