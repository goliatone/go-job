package job

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/goliatone/go-command"
)

type BaseEngine struct {
	FileExtensions []string
	Timeout        time.Duration
	MetadataParser MetadataParser
	FS             fs.FS
	SourceProvider SourceProvider
	EngineType     string
}

func NewBaseEngine(engingeType string, exts ...string) *BaseEngine {
	return &BaseEngine{
		Timeout:        30 * time.Second,
		MetadataParser: NewYAMLMetadataParser(),
		FS:             os.DirFS("."),
		EngineType:     engingeType,
		FileExtensions: exts,
	}
}

// CanHandle checks if this engine can process the
// given file based on its extension
func (e *BaseEngine) CanHandle(path string) bool {
	ext := strings.ToLower(filepath.Ext(path))
	for _, supportedExt := range e.FileExtensions {
		if ext == supportedExt {
			return true
		}
	}
	return false
}

// Name returns the engine identifier
func (e *BaseEngine) Name() string {
	return "engine:" + e.EngineType
}

// ParseJob extracts metadata and content from a job script file
func (e *BaseEngine) ParseJob(path string, content []byte) (Task, error) {
	config, scriptContent, err := e.MetadataParser.Parse(content)
	if err != nil {
		return nil, fmt.Errorf("failed to parse metadata: %w", err)
	}

	jobID := filepath.Base(path)
	job := NewBaseTask(jobID, path, e.EngineType, config.ToMap(), scriptContent, e)
	return job, nil
}

func (e *BaseEngine) GetScriptContent(msg ExecutionMessage) (string, error) {
	if content, ok := msg.Parameters["script"].(string); ok {
		return content, nil
	}

	content, err := e.SourceProvider.GetScript(msg.ScriptPath)
	if err != nil {
		return "", command.WrapError(
			fmt.Sprintf("%sEngineError", e.EngineType),
			"failed to read script file",
			err,
		)
	}
	_, scriptContent, err := e.MetadataParser.Parse(content)
	if err != nil {
		return "", command.WrapError(
			fmt.Sprintf("%sEngineError", e.EngineType),
			"failed to parse script content",
			err,
		)
	}
	return scriptContent, nil
}

func (e *BaseEngine) GetExecutionTimeout(ctx context.Context) time.Duration {
	execTimeout := e.Timeout
	if deadline, ok := ctx.Deadline(); ok {
		execTimeout = time.Until(deadline)
	}
	return execTimeout
}

func (e *BaseEngine) GetExecutionContext(ctx context.Context) (context.Context, context.CancelFunc) {
	execTimeout := e.GetExecutionTimeout(ctx)
	return context.WithTimeout(ctx, execTimeout)
}

func (e *BaseEngine) Execute(ctx context.Context, msg ExecutionMessage) error {
	panic("method Execute must be implemented by the specific engine")
}
