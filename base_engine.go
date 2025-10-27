package job

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/goliatone/go-errors"
)

type BaseEngine struct {
	FileExtensions []string
	Timeout        time.Duration
	MetadataParser MetadataParser
	FS             fs.FS
	SourceProvider SourceProvider
	EngineType     string
	Self           Engine
	logger         Logger
	taskIDProvider TaskIDProvider
}

func NewBaseEngine(self Engine, engingeType string, exts ...string) *BaseEngine {
	return &BaseEngine{
		Timeout:        30 * time.Second,
		MetadataParser: NewYAMLMetadataParser(),
		FS:             os.DirFS("."),
		EngineType:     engingeType,
		FileExtensions: exts,
		Self:           self,
		logger:         &defaultLogger{},
		taskIDProvider: DefaultTaskIDProvider,
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

	provider := e.taskIDProvider
	if provider == nil {
		provider = DefaultTaskIDProvider
	}
	jobID := provider(path)
	job := NewBaseTask(jobID, path, e.EngineType, config, scriptContent, e.Self)
	return job, nil
}

// SetTaskIDProvider allows engines to override the default ID generation strategy.
func (e *BaseEngine) SetTaskIDProvider(provider TaskIDProvider) {
	e.taskIDProvider = provider
}

func (e *BaseEngine) GetScriptContent(msg *ExecutionMessage) (string, error) {
	if content, ok := msg.Parameters["script"].(string); ok {
		return content, nil
	}

	if e.SourceProvider == nil {
		e.SourceProvider = NewFileSystemSourceProvider(".", e.FS)
	}

	content, err := e.SourceProvider.GetScript(msg.ScriptPath)
	if err != nil {
		return "", errors.Wrap(err, errors.CategoryExternal, "failed to read script file").
			WithTextCode("SCRIPT_READ_ERROR").
			WithMetadata(map[string]any{
				"operation":   "read_script",
				"script_path": msg.ScriptPath,
				"engine_type": e.EngineType,
			})
	}

	_, scriptContent, err := e.MetadataParser.Parse(content)
	if err != nil {
		return "", errors.Wrap(err, errors.CategoryInternal, "failed to parse script content").
			WithTextCode("SCRIPT_PARSE_ERROR").
			WithMetadata(map[string]any{
				"operation":    "parse_script",
				"script_path":  msg.ScriptPath,
				"engine_type":  e.EngineType,
				"content_size": len(content),
			})
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
