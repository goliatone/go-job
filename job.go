package job

import (
	"context"
	"maps"
	"time"

	"github.com/goliatone/go-command"
)

type SourceProvider interface {
	GetScript(path string) (content []byte, err error)
	ListScripts(ctx context.Context) ([]ScriptInfo, error)
}

type ScriptInfo struct {
	ID      string
	Path    string
	Content []byte
}

type TaskCreator interface {
	CreateTasks(ctx context.Context) ([]Task, error)
}

// ExecutionMessage represents a request to execute a job script
type ExecutionMessage struct {
	command.BaseMessage
	JobID      string
	ScriptPath string
	Config     Config
	Parameters map[string]any
}

// Type returns the message type for the command system
func (msg ExecutionMessage) Type() string {
	return "job:runner:execution"
}

// Validate ensures the message contains required fields
func (msg ExecutionMessage) Validate() error {
	if msg.JobID == "" {
		return command.WrapError("InvalidJobMessage", "job ID cannot be empty", nil)
	}

	if msg.ScriptPath == "" {
		return command.WrapError("InvalidJobMessage", "script path cannot be empty", nil)
	}
	return nil
}

// Task represents a schedulable job discovered from the filesystem
type Task interface {
	GetID() string
	GetHandler() command.Commander[ExecutionMessage]
	GetHandlerConfig() command.HandlerConfig
	GetConfig() Config
}

type Engine interface {
	Name() string
	ParseJob(path string, content []byte) (Task, error)
	CanHandle(path string) bool
	Execute(ctx context.Context, msg ExecutionMessage) error
}

type TaskRunner interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	RegisteredTasks() []Task
	AddEngine(engine Engine)
}

type Registry interface {
	Add(job Task) error
	Get(id string) (Task, bool)
	List() []Task
}

type MetadataParser interface {
	Parse(content []byte) (Config, string, error)
}

type Config struct {
	Schedule    string            `yaml:"schedule"`
	Retries     int               `yaml:"retries"`
	Timeout     time.Duration     `yaml:"duration"`
	Debug       bool              `yaml:"debug"`
	RunOnce     bool              `yaml:"run_once"`
	Env         map[string]string `yaml:"env"`
	ScriptType  string            `yaml:"script_type"`
	Transaction bool              `yaml:"transaction"`
	Metadata    map[string]any    `yaml:"metadata"`
}

func (c Config) ToMap() map[string]any {
	result := make(map[string]any)

	result["schedule"] = c.Schedule
	result["retries"] = c.Retries
	result["timeout"] = c.Timeout
	result["debug"] = c.Debug
	result["run_once"] = c.RunOnce
	result["script_type"] = c.ScriptType
	result["transaction"] = c.Transaction

	if c.Env != nil {
		result["env"] = c.Env
	}

	if c.Metadata != nil {
		maps.Copy(result, c.Metadata)
	}

	return result
}
