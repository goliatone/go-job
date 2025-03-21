package job

import (
	"context"
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
	JobID          string
	ScriptPath     string
	Config         Config
	Parameters     map[string]any
	OutputCallback func(stdout, stderr string)
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
	GetHandler() func() error
	GetHandlerConfig() command.HandlerConfig
	GetConfig() Config
}

type Engine interface {
	Name() string
	ParseJob(path string, content []byte) (Task, error)
	CanHandle(path string) bool
	Execute(ctx context.Context, msg *ExecutionMessage) error
}

type TaskRunner interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	RegisteredTasks() []Task
}

type Registry interface {
	Add(job Task) error
	Get(id string) (Task, bool)
	List() []Task
}

type MetadataParser interface {
	Parse(content []byte) (Config, string, error)
}

// handler options
// Deadline   time.Time     `json:"deadline"`
// MaxRetries int           `json:"max_retries"`
// MaxRuns    int           `json:"max_runs"`
// RunOnce    bool          `json:"run_once"`
type Config struct {
	Schedule    string            `yaml:"schedule" json:"schedule"`
	Retries     int               `yaml:"retries" json:"retries"`
	Timeout     time.Duration     `yaml:"duration" json:"duration"`
	NoTimeout   bool              `yaml:"no_timeout" json:"no_timeout"`
	Debug       bool              `yaml:"debug" json:"debug"`
	RunOnce     bool              `yaml:"run_once" json:"run_once"`
	ScriptType  string            `yaml:"script_type" json:"script_type"`
	Transaction bool              `yaml:"transaction" json:"transaction"`
	Metadata    map[string]any    `yaml:"metadata" json:"metadata"`
	Env         map[string]string `yaml:"env" json:"env"`
}

var (
	// DefaultTimeout is used to setup the default timeout for tasks
	DefaultTimeout  = time.Minute
	DefaultSchedule = "* * * * *"
)
