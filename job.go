package job

import (
	"context"
	"time"

	"github.com/goliatone/go-errors"
)

type SourceProvider interface {
	GetScript(path string) (content []byte, err error)
	ListScripts(ctx context.Context) ([]ScriptInfo, error)
}

type ScriptInfo struct {
	ID      string         `json:"id"`
	Path    string         `json:"path"`
	Content []byte         `json:"content"`
	Meta    map[string]any `json:"metadata"`
}

type TaskCreator interface {
	CreateTasks(ctx context.Context) ([]Task, error)
}

// ExecutionMessage represents a request to execute a job script.
type ExecutionMessage struct {
	JobID          string                      `json:"job_id" yaml:"job_id"`
	ScriptPath     string                      `json:"script_path" yaml:"script_path"`
	Config         Config                      `json:"config" yaml:"config"`
	Parameters     map[string]any              `json:"parameters" yaml:"parameters"`
	IdempotencyKey string                      `json:"idempotency_key" yaml:"idempotency_key"`
	DedupPolicy    DeduplicationPolicy         `json:"dedup_policy" yaml:"dedup_policy"`
	Result         *Result                     `json:"result,omitempty" yaml:"result,omitempty"`
	OutputCallback func(stdout, stderr string) `json:"-" yaml:"-"`
}

// Type returns the message type for the command system
func (msg ExecutionMessage) Type() string {
	return "job:runner:execution"
}

// Validate ensures the message contains required fields
func (msg ExecutionMessage) Validate() error {
	var fieldErrors []errors.FieldError

	if msg.JobID == "" {
		fieldErrors = append(fieldErrors, errors.FieldError{
			Field:   "job_id",
			Message: "cannot be empty",
			Value:   msg.JobID,
		})
	}

	if msg.ScriptPath == "" {
		fieldErrors = append(fieldErrors, errors.FieldError{
			Field:   "script_path",
			Message: "cannot be empty",
			Value:   msg.ScriptPath,
		})
	}

	if len(msg.IdempotencyKey) > MaxIdempotencyKeyLength {
		fieldErrors = append(fieldErrors, errors.FieldError{
			Field:   "idempotency_key",
			Message: "exceeds maximum length",
			Value:   msg.IdempotencyKey,
		})
	}

	if !isValidDedupPolicy(msg.DedupPolicy) {
		fieldErrors = append(fieldErrors, errors.FieldError{
			Field:   "dedup_policy",
			Message: "invalid policy; expected drop|merge|replace|ignore",
			Value:   msg.DedupPolicy,
		})
	}

	if msg.Result != nil {
		if err := msg.Result.Validate(); err != nil {
			fieldErrors = append(fieldErrors, errors.FieldError{
				Field:   "result",
				Message: err.Error(),
			})
		}
	}

	if len(fieldErrors) > 0 {
		return errors.NewValidation("execution message validation failed", fieldErrors...)
	}

	return nil
}

// Task represents a schedulable job discovered from the filesystem
type Task interface {
	GetID() string
	// GetHandler is the function that we a command needs to implement in order to be able to execute it in the background
	GetHandler() func() error
	GetHandlerConfig() HandlerOptions
	GetConfig() Config
	GetPath() string
	GetEngine() Engine
	Execute(ctx context.Context, msg *ExecutionMessage) error
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
	List() []Task
	Add(job Task) error
	Get(id string) (Task, bool)
	SetResult(id string, result Result) error
	GetResult(id string) (Result, bool)
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
	Schedule       string            `yaml:"schedule" json:"schedule"`
	Retries        int               `yaml:"retries" json:"retries"`
	Timeout        time.Duration     `yaml:"duration" json:"duration"`
	Deadline       time.Time         `yaml:"deadline" json:"deadline"`
	NoTimeout      bool              `yaml:"no_timeout" json:"no_timeout"`
	Debug          bool              `yaml:"debug" json:"debug"`
	RunOnce        bool              `yaml:"run_once" json:"run_once"`
	MaxRuns        int               `yaml:"max_runs" json:"max_runs"`
	ExitOnError    bool              `yaml:"exit_on_error" json:"exit_on_error"`
	ScriptType     string            `yaml:"script_type" json:"script_type"`
	Transaction    bool              `yaml:"transaction" json:"transaction"`
	Metadata       map[string]any    `yaml:"metadata" json:"metadata"`
	Env            map[string]string `yaml:"env" json:"env"`
	Backoff        BackoffConfig     `yaml:"backoff" json:"backoff"`
	MaxConcurrency int               `yaml:"max_concurrency" json:"max_concurrency"`
}

var (
	// DefaultTimeout is used to setup the default timeout for tasks
	DefaultTimeout  = time.Minute
	DefaultSchedule = "* * * * *"
)
