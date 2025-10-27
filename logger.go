package job

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// Logger defines the leveled logging contract used across go-job.
type Logger interface {
	Trace(msg string, args ...any)
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
	Fatal(msg string, args ...any)
	WithContext(ctx context.Context) Logger
}

// LoggerProvider produces named loggers. Implementations may scope logs by name.
type LoggerProvider interface {
	GetLogger(name string) Logger
}

// FieldsLogger allows attaching persistent structured key/value pairs to a logger.
type FieldsLogger interface {
	WithFields(fields map[string]any) Logger
}

// LoggerAware components can accept a logger instance.
type LoggerAware interface {
	SetLogger(logger Logger)
}

// LoggerProviderAware components can accept a logger provider.
type LoggerProviderAware interface {
	SetLoggerProvider(provider LoggerProvider)
}

// LogLevel represents the minimum severity the standard logger should emit.
type LogLevel int

const (
	LevelTrace LogLevel = iota
	LevelDebug
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
)

func (l LogLevel) String() string {
	switch l {
	case LevelTrace:
		return "TRACE"
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	case LevelFatal:
		return "FATAL"
	default:
		return fmt.Sprintf("LEVEL(%d)", int(l))
	}
}

// StdLoggerOption customises the behaviour of the default stdout logger.
type StdLoggerOption func(*stdLoggerProvider)

// WithStdLoggerWriter overrides the destination for log lines.
func WithStdLoggerWriter(w io.Writer) StdLoggerOption {
	return func(p *stdLoggerProvider) {
		if w != nil {
			p.writer = w
		}
	}
}

// WithStdLoggerMinLevel changes the minimum level emitted by the logger.
func WithStdLoggerMinLevel(level LogLevel) StdLoggerOption {
	return func(p *stdLoggerProvider) {
		p.minLevel = level
	}
}

// WithStdLoggerTimestampFunc overrides the time source used for log entries.
func WithStdLoggerTimestampFunc(fn func() time.Time) StdLoggerOption {
	return func(p *stdLoggerProvider) {
		if fn != nil {
			p.now = fn
		}
	}
}

// NewStdLoggerProvider returns a lightweight logger provider that writes structured
// log lines to the supplied writer. By default it discards output, providing a silent
// fallback for dependants that do not configure logging explicitly.
func NewStdLoggerProvider(opts ...StdLoggerOption) LoggerProvider {
	return newStdLoggerProvider(opts...)
}

type stdLoggerProvider struct {
	mu       sync.Mutex
	writer   io.Writer
	minLevel LogLevel
	now      func() time.Time
}

func newStdLoggerProvider(opts ...StdLoggerOption) *stdLoggerProvider {
	provider := &stdLoggerProvider{
		writer:   io.Discard,
		minLevel: LevelInfo,
		now:      time.Now,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(provider)
		}
	}
	return provider
}

func (p *stdLoggerProvider) GetLogger(name string) Logger {
	return &stdLogger{
		provider: p,
		name:     name,
		ctx:      context.Background(),
		fields:   map[string]any{},
	}
}

type stdLogger struct {
	provider *stdLoggerProvider
	name     string
	fields   map[string]any
	ctx      context.Context
}

func (l *stdLogger) Trace(msg string, args ...any) { l.log(LevelTrace, msg, args...) }
func (l *stdLogger) Debug(msg string, args ...any) { l.log(LevelDebug, msg, args...) }
func (l *stdLogger) Info(msg string, args ...any)  { l.log(LevelInfo, msg, args...) }
func (l *stdLogger) Warn(msg string, args ...any)  { l.log(LevelWarn, msg, args...) }
func (l *stdLogger) Error(msg string, args ...any) { l.log(LevelError, msg, args...) }
func (l *stdLogger) Fatal(msg string, args ...any) { l.log(LevelFatal, msg, args...) }

func (l *stdLogger) WithContext(ctx context.Context) Logger {
	if ctx == nil {
		ctx = context.Background()
	}
	return &stdLogger{
		provider: l.provider,
		name:     l.name,
		fields:   cloneFields(l.fields),
		ctx:      ctx,
	}
}

func (l *stdLogger) WithFields(fields map[string]any) Logger {
	if len(fields) == 0 {
		return l
	}
	merged := cloneFields(l.fields)
	for k, v := range fields {
		merged[k] = v
	}
	return &stdLogger{
		provider: l.provider,
		name:     l.name,
		fields:   merged,
		ctx:      l.ctx,
	}
}

func (l *stdLogger) log(level LogLevel, msg string, args ...any) {
	if l == nil || l.provider == nil {
		return
	}

	fields := make([]string, 0, len(l.fields)+(len(args)+1)/2)

	if len(l.fields) > 0 {
		keys := make([]string, 0, len(l.fields))
		for key := range l.fields {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			fields = append(fields, fmt.Sprintf("%s=%v", key, l.fields[key]))
		}
	}

	pairs := len(args) - len(args)%2
	for i := 0; i < pairs; i += 2 {
		key := fmt.Sprintf("arg_%d", i)
		value := any("(missing)")
		if i < len(args) {
			key = fmt.Sprint(args[i])
		}
		if i+1 < len(args) {
			value = args[i+1]
		}
		fields = append(fields, fmt.Sprintf("%s=%v", key, value))
	}

	if len(args)%2 == 1 {
		// Preserve the dangling value so tooling can surface the mismatch.
		last := args[len(args)-1]
		fields = append(fields, fmt.Sprintf("extra_arg=%v", last))
	}

	l.provider.write(level, l.name, msg, fields)
}

func (p *stdLoggerProvider) write(level LogLevel, name, msg string, fields []string) {
	if p == nil || p.writer == nil {
		return
	}

	if level < p.minLevel {
		return
	}

	timestamp := p.now().Format(time.RFC3339Nano)

	var sb strings.Builder
	sb.Grow(64 + len(msg) + len(fields)*12)

	sb.WriteString(timestamp)
	sb.WriteByte(' ')
	sb.WriteString(level.String())
	if name != "" {
		sb.WriteString(" [")
		sb.WriteString(name)
		sb.WriteByte(']')
	}
	if msg != "" {
		sb.WriteByte(' ')
		sb.WriteString(msg)
	}
	if len(fields) > 0 {
		sb.WriteByte(' ')
		sb.WriteString(strings.Join(fields, " "))
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	fmt.Fprintln(p.writer, sb.String())
}

func cloneFields(fields map[string]any) map[string]any {
	if len(fields) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(fields))
	for k, v := range fields {
		out[k] = v
	}
	return out
}
