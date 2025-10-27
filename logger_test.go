package job

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/goliatone/go-logger/glog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStdLoggerDefaultSilent(t *testing.T) {
	provider := NewStdLoggerProvider()
	stdProvider, ok := provider.(*stdLoggerProvider)
	if !ok {
		require.Fail(t, "unexpected provider type")
	}

	assert.Equal(t, io.Discard, stdProvider.writer)

	logger := provider.GetLogger("test")
	assert.NotNil(t, logger)
	logger.Info("message that should be discarded")
}

func TestStdLoggerWritesWhenEnabled(t *testing.T) {
	buf := &bytes.Buffer{}
	provider := NewStdLoggerProvider(
		WithStdLoggerWriter(buf),
		WithStdLoggerMinLevel(LevelDebug),
	)

	logger := provider.GetLogger("demo")
	fieldsLogger, ok := logger.(FieldsLogger)
	require.True(t, ok)

	logger = fieldsLogger.WithFields(map[string]any{"component": "unit_test"})
	logger.Debug("hello world", "answer", 42)

	output := buf.String()
	assert.Contains(t, output, "DEBUG")
	assert.Contains(t, output, "demo")
	assert.Contains(t, output, "hello world")
	assert.Contains(t, output, "component=unit_test")
	assert.Contains(t, output, "answer=42")
}

func TestGoLoggerAdapterWithFields(t *testing.T) {
	stub := &stubGoLogger{}
	logger := GoLogger(stub)
	require.NotNil(t, logger)

	fieldsLogger, ok := logger.(FieldsLogger)
	require.True(t, ok)

	child := fieldsLogger.WithFields(map[string]any{"request_id": "abc", "user_id": 7})
	child.Info("processed", "status", "ok")

	assert.ElementsMatch(t, []any{"request_id", "abc", "user_id", 7}, stub.withArgs)
	assert.Equal(t, "processed", stub.lastMsg)
	assert.Equal(t, []any{"status", "ok"}, stub.lastArgs)

	ctx := context.WithValue(context.Background(), "key", "value")
	logger = logger.WithContext(ctx)
	_, ok = logger.(Logger)
	require.True(t, ok)
	assert.Equal(t, ctx, stub.ctx)
}

func TestGoLoggerProvider(t *testing.T) {
	stubProvider := &stubGoLoggerProvider{}
	provider := GoLoggerProvider(stubProvider)
	require.NotNil(t, provider)

	logger := provider.GetLogger("example")
	require.NotNil(t, logger)
	logger.Info("greetings")
	assert.True(t, stubProvider.called)
}

type stubGoLogger struct {
	lastMsg  string
	lastArgs []any
	withArgs []any
	ctx      context.Context
}

func (s *stubGoLogger) Trace(msg string, args ...any) { s.record(msg, args...) }
func (s *stubGoLogger) Debug(msg string, args ...any) { s.record(msg, args...) }
func (s *stubGoLogger) Info(msg string, args ...any)  { s.record(msg, args...) }
func (s *stubGoLogger) Warn(msg string, args ...any)  { s.record(msg, args...) }
func (s *stubGoLogger) Error(msg string, args ...any) { s.record(msg, args...) }
func (s *stubGoLogger) Fatal(msg string, args ...any) { s.record(msg, args...) }

func (s *stubGoLogger) record(msg string, args ...any) {
	s.lastMsg = msg
	s.lastArgs = append([]any{}, args...)
}

func (s *stubGoLogger) WithContext(ctx context.Context) glog.Logger {
	s.ctx = ctx
	return s
}

func (s *stubGoLogger) With(args ...any) glog.Logger {
	s.withArgs = append([]any{}, args...)
	return s
}

type stubGoLoggerProvider struct {
	called bool
}

func (s *stubGoLoggerProvider) GetLogger(name string) glog.Logger {
	s.called = true
	return &stubGoLogger{}
}

func TestStdLoggerOddArgsHandled(t *testing.T) {
	buf := &bytes.Buffer{}
	provider := NewStdLoggerProvider(
		WithStdLoggerWriter(buf),
	)
	logger := provider.GetLogger("odd")
	logger.Info("message", "one")
	output := buf.String()
	assert.True(t, strings.Contains(output, "extra_arg=one"))
}
