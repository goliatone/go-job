package job

import (
	"context"
	"sort"

	"github.com/goliatone/go-logger/glog"
)

// GoLoggerProvider converts a go-logger provider into the job LoggerProvider contract.
func GoLoggerProvider(provider glog.LoggerProvider) LoggerProvider {
	if provider == nil {
		return nil
	}
	return &goLoggerProviderAdapter{provider: provider}
}

// GoLogger wraps a go-logger Logger into the job Logger contract.
func GoLogger(logger glog.Logger) Logger {
	if logger == nil {
		return nil
	}
	return &goLoggerAdapter{logger: logger}
}

type goLoggerProviderAdapter struct {
	provider glog.LoggerProvider
}

func (g *goLoggerProviderAdapter) GetLogger(name string) Logger {
	return GoLogger(g.provider.GetLogger(name))
}

type goLoggerAdapter struct {
	logger glog.Logger
}

func (g *goLoggerAdapter) Trace(msg string, args ...any) { g.logger.Trace(msg, args...) }
func (g *goLoggerAdapter) Debug(msg string, args ...any) { g.logger.Debug(msg, args...) }
func (g *goLoggerAdapter) Info(msg string, args ...any)  { g.logger.Info(msg, args...) }
func (g *goLoggerAdapter) Warn(msg string, args ...any)  { g.logger.Warn(msg, args...) }
func (g *goLoggerAdapter) Error(msg string, args ...any) { g.logger.Error(msg, args...) }
func (g *goLoggerAdapter) Fatal(msg string, args ...any) { g.logger.Fatal(msg, args...) }

func (g *goLoggerAdapter) WithContext(ctx context.Context) Logger {
	return &goLoggerAdapter{logger: g.logger.WithContext(ctx)}
}

func (g *goLoggerAdapter) WithFields(fields map[string]any) Logger {
	if len(fields) == 0 {
		return g
	}

	pairs := make([]any, 0, len(fields)*2)
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		pairs = append(pairs, key, fields[key])
	}

	if withLogger, ok := g.logger.(interface {
		With(args ...any) glog.Logger
	}); ok {
		return &goLoggerAdapter{logger: withLogger.With(pairs...)}
	}

	if withLogger, ok := g.logger.(interface {
		With(args ...any) *glog.BaseLogger
	}); ok {
		return &goLoggerAdapter{logger: withLogger.With(pairs...)}
	}

	return g
}
