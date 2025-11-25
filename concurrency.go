package job

import (
	"fmt"
	"sync"

	"github.com/goliatone/go-errors"
)

var (
	ErrConcurrencyLimit = errors.New("concurrency limit reached", errors.CategoryRateLimit).
		WithCode(errors.CodeTooManyRequests)
)

// ConcurrencyLimiter enforces per-key concurrency limits.
type ConcurrencyLimiter struct {
	mu             sync.Mutex
	sem            map[string]chan struct{}
	scopeExtractor func(*ExecutionMessage) string
}

func NewConcurrencyLimiter() *ConcurrencyLimiter {
	return &ConcurrencyLimiter{
		sem: make(map[string]chan struct{}),
	}
}

// WithScopeExtractor sets a callback to derive scope keys (e.g., tenant) for per-scope limits.
func (c *ConcurrencyLimiter) WithScopeExtractor(fn func(*ExecutionMessage) string) *ConcurrencyLimiter {
	c.scopeExtractor = fn
	return c
}

// Acquire reserves a slot for the given message respecting the limit. Returns a release func.
func (c *ConcurrencyLimiter) Acquire(msg *ExecutionMessage, limit int) (func(), error) {
	if msg == nil || limit <= 0 {
		return func() {}, nil
	}

	key := msg.JobID
	if c.scopeExtractor != nil {
		if scope := c.scopeExtractor(msg); scope != "" {
			key = fmt.Sprintf("%s|%s", key, scope)
		}
	}

	c.mu.Lock()
	ch, ok := c.sem[key]
	if !ok {
		ch = make(chan struct{}, limit)
		c.sem[key] = ch
	}
	c.mu.Unlock()

	select {
	case ch <- struct{}{}:
		return func() { <-ch }, nil
	default:
		return nil, ErrConcurrencyLimit
	}
}
