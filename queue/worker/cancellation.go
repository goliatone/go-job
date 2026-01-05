package worker

import (
	"context"
	"sync"
	"time"

	job "github.com/goliatone/go-job"
	"github.com/goliatone/go-job/queue"
	"github.com/goliatone/go-job/queue/cancellation"
)

type cancelState struct {
	mu        sync.Mutex
	requested bool
	reason    string
}

func (c *cancelState) set(req cancellation.Request) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requested = true
	c.reason = req.Reason
}

func (c *cancelState) requested() bool {
	if c == nil {
		return false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.requested
}

func (c *cancelState) reason() string {
	if c == nil {
		return ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reason
}

func (w *Worker) cancellationKey(msg *job.ExecutionMessage) string {
	if msg == nil {
		return ""
	}
	if w != nil && w.cancelKeyFn != nil {
		return w.cancelKeyFn(msg)
	}
	return msg.IdempotencyKey
}

func (w *Worker) checkCancellation(ctx context.Context, key string, state *cancelState) bool {
	if w == nil || w.cancelStore == nil || key == "" {
		return false
	}
	req, found, err := w.cancelStore.Get(ctx, key)
	if err != nil {
		w.logCancelCheckError(key, err)
		return false
	}
	if found {
		state.set(req)
	}
	return found
}

func (w *Worker) monitorCancellation(ctx context.Context, key string, state *cancelState, cancel context.CancelFunc) {
	if w == nil || w.cancelStore == nil || key == "" || cancel == nil {
		return
	}
	interval := w.cancelPoll
	if interval <= 0 {
		interval = defaultCancelPoll
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		if ctx.Err() != nil {
			return
		}
		req, found, err := w.cancelStore.Get(ctx, key)
		if err != nil {
			w.logCancelCheckError(key, err)
		}
		if found {
			state.set(req)
			cancel()
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *Worker) cancelNackOptions(reason string) queue.NackOptions {
	if reason == "" {
		reason = "canceled"
	}
	return queue.NackOptions{Reason: reason}
}
