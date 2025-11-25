package job

import (
	"sync"

	"github.com/goliatone/go-errors"
)

type DeduplicationPolicy string

const (
	DedupPolicyIgnore  DeduplicationPolicy = "ignore"
	DedupPolicyDrop    DeduplicationPolicy = "drop"
	DedupPolicyMerge   DeduplicationPolicy = "merge"
	DedupPolicyReplace DeduplicationPolicy = "replace"
)

var (
	ErrIdempotentDrop = errors.New("job dropped due to idempotency policy", errors.CategoryConflict).
		WithCode(errors.CodeConflict)
)

type dedupDecision int

const (
	dedupProceed dedupDecision = iota
	dedupDrop
	dedupMerge
)

type dedupEntry struct {
	lastErr error
}

// IdempotencyTracker tracks idempotency keys to enforce deduplication policies.
type IdempotencyTracker struct {
	mu      sync.Mutex
	entries map[string]*dedupEntry
}

func NewIdempotencyTracker() *IdempotencyTracker {
	return &IdempotencyTracker{
		entries: make(map[string]*dedupEntry),
	}
}

func (t *IdempotencyTracker) BeforeExecute(key string, policy DeduplicationPolicy) (dedupDecision, error) {
	if key == "" || policy == "" || policy == DedupPolicyIgnore {
		return dedupProceed, nil
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	entry, exists := t.entries[key]
	if !exists {
		t.entries[key] = &dedupEntry{}
		return dedupProceed, nil
	}

	switch policy {
	case DedupPolicyDrop:
		return dedupDrop, entry.lastErr
	case DedupPolicyMerge:
		return dedupMerge, entry.lastErr
	case DedupPolicyReplace:
		t.entries[key] = &dedupEntry{}
		return dedupProceed, nil
	default:
		return dedupProceed, nil
	}
}

func (t *IdempotencyTracker) AfterExecute(key string, policy DeduplicationPolicy, execErr error) {
	if key == "" || policy == "" || policy == DedupPolicyIgnore {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	entry, exists := t.entries[key]
	if !exists {
		entry = &dedupEntry{}
		t.entries[key] = entry
	}

	entry.lastErr = execErr
}

func isValidDedupPolicy(policy DeduplicationPolicy) bool {
	switch policy {
	case "", DedupPolicyIgnore, DedupPolicyDrop, DedupPolicyMerge, DedupPolicyReplace:
		return true
	default:
		return false
	}
}
