package job

import (
	"encoding/json"
	"fmt"

	"github.com/goliatone/go-errors"
)

var (
	ErrQuotaExceeded = errors.New("quota exceeded", errors.CategoryRateLimit).
		WithCode(errors.CodeTooManyRequests)
)

type QuotaChecker interface {
	Check(*ExecutionMessage) error
}

type quotaCheckerFunc func(*ExecutionMessage) error

func (f quotaCheckerFunc) Check(msg *ExecutionMessage) error {
	if f == nil {
		return nil
	}
	return f(msg)
}

// BasicQuotaChecker enforces payload size and retry count limits.
type BasicQuotaChecker struct {
	PayloadSizeLimit int
	MaxRetries       int
}

func (q BasicQuotaChecker) Check(msg *ExecutionMessage) error {
	if msg == nil {
		return nil
	}

	if q.PayloadSizeLimit > 0 {
		payload, err := json.Marshal(msg.Parameters)
		if err != nil {
			return fmt.Errorf("quota marshal parameters: %w", err)
		}
		if len(payload) > q.PayloadSizeLimit {
			return ErrQuotaExceeded.WithTextCode("PAYLOAD_TOO_LARGE").
				WithMetadata(map[string]any{"size": len(payload), "limit": q.PayloadSizeLimit})
		}
	}

	if q.MaxRetries > 0 && msg.Config.Retries > q.MaxRetries {
		return ErrQuotaExceeded.WithTextCode("RETRY_LIMIT_EXCEEDED").
			WithMetadata(map[string]any{"retries": msg.Config.Retries, "limit": q.MaxRetries})
	}

	return nil
}

var defaultQuotaChecker QuotaChecker = quotaCheckerFunc(func(*ExecutionMessage) error { return nil })
