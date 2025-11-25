package job

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/goliatone/go-errors"
)

// Result captures execution metadata for a job.
type Result struct {
	Status    string         `json:"status,omitempty"`
	Message   string         `json:"message,omitempty"`
	OutputURL string         `json:"output_url,omitempty"`
	Size      int64          `json:"size,omitempty"`
	Duration  time.Duration  `json:"duration,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

const (
	// DefaultResultMaxBytes caps encoded result size unless overridden.
	DefaultResultMaxBytes = 32 * 1024
)

// ResultCodec allows custom serialization of result metadata.
type ResultCodec interface {
	Marshal(Result) ([]byte, error)
	Unmarshal([]byte) (Result, error)
}

type resultConfig struct {
	maxBytes int
	codec    ResultCodec
}

// ResultOption customizes encode/decode behaviour.
type ResultOption func(*resultConfig)

// WithResultMaxBytes sets the maximum allowed encoded size in bytes.
func WithResultMaxBytes(limit int) ResultOption {
	return func(cfg *resultConfig) {
		cfg.maxBytes = limit
	}
}

// WithResultCodec sets a custom codec for serialization.
func WithResultCodec(codec ResultCodec) ResultOption {
	return func(cfg *resultConfig) {
		if codec != nil {
			cfg.codec = codec
		}
	}
}

// EncodeResult marshals the result with validation and size checks.
func EncodeResult(res Result, opts ...ResultOption) ([]byte, error) {
	cfg := buildResultConfig(opts...)
	if err := res.Validate(); err != nil {
		return nil, err
	}

	payload, err := cfg.codec.Marshal(res)
	if err != nil {
		return nil, fmt.Errorf("encode result: %w", err)
	}

	if cfg.maxBytes > 0 && len(payload) > cfg.maxBytes {
		return nil, errors.NewValidation("result exceeds maximum bytes",
			errors.FieldError{
				Field:   "result",
				Message: fmt.Sprintf("encoded result size %d exceeds limit %d bytes", len(payload), cfg.maxBytes),
				Value:   len(payload),
			},
		)
	}

	return payload, nil
}

// DecodeResult unmarshals result payload enforcing size limits and validation.
func DecodeResult(data []byte, opts ...ResultOption) (Result, error) {
	cfg := buildResultConfig(opts...)
	if cfg.maxBytes > 0 && len(data) > cfg.maxBytes {
		return Result{}, errors.NewValidation("result exceeds maximum bytes",
			errors.FieldError{
				Field:   "result",
				Message: fmt.Sprintf("encoded result size %d exceeds limit %d bytes", len(data), cfg.maxBytes),
				Value:   len(data),
			},
		)
	}

	res, err := cfg.codec.Unmarshal(data)
	if err != nil {
		return Result{}, fmt.Errorf("decode result: %w", err)
	}

	if err := res.Validate(); err != nil {
		return Result{}, err
	}

	return res, nil
}

// Validate enforces simple constraints on the result metadata.
func (r Result) Validate() error {
	var fieldErrors []errors.FieldError

	if r.Size < 0 {
		fieldErrors = append(fieldErrors, errors.FieldError{
			Field:   "size",
			Message: "must be non-negative",
			Value:   r.Size,
		})
	}

	if r.Duration < 0 {
		fieldErrors = append(fieldErrors, errors.FieldError{
			Field:   "duration",
			Message: "must be non-negative",
			Value:   r.Duration,
		})
	}

	if len(fieldErrors) > 0 {
		return errors.NewValidation("result validation failed", fieldErrors...)
	}

	return nil
}

type jsonResultCodec struct{}

func (jsonResultCodec) Marshal(res Result) ([]byte, error) {
	return json.Marshal(res)
}

func (jsonResultCodec) Unmarshal(data []byte) (Result, error) {
	var res Result
	err := json.Unmarshal(data, &res)
	return res, err
}

func buildResultConfig(opts ...ResultOption) resultConfig {
	cfg := resultConfig{
		maxBytes: DefaultResultMaxBytes,
		codec:    jsonResultCodec{},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}
