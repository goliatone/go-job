package job

import (
	"encoding/json"
	"fmt"

	"github.com/goliatone/go-errors"
)

// EnvelopePayload exposes envelope metadata without tying callers to a struct type.
type EnvelopePayload interface {
	EnvelopeActor() any
	EnvelopeScope() any
	EnvelopeIdempotencyKey() string
	EnvelopeParams() map[string]any
}

// EnvelopeParamsSetter allows codecs to apply sanitized params.
type EnvelopeParamsSetter interface {
	SetEnvelopeParams(map[string]any)
}

// EnvelopeRawContentSetter allows codecs to capture decoded size.
type EnvelopeRawContentSetter interface {
	SetEnvelopeRawContentBytes(int)
}

// EnvelopeValidator allows envelopes to provide custom validation.
type EnvelopeValidator interface {
	Validate() error
}

// EnvelopeCodec encodes and decodes envelope payloads.
type EnvelopeCodec interface {
	Encode(value any) ([]byte, error)
	Decode(data []byte, value any) error
}

// JSONEnvelopeCodec marshals envelopes using JSON with size limits and sanitization.
type JSONEnvelopeCodec struct {
	maxBytes  int
	sanitizer EnvelopeSanitizer
}

// NewJSONEnvelopeCodec builds a JSON codec configured by envelope options.
func NewJSONEnvelopeCodec(opts ...EnvelopeOption) *JSONEnvelopeCodec {
	cfg := buildEnvelopeConfig(opts...)
	return &JSONEnvelopeCodec{
		maxBytes:  cfg.maxBytes,
		sanitizer: cfg.sanitizer,
	}
}

// Encode marshals the envelope, applying sanitization and size limits.
func (c *JSONEnvelopeCodec) Encode(value any) ([]byte, error) {
	if value == nil {
		return nil, fmt.Errorf("envelope value required")
	}
	if validator, ok := value.(EnvelopeValidator); ok {
		if err := validator.Validate(); err != nil {
			return nil, err
		}
	}

	payload := value
	if data, ok := value.(EnvelopePayload); ok {
		payload = envelopePayload{
			Actor:          data.EnvelopeActor(),
			Scope:          data.EnvelopeScope(),
			Params:         sanitizeParams(data.EnvelopeParams(), c.sanitizer),
			IdempotencyKey: data.EnvelopeIdempotencyKey(),
		}
	}

	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode envelope: %w", err)
	}

	if c.maxBytes > 0 && len(raw) > c.maxBytes {
		return nil, envelopeSizeError(len(raw), c.maxBytes)
	}
	return raw, nil
}

// Decode unmarshals an envelope, enforcing size limits and sanitization.
func (c *JSONEnvelopeCodec) Decode(data []byte, value any) error {
	if value == nil {
		return fmt.Errorf("envelope value required")
	}
	if c.maxBytes > 0 && len(data) > c.maxBytes {
		return envelopeSizeError(len(data), c.maxBytes)
	}
	if err := json.Unmarshal(data, value); err != nil {
		return fmt.Errorf("decode envelope: %w", err)
	}

	if setter, ok := value.(EnvelopeRawContentSetter); ok {
		setter.SetEnvelopeRawContentBytes(len(data))
	}
	if data, ok := value.(EnvelopePayload); ok {
		if setter, ok := value.(EnvelopeParamsSetter); ok {
			params := sanitizeParams(data.EnvelopeParams(), c.sanitizer)
			setter.SetEnvelopeParams(params)
		}
	}
	if validator, ok := value.(EnvelopeValidator); ok {
		if err := validator.Validate(); err != nil {
			return err
		}
	}
	return nil
}

type envelopePayload struct {
	Actor          any            `json:"actor,omitempty"`
	Scope          any            `json:"scope,omitempty"`
	Params         map[string]any `json:"params,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
}

func envelopeSizeError(size, limit int) error {
	return errors.NewValidation("envelope exceeds maximum bytes",
		errors.FieldError{
			Field:   "envelope",
			Message: fmt.Sprintf("encoded envelope size %d exceeds limit %d bytes", size, limit),
			Value:   size,
		},
	)
}
