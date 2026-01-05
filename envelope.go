package job

import (
	"fmt"

	"github.com/goliatone/go-errors"
)

// Envelope wraps the payload passed to job handlers with optional actor/scope metadata
// and an idempotency key for upstream deduplication.
type Envelope struct {
	Actor           *Actor         `json:"actor,omitempty"`
	Scope           Scope          `json:"scope,omitempty"`
	Params          map[string]any `json:"params,omitempty"`
	IdempotencyKey  string         `json:"idempotency_key,omitempty"`
	RawContentBytes int            `json:"-"`
}

// Actor captures who initiated the job.
type Actor struct {
	ID             string            `json:"id,omitempty"`
	Subject        string            `json:"subject,omitempty"`
	Role           string            `json:"role,omitempty"`
	ResourceRoles  map[string]string `json:"resource_roles,omitempty"`
	Metadata       map[string]any    `json:"metadata,omitempty"`
	ImpersonatorID string            `json:"impersonator_id,omitempty"`
	IsImpersonated bool              `json:"is_impersonated,omitempty"`
}

// Scope captures tenant/organization or other scoping information for the job.
type Scope struct {
	TenantID       string            `json:"tenant_id,omitempty"`
	OrganizationID string            `json:"organization_id,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
}

const (
	// DefaultEnvelopeMaxBytes caps encoded envelope size unless overridden via options.
	DefaultEnvelopeMaxBytes = 64 * 1024
	// MaxIdempotencyKeyLength enforces sane limits on idempotency keys.
	MaxIdempotencyKeyLength = 256
)

type envelopeConfig struct {
	maxBytes  int
	sanitizer EnvelopeSanitizer
}

// EnvelopeOption customizes encode/decode behaviour.
type EnvelopeOption func(*envelopeConfig)

// EnvelopeSanitizer allows callers to scrub params before encoding or after decoding.
type EnvelopeSanitizer func(map[string]any) map[string]any

// WithEnvelopeMaxBytes sets the maximum allowed encoded size in bytes.
func WithEnvelopeMaxBytes(limit int) EnvelopeOption {
	return func(cfg *envelopeConfig) {
		cfg.maxBytes = limit
	}
}

// WithEnvelopeSanitizer applies a sanitizer to Params before encoding/after decoding.
func WithEnvelopeSanitizer(fn EnvelopeSanitizer) EnvelopeOption {
	return func(cfg *envelopeConfig) {
		cfg.sanitizer = fn
	}
}

// EncodeEnvelope marshals the envelope to JSON applying validation, sanitization, and size limits.
func EncodeEnvelope(env Envelope, opts ...EnvelopeOption) ([]byte, error) {
	codec := NewJSONEnvelopeCodec(opts...)
	return codec.Encode(env)
}

// DecodeEnvelope unmarshals JSON data into an Envelope, enforcing size limits and validation.
func DecodeEnvelope(data []byte, opts ...EnvelopeOption) (Envelope, error) {
	codec := NewJSONEnvelopeCodec(opts...)
	var env Envelope
	if err := codec.Decode(data, &env); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

// Validate enforces basic constraints on the envelope fields.
func (env Envelope) Validate() error {
	var fieldErrors []errors.FieldError

	if len(env.IdempotencyKey) > MaxIdempotencyKeyLength {
		fieldErrors = append(fieldErrors, errors.FieldError{
			Field:   "idempotency_key",
			Message: fmt.Sprintf("must be at most %d characters", MaxIdempotencyKeyLength),
			Value:   env.IdempotencyKey,
		})
	}

	if env.Actor != nil && env.Actor.IsImpersonated && env.Actor.ImpersonatorID == "" {
		fieldErrors = append(fieldErrors, errors.FieldError{
			Field:   "actor.impersonator_id",
			Message: "must be set when actor is impersonated",
		})
	}

	if len(fieldErrors) > 0 {
		return errors.NewValidation("envelope validation failed", fieldErrors...)
	}

	return nil
}

// EnvelopeActor returns the actor metadata for envelope codecs.
func (env Envelope) EnvelopeActor() any { return env.Actor }

// EnvelopeScope returns the scope metadata for envelope codecs.
func (env Envelope) EnvelopeScope() any { return env.Scope }

// EnvelopeIdempotencyKey returns the idempotency key for envelope codecs.
func (env Envelope) EnvelopeIdempotencyKey() string { return env.IdempotencyKey }

// EnvelopeParams returns params for envelope codecs.
func (env Envelope) EnvelopeParams() map[string]any { return env.Params }

// SetEnvelopeParams updates params after sanitization.
func (env *Envelope) SetEnvelopeParams(params map[string]any) {
	if env == nil {
		return
	}
	env.Params = params
}

// SetEnvelopeRawContentBytes stores the decoded payload size.
func (env *Envelope) SetEnvelopeRawContentBytes(size int) {
	if env == nil {
		return
	}
	env.RawContentBytes = size
}

func buildEnvelopeConfig(opts ...EnvelopeOption) envelopeConfig {
	cfg := envelopeConfig{
		maxBytes:  DefaultEnvelopeMaxBytes,
		sanitizer: copyParams,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

func (env Envelope) clone() Envelope {
	clone := env
	if env.Actor != nil {
		clone.Actor = env.Actor.clone()
	}
	clone.Scope = env.Scope.clone()
	clone.Params = copyParams(env.Params)
	return clone
}

func (actor *Actor) clone() *Actor {
	if actor == nil {
		return nil
	}
	cp := *actor
	if actor.ResourceRoles != nil {
		cp.ResourceRoles = copyStringMap(actor.ResourceRoles)
	}
	if actor.Metadata != nil {
		cp.Metadata = copyAnyMap(actor.Metadata)
	}
	return &cp
}

func (scope Scope) clone() Scope {
	cp := scope
	if scope.Labels != nil {
		cp.Labels = copyStringMap(scope.Labels)
	}
	return cp
}

func sanitizeParams(params map[string]any, sanitizer EnvelopeSanitizer) map[string]any {
	if sanitizer == nil {
		return copyParams(params)
	}
	return sanitizer(copyParams(params))
}

func copyParams(params map[string]any) map[string]any {
	if len(params) == 0 {
		return nil
	}
	out := make(map[string]any, len(params))
	for key, value := range params {
		out[key] = value
	}
	return out
}

func copyAnyMap(values map[string]any) map[string]any {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]any, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func copyStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[key] = value
	}
	return out
}

func (scope Scope) isEmpty() bool {
	return scope.TenantID == "" && scope.OrganizationID == "" && len(scope.Labels) == 0
}
