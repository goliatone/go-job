package job_test

import (
	"strings"
	"testing"

	"github.com/goliatone/go-errors"
	"github.com/goliatone/go-job"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnvelopeEncodeDecodeRoundTrip(t *testing.T) {
	env := job.Envelope{
		Actor: &job.Actor{
			ID:      "user-123",
			Role:    "admin",
			Subject: "user-123",
			ResourceRoles: map[string]string{
				"bucket:reports": "reader",
			},
			Metadata: map[string]any{"team": "data"},
		},
		Scope: job.Scope{
			TenantID:       "tenant-1",
			OrganizationID: "org-9",
			Labels: map[string]string{
				"region": "us-west",
			},
		},
		Params: map[string]any{
			"payload": map[string]any{"id": 42},
		},
		IdempotencyKey: "idempo-abc",
	}

	payload, err := job.EncodeEnvelope(env)
	require.NoError(t, err)
	require.NotEmpty(t, payload)

	decoded, err := job.DecodeEnvelope(payload)
	require.NoError(t, err)

	require.NotNil(t, decoded.Actor)
	assert.Equal(t, env.Actor.ID, decoded.Actor.ID)
	assert.Equal(t, env.Actor.Role, decoded.Actor.Role)
	assert.Equal(t, env.Actor.ResourceRoles, decoded.Actor.ResourceRoles)
	assert.Equal(t, env.Actor.Metadata, decoded.Actor.Metadata)
	assert.Equal(t, env.Scope, decoded.Scope)
	assert.Equal(t, env.IdempotencyKey, decoded.IdempotencyKey)
	decodedPayload := decoded.Params["payload"].(map[string]any)
	assert.Equal(t, float64(42), decodedPayload["id"])
	assert.Equal(t, len(payload), decoded.RawContentBytes)
}

func TestEnvelopeSizeLimit(t *testing.T) {
	env := job.Envelope{
		Params: map[string]any{
			"big": strings.Repeat("x", 32),
		},
	}

	_, err := job.EncodeEnvelope(env, job.WithEnvelopeMaxBytes(16))
	require.Error(t, err)

	fields, ok := errors.GetValidationErrors(err)
	require.True(t, ok)
	require.Len(t, fields, 1)
	assert.Equal(t, "envelope", fields[0].Field)
}

func TestEnvelopeValidationIdempotencyKeyLength(t *testing.T) {
	env := job.Envelope{
		IdempotencyKey: strings.Repeat("a", job.MaxIdempotencyKeyLength+1),
	}

	_, err := job.EncodeEnvelope(env)
	require.Error(t, err)

	fields, ok := errors.GetValidationErrors(err)
	require.True(t, ok)
	require.Len(t, fields, 1)
	assert.Equal(t, "idempotency_key", fields[0].Field)
}

func TestEnvelopeSanitizerDoesNotMutateInputs(t *testing.T) {
	params := map[string]any{
		"keep":   "value",
		"remove": "secret",
	}

	sanitizer := func(in map[string]any) map[string]any {
		delete(in, "remove")
		in["sanitized"] = true
		return in
	}

	payload, err := job.EncodeEnvelope(job.Envelope{Params: params}, job.WithEnvelopeSanitizer(sanitizer))
	require.NoError(t, err)

	assert.Equal(t, map[string]any{
		"keep":   "value",
		"remove": "secret",
	}, params)

	decoded, err := job.DecodeEnvelope(payload, job.WithEnvelopeSanitizer(sanitizer))
	require.NoError(t, err)

	assert.NotContains(t, decoded.Params, "remove")
	assert.Equal(t, true, decoded.Params["sanitized"])
	assert.Equal(t, "value", decoded.Params["keep"])
}

func TestJSONEnvelopeCodecCustomEnvelope(t *testing.T) {
	params := map[string]any{
		"keep":   "value",
		"secret": "drop",
	}

	sanitizer := func(in map[string]any) map[string]any {
		delete(in, "secret")
		return in
	}

	codec := job.NewJSONEnvelopeCodec(job.WithEnvelopeSanitizer(sanitizer))
	env := customEnvelope{
		Actor:          map[string]any{"id": "user-1"},
		Scope:          map[string]any{"tenant": "t1"},
		Params:         params,
		IdempotencyKey: "custom-key",
	}

	payload, err := codec.Encode(env)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{
		"keep":   "value",
		"secret": "drop",
	}, params)

	var decoded customEnvelope
	require.NoError(t, codec.Decode(payload, &decoded))
	assert.NotContains(t, decoded.Params, "secret")
	assert.Equal(t, "value", decoded.Params["keep"])
	assert.Equal(t, len(payload), decoded.RawBytes)
	assert.Equal(t, "custom-key", decoded.IdempotencyKey)
}

type customEnvelope struct {
	Actor          any            `json:"actor,omitempty"`
	Scope          any            `json:"scope,omitempty"`
	Params         map[string]any `json:"params,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	RawBytes       int            `json:"-"`
}

func (e customEnvelope) EnvelopeActor() any {
	return e.Actor
}

func (e customEnvelope) EnvelopeScope() any {
	return e.Scope
}

func (e customEnvelope) EnvelopeIdempotencyKey() string {
	return e.IdempotencyKey
}

func (e customEnvelope) EnvelopeParams() map[string]any {
	return e.Params
}

func (e *customEnvelope) SetEnvelopeParams(params map[string]any) {
	if e == nil {
		return
	}
	e.Params = params
}

func (e *customEnvelope) SetEnvelopeRawContentBytes(size int) {
	if e == nil {
		return
	}
	e.RawBytes = size
}
