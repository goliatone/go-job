package job

import (
	"context"
	"fmt"

	"github.com/go-viper/mapstructure/v2"
)

// ActorAuthenticator abstracts actor context extraction/injection so go-auth is optional.
type ActorAuthenticator interface {
	ActorFromContext(ctx context.Context) (any, bool)
	WithActorContext(ctx context.Context, actor any) context.Context
}

// GoAuthAdapter bridges an ActorAuthenticator into Envelope handling.
// MapAuthActor converts authenticator-specific actor contexts into our Actor/Scope.
// BuildAuthContext maps Envelope actor/scope back to the authenticator context shape.
type GoAuthAdapter struct {
	Sanitizer        EnvelopeSanitizer
	Authenticator    ActorAuthenticator
	MapAuthActor     func(src any) (*Actor, Scope, error)
	BuildAuthContext func(env Envelope) any
}

// AttachActor fills the envelope with actor/scope metadata from the authenticator context when missing.
func (a GoAuthAdapter) AttachActor(ctx context.Context, env Envelope) Envelope {
	if a.Authenticator == nil {
		env.Params = sanitizeParams(env.Params, a.Sanitizer)
		return env
	}

	src, ok := a.Authenticator.ActorFromContext(ctx)
	if ok && src != nil && env.Actor == nil {
		if mapped, scope, err := a.mapAuthActor(src); err == nil {
			env.Actor = mapped
			if env.Scope.isEmpty() {
				env.Scope = scope
			}
		}
	}

	env.Params = sanitizeParams(env.Params, a.Sanitizer)
	return env
}

// InjectActor writes the envelope actor/scope metadata back into the authenticator context for downstream consumers.
func (a GoAuthAdapter) InjectActor(ctx context.Context, env Envelope) context.Context {
	if a.Authenticator == nil {
		return ctx
	}
	if env.Actor == nil && env.Scope.isEmpty() {
		return ctx
	}

	var payload any
	if a.BuildAuthContext != nil {
		payload = a.BuildAuthContext(env)
	} else {
		payload = mapAuthContextPayload(env)
	}

	if payload == nil {
		return ctx
	}
	return a.Authenticator.WithActorContext(ctx, payload)
}

func (a GoAuthAdapter) mapAuthActor(src any) (*Actor, Scope, error) {
	if a.MapAuthActor != nil {
		return a.MapAuthActor(src)
	}
	return defaultMapAuthActor(src)
}

func defaultMapAuthActor(src any) (*Actor, Scope, error) {
	if src == nil {
		return nil, Scope{}, nil
	}

	var target struct {
		ActorID        string            `mapstructure:"actor_id"`
		Subject        string            `mapstructure:"subject"`
		Role           string            `mapstructure:"role"`
		ResourceRoles  map[string]string `mapstructure:"resource_roles"`
		Metadata       map[string]any    `mapstructure:"metadata"`
		ImpersonatorID string            `mapstructure:"impersonator_id"`
		IsImpersonated bool              `mapstructure:"is_impersonated"`
		TenantID       string            `mapstructure:"tenant_id"`
		OrganizationID string            `mapstructure:"organization_id"`
		ScopeLabels    map[string]string `mapstructure:"labels"`
	}

	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		WeaklyTypedInput: true,
		TagName:          "mapstructure",
		Result:           &target,
	})
	if err != nil {
		return nil, Scope{}, fmt.Errorf("build decoder: %w", err)
	}
	if err := decoder.Decode(src); err != nil {
		return nil, Scope{}, fmt.Errorf("map auth actor: %w", err)
	}

	scope := Scope{
		TenantID:       target.TenantID,
		OrganizationID: target.OrganizationID,
		Labels:         copyStringMap(target.ScopeLabels),
	}
	actor := &Actor{
		ID:             target.ActorID,
		Subject:        target.Subject,
		Role:           target.Role,
		ResourceRoles:  copyStringMap(target.ResourceRoles),
		Metadata:       copyAnyMap(target.Metadata),
		ImpersonatorID: target.ImpersonatorID,
		IsImpersonated: target.IsImpersonated,
	}
	return actor, scope, nil
}

func mapAuthContextPayload(env Envelope) any {
	if env.Actor == nil && env.Scope.isEmpty() {
		return nil
	}

	payload := map[string]any{
		"tenant_id":       env.Scope.TenantID,
		"organization_id": env.Scope.OrganizationID,
		"actor_id":        "",
		"subject":         "",
		"role":            "",
		"resource_roles":  map[string]string{},
		"metadata":        map[string]any{},
		"impersonator_id": "",
		"is_impersonated": false,
	}

	if env.Actor != nil {
		payload["actor_id"] = env.Actor.ID
		payload["subject"] = env.Actor.Subject
		payload["role"] = env.Actor.Role
		if len(env.Actor.ResourceRoles) > 0 {
			payload["resource_roles"] = copyStringMap(env.Actor.ResourceRoles)
		}
		if len(env.Actor.Metadata) > 0 {
			payload["metadata"] = copyAnyMap(env.Actor.Metadata)
		}
		payload["impersonator_id"] = env.Actor.ImpersonatorID
		payload["is_impersonated"] = env.Actor.IsImpersonated
	}

	if len(env.Scope.Labels) > 0 {
		payload["labels"] = copyStringMap(env.Scope.Labels)
	}

	return payload
}
