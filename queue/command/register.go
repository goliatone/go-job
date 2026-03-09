package command

import (
	"context"
	"fmt"
	"reflect"

	"github.com/goliatone/go-command"
	"github.com/goliatone/go-command/dispatcher"
	commandregistry "github.com/goliatone/go-command/registry"
	"github.com/goliatone/go-command/runner"

	job "github.com/goliatone/go-job"
)

// ConfigProvider allows commands to specify job config.
type ConfigProvider interface {
	JobConfig() job.Config
}

type registerConfig struct {
	strictDuplicates bool
}

// RegisterOption customizes queue command registration behavior.
type RegisterOption func(*registerConfig)

// WithStrictDuplicateRegistration controls duplicate command_id behavior.
// When enabled, duplicate registrations return an explicit conflict error.
func WithStrictDuplicateRegistration(enabled bool) RegisterOption {
	return func(cfg *registerConfig) {
		cfg.strictDuplicates = enabled
	}
}

// RegisterCommand registers a command with the queue registry.
func RegisterCommand[T any](reg *Registry, cmd command.Commander[T]) error {
	return RegisterCommandWithOptions(reg, cmd)
}

// RegisterCommandWithOptions registers a command with queue registration options.
func RegisterCommandWithOptions[T any](reg *Registry, cmd command.Commander[T], opts ...RegisterOption) error {
	if reg == nil {
		return fmt.Errorf("command registry not configured")
	}
	if cmd == nil {
		return fmt.Errorf("command cannot be nil")
	}

	meta := command.MessageTypeForCommand(cmd)
	return RegisterCommandByTypeWithOptions(reg, cmd, meta, opts...)
}

// RegisterCommandByType registers a command using resolver metadata.
func RegisterCommandByType(reg *Registry, cmd any, meta command.CommandMeta) error {
	return RegisterCommandByTypeWithOptions(reg, cmd, meta)
}

// RegisterCommandByTypeWithOptions registers a command using resolver metadata and registration options.
func RegisterCommandByTypeWithOptions(reg *Registry, cmd any, meta command.CommandMeta, opts ...RegisterOption) error {
	if reg == nil {
		return fmt.Errorf("command registry not configured")
	}
	if cmd == nil {
		return fmt.Errorf("command cannot be nil")
	}

	method, methodMsgType, err := executeMethod(cmd)
	if err != nil {
		return err
	}

	msgType := methodMsgType
	if meta.MessageTypeValue != nil {
		msgType = meta.MessageTypeValue
		if !msgType.AssignableTo(methodMsgType) {
			return fmt.Errorf("command message type mismatch")
		}
	}

	messageType, err := messageTypeFromMeta(meta, msgType)
	if err != nil {
		return err
	}

	config := registerConfig{}
	for _, opt := range opts {
		if opt != nil {
			opt(&config)
		}
	}

	if _, ok := reg.Get(messageType); ok {
		if config.strictDuplicates {
			return fmt.Errorf("queued command registration conflict for %q", messageType)
		}
		return nil
	}

	entry := Entry{
		ID:          messageType,
		MessageType: messageType,
		Handler: func(ctx context.Context, params map[string]any) error {
			payload, err := decodeParamsByType(msgType, params)
			if err != nil {
				return err
			}
			return callExecute(method, ctx, payload)
		},
	}

	if provider, ok := cmd.(ConfigProvider); ok {
		entry.Config = provider.JobConfig()
	}

	return reg.Register(entry)
}

func messageTypeFromMeta(meta command.CommandMeta, msgType reflect.Type) (string, error) {
	if meta.MessageType != "" {
		return meta.MessageType, nil
	}
	if meta.MessageValue != nil {
		return command.GetMessageType(meta.MessageValue), nil
	}
	if msgType == nil {
		return "", fmt.Errorf("command message type required")
	}
	msgValue := newValue(msgType)
	return command.GetMessageType(msgValue.Interface()), nil
}

func executeMethod(cmd any) (reflect.Value, reflect.Type, error) {
	method := reflect.ValueOf(cmd).MethodByName("Execute")
	if !method.IsValid() {
		return reflect.Value{}, nil, fmt.Errorf("command must implement Execute(ctx, msg) error")
	}

	methodType := method.Type()
	if methodType.NumIn() != 2 {
		return reflect.Value{}, nil, fmt.Errorf("command Execute signature must be Execute(ctx, msg) error")
	}

	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()
	if !ctxType.AssignableTo(methodType.In(0)) {
		return reflect.Value{}, nil, fmt.Errorf("command Execute signature must accept context.Context")
	}

	errType := reflect.TypeOf((*error)(nil)).Elem()
	if methodType.NumOut() != 1 || !methodType.Out(0).Implements(errType) {
		return reflect.Value{}, nil, fmt.Errorf("command Execute signature must return error")
	}

	return method, methodType.In(1), nil
}

func callExecute(method reflect.Value, ctx context.Context, msg reflect.Value) error {
	results := method.Call([]reflect.Value{reflect.ValueOf(ctx), msg})
	if len(results) == 0 {
		return nil
	}
	if results[0].IsNil() {
		return nil
	}
	if err, ok := results[0].Interface().(error); ok {
		return err
	}
	return fmt.Errorf("command Execute returned non-error")
}

// RegisterCommandWithRegistry registers a command with go-command registry.
// Queue registration is handled by QueueResolver during registry initialization.
func RegisterCommandWithRegistry[T any](_ *Registry, cmd command.Commander[T], runnerOpts ...runner.Option) (dispatcher.Subscription, error) {
	return commandregistry.RegisterCommand(cmd, runnerOpts...)
}
