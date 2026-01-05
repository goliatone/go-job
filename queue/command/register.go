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

// RegisterCommand registers a command with the queue registry.
func RegisterCommand[T any](reg *Registry, cmd command.Commander[T]) error {
	if reg == nil {
		return fmt.Errorf("command registry not configured")
	}
	if cmd == nil {
		return fmt.Errorf("command cannot be nil")
	}

	messageType := messageTypeFor[T]()

	entry := Entry{
		ID:          messageType,
		MessageType: messageType,
		Handler: func(ctx context.Context, params map[string]any) error {
			payload, err := decodeParams[T](params)
			if err != nil {
				return err
			}
			return cmd.Execute(ctx, payload)
		},
	}

	if provider, ok := any(cmd).(ConfigProvider); ok {
		entry.Config = provider.JobConfig()
	}

	return reg.Register(entry)
}

func messageTypeFor[T any]() string {
	msgType := reflect.TypeOf((*T)(nil)).Elem()
	var msgValue reflect.Value
	if msgType.Kind() == reflect.Ptr {
		msgValue = reflect.New(msgType.Elem())
	} else {
		msgValue = reflect.New(msgType).Elem()
	}
	return command.GetMessageType(msgValue.Interface())
}

// RegisterCommandWithRegistry registers a command with go-command registry and the queue registry.
func RegisterCommandWithRegistry[T any](reg *Registry, cmd command.Commander[T], runnerOpts ...runner.Option) (dispatcher.Subscription, error) {
	sub, err := commandregistry.RegisterCommand(cmd, runnerOpts...)
	if err != nil {
		return sub, err
	}
	return sub, RegisterCommand(reg, cmd)
}
