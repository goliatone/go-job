package command

import "github.com/goliatone/go-command"

// QueueResolver registers go-command handlers with the queue registry.
func QueueResolver(reg *Registry) command.Resolver {
	return QueueResolverWithOptions(reg)
}

// QueueResolverWithOptions registers go-command handlers with queue registration options.
func QueueResolverWithOptions(reg *Registry, opts ...RegisterOption) command.Resolver {
	return func(cmd any, meta command.CommandMeta, _ *command.Registry) error {
		if meta.MessageType == "" {
			return nil
		}
		return RegisterCommandByTypeWithOptions(reg, cmd, meta, opts...)
	}
}
