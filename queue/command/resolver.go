package command

import "github.com/goliatone/go-command"

// QueueResolver registers go-command handlers with the queue registry.
func QueueResolver(reg *Registry) command.Resolver {
	return func(cmd any, meta command.CommandMeta, _ *command.Registry) error {
		if meta.MessageType == "" {
			return nil
		}
		return RegisterCommandByType(reg, cmd, meta)
	}
}
