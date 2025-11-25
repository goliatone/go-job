package job

import (
	"github.com/goliatone/go-command/dispatcher"
)

// RegisterTasksWithDispatcher subscribes tasks into the go-command dispatcher.
// Returns subscriptions so callers can manage lifecycle.
func RegisterTasksWithDispatcher(tasks []Task) []dispatcher.Subscription {
	var subs []dispatcher.Subscription
	for _, task := range tasks {
		if task == nil {
			continue
		}
		cmd := NewTaskCommander(task)
		sub := dispatcher.SubscribeCommand[*ExecutionMessage](cmd)
		subs = append(subs, sub)
	}
	return subs
}
