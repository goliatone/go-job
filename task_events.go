package job

import "path/filepath"

// TaskIDProvider defines the strategy used to derive a task identifier from a script path.
type TaskIDProvider func(scriptPath string) string

// DefaultTaskIDProvider preserves the existing behaviour of using the filename as the task ID.
func DefaultTaskIDProvider(scriptPath string) string {
	return filepath.Base(scriptPath)
}

// TaskEventType discriminates between different kinds of task registration events.
type TaskEventType string

const (
	// TaskEventRegistered signals that a task was successfully registered.
	TaskEventRegistered TaskEventType = "registered"
	// TaskEventRegistrationFailed signals that a task failed to register.
	TaskEventRegistrationFailed TaskEventType = "registration_failed"
)

// TaskEvent captures contextual information about task registration outcomes.
type TaskEvent struct {
	Type       TaskEventType
	TaskID     string
	ScriptPath string
	Task       Task
	Err        error
}

// TaskEventHandler consumes task registration events emitted by the runner lifecycle.
type TaskEventHandler func(TaskEvent)

// TaskIDProviderAware engines can implement this to receive the active TaskIDProvider.
type TaskIDProviderAware interface {
	SetTaskIDProvider(TaskIDProvider)
}

// TaskEventEmitter task creators can implement this to publish registration events upstream.
type TaskEventEmitter interface {
	AddTaskEventHandler(TaskEventHandler)
}
