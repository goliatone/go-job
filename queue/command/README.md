# queue/command

This package will adapt go-command commands so they can run as background jobs via go-job queue workers.
It keeps go-command as the single source of truth and builds job Tasks from the command registry.

## Goals
- Reuse go-command registry for discovery/metadata.
- Run the same command logic from CLI/HTTP/cron and from the queue worker.
- Avoid duplicating command definitions or wiring.

## Conceptual Model
- **Command registry** (go-command) owns commands and metadata.
- **Task registry** (go-job worker) is derived from the command registry.
- **ExecutionMessage.JobID** == command ID.
- **ExecutionMessage.Parameters** == command params.
- **ScriptPath** is optional and unused for non-script commands.

## API Overview

### Adapter Task
Wraps a command from the registry into a `job.Task` so the worker can execute it.

```go
// queue/command/task.go
package command

type Registry struct { /* ... */ }

type Task struct { /* ... */ }

func NewRegistry() *Registry
func NewTask(reg *Registry, id string) *Task
```

`Task.Execute` looks up the command by ID and decodes `ExecutionMessage.Parameters` into
the command's message type before calling `Execute(ctx, msg)`.

### Registration Helpers
Register commands with the queue registry (and optionally the go-command registry).

```go
// queue/command/register.go
func RegisterCommand[T any](reg *Registry, cmd command.Commander[T]) error
func RegisterCommandWithRegistry[T any](reg *Registry, cmd command.Commander[T], runnerOpts ...runner.Option) (dispatcher.Subscription, error)

// queue/command/worker.go
func RegisterAll(w *worker.Worker, reg *Registry, ids []string) error
```

### Enqueue Helper
Validate that a command exists in the registry, then enqueue a background job.

```go
// queue/command/enqueue.go
func Enqueue(ctx context.Context, enq queue.Enqueuer, reg *Registry, id string, params map[string]any) error
```

## Optional Command Metadata
If go-command commands expose metadata interfaces, we can map them to job behavior:
- Retries/backoff -> `ExecutionMessage.Config`
- Idempotency key -> `ExecutionMessage.IdempotencyKey`
- Dedup policy -> `ExecutionMessage.DedupPolicy`

These will be additive and optional; commands without metadata still work.

## Example Usage (conceptual)

```go
cmdReg := queuecmd.NewRegistry()
_, _ = queuecmd.RegisterCommandWithRegistry(cmdReg, exportCmd)
_, _ = queuecmd.RegisterCommandWithRegistry(cmdReg, deliverCmd)

worker := worker.NewWorker(dequeuer)
_ = queuecmd.RegisterAll(worker, cmdReg, nil)

id := gocmd.GetMessageType(ExportMessage{})
_ = queuecmd.Enqueue(ctx, enqueuer, cmdReg, id, map[string]any{"export_id": "123"})
```

## Notes
- go-command remains the source of truth; go-job only adapts it.
- This package is intentionally small and focused on wiring.
- Tests should validate registry lookup, Execute wiring, and enqueue validation.
