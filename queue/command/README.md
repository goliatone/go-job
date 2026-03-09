# queue/command

`queue/command` lets you run [go-command](https://github.com/goliatone/go-command) handlers as background jobs using go-job’s queue worker, without duplicating business logic. You register commands once, then decide to execute them via CLI/HTTP/cron or by enqueuing a job.

## Concepts

- **Command registry** (`queue/command.Registry`) stores handlers by command id.
- **Command id** is derived from `go-command`’s `GetMessageType` for the command’s message type.
- **ExecutionMessage.Parameters** is decoded into the command message type before `Execute`.
- **ExecutionMessage.JobID** and **ScriptPath** are set to the command id for queue routing.

## Registering Commands

You can register commands with the queue registry only, or attach the queue resolver to
`go-command` so queue registration happens during `Registry.Initialize()`.

```go
cmdReg := queuecmd.NewRegistry()

// Queue only registration
_ = queuecmd.RegisterCommand(cmdReg, exportCmd)
```

Resolver based registration (local registry):

```go
cmdReg := command.NewRegistry()
queueReg := queuecmd.NewRegistry()

_ = cmdReg.AddResolver("queue", queuecmd.QueueResolver(queueReg))
_, _ = queuecmd.RegisterCommandWithRegistry(queueReg, deliverCmd)
_ = cmdReg.Initialize()
```

`RegisterCommandWithRegistry` only registers with `go-command`, queue registration happens through the resolver during `Initialize()`.

Resolver based registration (global registry):

```go
queueReg := queuecmd.NewRegistry()
_ = commandregistry.AddResolver("queue", queuecmd.QueueResolver(queueReg))
_, _ = queuecmd.RegisterCommandWithRegistry(queueReg, deliverCmd)
_ = commandregistry.Start(ctx)
```

## Running Commands in the Worker

The worker registers tasks derived from the command registry.

- When `ids` is `nil` or empty, all registered commands are added.
- When `ids` is explicit, each id must be non-empty and pre-registered.
- Duplicate ids in explicit lists are de-duplicated before registration.

```go
w := worker.NewWorker(dequeuer)
_ = queuecmd.RegisterAll(w, cmdReg, nil)
_ = w.Start(ctx)
```

For same-machine topology bootstrap:

```go
w, _ := queuecmd.StartLocalWorker(ctx, dequeuer, cmdReg, queuecmd.LocalWorkerConfig{
	IDs: nil, // register all commands
	WorkerOptions: []worker.Option{
		worker.WithConcurrency(4),
		worker.WithIdleDelay(100 * time.Millisecond),
	},
})
defer w.Stop(context.Background())
```

## Enqueueing a Command

Use `go-command` to get the id from the message type, then enqueue with params.

```go
id := gocmd.GetMessageType(ExportMessage{})
_ = queuecmd.Enqueue(ctx, enqueuer, cmdReg, id, map[string]any{
	"export_id": "123",
})
```

Scheduled variants are also available:

```go
_ = queuecmd.EnqueueAt(ctx, enqueuer, cmdReg, id, map[string]any{
	"export_id": "123",
}, time.Now().Add(10*time.Minute))

_ = queuecmd.EnqueueAfter(ctx, enqueuer, cmdReg, id, map[string]any{
	"export_id": "123",
}, 30*time.Second)
```

## Parameter Decoding

`ExecutionMessage.Parameters` is decoded into the command’s message type using
`mapstructure` with `json` tags and weak typing enabled. This lets you pass a map
from HTTP/CLI and have it materialize as the strongly typed command input.

## Defining a Command (with CLI/Cron Auto Registration)

Commands implement `go-command`’s `Commander[T]` interface:

```go
type ExportMessage struct {
	ExportID string `json:"export_id"`
}

type ExportCommand struct{}

func (c *ExportCommand) Execute(ctx context.Context, msg ExportMessage) error {
	// business logic
	return nil
}
```

Add optional interfaces to autoregister the same command for CLI and cron:

```go
// CLICommand
func (c *ExportCommand) CLIHandler() any { return &ExportCLI{} }

func (c *ExportCommand) CLIOptions() command.CLIConfig {
	return command.CLIConfig{
		Path:        []string{"export", "run"},
		Description: "Run export on demand",
	}
}

// CronCommand
func (c *ExportCommand) CronHandler() func() error {
	return func() error { return nil }
}

func (c *ExportCommand) CronOptions() command.HandlerConfig {
	return command.HandlerConfig{
		Expression: "0 * * * *",
		MaxRetries: 3,
	}
}
```

Register the command with `go-command`, and let the queue resolver handle queue registration:

```go
cmdReg := command.NewRegistry()
queueReg := queuecmd.NewRegistry()
_ = cmdReg.AddResolver("queue", queuecmd.QueueResolver(queueReg))
_, _ = queuecmd.RegisterCommandWithRegistry(queueReg, &ExportCommand{})
_ = cmdReg.Initialize()
```

Now the same command can be triggered by CLI/cron/HTTP and enqueued as a background
job via `queuecmd.Enqueue`:

```go
id := gocmd.GetMessageType(ExportMessage{})
_ = queuecmd.Enqueue(ctx, enqueuer, cmdReg, id, map[string]any{
	"export_id": "123",
})
```

## Optional Metadata

Commands can optionally implement:

```go
type ConfigProvider interface {
	JobConfig() job.Config
}
```

When present, the config is attached to the task and used by the worker.

## API Summary

- `NewRegistry() *Registry`
- `RegisterCommand[T any](reg *Registry, cmd command.Commander[T]) error`
- `RegisterCommandByType(reg *Registry, cmd any, meta command.CommandMeta) error`
- `RegisterCommandWithRegistry[T any](reg *Registry, cmd command.Commander[T], runnerOpts ...runner.Option) (dispatcher.Subscription, error)`
- `QueueResolver(reg *Registry) command.Resolver`
- `RegisterAll(w *worker.Worker, reg *Registry, ids []string) error`
- `LocalWorkerConfig`
- `NewLocalWorker(dequeuer queue.Dequeuer, reg *Registry, cfg LocalWorkerConfig) (*worker.Worker, error)`
- `StartLocalWorker(ctx context.Context, dequeuer queue.Dequeuer, reg *Registry, cfg LocalWorkerConfig) (*worker.Worker, error)`
- `Enqueue(ctx context.Context, enq queue.Enqueuer, reg *Registry, id string, params map[string]any) error`
- `EnqueueAt(ctx context.Context, enq queue.ScheduledEnqueuer, reg *Registry, id string, params map[string]any, at time.Time) error`
- `EnqueueAfter(ctx context.Context, enq queue.ScheduledEnqueuer, reg *Registry, id string, params map[string]any, delay time.Duration) error`
