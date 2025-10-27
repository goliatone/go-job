# go-job

go-job is a flexible job runner and scheduler written in Go that allows you to embed configuration metadata directly into your job files. By extracting configuration options from comments across different file types (YAML, shell, JavaScript, SQL, etc.), go-job makes it easy to define job behavior alongside your scripts.

## Features

- **Multi-format Metadata Extraction:**
  Supports extracting configuration from:
  - **YAML Front Matter:** Using the standard `---` markers.
  - **Shell Scripts:** Metadata specified in comment lines using `#`.
  - **SQL Scripts:** Metadata specified using `--` comments.
  - **JavaScript:**
    - **Single-line Comments:** e.g. `// config ...`
    - **Block Comments:** e.g.
      ```js
      /** config
       * schedule: "0 12 * * *"
       * timeout: 300s
       * retries: 3
       * debug: true
       * run_once: true
       * script_type: shell
       * transaction: true
       * env:
       *  APP_NAME: "test"
       *  API_KEY: "my-secret-key"
       * metadata:
       *  key: value
       */
      ```
- **Multiple Execution Engines:**
  - **Shell Engine:** Execute shell scripts with environment variables and timeout control
  - **JavaScript Engine:** Run JavaScript code with Node.js-like environment (uses goja)
  - **SQL Engine:** Execute SQL scripts with transaction support
- **Source Providers:** Flexible system for loading script content from different sources:
  - **FileSystem Provider:** Load scripts from local directories
  - **Database Provider:** Load scripts from database tables
- **Configurable Registry:** Store and retrieve jobs with the in-memory registry
- **Runner:** Orchestrates job discovery, task creation, and registration
- **Task Scheduling:** Integration with cron-based schedulers for automated job execution
- **Robust Timeout Handling:** Configure timeouts at both the engine and job level
- **Metadata-driven Configuration:** Extract job configuration directly from script file comments
- **Customizable Logging:** Pluggable logger interface for integration with existing logging frameworks
- **Error Handling:** Configurable error handlers for task creation and execution failures
- **Extensible Architecture:** Easily add new script types or execution engines

## Installation

```bash
go get github.com/goliatone/go-job
```

## Usage

### Complete Example with Scheduler

```go
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/goliatone/go-command/cron"
    "github.com/goliatone/go-job"
)

func main() {
    // Create a context that can be cancelled
    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Set up signal handling for graceful shutdown
    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
    go func() {
        <-sigCh
        log.Println("Shutdown signal received, stopping job runner...")
        cancel()
    }()

    // Create a task creator with filesystem source provider
    taskCreator := job.NewTaskCreator(
        job.NewFileSystemSourceProvider("./scripts"),
        []job.Engine{
            job.NewShellRunner(
                job.WithShellTimeout(60*time.Second),
            ),
            job.NewJSRunner(
                job.WithJSTimeout(30*time.Second),
            ),
            job.NewSQLRunner(
                job.WithSQLDatabase("postgres", os.Getenv("DATABASE_DSN")),
                job.WithSQLTimeout(60*time.Second),
            ),
        },
    )

    // Create the job runner
    runner := job.NewRunner(
        job.WithTaskCreator(taskCreator),
    )

    // Start the job runner to discover and register tasks
    if err := runner.Start(ctx); err != nil {
        log.Fatalf("Failed to start job runner: %v", err)
    }

    // Get all discovered tasks
    tasks := runner.RegisteredTasks()
    log.Printf("Discovered %d tasks\n", len(tasks))

    // Create a scheduler
    scheduler := cron.NewScheduler()

    // Register tasks with the scheduler
    for _, task := range tasks {
        _, err := scheduler.AddHandler(task.GetHandlerConfig(), task.GetHandler())
        if err != nil {
            log.Printf("Failed to register task %s: %v\n", task.GetID(), err)
            continue
        }
        log.Printf("Registered task: %s\n", task.GetID())
    }

    // Start the scheduler
    if err := scheduler.Start(ctx); err != nil {
        log.Fatalf("Failed to start scheduler: %v", err)
    }

    // Wait for context cancellation (from signals)
    <-ctx.Done()

    // Graceful shutdown
    scheduler.Stop(ctx)
    runner.Stop(ctx)
    log.Println("Shutdown complete")
}
```

### Basic Example (Manual Execution)

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/goliatone/go-job"
)

func main() {
    // Create a task creator with filesystem source provider
    taskCreator := job.NewTaskCreator(
        job.NewFileSystemSourceProvider("./scripts"),
        []job.Engine{
            job.NewShellRunner(job.WithShellTimeout(time.Minute)),
            job.NewJSRunner(job.WithJSTimeout(time.Minute)),
        },
    )

    // Create context
    ctx := context.Background()

    // Discover and create tasks
    tasks, err := taskCreator.CreateTasks(ctx)
    if err != nil {
        fmt.Printf("Error creating tasks: %v\n", err)
        return
    }

    // Execute a specific task
    for _, task := range tasks {
        if task.GetID() == "my-script.js" {
            handler := task.GetHandler()
            if err := handler(); err != nil {
                fmt.Printf("Error executing task: %v\n", err)
            }
            break
        }
    }
}
```

### Creating a Script with Metadata

#### JavaScript Example

```javascript
/** config
 * schedule: "0 */5 * * * *"  // Run every 5 minutes
 * timeout: 30s
 * retries: 2
 * run_once: false
 * env:
 *   API_KEY: "my-secret-key"
 *   DEBUG: "true"
 */

console.log("Starting job execution");

// Access environment variables
console.log(`API Key: ${API_KEY}`);

// Make HTTP requests using fetch
fetch("https://api.example.com/data")
  .then(response => response.json())
  .then(data => {
    console.log("Received data:", data);
  })
  .catch(error => {
    console.error("Error fetching data:", error);
  });
```

#### Shell Script Example

```bash
#!/bin/bash
# config
# schedule: "0 0 * * *"  # Run daily at midnight
# timeout: 120s
# retries: 3
# env:
#   DB_HOST: localhost
#   DB_USER: admin

echo "Running backup script"
pg_dump -h "$DB_HOST" -U "$DB_USER" my_database > /backups/backup-$(date +%Y%m%d).sql
```

#### SQL Script Example

```sql
-- config
-- schedule: "0 4 * * *"
-- timeout: 60s
-- transaction: true
-- metadata:
--   driver: postgres
--   dsn: postgres://user:password@localhost/mydb

-- This script will run in a transaction
INSERT INTO audit_log (event_type, description)
VALUES ('DAILY_CLEANUP', 'Removing old records');

DELETE FROM temporary_data
WHERE created_at < NOW() - INTERVAL '30 days';
```

### Using Database Source Provider

```go
package main

import (
    "context"
    "database/sql"
    "log"
    "time"

    "github.com/goliatone/go-job"
    _ "github.com/lib/pq"
)

func main() {
    // Connect to database
    db, err := sql.Open("postgres", "postgres://user:pass@localhost/jobs")
    if err != nil {
        log.Fatalf("Failed to connect to database: %v", err)
    }
    defer db.Close()

    // Create database source provider
    // Expects a table with columns: path (string), content (bytea/text)
    dbProvider := job.NewDBSourceProvider(db, "scripts")

    // Create task creator with database provider
    taskCreator := job.NewTaskCreator(
        dbProvider,
        []job.Engine{
            job.NewShellRunner(job.WithShellTimeout(time.Minute)),
            job.NewJSRunner(job.WithJSTimeout(time.Minute)),
        },
    )

    // Discover and create tasks from database
    tasks, err := taskCreator.CreateTasks(context.Background())
    if err != nil {
        log.Fatalf("Error creating tasks: %v", err)
    }

    log.Printf("Discovered %d tasks from database\n", len(tasks))
}
```

### Executing a Job Manually with Engine

```go
package main

import (
    "context"
    "fmt"
    "os"
    "time"

    "github.com/goliatone/go-job"
)

func main() {
    // Create a shell engine
    shellEngine := job.NewShellRunner(
        job.WithShellTimeout(time.Minute),
    )

    // Read script content
    content, err := os.ReadFile("./scripts/backup.sh")
    if err != nil {
        fmt.Printf("Error reading file: %v\n", err)
        return
    }

    // Parse metadata and script content
    config, scriptContent, err := job.NewYAMLMetadataParser().Parse(content)
    if err != nil {
        fmt.Printf("Error parsing metadata: %v\n", err)
    }

    // Create execution message
    msg := &job.ExecutionMessage{
        JobID:      "backup.sh",
        ScriptPath: "./scripts/backup.sh",
        Config:     config,
        Parameters: map[string]interface{}{
            "script": scriptContent,
        },
    }

    // Execute the script
    ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
    defer cancel()

    if err := shellEngine.Execute(ctx, msg); err != nil {
        fmt.Printf("Error executing script: %v\n", err)
        return
    }

    fmt.Println("Script executed successfully")
}
```

## Configuration Options

### Common Configuration Options

| Option | Description | Default |
|--------|-------------|---------|
| `schedule` | Cron expression for scheduling | `* * * * *` |
| `timeout` | Maximum execution time | 1 minute |
| `no_timeout` | Disable execution timeout | `false` |
| `retries` | Number of retry attempts | `0` |
| `debug` | Enable debug mode | `false` |
| `run_once` | Run job only once | `false` |
| `script_type` | Override script type detection | Auto-detected |
| `env` | Environment variables for execution | `{}` |
| `metadata` | Additional metadata for engines | `{}` |

### Engine-Specific Options

#### SQL Engine

| Option | Description |
|--------|-------------|
| `transaction` | Execute SQL in a transaction |
| `driver` | SQL driver name (in metadata) |
| `dsn` | Data source name (in metadata) |

#### Shell Engine

| Option | Description |
|--------|-------------|
| `use_env` | Pass system environment variables (in metadata) |

## Advanced Features

### Custom Logger

Integrate your own logging framework by implementing the Logger interface:

```go
type Logger interface {
    Debug(format string, args ...any)
    Info(format string, args ...any)
    Warn(format string, args ...any)
    Error(format string, args ...any)
}
```

Example with a custom logger:

```go
taskCreator := job.NewTaskCreator(provider, engines).
    WithLogger(myCustomLogger)
```

### Custom Error Handling

Configure custom error handlers for task creation failures:

```go
taskCreator := job.NewTaskCreator(provider, engines).
    WithErrorHandler(func(task job.Task, err error) {
        if task != nil {
            log.Printf("Task %s failed: %v", task.GetID(), err)
        } else {
            log.Printf("Task creation failed: %v", err)
        }
    })
```

### Runner Configuration

The Runner orchestrates job discovery and task registration:

```go
runner := job.NewRunner(
    job.WithTaskCreator(taskCreator),
    job.WithRegistry(customRegistry),
    job.WithErrorHandler(customErrorHandler),
)
```

## Architecture

go-job uses a modular architecture with several key components:

- **Runner:** Orchestrates the job discovery, task creation, and registration process
- **Engines:** Execute specific script types (Shell, JavaScript, SQL)
- **Registry:** Stores and manages task definitions (in-memory implementation provided)
- **MetadataParser:** Extracts configuration from script file comments (supports YAML front matter)
- **SourceProvider:** Loads script content from various sources (filesystem or database)
- **TaskCreator:** Discovers scripts and creates task instances using appropriate engines
- **Task:** Represents a schedulable job with its configuration, handler, and execution context

## License
MIT
