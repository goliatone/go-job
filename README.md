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
- **Source Providers:** Flexible system for loading script content from different sources
- **Configurable Registry:** Store and retrieve jobs with the in-memory registry
- **Task Scheduling:** Support for cron-like scheduling expressions
- **Robust Timeout Handling:** Configure timeouts at both the engine and job level
- **Metadata-driven Configuration:** Extract job configuration directly from script file comments
- **Extensible Architecture:** Easily add new script types or execution engines

## Installation

```bash
go get github.com/goliatone/go-job
```

## Usage

### Basic Example

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/goliatone/go-job"
)

func main() {
    // Create a registry to store jobs
    registry := job.NewMemoryRegistry()

    // Create execution engines
    shellEngine := job.NewShellRunner(
        job.WithShellTimeout(time.Minute),
    )

    jsEngine := job.NewJSRunner(
        job.WithJSTimeout(time.Minute),
    )

    sqlEngine := job.NewSQLRunner(
        job.WithSQLDatabase("postgres", "postgres://user:pass@localhost/db"),
    )

    // Create a source provider to discover scripts
    sourceProvider := job.NewFileSystemSourceProvider("./scripts")

    // Create a task creator with all available engines
    taskCreator := job.NewTaskCreator(
        sourceProvider,
        []job.Engine{shellEngine, jsEngine, sqlEngine},
    )

    // Create context
    ctx := context.Background()

    // Discover and create tasks
    tasks, err := taskCreator.CreateTasks(ctx)
    if err != nil {
        fmt.Printf("Error creating tasks: %v\n", err)
        return
    }

    // Register tasks
    for _, task := range tasks {
        if err := registry.Add(task); err != nil {
            fmt.Printf("Error adding task %s: %v\n", task.GetID(), err)
        }
    }

    // List all registered tasks
    allTasks := registry.List()
    fmt.Printf("Registered %d tasks\n", len(allTasks))

    // Execute a specific task by ID
    taskID := "my-task.js"
    if task, found := registry.Get(taskID); found {
        handler := task.GetHandler()
        if err := handler(); err != nil {
            fmt.Printf("Error executing task %s: %v\n", taskID, err)
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

### Executing a Job Manually

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

## Architecture

go-job uses a modular architecture with several key components:

- **Engines:** Responsible for executing specific script types
- **Registry:** Stores and manages job definitions
- **MetadataParser:** Extracts configuration from script content
- **SourceProvider:** Loads script content from various sources
- **TaskCreator:** Creates task instances from scripts
- **Task:** Represents a job with its configuration and handler

## License
MIT
