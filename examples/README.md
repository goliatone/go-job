# go-job Examples

This directory contains example scripts and a working example application demonstrating go-job functionality.

## Quick Start

The example application automatically adapts based on available configuration:

### Running Shell and JavaScript Examples (No Database Required)

```bash
cd examples
go run main.go
```

This will discover and schedule the example shell and JavaScript scripts from the `./data` directory.

### Running SQL Examples (Database Required)

To run SQL job examples, set the database connection string:

```bash
export APP_DATABASE__DSN='postgres://user:password@localhost:5432/dbname?sslmode=disable'
cd examples
go run main.go
```

The application will automatically detect the database configuration and load SQL scripts from `./data/sql`.

## Example Scripts

### Shell Scripts

Located in `./data/shell/`:
- `example.sh` - Simple shell script that runs every 2 minutes

### JavaScript Scripts

Located in `./data/js/`:
- `example.js` - JavaScript example demonstrating async/await and fetch API

### SQL Scripts

Located in `./data/sql/`:
- `example.sql` - Simple SQL query
- `message_update_thread_hierarchy.sql` - Complex example with multiple statements
- `actions_update_project_id.sql` - SQL update example
- `actions_update_workspace_id.sql` - SQL update example
- `messages_update_parent_hierarchy.sql` - SQL update example

## Script Configuration

All scripts support configuration through embedded metadata. For example:

**Shell Script:**
```bash
#!/bin/bash
# config
# schedule: "*/5 * * * *"
# timeout: 60s
# retries: 3

echo "This runs every 5 minutes"
```

**JavaScript:**
```javascript
// config
// schedule: "@every 30s"
// timeout: 30s

console.log("This runs every 30 seconds");
```

**SQL:**
```sql
-- config
-- schedule: "0 0 * * *"
-- transaction: true
-- metadata:
--   driver: postgres
--   dsn: postgres://user:pass@localhost/db

SELECT * FROM users;
```

## Configuration Options

Common configuration options supported in script metadata:

- `schedule` - Cron expression or `@every` syntax
- `timeout` - Maximum execution time (e.g., "30s", "5m")
- `retries` - Number of retry attempts on failure
- `transaction` - (SQL only) Execute in a transaction
- `metadata` - Engine-specific metadata (driver, dsn, etc.)

## Notes

- The example uses graceful shutdown with signal handling (Ctrl+C)
- Tasks are discovered automatically from the configured directory
- Scheduler logs show when tasks are registered and executed
- Failed tasks are retried according to their retry configuration
