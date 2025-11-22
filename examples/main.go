package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/goliatone/go-command/cron"
	"github.com/goliatone/go-job"
	"github.com/goliatone/go-logger/glog"

	// Make sure to import your database driver
	_ "github.com/lib/pq"
	_ "github.com/mattn/go-sqlite3"
)

type baseLoggerProvider struct {
	root *glog.BaseLogger
}

func (p baseLoggerProvider) GetLogger(name string) glog.Logger {
	return p.root.GetLogger(name)
}

func main() {
	lgr := glog.NewLogger(
		glog.WithLoggerTypePretty(), glog.WithLevel(glog.Trace),
		glog.WithName("app"),
	)

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

	// Determine which engines to enable based on environment
	var engines []job.Engine
	sourceDir := "./data"
	fsProvider := job.NewFileSystemSourceProvider(sourceDir).WithIgnoreGlobs("*.db")

	// Always enable Shell and JavaScript engines
	engines = append(engines,
		job.NewShellRunner(
			job.WithShellExtension(".sh"),
			job.WithShellTimeout(60*time.Second),
		),
		job.NewJSRunner(
			job.WithJSExtension(".js"),
			job.WithJSTimeout(30*time.Second),
		),
	)

	// Check if database DSN is provided for SQL engine; otherwise bootstrap a local SQLite DB
	dbDSN := os.Getenv("APP_DATABASE__DSN")
	dbDriver := "postgres"
	if dbDSN == "" {
		dbDriver = "sqlite3"
		dbDSN = "file:./data/example.db?_busy_timeout=5000&_journal_mode=WAL&_foreign_keys=on"
		lgr.Info("No database configured, using local SQLite demo at ./data/example.db")
		if err := ensureSQLiteDemo(ctx, lgr, dbDSN); err != nil {
			lgr.Fatal("Failed to prepare SQLite demo database", "error", err)
		}
	} else {
		lgr.Info("Database connection configured, loading SQL jobs from ./data/sql")
		lgr.Info("To use the built-in SQLite demo instead, unset APP_DATABASE__DSN")
	}

	engines = append(engines, job.NewSQLRunner(
		job.WithSQLExtension(".sql"),
		job.WithSQLTimeout(60*time.Second),
		job.WithSQLDatabase(dbDriver, dbDSN),
		job.WithSQLLogger(job.GoLogger(lgr.GetLogger("job:sql"))),
	))

	fsCreator := job.NewTaskCreator(
		fsProvider,
		engines,
	)

	// Create the job runner
	runner := job.NewRunner(
		job.WithLoggerProvider(job.GoLoggerProvider(baseLoggerProvider{root: lgr})),
		job.WithTaskCreator(fsCreator),
	)

	// Start the job runner
	lgr.Debug("===> Starting job discovery...")
	if err := runner.Start(ctx); err != nil {
		lgr.Fatal("Failed to start job runner", "error", err)
	}

	// Get all discovered tasks
	tasks := runner.RegisteredTasks()
	lgr.Debug("Discovered tasks", "total", len(tasks))
	for _, task := range tasks {
		schedule := job.TaskScheduleFromTask(task)
		lgr.Info("Task schedule",
			"task_id", task.GetID(),
			"expression", schedule.Expression,
			"run_once", schedule.RunOnce,
			"max_retries", schedule.MaxRetries,
			"timeout", schedule.Timeout,
		)
	}

	// Create a scheduler
	scheduler := cron.NewScheduler(
		cron.WithErrorHandler(func(err error) {
			lgr.Error("cron scheduler error", "error", err)
		}),
		cron.WithLogLevel(cron.LogLevelDebug),
		cron.WithLogger(lgr.GetLogger("cron")),
	)

	// Register tasks with the scheduler
	for _, task := range tasks {
		// id, err := cron.AddCommand(scheduler, task.GetHandlerConfig(), task.GetHandler())
		id, err := scheduler.AddHandler(task.GetHandlerConfig().ToCommandConfig(), task.GetHandler())
		if err != nil {
			lgr.Error("Failed to register task", "task_id", task.GetID(), "error", err)
			continue
		}
		lgr.Debug("Registered task with scheduler", "task_id", task.GetID(), "entry_id", id)
	}

	// dispatcher.SubscribeCommand(job.NewJSRunner(
	// 	job.WithJSExtension(".js"),
	// 	job.WithJSTimeout(30_000*time.Second),
	// ))
	// dispatcher.Dispatch(context.Background(), &job.ExecutionMessage{
	// 	JobID:      "job-01",
	// 	ScriptPath: "./data/js/example.js",
	// 	Config: job.Config{
	// 		Retries: 1,
	// 	},
	// })

	// dispatcher.SubscribeCommand(job.NewShellRunner(
	// 	job.WithShellExtension(".sh"),
	// 	job.WithShellTimeout(60*time.Second),
	// 	job.WithShellFS(os.DirFS(".")),
	// ))
	// dispatcher.Dispatch(context.Background(), &job.ExecutionMessage{
	// 	JobID:      "job-01",
	// 	ScriptPath: "./data/shell/example.sh",
	// 	Config: job.Config{
	// 		Retries: 1,
	// 	},
	// })

	// Start the scheduler
	lgr.Debug("Starting scheduler...")
	if err := scheduler.Start(ctx); err != nil {
		lgr.Fatal("Failed to start scheduler", "error", err)
	}

	// Wait for context cancellation (from signals)
	<-ctx.Done()

	// Stop the scheduler
	lgr.Debug("Stopping scheduler...")
	if err := scheduler.Stop(ctx); err != nil {
		lgr.Fatal("Error stopping scheduler", "error", err)
	}

	// Stop the job runner
	lgr.Debug("Stopping job runner...")
	if err := runner.Stop(ctx); err != nil {
		lgr.Fatal("Error stopping job runner", "error", err)
	}

	lgr.Debug("Shutdown complete")
}

func ensureSQLiteDemo(ctx context.Context, logger glog.Logger, dsn string) error {
	dbPath := "data/example.db"
	if dir := filepath.Dir(dbPath); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return fmt.Errorf("create sqlite demo dir: %w", err)
		}
	}

	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return fmt.Errorf("open sqlite demo: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping sqlite demo: %w", err)
	}

	schema := []string{
		`CREATE TABLE IF NOT EXISTS messages (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			message_id TEXT,
			parent_id INTEGER,
			project_id INTEGER,
			workspace_id INTEGER,
			in_reply_to TEXT,
			subject TEXT,
			date TEXT
		);`,
		`CREATE TABLE IF NOT EXISTS actions (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			message_id INTEGER,
			project_id INTEGER,
			workspace_id INTEGER
		);`,
	}

	for _, stmt := range schema {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("apply schema: %w", err)
		}
	}

	var msgCount int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages;`).Scan(&msgCount); err != nil {
		return fmt.Errorf("count messages: %w", err)
	}

	if msgCount == 0 {
		seed := []string{
			`INSERT INTO messages (message_id, project_id, workspace_id, in_reply_to, subject, date) VALUES
				('msg-1', 1, 1, NULL, 'Hello', '2025-01-01T00:00:00Z'),
				('msg-2', NULL, 1, 'msg-1', 'Re: Hello', '2025-01-01T01:00:00Z'),
				('msg-3', NULL, NULL, 'msg-2', 'Re: Hello again', '2025-01-01T02:00:00Z');`,
			`INSERT INTO actions (message_id, project_id, workspace_id) VALUES
				(2, NULL, NULL),
				(3, NULL, NULL);`,
		}
		for _, stmt := range seed {
			if _, err := db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("seed sqlite demo: %w", err)
			}
		}
		logger.Info("Seeded SQLite demo database with sample data", "path", dsn)
	}

	return nil
}
