package job

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/goliatone/go-command"
)

type SQLEngine struct {
	*BaseEngine
	db             *sql.DB
	driverName     string
	dataSourceName string
	scriptBoundary string
}

func NewSQLEngine(opts ...SQLOption) *SQLEngine {
	e := &SQLEngine{
		scriptBoundary: "--job",
	}
	e.BaseEngine = NewBaseEngine(e, "sql", ".sql")

	for _, opt := range opts {
		if opt != nil {
			opt(e)
		}
	}

	return e
}

func (e *SQLEngine) Execute(ctx context.Context, msg ExecutionMessage) error {
	scriptContent, err := e.GetScriptContent(msg)
	if err != nil {
		return err
	}

	execCtx, cancel := e.GetExecutionContext(ctx)
	defer cancel()

	db, err := e.getDBConnection(execCtx, msg)
	if err != nil {
		return command.WrapError("SQLEngineError", "failed to establish database connection", err)
	}

	if e.db == nil {
		defer db.Close()
	}

	useTransaction := true
	if val, ok := msg.Config.Metadata["transaction"].(bool); ok {
		useTransaction = val
	} else if !msg.Config.Transaction {
		useTransaction = false
	}

	if useTransaction {
		return e.executeInTransaction(execCtx, db, scriptContent)
	}

	return e.executeDirectly(execCtx, db, scriptContent)
}

func (e *SQLEngine) getDBConnection(ctx context.Context, msg ExecutionMessage) (*sql.DB, error) {
	if e.db != nil {
		return e.db, nil
	}

	var driverName, dataSourceName string

	if driver, ok := msg.Config.Metadata["driver"].(string); ok {
		driverName = driver
	} else {
		driverName = e.driverName
	}

	if dsn, ok := msg.Config.Metadata["dsn"].(string); ok {
		dataSourceName = dsn
	} else {
		dataSourceName = e.dataSourceName
	}

	if driverName == "" || dataSourceName == "" {
		return nil, fmt.Errorf("database connection details not provided")
	}

	db, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

func (e *SQLEngine) executeInTransaction(ctx context.Context, db *sql.DB, script string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return command.WrapError("SQLTransactionError", "failed to start transaction", err)
	}

	statements := splitSQLStatements(script, e.scriptBoundary)

	for i, stmt := range statements {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			tx.Rollback()
			return command.WrapError(
				"SQLExecutionError",
				fmt.Sprintf("failed to execute statement %d", i+1),
				err,
			)
		}
	}

	if err := tx.Commit(); err != nil {
		return command.WrapError("SQLTransactionError", "failed to commit transaction", err)
	}

	return nil
}

func (e *SQLEngine) executeDirectly(ctx context.Context, db *sql.DB, script string) error {
	// Split script into individual statements
	statements := splitSQLStatements(script, e.scriptBoundary)

	for i, stmt := range statements {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return command.WrapError(
				"SQLExecutionError",
				fmt.Sprintf("failed to execute statement %d", i+1),
				err,
			)
		}
	}

	return nil
}

func splitSQLStatements(script, boundary string) []string {
	statements := make([]string, 0)

	if boundary != "" {
		for _, stmt := range strings.Split(script, boundary) {
			trimmed := strings.TrimSpace(stmt)
			if trimmed != "" {
				statements = append(statements, trimmed)
			}
		}
		return statements
	}

	//by default we split by semicolons
	//NOTE: This does not handle cases like PL/SQL blocks or quoted simicolos
	for _, stmt := range strings.Split(script, ";") {
		trimmed := strings.TrimSpace(stmt)
		if trimmed != "" {
			statements = append(statements, trimmed+";")
		}
	}

	return statements
}
