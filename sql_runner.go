package job

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/goliatone/go-errors"
)

type SQLEngine struct {
	*BaseEngine
	db             *sql.DB
	driverName     string
	dataSourceName string
	scriptBoundary string
	execCallback   func(e *SQLEngine, db *sql.DB, statement string, res sql.Result, err error) error
}

func NewSQLRunner(opts ...SQLOption) *SQLEngine {
	e := &SQLEngine{
		scriptBoundary: "--job",
		execCallback:   defaultExecuteCallback,
	}
	e.BaseEngine = NewBaseEngine(e, "sql", ".sql")

	for _, opt := range opts {
		if opt != nil {
			opt(e)
		}
	}

	return e
}

// SetTaskIDProvider overrides the ID derivation strategy for tasks parsed by the SQL engine.
func (e *SQLEngine) SetTaskIDProvider(provider TaskIDProvider) {
	if e.BaseEngine != nil {
		e.BaseEngine.SetTaskIDProvider(provider)
	}
}

func (e *SQLEngine) Execute(ctx context.Context, msg *ExecutionMessage) error {
	scriptContent, err := e.GetScriptContent(msg)
	if err != nil {
		return err
	}

	logger := e.logger
	if fl, ok := logger.(FieldsLogger); ok {
		logger = fl.WithFields(map[string]any{
			"engine":      e.EngineType,
			"script_path": msg.ScriptPath,
		})
	}

	logger.Debug("sql script starting", "script_path", msg.ScriptPath)
	start := time.Now()

	execCtx, cancel := e.GetExecutionContext(ctx)
	defer cancel()

	db, err := e.getDBConnection(execCtx, msg)
	if err != nil {
		return errors.Wrap(err, errors.CategoryExternal, "failed to establish database connection").
			WithTextCode("SQL_CONNECTION_ERROR").
			WithMetadata(map[string]any{
				"operation":   "establish_connection",
				"script_path": msg.ScriptPath,
				"config":      msg.Config,
				"message_id":  msg.JobID,
				"parameters":  msg.Parameters,
			})
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

	var execErr error
	if useTransaction {
		execErr = e.executeInTransaction(execCtx, db, scriptContent)
	} else {
		execErr = e.executeDirectly(execCtx, db, scriptContent)
	}

	duration := time.Since(start)
	if execErr != nil {
		logger.Error("sql script failed", "script_path", msg.ScriptPath, "duration", duration, "error", execErr)
		return execErr
	}

	logger.Info("sql script completed", "script_path", msg.ScriptPath, "duration", duration)
	return nil
}

func (e *SQLEngine) getDBConnection(ctx context.Context, msg *ExecutionMessage) (*sql.DB, error) {
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
		return errors.Wrap(err, errors.CategoryExternal, "failed to start transaction").
			WithTextCode("SQL_TRANSACTION_ERROR").
			WithMetadata(map[string]any{
				"operation": "begin_transaction",
			})
	}

	statements := splitSQLStatements(script, e.scriptBoundary)

	for i, stmt := range statements {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			tx.Rollback()
			return errors.Wrap(
				err,
				errors.CategoryExternal,
				fmt.Sprintf("failed to execute statement %d in transaction", i+1),
			).
				WithTextCode("SQL_EXECUTION_ERROR").
				WithMetadata(map[string]any{
					"operation":        "execute_statement",
					"statement_index":  i + 1,
					"total_statements": len(statements),
					"statement":        stmt,
				})
		}
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, errors.CategoryExternal, "failed to commit transaction").
			WithTextCode("SQL_TRANSACTION_ERROR").
			WithMetadata(map[string]any{
				"operation": "commit_transaction",
			})
	}

	return nil
}

func (e *SQLEngine) executeDirectly(ctx context.Context, db *sql.DB, script string) error {
	// Split script into individual statements
	statements := splitSQLStatements(script, e.scriptBoundary)

	for i, stmt := range statements {
		res, err := db.ExecContext(ctx, stmt)
		var callbackErr error
		if err != nil {
			callbackErr = errors.Wrap(
				err,
				errors.CategoryExternal,
				fmt.Sprintf("failed to execute statement %d", i+1),
			).
				WithTextCode("SQL_EXECUTION_ERROR").
				WithMetadata(map[string]any{
					"operation":        "execute_statement",
					"statement_index":  i + 1,
					"total_statements": len(statements),
					"statement":        stmt,
				})
		}

		e.execCallback(e, db, stmt, res, callbackErr)
	}

	return nil
}

func defaultExecuteCallback(e *SQLEngine, db *sql.DB, statement string, res sql.Result, err error) error {
	e.logger.Debug("execute statement", "sql", statement)
	if err != nil {
		e.logger.Error("error executing statement", err)
		return err
	}

	var id int64
	var rows int64
	id, _ = res.LastInsertId()
	rows, _ = res.RowsAffected()
	e.logger.Debug("SQL result", "rows", rows, "id", id)

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
