package job

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
)

var _ SourceProvider = &DBSourceProvider{}

type DBSourceProvider struct {
	Table       string
	DB          *sql.DB
	placeholder func(int) string
}

func NewDBSourceProvider(db *sql.DB, table string) *DBSourceProvider {
	return &DBSourceProvider{
		DB:          db,
		Table:       table,
		placeholder: defaultPostgresPlaceholder,
	}
}

func (p *DBSourceProvider) GetScript(path string) ([]byte, error) {

	path = filepath.Clean(path)

	query := fmt.Sprintf("SELECT content FROM %s WHERE path = %s LIMIT 1", p.Table, p.placeholderFor(1))
	var content []byte
	err := p.DB.QueryRow(query, path).Scan(&content)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("script not found at path %s", path)
		}
		return nil, fmt.Errorf("failed to get script %s: %w", path, err)
	}

	return content, nil
}

func (p *DBSourceProvider) ListScripts(ctx context.Context) ([]ScriptInfo, error) {
	var scripts []ScriptInfo

	query := fmt.Sprintf("SELECT path, content FROM %s", p.Table)

	rows, err := p.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query scripts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		var path string
		var content []byte

		if err := rows.Scan(&path, &content); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		scripts = append(scripts, ScriptInfo{
			ID:      filepath.Base(path),
			Path:    path,
			Content: content,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return scripts, nil
}

// WithPlaceholder overrides the SQL placeholder generator used in parameterised queries.
func (p *DBSourceProvider) WithPlaceholder(fn func(int) string) *DBSourceProvider {
	if fn == nil {
		p.placeholder = defaultPostgresPlaceholder
		return p
	}
	p.placeholder = fn
	return p
}

func (p *DBSourceProvider) placeholderFor(index int) string {
	if p.placeholder == nil {
		return defaultPostgresPlaceholder(index)
	}
	return p.placeholder(index)
}

func defaultPostgresPlaceholder(index int) string {
	return fmt.Sprintf("$%d", index)
}

// SQLQuestionPlaceholder returns the standard question-mark placeholder used by drivers like SQLite or MySQL.
func SQLQuestionPlaceholder(int) string {
	return "?"
}
