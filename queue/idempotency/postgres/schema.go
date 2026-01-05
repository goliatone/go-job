package postgres

import "fmt"

func schemaStatements(table string) []string {
	return []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
key TEXT PRIMARY KEY,
status TEXT NOT NULL,
payload TEXT,
expires_at BIGINT NOT NULL DEFAULT 0,
created_at BIGINT NOT NULL,
updated_at BIGINT NOT NULL
)`, table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_expires_at_idx ON %s (expires_at)`, table, table),
	}
}

func dropStatements(table string) []string {
	return []string{
		fmt.Sprintf("DROP TABLE IF EXISTS %s", table),
	}
}
