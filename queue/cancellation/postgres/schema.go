package postgres

import "fmt"

func schemaStatements(table string) []string {
	return []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
key TEXT PRIMARY KEY,
reason TEXT,
requested_at BIGINT NOT NULL,
created_at BIGINT NOT NULL,
updated_at BIGINT NOT NULL
)`, table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_requested_at_idx ON %s (requested_at)`, table, table),
	}
}

func dropStatements(table string) []string {
	return []string{
		fmt.Sprintf("DROP TABLE IF EXISTS %s", table),
	}
}
