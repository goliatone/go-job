package postgres

import "fmt"

func schemaStatements(table, dlqTable, statusTable string) []string {
	return []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
id TEXT PRIMARY KEY,
payload TEXT NOT NULL,
attempts INTEGER NOT NULL DEFAULT 0,
available_at BIGINT NOT NULL,
leased_until BIGINT NOT NULL DEFAULT 0,
token TEXT,
last_error TEXT,
created_at BIGINT NOT NULL,
updated_at BIGINT NOT NULL
)`, table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_available_at_idx ON %s (available_at)`, table, table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_leased_until_idx ON %s (leased_until)`, table, table),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
id TEXT PRIMARY KEY,
payload TEXT NOT NULL,
attempts INTEGER NOT NULL,
last_error TEXT,
dead_lettered_at BIGINT NOT NULL,
created_at BIGINT NOT NULL,
updated_at BIGINT NOT NULL
)`, dlqTable),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
dispatch_id TEXT PRIMARY KEY,
state TEXT NOT NULL,
attempt INTEGER NOT NULL DEFAULT 0,
enqueued_at BIGINT NOT NULL,
updated_at BIGINT NOT NULL,
next_run_at BIGINT NOT NULL DEFAULT 0,
terminal_reason TEXT,
expires_at BIGINT NOT NULL
)`, statusTable),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s_expires_at_idx ON %s (expires_at)`, statusTable, statusTable),
	}
}

func dropStatements(table, dlqTable, statusTable string) []string {
	return []string{
		fmt.Sprintf("DROP TABLE IF EXISTS %s", table),
		fmt.Sprintf("DROP TABLE IF EXISTS %s", dlqTable),
		fmt.Sprintf("DROP TABLE IF EXISTS %s", statusTable),
	}
}
