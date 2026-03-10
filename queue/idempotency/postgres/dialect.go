package postgres

import "github.com/goliatone/go-job/queue/internal/sqlutil"

// Dialect controls SQL placeholder formatting.
type Dialect = sqlutil.Dialect

const (
	DialectPostgres = sqlutil.DialectPostgres
	DialectSQLite   = sqlutil.DialectSQLite
)

type placeholderFunc = sqlutil.PlaceholderFunc

func placeholderFor(dialect Dialect) placeholderFunc {
	return sqlutil.PlaceholderFor(dialect)
}
