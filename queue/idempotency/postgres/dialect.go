package postgres

// Dialect controls SQL placeholder formatting.
type Dialect string

const (
	DialectPostgres Dialect = "postgres"
	DialectSQLite   Dialect = "sqlite"
)

type placeholderFunc func(int) string

func placeholderFor(dialect Dialect) placeholderFunc {
	switch dialect {
	case DialectSQLite:
		return func(_ int) string { return "?" }
	default:
		return func(i int) string { return "$" + itoa(i) }
	}
}

func itoa(value int) string {
	if value == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = byte('0' + value%10)
		value /= 10
	}
	return string(buf[i:])
}
