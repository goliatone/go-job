package postgres

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/goliatone/go-job/queue/idempotency"
	"github.com/goliatone/go-job/queue/internal/sqlutil"
)

const (
	defaultTableName = "idempotency_records"
)

// Option configures the postgres idempotency store.
type Option func(*Store)

// WithTableName sets the table name.
func WithTableName(name string) Option {
	return func(s *Store) {
		if name != "" {
			s.table = name
		}
	}
}

// WithClock overrides the time source.
func WithClock(now func() time.Time) Option {
	return func(s *Store) {
		if now != nil {
			s.now = now
		}
	}
}

// WithDialect sets the SQL placeholder style.
func WithDialect(dialect Dialect) Option {
	return func(s *Store) {
		s.placeholder = placeholderFor(dialect)
		if dialect == DialectSQLite {
			s.useForUpdate = false
		}
	}
}

// WithUseForUpdate toggles row locking in transactions.
func WithUseForUpdate(enabled bool) Option {
	return func(s *Store) {
		s.useForUpdate = enabled
	}
}

// Store implements idempotency.Store backed by SQL.
type Store struct {
	db           *sql.DB
	table        string
	now          func() time.Time
	placeholder  placeholderFunc
	useForUpdate bool
}

// NewStore builds a postgres idempotency store.
func NewStore(db *sql.DB, opts ...Option) *Store {
	store := &Store{
		db:           db,
		table:        defaultTableName,
		now:          time.Now,
		placeholder:  placeholderFor(DialectPostgres),
		useForUpdate: true,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(store)
		}
	}
	if store.placeholder == nil {
		store.placeholder = placeholderFor(DialectPostgres)
	}
	return store
}

// Migrate creates the idempotency table.
func (s *Store) Migrate(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("postgres idempotency store not configured")
	}
	if err := s.validateIdentifier(); err != nil {
		return err
	}
	for _, stmt := range schemaStatements(s.table) {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// Cleanup drops the idempotency table (for tests).
func (s *Store) Cleanup(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("postgres idempotency store not configured")
	}
	if err := s.validateIdentifier(); err != nil {
		return err
	}
	for _, stmt := range dropStatements(s.table) {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// Acquire creates or returns an existing record.
func (s *Store) Acquire(ctx context.Context, key string, ttl time.Duration) (idempotency.Record, bool, error) {
	if s == nil || s.db == nil {
		return idempotency.Record{}, false, fmt.Errorf("postgres idempotency store not configured")
	}
	if err := s.validateIdentifier(); err != nil {
		return idempotency.Record{}, false, err
	}
	if key == "" {
		return idempotency.Record{}, false, fmt.Errorf("idempotency key required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return idempotency.Record{}, false, err
	}
	defer sqlutil.Rollback(tx)

	now := s.now().UTC()
	next := newRecord(key, nil, now, ttl)

	created, err := s.insertRecordIfMissing(ctx, tx, next)
	if err != nil {
		return idempotency.Record{}, false, err
	}
	if created {
		if err := tx.Commit(); err != nil {
			return idempotency.Record{}, false, err
		}
		return next, true, nil
	}

	record, found, err := s.selectRecord(ctx, tx, key)
	if err != nil {
		return idempotency.Record{}, false, err
	}
	if !found {
		created, err := s.insertRecordIfMissing(ctx, tx, next)
		if err != nil {
			return idempotency.Record{}, false, err
		}
		if created {
			if err := tx.Commit(); err != nil {
				return idempotency.Record{}, false, err
			}
			return next, true, nil
		}
		record, found, err = s.selectRecord(ctx, tx, key)
		if err != nil {
			return idempotency.Record{}, false, err
		}
		if !found {
			return idempotency.Record{}, false, idempotency.ErrNotFound
		}
	}

	if idempotency.IsExpired(record, now) {
		if err := s.updateRecord(ctx, tx, next); err != nil {
			return idempotency.Record{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return idempotency.Record{}, false, err
		}
		return next, true, nil
	}

	if err := tx.Commit(); err != nil {
		return idempotency.Record{}, false, err
	}
	return record, false, nil
}

// Get retrieves a record.
func (s *Store) Get(ctx context.Context, key string) (idempotency.Record, bool, error) {
	if s == nil || s.db == nil {
		return idempotency.Record{}, false, fmt.Errorf("postgres idempotency store not configured")
	}
	if err := s.validateIdentifier(); err != nil {
		return idempotency.Record{}, false, err
	}
	if key == "" {
		return idempotency.Record{}, false, fmt.Errorf("idempotency key required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return idempotency.Record{}, false, err
	}
	defer sqlutil.Rollback(tx)

	record, found, err := s.selectRecord(ctx, tx, key)
	if err != nil {
		return idempotency.Record{}, false, err
	}
	if !found {
		return idempotency.Record{}, false, tx.Commit()
	}

	now := s.now()
	if idempotency.IsExpired(record, now) {
		if err := s.deleteRecord(ctx, tx, key); err != nil {
			return idempotency.Record{}, false, err
		}
		if err := tx.Commit(); err != nil {
			return idempotency.Record{}, false, err
		}
		return idempotency.Record{}, false, nil
	}

	if err := tx.Commit(); err != nil {
		return idempotency.Record{}, false, err
	}
	return record, true, nil
}

// Update applies a partial update.
func (s *Store) Update(ctx context.Context, key string, update idempotency.Update) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("postgres idempotency store not configured")
	}
	if err := s.validateIdentifier(); err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("idempotency key required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer sqlutil.Rollback(tx)

	record, found, err := s.selectRecord(ctx, tx, key)
	if err != nil {
		return err
	}
	if !found {
		return idempotency.ErrNotFound
	}

	now := s.now()
	if update.Status != nil {
		record.Status = *update.Status
	}
	if update.Payload != nil {
		record.Payload = idempotency.CloneBytes(*update.Payload)
	}
	if update.ExpiresAt != nil {
		record.ExpiresAt = update.ExpiresAt.UTC()
	}
	record.UpdatedAt = now

	if idempotency.IsExpired(record, now) {
		if err := s.deleteRecord(ctx, tx, key); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
		return idempotency.ErrNotFound
	}

	if err := s.updateRecord(ctx, tx, record); err != nil {
		return err
	}
	return tx.Commit()
}

// Delete removes a record.
func (s *Store) Delete(ctx context.Context, key string) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("postgres idempotency store not configured")
	}
	if err := s.validateIdentifier(); err != nil {
		return err
	}
	if key == "" {
		return fmt.Errorf("idempotency key required")
	}
	_, err := s.db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE key = %s`, s.table, s.placeholder(1)), key)
	return err
}

func (s *Store) selectRecord(ctx context.Context, tx *sql.Tx, key string) (idempotency.Record, bool, error) {
	lock := ""
	if s.useForUpdate {
		lock = " FOR UPDATE"
	}
	p := s.placeholder
	query := fmt.Sprintf(`SELECT status, payload, expires_at, created_at, updated_at FROM %s WHERE key = %s%s`, s.table, p(1), lock)
	row := tx.QueryRowContext(ctx, query, key)

	var status string
	var payload string
	var expiresAt int64
	var createdAt int64
	var updatedAt int64
	if err := row.Scan(&status, &payload, &expiresAt, &createdAt, &updatedAt); err != nil {
		if err == sql.ErrNoRows {
			return idempotency.Record{}, false, nil
		}
		return idempotency.Record{}, false, err
	}

	record := idempotency.Record{
		Key:       key,
		Status:    idempotency.DefaultStatus(idempotency.Status(status)),
		Payload:   decodePayload(payload),
		CreatedAt: time.Unix(0, createdAt).UTC(),
		UpdatedAt: time.Unix(0, updatedAt).UTC(),
	}
	if expiresAt != 0 {
		record.ExpiresAt = time.Unix(0, expiresAt).UTC()
	}
	return record, true, nil
}

func (s *Store) insertRecord(ctx context.Context, tx *sql.Tx, record idempotency.Record) error {
	p := s.placeholder
	query := fmt.Sprintf(`INSERT INTO %s (key, status, payload, expires_at, created_at, updated_at)
VALUES (%s, %s, %s, %s, %s, %s)`, s.table, p(1), p(2), p(3), p(4), p(5), p(6))
	_, err := tx.ExecContext(ctx, query, record.Key, string(idempotency.DefaultStatus(record.Status)), encodePayload(record.Payload), expiresAtValue(record), record.CreatedAt.UTC().UnixNano(), record.UpdatedAt.UTC().UnixNano())
	return err
}

func (s *Store) insertRecordIfMissing(ctx context.Context, tx *sql.Tx, record idempotency.Record) (bool, error) {
	p := s.placeholder
	query := fmt.Sprintf(`INSERT INTO %s (key, status, payload, expires_at, created_at, updated_at)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (key) DO NOTHING`, s.table, p(1), p(2), p(3), p(4), p(5), p(6))
	res, err := tx.ExecContext(ctx, query,
		record.Key,
		string(idempotency.DefaultStatus(record.Status)),
		encodePayload(record.Payload),
		expiresAtValue(record),
		record.CreatedAt.UTC().UnixNano(),
		record.UpdatedAt.UTC().UnixNano(),
	)
	if err != nil {
		return false, err
	}
	affected, _ := res.RowsAffected()
	return affected > 0, nil
}

func (s *Store) updateRecord(ctx context.Context, tx *sql.Tx, record idempotency.Record) error {
	p := s.placeholder
	query := fmt.Sprintf(`UPDATE %s SET status = %s, payload = %s, expires_at = %s, updated_at = %s WHERE key = %s`, s.table, p(1), p(2), p(3), p(4), p(5))
	res, err := tx.ExecContext(ctx, query, string(idempotency.DefaultStatus(record.Status)), encodePayload(record.Payload), expiresAtValue(record), record.UpdatedAt.UTC().UnixNano(), record.Key)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return idempotency.ErrNotFound
	}
	return nil
}

func (s *Store) deleteRecord(ctx context.Context, tx *sql.Tx, key string) error {
	p := s.placeholder
	_, err := tx.ExecContext(ctx, fmt.Sprintf(`DELETE FROM %s WHERE key = %s`, s.table, p(1)), key)
	return err
}

func newRecord(key string, payload []byte, now time.Time, ttl time.Duration) idempotency.Record {
	record := idempotency.Record{
		Key:       key,
		Status:    idempotency.StatusPending,
		Payload:   idempotency.CloneBytes(payload),
		CreatedAt: now,
		UpdatedAt: now,
	}
	if ttl > 0 {
		record.ExpiresAt = now.Add(ttl).UTC()
	}
	return record
}

func expiresAtValue(record idempotency.Record) int64 {
	if record.ExpiresAt.IsZero() {
		return 0
	}
	return record.ExpiresAt.UTC().UnixNano()
}

func encodePayload(payload []byte) string {
	if len(payload) == 0 {
		return ""
	}
	return base64.StdEncoding.EncodeToString(payload)
}

func decodePayload(payload string) []byte {
	if payload == "" {
		return nil
	}
	decoded, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return nil
	}
	return decoded
}

func (s *Store) validateIdentifier() error {
	return sqlutil.ValidateIdentifier("idempotency table", s.table)
}
