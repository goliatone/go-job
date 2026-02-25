package postgres

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	job "github.com/goliatone/go-job"
	"github.com/goliatone/go-job/queue"
)

const (
	defaultVisibilityTimeout = 60 * time.Second
	defaultTableName         = "queue_messages"
	defaultDLQTableName      = "queue_dlq"
)

// Option configures the postgres storage adapter.
type Option func(*Storage)

// WithTableName sets the primary table name.
func WithTableName(name string) Option {
	return func(s *Storage) {
		if name != "" {
			s.table = name
		}
	}
}

// WithDLQTableName sets the DLQ table name.
func WithDLQTableName(name string) Option {
	return func(s *Storage) {
		if name != "" {
			s.dlqTable = name
		}
	}
}

// WithVisibilityTimeout sets the lease timeout.
func WithVisibilityTimeout(timeout time.Duration) Option {
	return func(s *Storage) {
		if timeout > 0 {
			s.visibilityTimeout = timeout
		}
	}
}

// WithClock overrides the time source.
func WithClock(now func() time.Time) Option {
	return func(s *Storage) {
		if now != nil {
			s.now = now
		}
	}
}

// WithIDFunc overrides ID generation.
func WithIDFunc(fn func() string) Option {
	return func(s *Storage) {
		if fn != nil {
			s.idFunc = fn
		}
	}
}

// WithTokenFunc overrides token generation.
func WithTokenFunc(fn func() string) Option {
	return func(s *Storage) {
		if fn != nil {
			s.tokenFunc = fn
		}
	}
}

// WithDialect sets the SQL placeholder style.
func WithDialect(dialect Dialect) Option {
	return func(s *Storage) {
		s.placeholder = placeholderFor(dialect)
		if dialect == DialectSQLite {
			s.useSkipLocked = false
		}
	}
}

// WithUseSkipLocked toggles SKIP LOCKED usage in dequeue.
func WithUseSkipLocked(enabled bool) Option {
	return func(s *Storage) {
		s.useSkipLocked = enabled
	}
}

// Storage implements queue.Storage backed by a SQL database.
type Storage struct {
	db                *sql.DB
	table             string
	dlqTable          string
	visibilityTimeout time.Duration
	now               func() time.Time
	idFunc            func() string
	tokenFunc         func() string
	placeholder       placeholderFunc
	useSkipLocked     bool
}

// NewStorage builds a postgres storage adapter.
func NewStorage(db *sql.DB, opts ...Option) *Storage {
	s := &Storage{
		db:                db,
		table:             defaultTableName,
		dlqTable:          defaultDLQTableName,
		visibilityTimeout: defaultVisibilityTimeout,
		now:               time.Now,
		idFunc:            func() string { return randomHex(16) },
		tokenFunc:         func() string { return randomHex(16) },
		placeholder:       placeholderFor(DialectPostgres),
		useSkipLocked:     true,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	if s.visibilityTimeout <= 0 {
		s.visibilityTimeout = defaultVisibilityTimeout
	}
	if s.placeholder == nil {
		s.placeholder = placeholderFor(DialectPostgres)
	}
	return s
}

// Migrate creates the queue tables and indexes.
func (s *Storage) Migrate(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("postgres storage not configured")
	}
	for _, stmt := range schemaStatements(s.table, s.dlqTable) {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// Cleanup drops queue tables (for tests).
func (s *Storage) Cleanup(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("postgres storage not configured")
	}
	for _, stmt := range dropStatements(s.table, s.dlqTable) {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// Enqueue stores the message for delivery.
func (s *Storage) Enqueue(ctx context.Context, msg *job.ExecutionMessage) error {
	return s.EnqueueAt(ctx, msg, s.now())
}

// EnqueueAt stores a message for delivery at the given time.
func (s *Storage) EnqueueAt(ctx context.Context, msg *job.ExecutionMessage, at time.Time) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("postgres storage not configured")
	}
	if err := queue.ValidateRequiredMessage(msg); err != nil {
		return err
	}

	payload, err := queue.EncodeExecutionMessage(msg)
	if err != nil {
		return err
	}

	now := s.now().UnixNano()
	availableAt := at.UnixNano()
	id := s.idFunc()
	p := s.placeholder
	query := fmt.Sprintf(`INSERT INTO %s
(id, payload, attempts, available_at, leased_until, token, created_at, updated_at)
VALUES (%s, %s, 0, %s, 0, '', %s, %s)`, s.table, p(1), p(2), p(3), p(4), p(5))
	_, err = s.db.ExecContext(ctx, query, id, string(payload), availableAt, now, now)
	return err
}

// EnqueueAfter stores a message for delivery after the provided delay.
func (s *Storage) EnqueueAfter(ctx context.Context, msg *job.ExecutionMessage, delay time.Duration) error {
	at := s.now()
	if delay > 0 {
		at = at.Add(delay)
	}
	return s.EnqueueAt(ctx, msg, at)
}

// Dequeue leases the next available message.
func (s *Storage) Dequeue(ctx context.Context) (*job.ExecutionMessage, queue.Receipt, error) {
	if s == nil || s.db == nil {
		return nil, queue.Receipt{}, fmt.Errorf("postgres storage not configured")
	}

	if s.useSkipLocked {
		return s.dequeueSkipLocked(ctx)
	}
	return s.dequeueCompatible(ctx)
}

// Ack removes the message when it has been processed.
func (s *Storage) Ack(ctx context.Context, receipt queue.Receipt) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("postgres storage not configured")
	}
	if receipt.ID == "" || receipt.Token == "" {
		return fmt.Errorf("receipt id and token required")
	}
	p := s.placeholder
	query := fmt.Sprintf(`DELETE FROM %s WHERE id = %s AND token = %s`, s.table, p(1), p(2))
	res, err := s.db.ExecContext(ctx, query, receipt.ID, receipt.Token)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("receipt token mismatch for %q", receipt.ID)
	}
	return nil
}

// Nack requeues, delays, or dead-letters a message.
func (s *Storage) Nack(ctx context.Context, receipt queue.Receipt, opts queue.NackOptions) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("postgres storage not configured")
	}
	if receipt.ID == "" || receipt.Token == "" {
		return fmt.Errorf("receipt id and token required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)

	payload, attempts, createdAt, err := s.loadForReceipt(ctx, tx, receipt)
	if err != nil {
		return err
	}

	now := s.now()
	if opts.DeadLetter {
		if err := s.insertDLQ(ctx, tx, receipt.ID, payload, attempts, createdAt, opts.Reason, now); err != nil {
			return err
		}
		if err := s.deleteMessage(ctx, tx, receipt); err != nil {
			return err
		}
		return tx.Commit()
	}

	if opts.Requeue {
		availableAt := now
		if opts.Delay > 0 {
			availableAt = now.Add(opts.Delay)
		}
		if err := s.updateForRetry(ctx, tx, receipt, availableAt, opts.Reason, now); err != nil {
			return err
		}
		return tx.Commit()
	}

	if err := s.deleteMessage(ctx, tx, receipt); err != nil {
		return err
	}
	return tx.Commit()
}

// ExtendLease extends a delivery lease for long-running handlers.
func (s *Storage) ExtendLease(ctx context.Context, receipt queue.Receipt, ttl time.Duration) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("postgres storage not configured")
	}
	if receipt.ID == "" || receipt.Token == "" {
		return fmt.Errorf("receipt id and token required")
	}
	if ttl <= 0 {
		ttl = s.visibilityTimeout
	}
	now := s.now()
	leaseUntil := now.Add(ttl).UnixNano()
	p := s.placeholder
	query := fmt.Sprintf(`UPDATE %s SET leased_until = %s, updated_at = %s WHERE id = %s AND token = %s`, s.table, p(1), p(2), p(3), p(4))
	res, err := s.db.ExecContext(ctx, query, leaseUntil, now.UnixNano(), receipt.ID, receipt.Token)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("receipt token mismatch for %q", receipt.ID)
	}
	return nil
}

func (s *Storage) dequeueSkipLocked(ctx context.Context) (*job.ExecutionMessage, queue.Receipt, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, queue.Receipt{}, err
	}
	defer rollback(tx)

	now := s.now()
	nowUnix := now.UnixNano()
	p := s.placeholder
	query := fmt.Sprintf(`SELECT id, payload, attempts, available_at, created_at, last_error
FROM %s
WHERE available_at <= %s AND (leased_until = 0 OR leased_until <= %s)
ORDER BY available_at ASC, created_at ASC
FOR UPDATE SKIP LOCKED
LIMIT 1`, s.table, p(1), p(2))
	row := tx.QueryRowContext(ctx, query, nowUnix, nowUnix)

	var id string
	var payload string
	var attempts int
	var availableAt int64
	var createdAt int64
	var lastError sql.NullString
	if err := row.Scan(&id, &payload, &attempts, &availableAt, &createdAt, &lastError); err != nil {
		if err == sql.ErrNoRows {
			return nil, queue.Receipt{}, tx.Commit()
		}
		return nil, queue.Receipt{}, err
	}

	msg, err := decodeMessage(payload)
	if err != nil {
		return nil, queue.Receipt{}, err
	}

	attempts++
	token := s.tokenFunc()
	leaseUntil := now.Add(s.visibilityTimeout).UnixNano()

	updateQuery := fmt.Sprintf(`UPDATE %s SET attempts = %s, token = %s, leased_until = %s, updated_at = %s WHERE id = %s`, s.table, p(1), p(2), p(3), p(4), p(5))
	if _, err := tx.ExecContext(ctx, updateQuery, attempts, token, leaseUntil, nowUnix, id); err != nil {
		return nil, queue.Receipt{}, err
	}

	if err := tx.Commit(); err != nil {
		return nil, queue.Receipt{}, err
	}

	return msg, queue.Receipt{
		ID:          id,
		Token:       token,
		Attempts:    attempts,
		LeasedAt:    now,
		AvailableAt: unixNanoTime(availableAt),
		CreatedAt:   unixNanoTime(createdAt),
		LastError:   lastError.String,
	}, nil
}

func (s *Storage) dequeueCompatible(ctx context.Context) (*job.ExecutionMessage, queue.Receipt, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, queue.Receipt{}, err
	}
	defer rollback(tx)

	now := s.now()
	nowUnix := now.UnixNano()
	p := s.placeholder
	query := fmt.Sprintf(`SELECT id, payload, attempts, available_at, created_at, last_error
FROM %s
WHERE available_at <= %s AND (leased_until = 0 OR leased_until <= %s)
ORDER BY available_at ASC, created_at ASC
LIMIT 1`, s.table, p(1), p(2))
	row := tx.QueryRowContext(ctx, query, nowUnix, nowUnix)

	var id string
	var payload string
	var attempts int
	var availableAt int64
	var createdAt int64
	var lastError sql.NullString
	if err := row.Scan(&id, &payload, &attempts, &availableAt, &createdAt, &lastError); err != nil {
		if err == sql.ErrNoRows {
			return nil, queue.Receipt{}, tx.Commit()
		}
		return nil, queue.Receipt{}, err
	}

	msg, err := decodeMessage(payload)
	if err != nil {
		return nil, queue.Receipt{}, err
	}

	attempts++
	token := s.tokenFunc()
	leaseUntil := now.Add(s.visibilityTimeout).UnixNano()
	updateQuery := fmt.Sprintf(`UPDATE %s
SET attempts = %s, token = %s, leased_until = %s, updated_at = %s
WHERE id = %s AND (leased_until = 0 OR leased_until <= %s)`, s.table, p(1), p(2), p(3), p(4), p(5), p(6))
	res, err := tx.ExecContext(ctx, updateQuery, attempts, token, leaseUntil, nowUnix, id, nowUnix)
	if err != nil {
		return nil, queue.Receipt{}, err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return nil, queue.Receipt{}, tx.Commit()
	}

	if err := tx.Commit(); err != nil {
		return nil, queue.Receipt{}, err
	}

	return msg, queue.Receipt{
		ID:          id,
		Token:       token,
		Attempts:    attempts,
		LeasedAt:    now,
		AvailableAt: unixNanoTime(availableAt),
		CreatedAt:   unixNanoTime(createdAt),
		LastError:   lastError.String,
	}, nil
}

func (s *Storage) loadForReceipt(ctx context.Context, tx *sql.Tx, receipt queue.Receipt) (string, int, int64, error) {
	p := s.placeholder
	query := fmt.Sprintf(`SELECT payload, attempts, created_at FROM %s WHERE id = %s AND token = %s`, s.table, p(1), p(2))
	row := tx.QueryRowContext(ctx, query, receipt.ID, receipt.Token)
	var payload string
	var attempts int
	var createdAt int64
	if err := row.Scan(&payload, &attempts, &createdAt); err != nil {
		if err == sql.ErrNoRows {
			return "", 0, 0, fmt.Errorf("receipt token mismatch for %q", receipt.ID)
		}
		return "", 0, 0, err
	}
	return payload, attempts, createdAt, nil
}

func (s *Storage) insertDLQ(ctx context.Context, tx *sql.Tx, id, payload string, attempts int, createdAt int64, reason string, now time.Time) error {
	nowUnix := now.UnixNano()
	p := s.placeholder
	query := fmt.Sprintf(`INSERT INTO %s
(id, payload, attempts, last_error, dead_lettered_at, created_at, updated_at)
VALUES (%s, %s, %s, %s, %s, %s, %s)`, s.dlqTable, p(1), p(2), p(3), p(4), p(5), p(6), p(7))
	_, err := tx.ExecContext(ctx, query, id, payload, attempts, reason, nowUnix, createdAt, nowUnix)
	return err
}

func (s *Storage) updateForRetry(ctx context.Context, tx *sql.Tx, receipt queue.Receipt, availableAt time.Time, reason string, now time.Time) error {
	nowUnix := now.UnixNano()
	availableUnix := availableAt.UnixNano()
	p := s.placeholder
	query := fmt.Sprintf(`UPDATE %s
SET token = '', leased_until = 0, available_at = %s, updated_at = %s, last_error = %s
WHERE id = %s AND token = %s`, s.table, p(1), p(2), p(3), p(4), p(5))
	res, err := tx.ExecContext(ctx, query, availableUnix, nowUnix, reason, receipt.ID, receipt.Token)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("receipt token mismatch for %q", receipt.ID)
	}
	return nil
}

func (s *Storage) deleteMessage(ctx context.Context, tx *sql.Tx, receipt queue.Receipt) error {
	p := s.placeholder
	query := fmt.Sprintf(`DELETE FROM %s WHERE id = %s AND token = %s`, s.table, p(1), p(2))
	res, err := tx.ExecContext(ctx, query, receipt.ID, receipt.Token)
	if err != nil {
		return err
	}
	affected, _ := res.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("receipt token mismatch for %q", receipt.ID)
	}
	return nil
}

func decodeMessage(payload string) (*job.ExecutionMessage, error) {
	return queue.DecodeExecutionMessage([]byte(payload))
}

func rollback(tx *sql.Tx) {
	if tx == nil {
		return
	}
	_ = tx.Rollback()
}

func unixNanoTime(ts int64) time.Time {
	if ts <= 0 {
		return time.Time{}
	}
	return time.Unix(0, ts).UTC()
}

func randomHex(size int) string {
	if size <= 0 {
		size = 8
	}
	buf := make([]byte, size)
	if _, err := rand.Read(buf); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 10)
	}
	return hex.EncodeToString(buf)
}
