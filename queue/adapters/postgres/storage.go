package postgres

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	job "github.com/goliatone/go-job"
	"github.com/goliatone/go-job/queue"
	"github.com/goliatone/go-job/queue/internal/sqlutil"
)

const (
	defaultVisibilityTimeout = 60 * time.Second
	defaultTableName         = "queue_messages"
	defaultDLQTableName      = "queue_dlq"
	defaultStatusTableName   = "queue_dispatch_status"
	defaultStatusTTL         = 7 * 24 * time.Hour
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

// WithStatusTableName sets the dispatch status table name.
func WithStatusTableName(name string) Option {
	return func(s *Storage) {
		if name != "" {
			s.statusTable = name
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

// WithStatusTTL sets dispatch status retention TTL.
func WithStatusTTL(ttl time.Duration) Option {
	return func(s *Storage) {
		if ttl > 0 {
			s.statusTTL = ttl
		}
	}
}

// Storage implements queue.Storage backed by a SQL database.
type Storage struct {
	db                *sql.DB
	table             string
	dlqTable          string
	statusTable       string
	visibilityTimeout time.Duration
	statusTTL         time.Duration
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
		statusTable:       defaultStatusTableName,
		visibilityTimeout: defaultVisibilityTimeout,
		statusTTL:         defaultStatusTTL,
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
	if s.statusTTL <= 0 {
		s.statusTTL = defaultStatusTTL
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
	if err := s.validateIdentifiers(); err != nil {
		return err
	}
	for _, stmt := range schemaStatements(s.table, s.dlqTable, s.statusTable) {
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
	if err := s.validateIdentifiers(); err != nil {
		return err
	}
	for _, stmt := range dropStatements(s.table, s.dlqTable, s.statusTable) {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// Enqueue stores the message for delivery.
func (s *Storage) Enqueue(ctx context.Context, msg *job.ExecutionMessage) (queue.EnqueueReceipt, error) {
	return s.EnqueueAt(ctx, msg, s.now())
}

// EnqueueAt stores a message for delivery at the given time.
func (s *Storage) EnqueueAt(ctx context.Context, msg *job.ExecutionMessage, at time.Time) (queue.EnqueueReceipt, error) {
	if s == nil || s.db == nil {
		return queue.EnqueueReceipt{}, fmt.Errorf("postgres storage not configured")
	}
	if err := s.validateIdentifiers(); err != nil {
		return queue.EnqueueReceipt{}, err
	}
	if err := queue.ValidateRequiredMessage(msg); err != nil {
		return queue.EnqueueReceipt{}, err
	}

	payload, err := queue.EncodeExecutionMessage(msg)
	if err != nil {
		return queue.EnqueueReceipt{}, err
	}

	now := s.now().UTC()
	nowUnix := now.UnixNano()
	availableAt := at.UnixNano()
	id := s.idFunc()
	p := s.placeholder
	query := fmt.Sprintf(`INSERT INTO %s
(id, payload, attempts, available_at, leased_until, token, created_at, updated_at)
VALUES (%s, %s, 0, %s, 0, '', %s, %s)`, s.table, p(1), p(2), p(3), p(4), p(5))
	_, err = s.db.ExecContext(ctx, query, id, string(payload), availableAt, nowUnix, nowUnix)
	if err != nil {
		return queue.EnqueueReceipt{}, err
	}

	enqueuedAt := unixNanoTime(nowUnix)
	state := queue.DispatchStateAccepted
	nextRunAt := unixNanoTime(availableAt)
	if !nextRunAt.After(now) {
		nextRunAt = time.Time{}
	}
	if err := s.upsertStatus(ctx, s.db, statusRecord{
		DispatchID:     id,
		State:          state,
		Attempt:        0,
		EnqueuedAt:     enqueuedAt,
		UpdatedAt:      now,
		NextRunAt:      nextRunAt,
		TerminalReason: "",
	}); err != nil {
		return queue.EnqueueReceipt{}, err
	}

	return queue.EnqueueReceipt{
		DispatchID: id,
		EnqueuedAt: enqueuedAt,
	}, nil
}

// EnqueueAfter stores a message for delivery after the provided delay.
func (s *Storage) EnqueueAfter(ctx context.Context, msg *job.ExecutionMessage, delay time.Duration) (queue.EnqueueReceipt, error) {
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
	if err := s.validateIdentifiers(); err != nil {
		return nil, queue.Receipt{}, err
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
	if err := s.validateIdentifiers(); err != nil {
		return err
	}
	if receipt.ID == "" || receipt.Token == "" {
		return fmt.Errorf("receipt id and token required")
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer rollback(tx)

	_, attempts, createdAt, err := s.loadForReceipt(ctx, tx, receipt)
	if err != nil {
		return err
	}
	if err := s.deleteMessage(ctx, tx, receipt); err != nil {
		return err
	}

	now := s.now().UTC()
	enqueuedAt := unixNanoTime(createdAt)
	if enqueuedAt.IsZero() {
		enqueuedAt = receipt.CreatedAt.UTC()
	}
	if enqueuedAt.IsZero() {
		enqueuedAt = now
	}
	if err := s.upsertStatus(ctx, tx, statusRecord{
		DispatchID:     receipt.ID,
		State:          queue.DispatchStateSucceeded,
		Attempt:        attempts,
		EnqueuedAt:     enqueuedAt,
		UpdatedAt:      now,
		NextRunAt:      time.Time{},
		TerminalReason: "",
	}); err != nil {
		return err
	}

	return tx.Commit()
}

// Nack requeues, delays, or dead-letters a message.
func (s *Storage) Nack(ctx context.Context, receipt queue.Receipt, opts queue.NackOptions) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("postgres storage not configured")
	}
	if err := s.validateIdentifiers(); err != nil {
		return err
	}
	if receipt.ID == "" || receipt.Token == "" {
		return fmt.Errorf("receipt id and token required")
	}
	if err := queue.ValidateNackOptions(opts); err != nil {
		return err
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

	now := s.now().UTC()
	enqueuedAt := unixNanoTime(createdAt)
	if enqueuedAt.IsZero() {
		enqueuedAt = receipt.CreatedAt.UTC()
	}
	if enqueuedAt.IsZero() {
		enqueuedAt = now
	}

	if opts.Disposition == queue.NackDispositionDeadLetter {
		if err := s.insertDLQ(ctx, tx, receipt.ID, payload, attempts, createdAt, opts.Reason, now); err != nil {
			return err
		}
		if err := s.deleteMessage(ctx, tx, receipt); err != nil {
			return err
		}
		if err := s.upsertStatus(ctx, tx, statusRecord{
			DispatchID:     receipt.ID,
			State:          queue.DispatchStateDeadLetter,
			Attempt:        attempts,
			EnqueuedAt:     enqueuedAt,
			UpdatedAt:      now,
			NextRunAt:      time.Time{},
			TerminalReason: strings.TrimSpace(opts.Reason),
		}); err != nil {
			return err
		}
		return tx.Commit()
	}

	if opts.Disposition == queue.NackDispositionRetry {
		availableAt := now
		if opts.Delay > 0 {
			availableAt = now.Add(opts.Delay)
		}
		if err := s.updateForRetry(ctx, tx, receipt, availableAt, opts.Reason, now); err != nil {
			return err
		}
		if err := s.upsertStatus(ctx, tx, statusRecord{
			DispatchID:     receipt.ID,
			State:          queue.DispatchStateRetrying,
			Attempt:        attempts,
			EnqueuedAt:     enqueuedAt,
			UpdatedAt:      now,
			NextRunAt:      availableAt,
			TerminalReason: strings.TrimSpace(opts.Reason),
		}); err != nil {
			return err
		}
		return tx.Commit()
	}

	if err := s.deleteMessage(ctx, tx, receipt); err != nil {
		return err
	}
	state := queue.DispatchStateFailed
	if opts.Disposition == queue.NackDispositionCanceled {
		state = queue.DispatchStateCanceled
	}
	if err := s.upsertStatus(ctx, tx, statusRecord{
		DispatchID:     receipt.ID,
		State:          state,
		Attempt:        attempts,
		EnqueuedAt:     enqueuedAt,
		UpdatedAt:      now,
		NextRunAt:      time.Time{},
		TerminalReason: strings.TrimSpace(opts.Reason),
	}); err != nil {
		return err
	}
	return tx.Commit()
}

// ExtendLease extends a delivery lease for long-running handlers.
func (s *Storage) ExtendLease(ctx context.Context, receipt queue.Receipt, ttl time.Duration) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("postgres storage not configured")
	}
	if err := s.validateIdentifiers(); err != nil {
		return err
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
	enqueuedAt := receipt.CreatedAt.UTC()
	if enqueuedAt.IsZero() {
		enqueuedAt = now
	}
	if err := s.upsertStatus(ctx, s.db, statusRecord{
		DispatchID:     receipt.ID,
		State:          queue.DispatchStateRunning,
		Attempt:        receipt.Attempts,
		EnqueuedAt:     enqueuedAt,
		UpdatedAt:      now,
		NextRunAt:      time.Time{},
		TerminalReason: "",
	}); err != nil {
		return err
	}
	return nil
}

// GetDispatchStatus reads durable dispatch status records for a dispatch id.
func (s *Storage) GetDispatchStatus(ctx context.Context, dispatchID string) (queue.DispatchStatus, error) {
	status := queue.DispatchStatus{
		DispatchID: strings.TrimSpace(dispatchID),
	}
	if s == nil || s.db == nil {
		return status, fmt.Errorf("postgres storage not configured")
	}
	if err := s.validateIdentifiers(); err != nil {
		return status, err
	}
	if status.DispatchID == "" {
		return status, queue.ErrDispatchNotFound
	}

	nowUnix := s.now().UTC().UnixNano()
	p := s.placeholder
	query := fmt.Sprintf(`SELECT state, attempt, enqueued_at, updated_at, next_run_at, terminal_reason
FROM %s
WHERE dispatch_id = %s AND expires_at > %s`, s.statusTable, p(1), p(2))
	row := s.db.QueryRowContext(ctx, query, status.DispatchID, nowUnix)

	var state string
	var attempt int
	var enqueuedAt int64
	var updatedAt int64
	var nextRunAt int64
	var terminalReason sql.NullString
	if err := row.Scan(&state, &attempt, &enqueuedAt, &updatedAt, &nextRunAt, &terminalReason); err != nil {
		if err == sql.ErrNoRows {
			return status, queue.ErrDispatchNotFound
		}
		return status, err
	}

	status.State = queue.DispatchState(state)
	status.Attempt = attempt
	status.EnqueuedAt = ptrTime(unixNanoTime(enqueuedAt))
	status.UpdatedAt = ptrTime(unixNanoTime(updatedAt))
	status.NextRunAt = ptrTime(unixNanoTime(nextRunAt))
	if terminalReason.Valid {
		status.TerminalReason = terminalReason.String
	}
	return status, nil
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
	if err := s.upsertStatus(ctx, tx, statusRecord{
		DispatchID:     id,
		State:          queue.DispatchStateRunning,
		Attempt:        attempts,
		EnqueuedAt:     unixNanoTime(createdAt),
		UpdatedAt:      now,
		NextRunAt:      time.Time{},
		TerminalReason: "",
	}); err != nil {
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
	if err := s.upsertStatus(ctx, tx, statusRecord{
		DispatchID:     id,
		State:          queue.DispatchStateRunning,
		Attempt:        attempts,
		EnqueuedAt:     unixNanoTime(createdAt),
		UpdatedAt:      now,
		NextRunAt:      time.Time{},
		TerminalReason: "",
	}); err != nil {
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

type statusRecord struct {
	DispatchID     string
	State          queue.DispatchState
	Attempt        int
	EnqueuedAt     time.Time
	UpdatedAt      time.Time
	NextRunAt      time.Time
	TerminalReason string
}

type sqlExecutor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func (s *Storage) upsertStatus(ctx context.Context, exec sqlExecutor, record statusRecord) error {
	if exec == nil {
		return fmt.Errorf("sql executor required")
	}
	dispatchID := strings.TrimSpace(record.DispatchID)
	if dispatchID == "" {
		return fmt.Errorf("dispatch id required")
	}
	if record.State == "" {
		return fmt.Errorf("dispatch state required")
	}

	enqueuedAt := record.EnqueuedAt.UTC()
	if enqueuedAt.IsZero() {
		enqueuedAt = s.now().UTC()
	}
	updatedAt := record.UpdatedAt.UTC()
	if updatedAt.IsZero() {
		updatedAt = s.now().UTC()
	}
	nextRunAt := int64(0)
	if !record.NextRunAt.IsZero() {
		nextRunAt = record.NextRunAt.UTC().UnixNano()
	}
	expiresAt := updatedAt.Add(s.statusTTL).UnixNano()
	terminalReason := strings.TrimSpace(record.TerminalReason)

	p := s.placeholder
	query := fmt.Sprintf(`INSERT INTO %s
(dispatch_id, state, attempt, enqueued_at, updated_at, next_run_at, terminal_reason, expires_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(dispatch_id) DO UPDATE SET
state = excluded.state,
attempt = excluded.attempt,
enqueued_at = %s.enqueued_at,
updated_at = excluded.updated_at,
next_run_at = excluded.next_run_at,
terminal_reason = excluded.terminal_reason,
expires_at = excluded.expires_at`, s.statusTable, p(1), p(2), p(3), p(4), p(5), p(6), p(7), p(8), s.statusTable)
	_, err := exec.ExecContext(ctx, query,
		dispatchID,
		string(record.State),
		record.Attempt,
		enqueuedAt.UnixNano(),
		updatedAt.UnixNano(),
		nextRunAt,
		terminalReason,
		expiresAt,
	)
	return err
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

func ptrTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	v := value.UTC()
	return &v
}

func (s *Storage) validateIdentifiers() error {
	if err := sqlutil.ValidateIdentifier("queue table", s.table); err != nil {
		return err
	}
	if err := sqlutil.ValidateIdentifier("queue dlq table", s.dlqTable); err != nil {
		return err
	}
	return sqlutil.ValidateIdentifier("queue status table", s.statusTable)
}
