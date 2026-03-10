package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/goliatone/go-job/queue/cancellation"
)

const (
	defaultTableName    = "queue_cancellations"
	defaultPollInterval = 200 * time.Millisecond
	defaultBatchSize    = 100
)

// Option configures the postgres cancellation store.
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
	}
}

// WithPollInterval configures subscription polling interval.
func WithPollInterval(interval time.Duration) Option {
	return func(s *Store) {
		if interval >= 0 {
			s.pollInterval = interval
		}
	}
}

// WithBatchSize configures subscription batch size.
func WithBatchSize(size int) Option {
	return func(s *Store) {
		if size > 0 {
			s.batchSize = size
		}
	}
}

// Store implements cancellation.Store backed by SQL.
type Store struct {
	db           *sql.DB
	table        string
	now          func() time.Time
	placeholder  placeholderFunc
	pollInterval time.Duration
	batchSize    int
}

// NewStore builds a postgres cancellation store.
func NewStore(db *sql.DB, opts ...Option) *Store {
	store := &Store{
		db:           db,
		table:        defaultTableName,
		now:          time.Now,
		placeholder:  placeholderFor(DialectPostgres),
		pollInterval: defaultPollInterval,
		batchSize:    defaultBatchSize,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(store)
		}
	}
	if store.placeholder == nil {
		store.placeholder = placeholderFor(DialectPostgres)
	}
	if store.pollInterval <= 0 {
		store.pollInterval = defaultPollInterval
	}
	if store.batchSize <= 0 {
		store.batchSize = defaultBatchSize
	}
	return store
}

// Migrate creates the cancellation table.
func (s *Store) Migrate(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("postgres cancellation store not configured")
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

// Cleanup drops the cancellation table (for tests).
func (s *Store) Cleanup(ctx context.Context) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("postgres cancellation store not configured")
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

// Request upserts a cancellation request.
func (s *Store) Request(ctx context.Context, req cancellation.Request) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("postgres cancellation store not configured")
	}
	if err := s.validateIdentifier(); err != nil {
		return err
	}
	if req.Key == "" {
		return fmt.Errorf("cancellation key required")
	}
	if req.RequestedAt.IsZero() {
		req.RequestedAt = s.now()
	}

	now := s.now()
	p := s.placeholder
	query := fmt.Sprintf(`INSERT INTO %s (key, reason, requested_at, created_at, updated_at)
VALUES (%s, %s, %s, %s, %s)
ON CONFLICT (key) DO UPDATE SET reason = excluded.reason, requested_at = excluded.requested_at, updated_at = excluded.updated_at`, s.table, p(1), p(2), p(3), p(4), p(5))
	_, err := s.db.ExecContext(ctx, query, req.Key, req.Reason, req.RequestedAt.UTC().UnixNano(), now.UTC().UnixNano(), now.UTC().UnixNano())
	return err
}

// Get retrieves a cancellation request by key.
func (s *Store) Get(ctx context.Context, key string) (cancellation.Request, bool, error) {
	if s == nil || s.db == nil {
		return cancellation.Request{}, false, fmt.Errorf("postgres cancellation store not configured")
	}
	if err := s.validateIdentifier(); err != nil {
		return cancellation.Request{}, false, err
	}
	if key == "" {
		return cancellation.Request{}, false, fmt.Errorf("cancellation key required")
	}

	p := s.placeholder
	query := fmt.Sprintf(`SELECT reason, requested_at FROM %s WHERE key = %s`, s.table, p(1))
	var reason string
	var requestedAt int64
	if err := s.db.QueryRowContext(ctx, query, key).Scan(&reason, &requestedAt); err != nil {
		if err == sql.ErrNoRows {
			return cancellation.Request{}, false, nil
		}
		return cancellation.Request{}, false, err
	}

	return cancellation.Request{
		Key:         key,
		Reason:      reason,
		RequestedAt: time.Unix(0, requestedAt).UTC(),
	}, true, nil
}

// Subscribe returns a channel that emits cancellation requests.
func (s *Store) Subscribe(ctx context.Context) (<-chan cancellation.Request, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("postgres cancellation store not configured")
	}
	if err := s.validateIdentifier(); err != nil {
		return nil, err
	}
	out := make(chan cancellation.Request, 1)
	go s.poll(ctx, out)
	return out, nil
}

func (s *Store) poll(ctx context.Context, out chan<- cancellation.Request) {
	defer close(out)

	interval := s.pollInterval
	if interval <= 0 {
		interval = defaultPollInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	var cursorUpdatedAt int64
	cursorKey := ""

	for {
		if ctx.Err() != nil {
			return
		}

		reqs, err := s.fetchSince(ctx, cursorUpdatedAt, cursorKey, s.batchSize)
		if err == nil {
			for _, evt := range reqs {
				cursorUpdatedAt = evt.updatedAt
				cursorKey = evt.request.Key
				select {
				case <-ctx.Done():
					return
				case out <- evt.request:
				}
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

type subscriptionEvent struct {
	request   cancellation.Request
	updatedAt int64
}

func (s *Store) fetchSince(ctx context.Context, sinceUpdatedAt int64, sinceKey string, limit int) ([]subscriptionEvent, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("postgres cancellation store not configured")
	}
	if limit <= 0 {
		limit = defaultBatchSize
	}

	p := s.placeholder
	query := fmt.Sprintf(`SELECT key, reason, requested_at, updated_at
FROM %s
WHERE (updated_at > %s OR (updated_at = %s AND key > %s))
ORDER BY updated_at ASC, key ASC
LIMIT %s`, s.table, p(1), p(2), p(3), p(4))
	rows, err := s.db.QueryContext(ctx, query, sinceUpdatedAt, sinceUpdatedAt, sinceKey, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []subscriptionEvent
	for rows.Next() {
		var key string
		var reason string
		var requestedAt int64
		var updatedAt int64
		if err := rows.Scan(&key, &reason, &requestedAt, &updatedAt); err != nil {
			return nil, err
		}
		out = append(out, subscriptionEvent{
			request: cancellation.Request{
				Key:         key,
				Reason:      reason,
				RequestedAt: time.Unix(0, requestedAt).UTC(),
			},
			updatedAt: updatedAt,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) validateIdentifier() error {
	return validateSQLIdentifier("cancellation table", s.table)
}

func validateSQLIdentifier(label, value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return fmt.Errorf("%s identifier required", label)
	}
	parts := strings.Split(value, ".")
	for _, part := range parts {
		if !isSQLIdentifierPart(part) {
			return fmt.Errorf("invalid %s identifier %q", label, value)
		}
	}
	return nil
}

func isSQLIdentifierPart(part string) bool {
	if part == "" {
		return false
	}
	for idx := 0; idx < len(part); idx++ {
		ch := part[idx]
		isLetter := (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z')
		isDigit := ch >= '0' && ch <= '9'
		if idx == 0 {
			if !isLetter && ch != '_' {
				return false
			}
			continue
		}
		if !isLetter && !isDigit && ch != '_' {
			return false
		}
	}
	return true
}
