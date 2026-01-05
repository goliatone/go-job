package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/goliatone/go-job/queue/cancellation"
)

const (
	defaultTableName   = "queue_cancellations"
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

	lastSeen := time.Unix(0, 0).UTC()
	seen := make(map[string]int64)

	for {
		if ctx.Err() != nil {
			return
		}

		reqs, err := s.fetchSince(ctx, lastSeen, s.batchSize)
		if err == nil {
			for _, req := range reqs {
				seenAt := req.RequestedAt.UTC().UnixNano()
				if prev, ok := seen[req.Key]; ok && prev == seenAt {
					continue
				}
				seen[req.Key] = seenAt
				if req.RequestedAt.After(lastSeen) {
					lastSeen = req.RequestedAt
				}
				select {
				case <-ctx.Done():
					return
				case out <- req:
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

func (s *Store) fetchSince(ctx context.Context, since time.Time, limit int) ([]cancellation.Request, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("postgres cancellation store not configured")
	}
	if limit <= 0 {
		limit = defaultBatchSize
	}

	p := s.placeholder
	query := fmt.Sprintf(`SELECT key, reason, requested_at FROM %s WHERE requested_at >= %s ORDER BY requested_at ASC LIMIT %s`, s.table, p(1), p(2))
	rows, err := s.db.QueryContext(ctx, query, since.UTC().UnixNano(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []cancellation.Request
	for rows.Next() {
		var key string
		var reason string
		var requestedAt int64
		if err := rows.Scan(&key, &reason, &requestedAt); err != nil {
			return nil, err
		}
		out = append(out, cancellation.Request{
			Key:         key,
			Reason:      reason,
			RequestedAt: time.Unix(0, requestedAt).UTC(),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
