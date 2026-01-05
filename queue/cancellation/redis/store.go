package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/goliatone/go-job/queue/cancellation"
)

const (
	defaultPrefix       = "cancel"
	defaultPollInterval = 200 * time.Millisecond

	fieldKey         = "key"
	fieldReason      = "reason"
	fieldRequestedAt = "requested_at"
)

// Client defines the Redis operations needed by the cancellation store.
type Client interface {
	HSet(ctx context.Context, key string, values map[string]string) error
	HGetAll(ctx context.Context, key string) (map[string]string, error)
	LPush(ctx context.Context, key string, values ...string) error
	RPop(ctx context.Context, key string) (string, error)
	Del(ctx context.Context, keys ...string) error
}

// Option configures the cancellation store.
type Option func(*Store)

// WithPrefix sets the key prefix.
func WithPrefix(prefix string) Option {
	return func(s *Store) {
		if prefix != "" {
			s.keys = newKeySet(prefix)
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

// WithPollInterval configures subscription polling interval.
func WithPollInterval(interval time.Duration) Option {
	return func(s *Store) {
		if interval >= 0 {
			s.pollInterval = interval
		}
	}
}

// Store implements cancellation.Store backed by Redis primitives.
type Store struct {
	client       Client
	keys         keySet
	now          func() time.Time
	pollInterval time.Duration
}

// NewStore builds a redis-backed cancellation store.
func NewStore(client Client, opts ...Option) *Store {
	store := &Store{
		client:       client,
		keys:         newKeySet(defaultPrefix),
		now:          time.Now,
		pollInterval: defaultPollInterval,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(store)
		}
	}
	if store.pollInterval <= 0 {
		store.pollInterval = defaultPollInterval
	}
	return store
}

// Request stores a cancellation request and publishes it to the stream.
func (s *Store) Request(ctx context.Context, req cancellation.Request) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("redis cancellation store not configured")
	}
	if req.Key == "" {
		return fmt.Errorf("cancellation key required")
	}
	if req.RequestedAt.IsZero() {
		req.RequestedAt = s.now()
	}

	fields := map[string]string{
		fieldKey:         req.Key,
		fieldReason:      req.Reason,
		fieldRequestedAt: strconv.FormatInt(req.RequestedAt.UTC().UnixNano(), 10),
	}
	if err := s.client.HSet(ctx, s.keys.request(req.Key), fields); err != nil {
		return err
	}
	return s.client.LPush(ctx, s.keys.stream(), req.Key)
}

// Get returns a cancellation request for the key when present.
func (s *Store) Get(ctx context.Context, key string) (cancellation.Request, bool, error) {
	if s == nil || s.client == nil {
		return cancellation.Request{}, false, fmt.Errorf("redis cancellation store not configured")
	}
	if key == "" {
		return cancellation.Request{}, false, fmt.Errorf("cancellation key required")
	}

	fields, err := s.client.HGetAll(ctx, s.keys.request(key))
	if err != nil {
		return cancellation.Request{}, false, err
	}
	if len(fields) == 0 {
		return cancellation.Request{}, false, nil
	}

	req := cancellation.Request{
		Key:    key,
		Reason: fields[fieldReason],
	}
	if raw := fields[fieldRequestedAt]; raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil {
			req.RequestedAt = time.Unix(0, parsed).UTC()
		}
	}
	return req, true, nil
}

// Subscribe returns a channel that emits cancellation requests.
func (s *Store) Subscribe(ctx context.Context) (<-chan cancellation.Request, error) {
	if s == nil || s.client == nil {
		return nil, fmt.Errorf("redis cancellation store not configured")
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

	for {
		if ctx.Err() != nil {
			return
		}
		key, err := s.client.RPop(ctx, s.keys.stream())
		if err != nil || key == "" {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				continue
			}
		}

		req, found, err := s.Get(ctx, key)
		if err != nil || !found {
			continue
		}

		select {
		case <-ctx.Done():
			return
		case out <- req:
		}
	}
}
