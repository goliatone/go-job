package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/goliatone/go-job/queue/idempotency"
)

const defaultPrefix = "idempotency"

// Client defines the Redis operations needed by the idempotency store.
type Client interface {
	Get(ctx context.Context, key string) (string, error)
	Set(ctx context.Context, key, value string, ttl time.Duration) error
	SetNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error)
	Del(ctx context.Context, key string) error
}

// Option configures the idempotency store.
type Option func(*Store)

// WithPrefix sets the key prefix.
func WithPrefix(prefix string) Option {
	return func(s *Store) {
		if prefix != "" {
			s.prefix = prefix
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

// Store implements idempotency.Store backed by Redis.
type Store struct {
	client Client
	prefix string
	now    func() time.Time
}

// NewStore builds a redis-backed idempotency store.
func NewStore(client Client, opts ...Option) *Store {
	store := &Store{
		client: client,
		prefix: defaultPrefix,
		now:    time.Now,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(store)
		}
	}
	return store
}

// Acquire creates or returns an existing idempotency record.
func (s *Store) Acquire(ctx context.Context, key string, ttl time.Duration) (idempotency.Record, bool, error) {
	if s == nil || s.client == nil {
		return idempotency.Record{}, false, fmt.Errorf("redis idempotency store not configured")
	}
	if key == "" {
		return idempotency.Record{}, false, fmt.Errorf("idempotency key required")
	}

	now := s.now()
	record := newRecord(key, nil, now, ttl)
	encoded, err := encodeRecord(record)
	if err != nil {
		return idempotency.Record{}, false, err
	}
	ttlValue := ttlForRecord(record, now)

	ok, err := s.client.SetNX(ctx, s.storageKey(key), encoded, ttlValue)
	if err != nil {
		return idempotency.Record{}, false, err
	}
	if ok {
		return record, true, nil
	}

	existing, found, err := s.Get(ctx, key)
	if err != nil {
		return idempotency.Record{}, false, err
	}
	if !found {
		return idempotency.Record{}, false, nil
	}
	if idempotency.IsExpired(existing, now) {
		if err := s.client.Del(ctx, s.storageKey(key)); err != nil {
			return idempotency.Record{}, false, err
		}
		if err := s.client.Set(ctx, s.storageKey(key), encoded, ttlValue); err != nil {
			return idempotency.Record{}, false, err
		}
		return record, true, nil
	}

	return existing, false, nil
}

// Get returns an existing record.
func (s *Store) Get(ctx context.Context, key string) (idempotency.Record, bool, error) {
	if s == nil || s.client == nil {
		return idempotency.Record{}, false, fmt.Errorf("redis idempotency store not configured")
	}
	if key == "" {
		return idempotency.Record{}, false, fmt.Errorf("idempotency key required")
	}

	payload, err := s.client.Get(ctx, s.storageKey(key))
	if err != nil {
		return idempotency.Record{}, false, err
	}
	if payload == "" {
		return idempotency.Record{}, false, nil
	}

	record, err := decodeRecord(payload)
	if err != nil {
		return idempotency.Record{}, false, err
	}
	now := s.now()
	if idempotency.IsExpired(record, now) {
		if err := s.client.Del(ctx, s.storageKey(key)); err != nil {
			return idempotency.Record{}, false, err
		}
		return idempotency.Record{}, false, nil
	}
	return record, true, nil
}

// Update applies a partial update to the record.
func (s *Store) Update(ctx context.Context, key string, update idempotency.Update) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("redis idempotency store not configured")
	}
	if key == "" {
		return fmt.Errorf("idempotency key required")
	}

	record, found, err := s.Get(ctx, key)
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
		if err := s.client.Del(ctx, s.storageKey(key)); err != nil {
			return err
		}
		return idempotency.ErrNotFound
	}

	encoded, err := encodeRecord(record)
	if err != nil {
		return err
	}

	return s.client.Set(ctx, s.storageKey(key), encoded, ttlForRecord(record, now))
}

// Delete removes a record from the store.
func (s *Store) Delete(ctx context.Context, key string) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("redis idempotency store not configured")
	}
	if key == "" {
		return fmt.Errorf("idempotency key required")
	}
	return s.client.Del(ctx, s.storageKey(key))
}

func (s *Store) storageKey(key string) string {
	return fmt.Sprintf("%s:%s", s.prefix, key)
}

type recordPayload struct {
	Key       string             `json:"key"`
	Status    idempotency.Status `json:"status"`
	Payload   []byte             `json:"payload,omitempty"`
	ExpiresAt int64              `json:"expires_at"`
	CreatedAt int64              `json:"created_at"`
	UpdatedAt int64              `json:"updated_at"`
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

func encodeRecord(record idempotency.Record) (string, error) {
	data := recordPayload{
		Key:       record.Key,
		Status:    idempotency.DefaultStatus(record.Status),
		Payload:   idempotency.CloneBytes(record.Payload),
		CreatedAt: record.CreatedAt.UTC().UnixNano(),
		UpdatedAt: record.UpdatedAt.UTC().UnixNano(),
	}
	if !record.ExpiresAt.IsZero() {
		data.ExpiresAt = record.ExpiresAt.UTC().UnixNano()
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

func decodeRecord(value string) (idempotency.Record, error) {
	if value == "" {
		return idempotency.Record{}, fmt.Errorf("empty record payload")
	}
	var data recordPayload
	if err := json.Unmarshal([]byte(value), &data); err != nil {
		return idempotency.Record{}, err
	}
	record := idempotency.Record{
		Key:       data.Key,
		Status:    idempotency.DefaultStatus(data.Status),
		Payload:   idempotency.CloneBytes(data.Payload),
		CreatedAt: time.Unix(0, data.CreatedAt).UTC(),
		UpdatedAt: time.Unix(0, data.UpdatedAt).UTC(),
	}
	if data.ExpiresAt != 0 {
		record.ExpiresAt = time.Unix(0, data.ExpiresAt).UTC()
	}
	return record, nil
}

func ttlForRecord(record idempotency.Record, now time.Time) time.Duration {
	if record.ExpiresAt.IsZero() {
		return 0
	}
	ttl := time.Until(record.ExpiresAt)
	if ttl < 0 {
		return 0
	}
	return ttl
}
