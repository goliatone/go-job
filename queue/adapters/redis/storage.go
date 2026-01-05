package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	job "github.com/goliatone/go-job"
	"github.com/goliatone/go-job/queue"
)

const (
	defaultVisibilityTimeout = 60 * time.Second
	defaultReleaseBatchSize  = 100

	fieldPayload   = "payload"
	fieldAttempts  = "attempts"
	fieldToken     = "token"
	fieldLeasedAt  = "leased_at"
	fieldCreatedAt = "created_at"
	fieldUpdatedAt = "updated_at"
	fieldLastError = "last_error"
)

// Option configures the redis storage adapter.
type Option func(*Storage)

// WithQueueName sets the key prefix for the queue.
func WithQueueName(queue string) Option {
	return func(s *Storage) {
		if queue != "" {
			s.keys = newKeySet(queue)
		}
	}
}

// WithVisibilityTimeout sets the default lease timeout.
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

// WithTokenFunc overrides lease token generation.
func WithTokenFunc(fn func() string) Option {
	return func(s *Storage) {
		if fn != nil {
			s.tokenFunc = fn
		}
	}
}

// WithReleaseBatchSize limits the number of delayed/inflight entries released per dequeue.
func WithReleaseBatchSize(size int64) Option {
	return func(s *Storage) {
		if size > 0 {
			s.releaseBatchSize = size
		}
	}
}

// Storage implements queue.Storage backed by Redis primitives.
type Storage struct {
	client            Client
	keys              keySet
	visibilityTimeout time.Duration
	releaseBatchSize  int64
	now               func() time.Time
	idFunc            func() string
	tokenFunc         func() string
}

// NewStorage builds a redis storage adapter.
func NewStorage(client Client, opts ...Option) *Storage {
	s := &Storage{
		client:            client,
		keys:              newKeySet("queue"),
		visibilityTimeout: defaultVisibilityTimeout,
		releaseBatchSize:  defaultReleaseBatchSize,
		now:               time.Now,
		idFunc:            func() string { return randomHex(16) },
		tokenFunc:         func() string { return randomHex(16) },
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	if s.visibilityTimeout <= 0 {
		s.visibilityTimeout = defaultVisibilityTimeout
	}
	if s.releaseBatchSize <= 0 {
		s.releaseBatchSize = defaultReleaseBatchSize
	}
	return s
}

// Enqueue stores a message and enqueues it for delivery.
func (s *Storage) Enqueue(ctx context.Context, msg *job.ExecutionMessage) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("redis storage not configured")
	}
	if err := queue.ValidateRequiredMessage(msg); err != nil {
		return err
	}

	payload, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	id := s.idFunc()
	now := s.now().UnixNano()

	if err := s.client.HSet(ctx, s.keys.message(id), map[string]string{
		fieldPayload:   string(payload),
		fieldAttempts:  "0",
		fieldToken:     "",
		fieldCreatedAt: strconv.FormatInt(now, 10),
		fieldUpdatedAt: strconv.FormatInt(now, 10),
	}); err != nil {
		return err
	}

	return s.client.LPush(ctx, s.keys.ready(), id)
}

// Dequeue leases the next available message.
func (s *Storage) Dequeue(ctx context.Context) (*job.ExecutionMessage, queue.Receipt, error) {
	if s == nil || s.client == nil {
		return nil, queue.Receipt{}, fmt.Errorf("redis storage not configured")
	}

	if err := s.releaseDue(ctx); err != nil {
		return nil, queue.Receipt{}, err
	}

	id, err := s.client.RPop(ctx, s.keys.ready())
	if err != nil {
		return nil, queue.Receipt{}, err
	}
	if id == "" {
		return nil, queue.Receipt{}, nil
	}

	fields, err := s.client.HGetAll(ctx, s.keys.message(id))
	if err != nil {
		return nil, queue.Receipt{}, err
	}
	payload := fields[fieldPayload]
	if payload == "" {
		return nil, queue.Receipt{}, fmt.Errorf("message payload missing for %q", id)
	}

	var msg job.ExecutionMessage
	if err := json.Unmarshal([]byte(payload), &msg); err != nil {
		return nil, queue.Receipt{}, err
	}

	attempts := parseInt(fields[fieldAttempts]) + 1
	token := s.tokenFunc()
	now := s.now()

	if err := s.client.HSet(ctx, s.keys.message(id), map[string]string{
		fieldAttempts:  strconv.Itoa(attempts),
		fieldToken:     token,
		fieldLeasedAt:  strconv.FormatInt(now.UnixNano(), 10),
		fieldUpdatedAt: strconv.FormatInt(now.UnixNano(), 10),
	}); err != nil {
		return nil, queue.Receipt{}, err
	}

	leaseUntil := now.Add(s.visibilityTimeout).UnixNano()
	if err := s.client.ZAdd(ctx, s.keys.inflight(), float64(leaseUntil), id); err != nil {
		return nil, queue.Receipt{}, err
	}

	return &msg, queue.Receipt{
		ID:       id,
		Token:    token,
		Attempts: attempts,
		LeasedAt: now,
	}, nil
}

// Ack removes the message from in-flight tracking and deletes its payload.
func (s *Storage) Ack(ctx context.Context, receipt queue.Receipt) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("redis storage not configured")
	}
	if receipt.ID == "" {
		return fmt.Errorf("receipt id required")
	}
	if err := s.ensureToken(ctx, receipt); err != nil {
		return err
	}

	if err := s.client.ZRem(ctx, s.keys.inflight(), receipt.ID); err != nil {
		return err
	}
	if err := s.client.ZRem(ctx, s.keys.delayed(), receipt.ID); err != nil {
		return err
	}
	return s.client.Del(ctx, s.keys.message(receipt.ID))
}

// Nack releases the message according to the provided options.
func (s *Storage) Nack(ctx context.Context, receipt queue.Receipt, opts queue.NackOptions) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("redis storage not configured")
	}
	if receipt.ID == "" {
		return fmt.Errorf("receipt id required")
	}
	if err := s.ensureToken(ctx, receipt); err != nil {
		return err
	}

	now := s.now()
	if err := s.client.ZRem(ctx, s.keys.inflight(), receipt.ID); err != nil {
		return err
	}

	update := map[string]string{
		fieldToken:     "",
		fieldLeasedAt:  "0",
		fieldUpdatedAt: strconv.FormatInt(now.UnixNano(), 10),
	}
	if opts.Reason != "" {
		update[fieldLastError] = opts.Reason
	}
	if err := s.client.HSet(ctx, s.keys.message(receipt.ID), update); err != nil {
		return err
	}

	if opts.DeadLetter {
		return s.client.LPush(ctx, s.keys.dlq(), receipt.ID)
	}

	if opts.Requeue {
		if opts.Delay > 0 {
			score := float64(now.Add(opts.Delay).UnixNano())
			return s.client.ZAdd(ctx, s.keys.delayed(), score, receipt.ID)
		}
		return s.client.LPush(ctx, s.keys.ready(), receipt.ID)
	}

	return s.client.Del(ctx, s.keys.message(receipt.ID))
}

func (s *Storage) releaseDue(ctx context.Context) error {
	now := float64(s.now().UnixNano())
	if err := s.releaseSet(ctx, s.keys.delayed(), now, false); err != nil {
		return err
	}
	return s.releaseSet(ctx, s.keys.inflight(), now, true)
}

func (s *Storage) releaseSet(ctx context.Context, key string, max float64, clearLease bool) error {
	items, err := s.client.ZRangeByScore(ctx, key, max, s.releaseBatchSize)
	if err != nil {
		return err
	}

	for _, item := range items {
		if err := s.client.ZRem(ctx, key, item.Member); err != nil {
			return err
		}
		if clearLease {
			now := s.now().UnixNano()
			if err := s.client.HSet(ctx, s.keys.message(item.Member), map[string]string{
				fieldToken:     "",
				fieldLeasedAt:  "0",
				fieldUpdatedAt: strconv.FormatInt(now, 10),
			}); err != nil {
				return err
			}
		}
		if err := s.client.LPush(ctx, s.keys.ready(), item.Member); err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) ensureToken(ctx context.Context, receipt queue.Receipt) error {
	token, err := s.client.HGet(ctx, s.keys.message(receipt.ID), fieldToken)
	if err != nil {
		return err
	}
	if token == "" {
		return fmt.Errorf("receipt token missing for %q", receipt.ID)
	}
	if receipt.Token != token {
		return fmt.Errorf("receipt token mismatch for %q", receipt.ID)
	}
	return nil
}

func parseInt(value string) int {
	if value == "" {
		return 0
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return parsed
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
