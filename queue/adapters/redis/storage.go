package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	job "github.com/goliatone/go-job"
	"github.com/goliatone/go-job/queue"
)

const (
	defaultVisibilityTimeout = 60 * time.Second
	defaultReleaseBatchSize  = 100
	defaultStatusTTL         = 7 * 24 * time.Hour

	fieldPayload   = "payload"
	fieldAttempts  = "attempts"
	fieldToken     = "token"
	fieldLeasedAt  = "leased_at"
	fieldAvailable = "available_at"
	fieldCreatedAt = "created_at"
	fieldUpdatedAt = "updated_at"
	fieldLastError = "last_error"
	fieldDeadAt    = "dead_lettered_at"

	statusFieldState     = "state"
	statusFieldAttempt   = "attempt"
	statusFieldEnqueued  = "enqueued_at"
	statusFieldUpdated   = "updated_at"
	statusFieldNextRunAt = "next_run_at"
	statusFieldReason    = "terminal_reason"
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

// WithStatusTTL sets dispatch status retention TTL.
func WithStatusTTL(ttl time.Duration) Option {
	return func(s *Storage) {
		if ttl > 0 {
			s.statusTTL = ttl
		}
	}
}

// Storage implements queue.Storage backed by Redis primitives.
type Storage struct {
	client            Client
	keys              keySet
	visibilityTimeout time.Duration
	releaseBatchSize  int64
	statusTTL         time.Duration
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
		statusTTL:         defaultStatusTTL,
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
	if s.statusTTL <= 0 {
		s.statusTTL = defaultStatusTTL
	}
	return s
}

// Enqueue stores a message and enqueues it for delivery.
func (s *Storage) Enqueue(ctx context.Context, msg *job.ExecutionMessage) (queue.EnqueueReceipt, error) {
	return s.EnqueueAt(ctx, msg, s.now())
}

// EnqueueAt stores a message and schedules it for delivery at the provided time.
func (s *Storage) EnqueueAt(ctx context.Context, msg *job.ExecutionMessage, at time.Time) (queue.EnqueueReceipt, error) {
	if s == nil || s.client == nil {
		return queue.EnqueueReceipt{}, fmt.Errorf("redis storage not configured")
	}
	if err := queue.ValidateRequiredMessage(msg); err != nil {
		return queue.EnqueueReceipt{}, err
	}

	payload, err := queue.EncodeExecutionMessage(msg)
	if err != nil {
		return queue.EnqueueReceipt{}, err
	}

	id := s.idFunc()
	now := s.now().UTC()
	nowUnix := now.UnixNano()

	if err := s.client.HSet(ctx, s.keys.message(id), map[string]string{
		fieldPayload:   string(payload),
		fieldAttempts:  "0",
		fieldToken:     "",
		fieldAvailable: strconv.FormatInt(at.UnixNano(), 10),
		fieldCreatedAt: strconv.FormatInt(nowUnix, 10),
		fieldUpdatedAt: strconv.FormatInt(nowUnix, 10),
	}); err != nil {
		return queue.EnqueueReceipt{}, err
	}

	if at.After(now) {
		if err := s.client.ZAdd(ctx, s.keys.delayed(), float64(at.UnixNano()), id); err != nil {
			return queue.EnqueueReceipt{}, err
		}
	} else {
		if err := s.client.LPush(ctx, s.keys.ready(), id); err != nil {
			return queue.EnqueueReceipt{}, err
		}
	}

	nextRunAt := at.UTC()
	if !nextRunAt.After(now) {
		nextRunAt = time.Time{}
	}
	if err := s.writeStatus(ctx, statusRecord{
		DispatchID:     id,
		State:          queue.DispatchStateAccepted,
		Attempt:        0,
		EnqueuedAt:     now,
		UpdatedAt:      now,
		NextRunAt:      nextRunAt,
		TerminalReason: "",
	}); err != nil {
		return queue.EnqueueReceipt{}, err
	}

	return queue.EnqueueReceipt{
		DispatchID: id,
		EnqueuedAt: time.Unix(0, nowUnix).UTC(),
	}, nil
}

// EnqueueAfter stores a message and schedules it after the provided delay.
func (s *Storage) EnqueueAfter(ctx context.Context, msg *job.ExecutionMessage, delay time.Duration) (queue.EnqueueReceipt, error) {
	at := s.now()
	if delay > 0 {
		at = at.Add(delay)
	}
	return s.EnqueueAt(ctx, msg, at)
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

	msg, err := queue.DecodeExecutionMessage([]byte(payload))
	if err != nil {
		return nil, queue.Receipt{}, err
	}

	attempts := parseInt(fields[fieldAttempts]) + 1
	token := s.tokenFunc()
	now := s.now()
	availableAt := parseInt64(fields[fieldAvailable])
	createdAt := parseInt64(fields[fieldCreatedAt])
	lastError := fields[fieldLastError]
	var availableAtTime time.Time
	if availableAt > 0 {
		availableAtTime = time.Unix(0, availableAt).UTC()
	}
	var createdAtTime time.Time
	if createdAt > 0 {
		createdAtTime = time.Unix(0, createdAt).UTC()
	}

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
	if err := s.writeStatus(ctx, statusRecord{
		DispatchID:     id,
		State:          queue.DispatchStateRunning,
		Attempt:        attempts,
		EnqueuedAt:     createdAtTime,
		UpdatedAt:      now,
		NextRunAt:      time.Time{},
		TerminalReason: "",
	}); err != nil {
		return nil, queue.Receipt{}, err
	}

	return msg, queue.Receipt{
		ID:          id,
		Token:       token,
		Attempts:    attempts,
		LeasedAt:    now,
		AvailableAt: availableAtTime,
		CreatedAt:   createdAtTime,
		LastError:   lastError,
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
	fields, err := s.client.HGetAll(ctx, s.keys.message(receipt.ID))
	if err != nil {
		return err
	}
	now := s.now().UTC()
	attempts := parseInt(fields[fieldAttempts])
	enqueuedAt := unixNanoTime(parseInt64(fields[fieldCreatedAt]))
	if enqueuedAt.IsZero() {
		enqueuedAt = receipt.CreatedAt.UTC()
	}
	if enqueuedAt.IsZero() {
		enqueuedAt = now
	}

	if err := s.client.ZRem(ctx, s.keys.inflight(), receipt.ID); err != nil {
		return err
	}
	if err := s.client.ZRem(ctx, s.keys.delayed(), receipt.ID); err != nil {
		return err
	}
	if err := s.client.Del(ctx, s.keys.message(receipt.ID)); err != nil {
		return err
	}
	return s.writeStatus(ctx, statusRecord{
		DispatchID:     receipt.ID,
		State:          queue.DispatchStateSucceeded,
		Attempt:        attempts,
		EnqueuedAt:     enqueuedAt,
		UpdatedAt:      now,
		NextRunAt:      time.Time{},
		TerminalReason: "",
	})
}

// Nack releases the message according to the provided options.
func (s *Storage) Nack(ctx context.Context, receipt queue.Receipt, opts queue.NackOptions) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("redis storage not configured")
	}
	if receipt.ID == "" {
		return fmt.Errorf("receipt id required")
	}
	if err := queue.ValidateNackOptions(opts); err != nil {
		return err
	}
	if err := s.ensureToken(ctx, receipt); err != nil {
		return err
	}
	fields, err := s.client.HGetAll(ctx, s.keys.message(receipt.ID))
	if err != nil {
		return err
	}
	attempts := parseInt(fields[fieldAttempts])
	enqueuedAt := unixNanoTime(parseInt64(fields[fieldCreatedAt]))

	now := s.now().UTC()
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

	if opts.Disposition == queue.NackDispositionDeadLetter {
		nowUnix := now.UnixNano()
		if err := s.client.HSet(ctx, s.keys.message(receipt.ID), map[string]string{
			fieldDeadAt:    strconv.FormatInt(nowUnix, 10),
			fieldUpdatedAt: strconv.FormatInt(nowUnix, 10),
		}); err != nil {
			return err
		}
		if err := s.client.LPush(ctx, s.keys.dlq(), receipt.ID); err != nil {
			return err
		}
		return s.writeStatus(ctx, statusRecord{
			DispatchID:     receipt.ID,
			State:          queue.DispatchStateDeadLetter,
			Attempt:        attempts,
			EnqueuedAt:     enqueuedAt,
			UpdatedAt:      now,
			NextRunAt:      time.Time{},
			TerminalReason: strings.TrimSpace(opts.Reason),
		})
	}

	if opts.Disposition == queue.NackDispositionRetry {
		availableAt := now
		if opts.Delay > 0 {
			availableAt = now.Add(opts.Delay)
			score := float64(availableAt.UnixNano())
			update[fieldAvailable] = strconv.FormatInt(availableAt.UnixNano(), 10)
			if err := s.client.HSet(ctx, s.keys.message(receipt.ID), update); err != nil {
				return err
			}
			if err := s.client.ZAdd(ctx, s.keys.delayed(), score, receipt.ID); err != nil {
				return err
			}
			return s.writeStatus(ctx, statusRecord{
				DispatchID:     receipt.ID,
				State:          queue.DispatchStateRetrying,
				Attempt:        attempts,
				EnqueuedAt:     enqueuedAt,
				UpdatedAt:      now,
				NextRunAt:      availableAt,
				TerminalReason: strings.TrimSpace(opts.Reason),
			})
		}
		update[fieldAvailable] = strconv.FormatInt(availableAt.UnixNano(), 10)
		if err := s.client.HSet(ctx, s.keys.message(receipt.ID), update); err != nil {
			return err
		}
		if err := s.client.LPush(ctx, s.keys.ready(), receipt.ID); err != nil {
			return err
		}
		return s.writeStatus(ctx, statusRecord{
			DispatchID:     receipt.ID,
			State:          queue.DispatchStateAccepted,
			Attempt:        attempts,
			EnqueuedAt:     enqueuedAt,
			UpdatedAt:      now,
			NextRunAt:      time.Time{},
			TerminalReason: strings.TrimSpace(opts.Reason),
		})
	}

	if err := s.client.Del(ctx, s.keys.message(receipt.ID)); err != nil {
		return err
	}
	state := queue.DispatchStateFailed
	if opts.Disposition == queue.NackDispositionCanceled {
		state = queue.DispatchStateCanceled
	}
	return s.writeStatus(ctx, statusRecord{
		DispatchID:     receipt.ID,
		State:          state,
		Attempt:        attempts,
		EnqueuedAt:     enqueuedAt,
		UpdatedAt:      now,
		NextRunAt:      time.Time{},
		TerminalReason: strings.TrimSpace(opts.Reason),
	})
}

// GetDispatchStatus reads durable status records for a dispatch id.
func (s *Storage) GetDispatchStatus(ctx context.Context, dispatchID string) (queue.DispatchStatus, error) {
	status := queue.DispatchStatus{
		DispatchID: strings.TrimSpace(dispatchID),
	}
	if s == nil || s.client == nil {
		return status, fmt.Errorf("redis storage not configured")
	}
	if status.DispatchID == "" {
		return status, queue.ErrDispatchNotFound
	}

	fields, err := s.client.HGetAll(ctx, s.keys.status(status.DispatchID))
	if err != nil {
		return status, err
	}
	if len(fields) == 0 {
		return status, queue.ErrDispatchNotFound
	}
	status.State = queue.DispatchState(fields[statusFieldState])
	status.Attempt = parseInt(fields[statusFieldAttempt])
	status.EnqueuedAt = ptrTime(unixNanoTime(parseInt64(fields[statusFieldEnqueued])))
	status.UpdatedAt = ptrTime(unixNanoTime(parseInt64(fields[statusFieldUpdated])))
	status.NextRunAt = ptrTime(unixNanoTime(parseInt64(fields[statusFieldNextRunAt])))
	status.TerminalReason = strings.TrimSpace(fields[statusFieldReason])
	return status, nil
}

// ExtendLease extends the in-flight lease for a delivery receipt.
func (s *Storage) ExtendLease(ctx context.Context, receipt queue.Receipt, ttl time.Duration) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("redis storage not configured")
	}
	if receipt.ID == "" {
		return fmt.Errorf("receipt id required")
	}
	if err := s.ensureToken(ctx, receipt); err != nil {
		return err
	}
	fields, err := s.client.HGetAll(ctx, s.keys.message(receipt.ID))
	if err != nil {
		return err
	}
	if ttl <= 0 {
		ttl = s.visibilityTimeout
	}
	now := s.now().UTC()
	until := now.Add(ttl).UnixNano()
	if err := s.client.ZAdd(ctx, s.keys.inflight(), float64(until), receipt.ID); err != nil {
		return err
	}
	if err := s.client.HSet(ctx, s.keys.message(receipt.ID), map[string]string{
		fieldLeasedAt:  strconv.FormatInt(now.UnixNano(), 10),
		fieldUpdatedAt: strconv.FormatInt(now.UnixNano(), 10),
	}); err != nil {
		return err
	}

	enqueuedAt := unixNanoTime(parseInt64(fields[fieldCreatedAt]))
	if enqueuedAt.IsZero() {
		enqueuedAt = receipt.CreatedAt.UTC()
	}
	if enqueuedAt.IsZero() {
		enqueuedAt = now
	}
	return s.writeStatus(ctx, statusRecord{
		DispatchID:     receipt.ID,
		State:          queue.DispatchStateRunning,
		Attempt:        parseInt(fields[fieldAttempts]),
		EnqueuedAt:     enqueuedAt,
		UpdatedAt:      now,
		NextRunAt:      time.Time{},
		TerminalReason: "",
	})
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
		fields, err := s.client.HGetAll(ctx, s.keys.message(item.Member))
		if err != nil {
			return err
		}
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
		now := s.now().UTC()
		enqueuedAt := unixNanoTime(parseInt64(fields[fieldCreatedAt]))
		if enqueuedAt.IsZero() {
			enqueuedAt = now
		}
		if err := s.writeStatus(ctx, statusRecord{
			DispatchID:     item.Member,
			State:          queue.DispatchStateAccepted,
			Attempt:        parseInt(fields[fieldAttempts]),
			EnqueuedAt:     enqueuedAt,
			UpdatedAt:      now,
			NextRunAt:      time.Time{},
			TerminalReason: strings.TrimSpace(fields[fieldLastError]),
		}); err != nil {
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

type statusRecord struct {
	DispatchID     string
	State          queue.DispatchState
	Attempt        int
	EnqueuedAt     time.Time
	UpdatedAt      time.Time
	NextRunAt      time.Time
	TerminalReason string
}

func (s *Storage) writeStatus(ctx context.Context, record statusRecord) error {
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

	if err := s.client.HSet(ctx, s.keys.status(dispatchID), map[string]string{
		statusFieldState:     string(record.State),
		statusFieldAttempt:   strconv.Itoa(record.Attempt),
		statusFieldEnqueued:  strconv.FormatInt(enqueuedAt.UnixNano(), 10),
		statusFieldUpdated:   strconv.FormatInt(updatedAt.UnixNano(), 10),
		statusFieldNextRunAt: strconv.FormatInt(nextRunAt, 10),
		statusFieldReason:    strings.TrimSpace(record.TerminalReason),
	}); err != nil {
		return err
	}
	if s.statusTTL > 0 {
		return s.client.Expire(ctx, s.keys.status(dispatchID), s.statusTTL)
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

func parseInt64(value string) int64 {
	if value == "" {
		return 0
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
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

func unixNanoTime(ts int64) time.Time {
	if ts <= 0 {
		return time.Time{}
	}
	return time.Unix(0, ts).UTC()
}

func ptrTime(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	v := value.UTC()
	return &v
}
