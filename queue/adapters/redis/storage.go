package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	job "github.com/goliatone/go-job"
	"github.com/goliatone/go-job/queue"
	"github.com/goliatone/go-job/queue/internal/randutil"
	"github.com/goliatone/go-job/queue/internal/timeutil"
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

	scriptErrTokenMissing   = "ERR_TOKEN_MISSING"
	scriptErrTokenMismatch  = "ERR_TOKEN_MISMATCH"
	scriptErrMissingPayload = "ERR_MISSING_PAYLOAD"
)

var (
	// DequeueScript atomically releases due entries and leases one ready entry.
	DequeueScript = `
local ready = KEYS[1]
local delayed = KEYS[2]
local inflight = KEYS[3]
local msgPrefix = KEYS[4]
local leaseSeq = KEYS[5]

local now = tonumber(ARGV[1])
local batch = tonumber(ARGV[2])
local leaseUntil = tonumber(ARGV[3])

local delayedDue = redis.call('ZRANGEBYSCORE', delayed, '-inf', now, 'LIMIT', 0, batch)
for _, id in ipairs(delayedDue) do
	redis.call('ZREM', delayed, id)
	redis.call('LPUSH', ready, id)
end

local inflightDue = redis.call('ZRANGEBYSCORE', inflight, '-inf', now, 'LIMIT', 0, batch)
for _, id in ipairs(inflightDue) do
	redis.call('ZREM', inflight, id)
	local msgKey = msgPrefix .. id
	redis.call('HSET', msgKey, 'token', '', 'leased_at', '0', 'updated_at', tostring(now))
	redis.call('LPUSH', ready, id)
end

local id = redis.call('RPOP', ready)
if not id then
	return {}
end

local msgKey = msgPrefix .. id
local payload = redis.call('HGET', msgKey, 'payload')
if not payload or payload == '' then
	return { '` + scriptErrMissingPayload + `', id }
end

local attempts = redis.call('HINCRBY', msgKey, 'attempts', 1)
local availableAt = redis.call('HGET', msgKey, 'available_at') or '0'
local createdAt = redis.call('HGET', msgKey, 'created_at') or '0'
local lastError = redis.call('HGET', msgKey, 'last_error') or ''
local seq = redis.call('INCR', leaseSeq)
local token = redis.sha1hex(id .. ':' .. tostring(now) .. ':' .. tostring(seq))

redis.call('HSET', msgKey, 'token', token, 'leased_at', tostring(now), 'updated_at', tostring(now))
redis.call('ZADD', inflight, leaseUntil, id)

return { id, payload, tostring(attempts), token, availableAt, createdAt, lastError }
`

	// AckScript atomically validates token and removes an in-flight entry.
	AckScript = `
local msgKey = KEYS[1]
local inflight = KEYS[2]
local delayed = KEYS[3]

local id = ARGV[1]
local token = ARGV[2]

local currentToken = redis.call('HGET', msgKey, 'token')
if not currentToken or currentToken == '' then
	return { '` + scriptErrTokenMissing + `', id }
end
if currentToken ~= token then
	return { '` + scriptErrTokenMismatch + `', id }
end

local attempts = redis.call('HGET', msgKey, 'attempts') or '0'
local createdAt = redis.call('HGET', msgKey, 'created_at') or '0'

redis.call('ZREM', inflight, id)
redis.call('ZREM', delayed, id)
redis.call('DEL', msgKey)

return { attempts, createdAt }
`

	// NackScript atomically validates token and applies nack disposition.
	NackScript = `
local msgKey = KEYS[1]
local ready = KEYS[2]
local delayed = KEYS[3]
local inflight = KEYS[4]
local dlq = KEYS[5]

local id = ARGV[1]
local token = ARGV[2]
local now = tonumber(ARGV[3])
local disposition = ARGV[4]
local delay = tonumber(ARGV[5])
local reason = ARGV[6]

local currentToken = redis.call('HGET', msgKey, 'token')
if not currentToken or currentToken == '' then
	return { '` + scriptErrTokenMissing + `', id }
end
if currentToken ~= token then
	return { '` + scriptErrTokenMismatch + `', id }
end

local attempts = redis.call('HGET', msgKey, 'attempts') or '0'
local createdAt = redis.call('HGET', msgKey, 'created_at') or '0'

redis.call('ZREM', inflight, id)
redis.call('ZREM', delayed, id)

if disposition == 'dead_letter' then
	redis.call('HSET', msgKey, 'token', '', 'leased_at', '0', 'updated_at', tostring(now), 'last_error', reason, 'dead_lettered_at', tostring(now))
	redis.call('LPUSH', dlq, id)
	return { attempts, createdAt, '0' }
end

if disposition == 'retry' then
	local availableAt = now + delay
	redis.call('HSET', msgKey, 'token', '', 'leased_at', '0', 'updated_at', tostring(now), 'last_error', reason, 'available_at', tostring(availableAt))
	if delay > 0 then
		redis.call('ZADD', delayed, availableAt, id)
	else
		redis.call('LPUSH', ready, id)
	end
	return { attempts, createdAt, tostring(availableAt) }
end

redis.call('DEL', msgKey)
return { attempts, createdAt, '0' }
`

	// ExtendLeaseScript atomically validates token and extends the lease.
	ExtendLeaseScript = `
local msgKey = KEYS[1]
local inflight = KEYS[2]

local id = ARGV[1]
local token = ARGV[2]
local now = tonumber(ARGV[3])
local leaseUntil = tonumber(ARGV[4])

local currentToken = redis.call('HGET', msgKey, 'token')
if not currentToken or currentToken == '' then
	return { '` + scriptErrTokenMissing + `', id }
end
if currentToken ~= token then
	return { '` + scriptErrTokenMismatch + `', id }
end

local attempts = redis.call('HGET', msgKey, 'attempts') or '0'
local createdAt = redis.call('HGET', msgKey, 'created_at') or '0'

redis.call('ZADD', inflight, leaseUntil, id)
redis.call('HSET', msgKey, 'leased_at', tostring(now), 'updated_at', tostring(now))

return { attempts, createdAt }
`
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
		idFunc:            func() string { return randutil.Hex(16) },
		tokenFunc:         func() string { return randutil.Hex(16) },
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

	now := s.now().UTC()
	nowUnix := now.UnixNano()
	leaseUntil := now.Add(s.visibilityTimeout).UnixNano()

	raw, err := s.client.Eval(ctx, DequeueScript, []string{
		s.keys.ready(),
		s.keys.delayed(),
		s.keys.inflight(),
		s.keys.messagePrefix(),
		s.keys.leaseSeq(),
	}, nowUnix, s.releaseBatchSize, leaseUntil)
	if err != nil {
		return nil, queue.Receipt{}, err
	}

	values, err := parseEvalSlice(raw)
	if err != nil {
		return nil, queue.Receipt{}, err
	}
	if len(values) == 0 {
		return nil, queue.Receipt{}, nil
	}
	if len(values) < 7 {
		return nil, queue.Receipt{}, fmt.Errorf("invalid dequeue script response")
	}

	id, err := evalString(values[0])
	if err != nil {
		return nil, queue.Receipt{}, err
	}
	if id == scriptErrMissingPayload {
		msgID, _ := evalString(values[1])
		return nil, queue.Receipt{}, fmt.Errorf("message payload missing for %q", msgID)
	}

	payload, err := evalString(values[1])
	if err != nil {
		return nil, queue.Receipt{}, err
	}
	msg, err := queue.DecodeExecutionMessage([]byte(payload))
	if err != nil {
		return nil, queue.Receipt{}, err
	}

	attempts, err := evalInt(values[2])
	if err != nil {
		return nil, queue.Receipt{}, err
	}
	token, err := evalString(values[3])
	if err != nil {
		return nil, queue.Receipt{}, err
	}
	availableAtUnix, err := evalInt64(values[4])
	if err != nil {
		return nil, queue.Receipt{}, err
	}
	createdAtUnix, err := evalInt64(values[5])
	if err != nil {
		return nil, queue.Receipt{}, err
	}
	lastError, err := evalString(values[6])
	if err != nil {
		return nil, queue.Receipt{}, err
	}

	availableAt := timeutil.UnixNanoTime(availableAtUnix)
	createdAt := timeutil.UnixNanoTime(createdAtUnix)

	if err := s.writeStatus(ctx, statusRecord{
		DispatchID:     id,
		State:          queue.DispatchStateRunning,
		Attempt:        attempts,
		EnqueuedAt:     createdAt,
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
		AvailableAt: availableAt,
		CreatedAt:   createdAt,
		LastError:   lastError,
	}, nil
}

// Ack removes the message from in-flight tracking and deletes its payload.
func (s *Storage) Ack(ctx context.Context, receipt queue.Receipt) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("redis storage not configured")
	}
	if receipt.ID == "" || receipt.Token == "" {
		return fmt.Errorf("receipt id and token required")
	}

	raw, err := s.client.Eval(ctx, AckScript, []string{
		s.keys.message(receipt.ID),
		s.keys.inflight(),
		s.keys.delayed(),
	}, receipt.ID, receipt.Token)
	if err != nil {
		return err
	}
	values, err := parseEvalSlice(raw)
	if err != nil {
		return err
	}
	if len(values) < 2 {
		return fmt.Errorf("invalid ack script response")
	}

	first, err := evalString(values[0])
	if err != nil {
		return err
	}
	switch first {
	case scriptErrTokenMissing:
		return fmt.Errorf("receipt token missing for %q", receipt.ID)
	case scriptErrTokenMismatch:
		return fmt.Errorf("receipt token mismatch for %q", receipt.ID)
	}

	attempts, err := strconv.Atoi(first)
	if err != nil {
		return fmt.Errorf("invalid ack attempts value: %w", err)
	}
	createdAtUnix, err := evalInt64(values[1])
	if err != nil {
		return err
	}

	now := s.now().UTC()
	enqueuedAt := timeutil.UnixNanoTime(createdAtUnix)
	if enqueuedAt.IsZero() {
		enqueuedAt = receipt.CreatedAt.UTC()
	}
	if enqueuedAt.IsZero() {
		enqueuedAt = now
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
	if receipt.ID == "" || receipt.Token == "" {
		return fmt.Errorf("receipt id and token required")
	}
	if err := queue.ValidateNackOptions(opts); err != nil {
		return err
	}

	now := s.now().UTC()
	reason := strings.TrimSpace(opts.Reason)
	delay := int64(0)
	if opts.Delay > 0 {
		delay = opts.Delay.Nanoseconds()
	}

	raw, err := s.client.Eval(ctx, NackScript, []string{
		s.keys.message(receipt.ID),
		s.keys.ready(),
		s.keys.delayed(),
		s.keys.inflight(),
		s.keys.dlq(),
	}, receipt.ID, receipt.Token, now.UnixNano(), string(opts.Disposition), delay, reason)
	if err != nil {
		return err
	}
	values, err := parseEvalSlice(raw)
	if err != nil {
		return err
	}
	if len(values) < 3 {
		return fmt.Errorf("invalid nack script response")
	}

	first, err := evalString(values[0])
	if err != nil {
		return err
	}
	switch first {
	case scriptErrTokenMissing:
		return fmt.Errorf("receipt token missing for %q", receipt.ID)
	case scriptErrTokenMismatch:
		return fmt.Errorf("receipt token mismatch for %q", receipt.ID)
	}
	attempts, err := strconv.Atoi(first)
	if err != nil {
		return fmt.Errorf("invalid nack attempts value: %w", err)
	}
	createdAtUnix, err := evalInt64(values[1])
	if err != nil {
		return err
	}
	availableAtUnix, err := evalInt64(values[2])
	if err != nil {
		return err
	}
	enqueuedAt := timeutil.UnixNanoTime(createdAtUnix)

	if opts.Disposition == queue.NackDispositionDeadLetter {
		return s.writeStatus(ctx, statusRecord{
			DispatchID:     receipt.ID,
			State:          queue.DispatchStateDeadLetter,
			Attempt:        attempts,
			EnqueuedAt:     enqueuedAt,
			UpdatedAt:      now,
			NextRunAt:      time.Time{},
			TerminalReason: reason,
		})
	}

	if opts.Disposition == queue.NackDispositionRetry {
		availableAt := timeutil.UnixNanoTime(availableAtUnix)
		state := queue.DispatchStateAccepted
		nextRunAt := time.Time{}
		if opts.Delay > 0 {
			state = queue.DispatchStateRetrying
			nextRunAt = availableAt
		}
		return s.writeStatus(ctx, statusRecord{
			DispatchID:     receipt.ID,
			State:          state,
			Attempt:        attempts,
			EnqueuedAt:     enqueuedAt,
			UpdatedAt:      now,
			NextRunAt:      nextRunAt,
			TerminalReason: reason,
		})
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
		TerminalReason: reason,
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
	status.EnqueuedAt = timeutil.PtrTime(timeutil.UnixNanoTime(parseInt64(fields[statusFieldEnqueued])))
	status.UpdatedAt = timeutil.PtrTime(timeutil.UnixNanoTime(parseInt64(fields[statusFieldUpdated])))
	status.NextRunAt = timeutil.PtrTime(timeutil.UnixNanoTime(parseInt64(fields[statusFieldNextRunAt])))
	status.TerminalReason = strings.TrimSpace(fields[statusFieldReason])
	return status, nil
}

// ExtendLease extends the in-flight lease for a delivery receipt.
func (s *Storage) ExtendLease(ctx context.Context, receipt queue.Receipt, ttl time.Duration) error {
	if s == nil || s.client == nil {
		return fmt.Errorf("redis storage not configured")
	}
	if receipt.ID == "" || receipt.Token == "" {
		return fmt.Errorf("receipt id and token required")
	}
	if ttl <= 0 {
		ttl = s.visibilityTimeout
	}
	now := s.now().UTC()
	until := now.Add(ttl).UnixNano()

	raw, err := s.client.Eval(ctx, ExtendLeaseScript, []string{
		s.keys.message(receipt.ID),
		s.keys.inflight(),
	}, receipt.ID, receipt.Token, now.UnixNano(), until)
	if err != nil {
		return err
	}
	values, err := parseEvalSlice(raw)
	if err != nil {
		return err
	}
	if len(values) < 2 {
		return fmt.Errorf("invalid lease extension script response")
	}

	first, err := evalString(values[0])
	if err != nil {
		return err
	}
	switch first {
	case scriptErrTokenMissing:
		return fmt.Errorf("receipt token missing for %q", receipt.ID)
	case scriptErrTokenMismatch:
		return fmt.Errorf("receipt token mismatch for %q", receipt.ID)
	}
	attempts, err := strconv.Atoi(first)
	if err != nil {
		return fmt.Errorf("invalid lease extension attempts value: %w", err)
	}
	createdAtUnix, err := evalInt64(values[1])
	if err != nil {
		return err
	}

	enqueuedAt := timeutil.UnixNanoTime(createdAtUnix)
	if enqueuedAt.IsZero() {
		enqueuedAt = receipt.CreatedAt.UTC()
	}
	if enqueuedAt.IsZero() {
		enqueuedAt = now
	}
	return s.writeStatus(ctx, statusRecord{
		DispatchID:     receipt.ID,
		State:          queue.DispatchStateRunning,
		Attempt:        attempts,
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
		enqueuedAt := timeutil.UnixNanoTime(parseInt64(fields[fieldCreatedAt]))
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

func parseEvalSlice(raw any) ([]any, error) {
	if raw == nil {
		return nil, nil
	}
	switch values := raw.(type) {
	case []any:
		return values, nil
	default:
		return nil, fmt.Errorf("unexpected redis eval response type %T", raw)
	}
}

func evalString(raw any) (string, error) {
	switch value := raw.(type) {
	case nil:
		return "", nil
	case string:
		return value, nil
	case []byte:
		return string(value), nil
	case fmt.Stringer:
		return value.String(), nil
	default:
		return fmt.Sprint(value), nil
	}
}

func evalInt(raw any) (int, error) {
	value, err := evalInt64(raw)
	if err != nil {
		return 0, err
	}
	return int(value), nil
}

func evalInt64(raw any) (int64, error) {
	switch value := raw.(type) {
	case nil:
		return 0, nil
	case int64:
		return value, nil
	case int:
		return int64(value), nil
	case uint64:
		return int64(value), nil
	case uint:
		return int64(value), nil
	case float64:
		return int64(value), nil
	case string:
		if value == "" {
			return 0, nil
		}
		parsed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	case []byte:
		if len(value) == 0 {
			return 0, nil
		}
		parsed, err := strconv.ParseInt(string(value), 10, 64)
		if err != nil {
			return 0, err
		}
		return parsed, nil
	default:
		return 0, fmt.Errorf("unsupported numeric redis value type %T", raw)
	}
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
