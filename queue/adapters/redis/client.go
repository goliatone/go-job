package redis

import "context"

// ZItem represents a sorted set entry.
type ZItem struct {
	Member string
	Score  float64
}

// Client defines the Redis operations needed by the adapter.
type Client interface {
	HSet(ctx context.Context, key string, values map[string]string) error
	HGetAll(ctx context.Context, key string) (map[string]string, error)
	HGet(ctx context.Context, key, field string) (string, error)
	HDel(ctx context.Context, key string, fields ...string) error
	LPush(ctx context.Context, key string, values ...string) error
	RPop(ctx context.Context, key string) (string, error)
	ZAdd(ctx context.Context, key string, score float64, member string) error
	ZRem(ctx context.Context, key string, members ...string) error
	ZRangeByScore(ctx context.Context, key string, max float64, limit int64) ([]ZItem, error)
	Del(ctx context.Context, keys ...string) error
}
