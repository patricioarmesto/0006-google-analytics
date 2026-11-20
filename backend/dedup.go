package main

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// Deduper es chico a propósito: el collector no necesita conocer Redis para
// ser testeable. En producción lo implementa Redis; en tests, un fake en memoria.
type Deduper interface {
	SetNX(ctx context.Context, key string, value any, ttl time.Duration) (bool, error)
	Delete(ctx context.Context, keys ...string) error
}

type RedisDeduper struct{ client *redis.Client }

func NewRedisDeduper(client *redis.Client) RedisDeduper { return RedisDeduper{client: client} }

func (d RedisDeduper) SetNX(ctx context.Context, key string, value any, ttl time.Duration) (bool, error) {
	return d.client.SetNX(ctx, key, value, ttl).Result()
}

func (d RedisDeduper) Delete(ctx context.Context, keys ...string) error {
	return d.client.Del(ctx, keys...).Err()
}
