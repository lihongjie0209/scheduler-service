package cache

import (
	"context"
	"fmt"

	"github.com/lihongjie0209/microservice-platform-go/distlock"
	"github.com/lihongjie0209/scheduler-service/internal/config"
	"github.com/redis/go-redis/v9"
)

func Open(ctx context.Context, cfg config.Redis) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{Addr: cfg.Address, Username: cfg.Username, Password: cfg.Password, DB: cfg.DB, DialTimeout: cfg.DialTimeout, ReadTimeout: cfg.ReadTimeout, WriteTimeout: cfg.WriteTimeout})
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}
	return client, nil
}

type Locker = distlock.RedisLocker

func NewLocker(client redis.UniversalClient) *Locker {
	return distlock.NewRedisLocker(client)
}
