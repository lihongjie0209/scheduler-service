//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/lihongjie0209/microservice-platform-go/distlock"
	"github.com/lihongjie0209/scheduler-service/internal/cache"
	"github.com/lihongjie0209/scheduler-service/internal/config"
	"github.com/lihongjie0209/scheduler-service/internal/idempotency"
	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
)

func TestRedisLockAndIdempotency(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	container, err := rediscontainer.Run(ctx, "redis:7.4-alpine")
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupContainer(t, container)
	connectionString, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	options, err := goredis.ParseURL(connectionString)
	if err != nil {
		t.Fatal(err)
	}
	client := goredis.NewClient(options)
	t.Cleanup(func() { _ = client.Close() })

	locker := cache.NewLocker(client)
	lock, acquired, err := locker.TryLock(ctx, "integration", 10*time.Second)
	if err != nil || !acquired {
		t.Fatalf("first lock acquired=%v err=%v", acquired, err)
	}
	_, secondAcquired, err := locker.TryLock(ctx, "integration", 10*time.Second)
	if err != nil || secondAcquired {
		t.Fatalf("competing lock acquired=%v err=%v", secondAcquired, err)
	}
	if err := lock.Unlock(ctx); err != nil {
		t.Fatal(err)
	}
	renewalStarted := make(chan struct{})
	renewalDone := make(chan error, 1)
	go func() {
		_, renewalErr := distlock.TryWithLock(ctx, locker, "renewal", 300*time.Millisecond, func(leaseCtx context.Context) error {
			close(renewalStarted)
			timer := time.NewTimer(700 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-leaseCtx.Done():
				return context.Cause(leaseCtx)
			case <-timer.C:
				return nil
			}
		})
		renewalDone <- renewalErr
	}()
	<-renewalStarted
	time.Sleep(500 * time.Millisecond)
	if _, acquired, err := locker.TryLock(ctx, "renewal", time.Second); err != nil || acquired {
		t.Fatalf("renewed lock contention acquired=%v err=%v", acquired, err)
	}
	if err := <-renewalDone; err != nil {
		t.Fatal(err)
	}

	manager := idempotency.New(client, config.Config{Idempotency: config.Idempotency{Enabled: true, ProcessingTTL: time.Minute, ResultTTL: time.Hour, FailureTTL: time.Minute}})
	first, err := manager.Begin(ctx, "request-0001", "fingerprint-a")
	if err != nil || first.State != idempotency.StateAcquired {
		t.Fatalf("begin = %+v, %v", first, err)
	}
	processing, err := manager.Begin(ctx, "request-0001", "fingerprint-a")
	if err != nil || processing.State != idempotency.StateProcessing {
		t.Fatalf("processing = %+v, %v", processing, err)
	}
	conflict, err := manager.Begin(ctx, "request-0001", "fingerprint-b")
	if err != nil || conflict.State != idempotency.StateConflict {
		t.Fatalf("conflict = %+v, %v", conflict, err)
	}
	response := map[string]string{"id": "user-1"}
	if err := manager.Complete(ctx, "request-0001", first.Owner, response); err != nil {
		t.Fatal(err)
	}
	replay, err := manager.Begin(ctx, "request-0001", "fingerprint-a")
	if err != nil || replay.State != idempotency.StateCompleted {
		t.Fatalf("replay = %+v, %v", replay, err)
	}
}
