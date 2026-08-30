//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	schedulerv1 "github.com/lihongjie0209/platform-protos/gen/go/platform/scheduler/v1"
	"github.com/lihongjie0209/scheduler-service/internal/app"
	"github.com/lihongjie0209/scheduler-service/internal/auth"
	"github.com/lihongjie0209/scheduler-service/internal/config"
	goredis "github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	rediscontainer "github.com/testcontainers/testcontainers-go/modules/redis"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	grpc_health_v1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
)

func TestDynamicGRPCSchedulerEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	postgresContainer, err := postgres.Run(ctx, "postgres:17-alpine", postgres.WithDatabase("app"), postgres.WithUsername("app"), postgres.WithPassword("app"), postgres.BasicWaitStrategies(), postgres.WithSQLDriver("pgx"))
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupContainer(t, postgresContainer)
	dsn, err := postgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	redisContainer, err := rediscontainer.Run(ctx, "redis:7.4-alpine")
	if err != nil {
		t.Fatal(err)
	}
	testcontainers.CleanupContainer(t, redisContainer)
	redisURL, err := redisContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatal(err)
	}
	redisOptions, err := goredis.ParseURL(redisURL)
	if err != nil {
		t.Fatal(err)
	}

	upstreamAddress := freeAddress(t)
	upstreamListener, err := net.Listen("tcp", upstreamAddress)
	if err != nil {
		t.Fatal(err)
	}
	upstream := grpc.NewServer()
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(upstream, healthServer)
	reflection.Register(upstream)
	go func() { _ = upstream.Serve(upstreamListener) }()
	t.Cleanup(upstream.Stop)

	httpAddress, grpcAddress := freeAddress(t), freeAddress(t)
	const secret = "01234567890123456789012345678901"
	migrationPath, _ := filepath.Abs(filepath.Join("..", "migrations", "postgres"))
	cfg := config.Config{
		Runtime: config.Runtime{ActiveProfile: "integration"}, App: config.App{Name: "scheduler-service", Env: "integration", ShutdownTimeout: 10 * time.Second},
		HTTP: config.HTTP{Address: httpAddress, ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, RequestTimeout: 10 * time.Second, MaxBodyBytes: 1 << 20},
		GRPC: config.GRPC{Enabled: true, Address: grpcAddress, MaxReceiveBytes: 4 << 20}, Log: config.Log{Level: "error", Format: "json", File: filepath.Join(t.TempDir(), "app.log"), MaxSizeMB: 1, MaxBackups: 1, MaxAgeDays: 1},
		Database:  config.Database{Enabled: true, Type: "postgres", DSN: dsn, MaxOpenConns: 5, MaxIdleConns: 2, ConnMaxLifetime: time.Minute, ConnMaxIdleTime: time.Minute, PingTimeout: 10 * time.Second},
		Migration: config.Migration{AutoUp: true, Path: migrationPath, DatabaseURL: dsn, Table: "scheduler_e2e_schema_migrations"},
		Redis:     config.Redis{Enabled: true, Address: redisOptions.Addr, DB: redisOptions.DB, DialTimeout: 5 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second}, Health: config.Health{DatabaseTimeout: 2 * time.Second, RedisTimeout: 2 * time.Second}, Observability: config.Observability{MetricsEnabled: true},
		JWT: config.JWT{Issuer: "integration", Secret: secret, TTL: time.Hour}, Auth: config.Auth{ClientID: "client", ClientSecret: "secret", SkipHTTPPaths: []string{"/api/v1/version"}, SkipGRPCMethods: []string{"/grpc.health.v1.Health/*"}}, Cron: config.Cron{Enabled: false, Timezone: "Asia/Shanghai"},
		Idempotency: config.Idempotency{Enabled: true, ProcessingTTL: 30 * time.Second, ResultTTL: time.Hour, FailureTTL: time.Minute},
		Outbound:    config.Outbound{GRPC: map[string]config.GRPCUpstream{"health": {Target: upstreamAddress, Timeout: 5 * time.Second, Retry: config.Retry{MaxAttempts: 1, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond}, Breaker: config.Breaker{FailureThreshold: 3, OpenTimeout: time.Second}}}},
	}
	application := app.New(cfg)
	if err := application.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		_ = application.Stop(stopCtx)
	})
	token, err := auth.New(cfg).Issue("scheduler-admin")
	if err != nil {
		t.Fatal(err)
	}

	createBody := `{"name":"health","cron_expression":"0 0 0 * * *","timezone":"Asia/Shanghai","upstream":"health","full_method":"/grpc.health.v1.Health/Check","request_json":"{\"service\":\"\"}","timeout_milliseconds":5000,"enabled":false}`
	data, statusCode := postJSONBody(t, "http://"+httpAddress+"/api/v1/scheduler/jobs/create", "Bearer "+token, createBody)
	if statusCode != http.StatusOK {
		t.Fatalf("create status=%d body=%s", statusCode, data)
	}
	var created struct {
		Body struct {
			ID string `json:"id"`
		} `json:"body"`
	}
	if err := json.Unmarshal(data, &created); err != nil || created.Body.ID == "" {
		t.Fatalf("created=%+v err=%v", created, err)
	}
	data, statusCode = postJSONBody(t, "http://"+httpAddress+"/api/v1/scheduler/jobs/trigger", "Bearer "+token, `{"id":"`+created.Body.ID+`"}`)
	if statusCode != http.StatusOK {
		t.Fatalf("trigger status=%d body=%s", statusCode, data)
	}
	var execution struct {
		Body struct{ Status, ResponseJSON string } `json:"body"`
	}
	if err := json.Unmarshal(data, &execution); err != nil || execution.Body.Status != "succeeded" {
		t.Fatalf("execution=%+v err=%v", execution, err)
	}

	connection, err := grpc.NewClient(grpcAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	grpcCtx := metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
	listed, err := schedulerv1.NewSchedulerServiceClient(connection).ListJobs(grpcCtx, &schedulerv1.ListJobsRequest{Page: 1, PageSize: 20})
	if err != nil || listed.GetTotal() != 1 {
		t.Fatalf("ListJobs()=%v,%v", listed, err)
	}
}

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}
func postJSONBody(t *testing.T, target, authorization, body string) ([]byte, int) {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, target, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", authorization)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return data, response.StatusCode
}
