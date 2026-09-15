//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lihongjie0209/microservice-platform-go/principal"
	"github.com/lihongjie0209/scheduler-service/internal/config"
	appdb "github.com/lihongjie0209/scheduler-service/internal/database"
	"github.com/lihongjie0209/scheduler-service/internal/job"
	"github.com/lihongjie0209/scheduler-service/internal/migration"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mysql"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestRepositoryAndMigrations(t *testing.T) {
	for _, databaseType := range []string{"postgres", "mysql"} {
		t.Run(databaseType, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			dsn, migrationURL := startDatabase(t, ctx, databaseType)
			migrationPath, err := filepath.Abs(filepath.Join("..", "migrations", databaseType))
			if err != nil {
				t.Fatal(err)
			}
			schema := ""
			if databaseType == "postgres" {
				schema = "integration_postgres"
			}
			migrationCfg := config.Migration{Path: migrationPath, DatabaseURL: migrationURL, Table: "integration_" + databaseType + "_schema_migrations", Schema: schema, CreateSchema: schema != ""}
			migrationErrors := make(chan error, 3)
			var migrations sync.WaitGroup
			for range 3 {
				migrations.Add(1)
				go func() {
					defer migrations.Done()
					migrationErrors <- migration.Run(migrationCfg, "up", 0)
				}()
			}
			migrations.Wait()
			close(migrationErrors)
			for err := range migrationErrors {
				if err != nil {
					t.Fatalf("concurrent migration up: %v", err)
				}
			}

			db, err := appdb.Open(ctx, config.Database{Type: databaseType, DSN: dsn, Schema: schema, MaxOpenConns: 5, MaxIdleConns: 2, ConnMaxLifetime: time.Minute, ConnMaxIdleTime: time.Minute, PingTimeout: 10 * time.Second})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = db.Close() })
			repository := job.NewRepository(db)
			transactor := appdb.NewTransactor(db)
			auditCtx := principal.WithContext(ctx, principal.Principal{ID: "integration", Type: principal.TypeSystem})
			now := time.Now().Truncate(time.Microsecond)
			created := job.Job{ID: "job-" + databaseType, TenantID: "tenant-1", ApplicationID: "application-1", Name: "health", CronExpression: "0 0 0 * * *", Timezone: "Asia/Shanghai", Upstream: "health", FullMethod: "/grpc.health.v1.Health/Check", RequestJSON: `{}`, TimeoutMilliseconds: 5000, Status: "enabled", Version: 1, CreatedAt: now, UpdatedAt: now, CreatedBy: "integration", UpdatedBy: "integration"}
			if err := transactor.Within(auditCtx, nil, func(tx *sqlx.Tx) error { return repository.CreateJob(auditCtx, tx, created) }); err != nil {
				t.Fatal(err)
			}
			loaded, err := repository.GetJob(auditCtx, created.ID)
			if err != nil || loaded.CreatedBy != "integration" || loaded.Version != 1 {
				t.Fatalf("GetJob()=%+v,%v", loaded, err)
			}
			loaded.Name, loaded.UpdatedAt = "health-updated", now.Add(time.Second)
			if err := transactor.Within(auditCtx, nil, func(tx *sqlx.Tx) error { return repository.UpdateJob(auditCtx, tx, loaded, 1) }); err != nil {
				t.Fatal(err)
			}
			jobs, total, err := repository.ListJobs(auditCtx, job.JobFilter{TenantID: created.TenantID, ApplicationID: created.ApplicationID, Keyword: "HEALTH-UP", IDs: []string{created.ID}, Statuses: []string{"enabled"}, Upstreams: []string{"health"}, CreatedFrom: timePointer(now.Add(-time.Hour)), CreatedTo: timePointer(now.Add(time.Hour))}, 20, 0)
			if err != nil || total != 1 || len(jobs) != 1 || jobs[0].ID != created.ID {
				t.Fatalf("filtered ListJobs()=%+v total=%d err=%v", jobs, total, err)
			}
			if err := transactor.Within(auditCtx, nil, func(tx *sqlx.Tx) error { return repository.UpdateJob(auditCtx, tx, loaded, 1) }); err != job.ErrStaleVersion {
				t.Fatalf("stale update error=%v", err)
			}
			execution := job.Execution{ID: "execution-" + databaseType, JobID: created.ID, TenantID: created.TenantID, ApplicationID: created.ApplicationID, TriggerType: "manual", Status: "running", StartedAt: now, Version: 1, CreatedAt: now, UpdatedAt: now, CreatedBy: "integration", UpdatedBy: "integration"}
			if err := transactor.Within(auditCtx, nil, func(tx *sqlx.Tx) error { return repository.CreateExecution(auditCtx, tx, execution) }); err != nil {
				t.Fatal(err)
			}
			finished := now.Add(time.Second)
			execution.Status, execution.FinishedAt, execution.UpdatedAt, execution.DurationMilliseconds = "succeeded", &finished, finished, 1000
			if err := transactor.Within(auditCtx, nil, func(tx *sqlx.Tx) error { return repository.FinishExecution(auditCtx, tx, execution) }); err != nil {
				t.Fatal(err)
			}
			loadedExecution, err := repository.GetExecution(ctx, execution.ID)
			if err != nil || loadedExecution.Status != "succeeded" {
				t.Fatalf("GetExecution()=%+v,%v", loadedExecution, err)
			}
			minimum, maximum := int64(900), int64(1100)
			executions, total, err := repository.ListExecutions(auditCtx, job.ExecutionFilter{JobID: created.ID, IDs: []string{execution.ID}, Statuses: []string{"succeeded"}, TriggerTypes: []string{"manual"}, StartedFrom: timePointer(now.Add(-time.Hour)), StartedTo: timePointer(now.Add(time.Hour)), DurationMinMilliseconds: &minimum, DurationMaxMilliseconds: &maximum}, 20, 0)
			if err != nil || total != 1 || len(executions) != 1 || executions[0].ID != execution.ID {
				t.Fatalf("filtered ListExecutions()=%+v total=%d err=%v", executions, total, err)
			}
			cleanupAt := finished.Add(time.Hour)
			var cleaned int64
			if err := transactor.Within(auditCtx, nil, func(tx *sqlx.Tx) error {
				var cleanupErr error
				cleaned, cleanupErr = repository.SoftDeleteTerminalExecutionsBefore(auditCtx, tx, cleanupAt, 10, job.AuditFields{UpdatedAt: cleanupAt, UpdatedBy: "integration"})
				return cleanupErr
			}); err != nil {
				t.Fatal(err)
			}
			if cleaned != 1 {
				t.Fatalf("soft-deleted executions=%d, want 1", cleaned)
			}
			if _, err := repository.GetExecution(auditCtx, execution.ID); !errors.Is(err, job.ErrExecutionNotFound) {
				t.Fatalf("GetExecution() after retention error=%v, want not found", err)
			}
			if databaseType == "postgres" {
				if _, err := db.ExecContext(auditCtx, `DELETE FROM job_executions WHERE id=$1`, execution.ID); err == nil {
					t.Fatal("physical delete of audited execution unexpectedly succeeded")
				}
			}
			if err := transactor.Within(auditCtx, nil, func(tx *sqlx.Tx) error {
				return repository.DeleteJob(auditCtx, tx, created.ID, 2, job.AuditFields{UpdatedAt: cleanupAt, UpdatedBy: "integration"})
			}); err != nil {
				t.Fatal(err)
			}
			if _, err := repository.GetJob(auditCtx, created.ID); !errors.Is(err, job.ErrNotFound) {
				t.Fatalf("GetJob() after delete error=%v, want not found", err)
			}
			var userTables int
			if databaseType == "postgres" {
				if err := db.GetContext(ctx, &userTables, `SELECT count(*) FROM pg_tables WHERE schemaname = current_schema() AND tablename = 'users'`); err != nil {
					t.Fatal(err)
				}
				var timezone string
				if err := db.GetContext(ctx, &timezone, `SHOW TIMEZONE`); err != nil || timezone != "Asia/Shanghai" {
					t.Fatalf("timezone=%q err=%v", timezone, err)
				}
			} else if err := db.GetContext(ctx, &userTables, `SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'users'`); err != nil {
				t.Fatal(err)
			}
			if userTables != 0 {
				t.Fatal("generic template migration must not create a users table")
			}
			var filterIndexes int
			if databaseType == "postgres" {
				if err := db.GetContext(ctx, &filterIndexes, `SELECT count(*) FROM pg_indexes WHERE schemaname=current_schema() AND indexname IN ('scheduled_jobs_scope_created_idx','scheduled_jobs_scope_upstream_created_idx','scheduled_jobs_scope_status_created_idx','job_executions_job_status_started_idx')`); err != nil {
					t.Fatal(err)
				}
			} else if err := db.GetContext(ctx, &filterIndexes, `SELECT count(DISTINCT index_name) FROM information_schema.statistics WHERE table_schema=DATABASE() AND index_name IN ('scheduled_jobs_scope_created_idx','scheduled_jobs_scope_upstream_created_idx','scheduled_jobs_scope_status_created_idx','job_executions_job_status_started_idx')`); err != nil {
				t.Fatal(err)
			}
			if filterIndexes != 4 {
				t.Fatalf("filter indexes=%d, want 4", filterIndexes)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if err := migration.Run(migrationCfg, "down", 0); err != nil {
				t.Fatalf("migration down: %v", err)
			}
		})
	}
}

func timePointer(value time.Time) *time.Time { return &value }

func startDatabase(t *testing.T, ctx context.Context, databaseType string) (string, string) {
	t.Helper()
	switch databaseType {
	case "postgres":
		container, err := postgres.Run(ctx, "postgres:17-alpine", postgres.WithDatabase("app"), postgres.WithUsername("app"), postgres.WithPassword("app"), postgres.BasicWaitStrategies(), postgres.WithSQLDriver("pgx"))
		if err != nil {
			t.Fatal(err)
		}
		testcontainers.CleanupContainer(t, container)
		dsn, err := container.ConnectionString(ctx, "sslmode=disable")
		if err != nil {
			t.Fatal(err)
		}
		return dsn, dsn
	case "mysql":
		container, err := mysql.Run(ctx, "mysql:8.4", mysql.WithDatabase("app"), mysql.WithUsername("app"), mysql.WithPassword("app"))
		if err != nil {
			t.Fatal(err)
		}
		testcontainers.CleanupContainer(t, container)
		dsn, err := container.ConnectionString(ctx, "parseTime=true")
		if err != nil {
			t.Fatal(err)
		}
		migrationDSN, err := container.ConnectionString(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return dsn, "mysql://" + migrationDSN
	default:
		t.Fatal(fmt.Errorf("unsupported database %q", databaseType))
		return "", ""
	}
}
