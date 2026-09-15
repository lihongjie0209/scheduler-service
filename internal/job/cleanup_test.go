package job

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/lihongjie0209/scheduler-service/internal/config"
	"github.com/lihongjie0209/scheduler-service/internal/database"
	"go.uber.org/fx/fxtest"
)

type executionRetentionRepository struct {
	Repository
	counts []int64
	before []time.Time
}

func (r *executionRetentionRepository) SoftDeleteTerminalExecutionsBefore(_ context.Context, _ sqlx.ExtContext, before time.Time, _ int, fields AuditFields) (int64, error) {
	r.before = append(r.before, before)
	if fields.UpdatedBy != "scheduler-service" {
		return 0, errors.New("missing system audit actor")
	}
	count := r.counts[0]
	r.counts = r.counts[1:]
	return count, nil
}

func TestExecutionCleanerSoftDeletesInBoundedBatches(t *testing.T) {
	t.Parallel()

	repository := &executionRetentionRepository{counts: []int64{2, 1}}
	transactor, mock := testTransactor(t)
	mock.ExpectBegin()
	mock.ExpectCommit()
	mock.ExpectBegin()
	mock.ExpectCommit()
	cleaner, err := NewExecutionCleaner(
		fxtest.NewLifecycle(t),
		repository,
		transactor,
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		config.Config{Database: config.Database{Enabled: true}, Cron: config.Cron{ExecutionRetention: 90 * 24 * time.Hour, ExecutionCleanupInterval: time.Hour, ExecutionCleanupBatchSize: 2}},
	)
	if err != nil {
		t.Fatalf("NewExecutionCleaner() error = %v", err)
	}
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	cleaner.now = func() time.Time { return now }

	if err := cleaner.clean(t.Context()); err != nil {
		t.Fatalf("clean() error = %v", err)
	}
	if len(repository.before) != 2 {
		t.Fatalf("delete calls = %d, want 2", len(repository.before))
	}
	if want := now.Add(-90 * 24 * time.Hour); !repository.before[0].Equal(want) {
		t.Fatalf("cutoff = %v, want %v", repository.before[0], want)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestNewExecutionCleanerAppliesSafeDefaults(t *testing.T) {
	t.Parallel()

	transactor, _ := testTransactor(t)
	cleaner, err := NewExecutionCleaner(fxtest.NewLifecycle(t), &executionRetentionRepository{}, transactor, slog.Default(), config.Config{})
	if err != nil {
		t.Fatalf("NewExecutionCleaner() error = %v", err)
	}
	if cleaner.retention != 90*24*time.Hour || cleaner.interval != time.Hour || cleaner.batchSize != 500 {
		t.Fatalf("unexpected defaults: retention=%v interval=%v batch=%d", cleaner.retention, cleaner.interval, cleaner.batchSize)
	}
}

func testTransactor(t *testing.T) (*database.Transactor, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return database.NewTransactor(sqlx.NewDb(db, "sqlmock")), mock
}
