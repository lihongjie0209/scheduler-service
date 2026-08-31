package job

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/lihongjie0209/scheduler-service/internal/config"
	"go.uber.org/fx/fxtest"
)

type executionRetentionRepository struct {
	Repository
	counts []int64
	before []time.Time
}

func (r *executionRetentionRepository) DeleteTerminalExecutionsBefore(_ context.Context, before time.Time, _ int) (int64, error) {
	r.before = append(r.before, before)
	count := r.counts[0]
	r.counts = r.counts[1:]
	return count, nil
}

func TestExecutionCleanerDeletesInBoundedBatches(t *testing.T) {
	t.Parallel()

	repository := &executionRetentionRepository{counts: []int64{2, 1}}
	cleaner, err := NewExecutionCleaner(
		fxtest.NewLifecycle(t),
		repository,
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
}

func TestNewExecutionCleanerAppliesSafeDefaults(t *testing.T) {
	t.Parallel()

	cleaner, err := NewExecutionCleaner(fxtest.NewLifecycle(t), &executionRetentionRepository{}, slog.Default(), config.Config{})
	if err != nil {
		t.Fatalf("NewExecutionCleaner() error = %v", err)
	}
	if cleaner.retention != 90*24*time.Hour || cleaner.interval != time.Hour || cleaner.batchSize != 500 {
		t.Fatalf("unexpected defaults: retention=%v interval=%v batch=%d", cleaner.retention, cleaner.interval, cleaner.batchSize)
	}
}
