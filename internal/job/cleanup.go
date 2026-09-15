package job

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/lihongjie0209/microservice-platform-go/principal"
	"github.com/lihongjie0209/scheduler-service/internal/config"
	"github.com/lihongjie0209/scheduler-service/internal/database"
	"go.uber.org/fx"
)

type ExecutionCleaner struct {
	repository Repository
	transactor *database.Transactor
	logger     *slog.Logger
	retention  time.Duration
	interval   time.Duration
	batchSize  int
	enabled    bool
	now        func() time.Time
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

func NewExecutionCleaner(lifecycle fx.Lifecycle, repository Repository, transactor *database.Transactor, logger *slog.Logger, cfg config.Config) (*ExecutionCleaner, error) {
	if repository == nil || transactor == nil || logger == nil {
		return nil, errors.New("scheduler execution cleaner dependencies are required")
	}
	if cfg.Cron.ExecutionRetention <= 0 {
		cfg.Cron.ExecutionRetention = 90 * 24 * time.Hour
	}
	if cfg.Cron.ExecutionCleanupInterval <= 0 {
		cfg.Cron.ExecutionCleanupInterval = time.Hour
	}
	if cfg.Cron.ExecutionCleanupBatchSize <= 0 {
		cfg.Cron.ExecutionCleanupBatchSize = 500
	}
	cleaner := &ExecutionCleaner{
		repository: repository,
		transactor: transactor,
		logger:     logger,
		retention:  cfg.Cron.ExecutionRetention,
		interval:   cfg.Cron.ExecutionCleanupInterval,
		batchSize:  cfg.Cron.ExecutionCleanupBatchSize,
		enabled:    cfg.Database.Enabled,
		now:        time.Now,
	}
	lifecycle.Append(fx.Hook{OnStart: cleaner.start, OnStop: cleaner.stop})
	return cleaner, nil
}

func (c *ExecutionCleaner) clean(ctx context.Context) error {
	for {
		now := c.now()
		auditCtx := principal.WithContext(ctx, principal.Principal{ID: "scheduler-service", Type: principal.TypeSystem})
		var deleted int64
		err := c.transactor.Within(auditCtx, &sql.TxOptions{}, func(tx *sqlx.Tx) error {
			var deleteErr error
			deleted, deleteErr = c.repository.SoftDeleteTerminalExecutionsBefore(auditCtx, tx, now.Add(-c.retention), c.batchSize, AuditFields{UpdatedAt: now, UpdatedBy: "scheduler-service"})
			return deleteErr
		})
		if err != nil {
			return err
		}
		if deleted > 0 {
			c.logger.InfoContext(ctx, "deleted expired scheduler executions", "count", deleted)
		}
		if deleted < int64(c.batchSize) {
			return nil
		}
	}
}

func (c *ExecutionCleaner) start(context.Context) error {
	if !c.enabled {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.interval)
		defer ticker.Stop()
		for {
			if err := c.clean(ctx); err != nil && !errors.Is(err, context.Canceled) {
				c.logger.ErrorContext(ctx, "clean expired scheduler executions", "error", err)
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return nil
}

func (c *ExecutionCleaner) stop(context.Context) error {
	if c.cancel != nil {
		c.cancel()
		c.wg.Wait()
	}
	return nil
}
