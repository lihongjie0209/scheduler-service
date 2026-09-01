package job

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/lihongjie0209/scheduler-service/internal/config"
	"github.com/lihongjie0209/scheduler-service/internal/observability"
	"github.com/robfig/cron/v3"
	"go.uber.org/fx"
)

type Runtime struct {
	repository Repository
	service    *Service
	metrics    *observability.Metrics
	logger     *slog.Logger
	runner     *cron.Cron
	entries    map[string]cron.EntryID
	mu         sync.Mutex
	cancel     context.CancelFunc
	done       chan struct{}
}

func NewRuntime(lifecycle fx.Lifecycle, cfg config.Config, repository Repository, service *Service, metrics *observability.Metrics, logger *slog.Logger) *Runtime {
	runtime := &Runtime{repository: repository, service: service, metrics: metrics, logger: logger, runner: cron.New(cron.WithSeconds(), cron.WithChain(cron.Recover(cron.PrintfLogger(slogWriter{logger})), cron.SkipIfStillRunning(cron.PrintfLogger(slogWriter{logger})))), entries: map[string]cron.EntryID{}, done: make(chan struct{})}
	lifecycle.Append(fx.Hook{OnStart: func(ctx context.Context) error {
		if !cfg.Cron.Enabled {
			logger.Warn("dynamic scheduler is disabled")
			return nil
		}
		if err := runtime.reload(ctx); err != nil {
			return err
		}
		runCtx, cancel := context.WithCancel(context.Background())
		runtime.cancel = cancel
		runtime.runner.Start()
		go runtime.watch(runCtx)
		logger.Info("dynamic scheduler started", "jobs", len(runtime.entries))
		return nil
	}, OnStop: runtime.stop})
	return runtime
}
func (r *Runtime) watch(ctx context.Context) {
	defer close(r.done)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.service.Changes():
			r.reloadLogged(ctx)
		case <-ticker.C:
			r.reloadLogged(ctx)
		}
	}
}
func (r *Runtime) reloadLogged(ctx context.Context) {
	if err := r.reload(ctx); err != nil {
		r.logger.Error("reload scheduled jobs failed", "error", err)
	}
}
func (r *Runtime) reload(ctx context.Context) error {
	values, err := r.repository.ListEnabled(ctx)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, entryID := range r.entries {
		r.runner.Remove(entryID)
	}
	r.entries = make(map[string]cron.EntryID, len(values))
	for _, value := range values {
		value := value
		entryID, err := r.runner.AddFunc("CRON_TZ="+value.Timezone+" "+value.CronExpression, func() {
			started := time.Now()
			result := "success"
			if _, err := r.service.ExecuteScheduled(context.Background(), value); err != nil {
				result = "error"
				r.logger.Error("scheduled job execution failed", "job_id", value.ID, "method", value.FullMethod, "error", err)
			}
			r.metrics.ObserveCron(value.ID, result, started)
		})
		if err != nil {
			return err
		}
		r.entries[value.ID] = entryID
	}
	return nil
}
func (r *Runtime) stop(ctx context.Context) error {
	if r.cancel == nil {
		return nil
	}
	r.cancel()
	stopped := r.runner.Stop()
	select {
	case <-r.done:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-stopped.Done():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type slogWriter struct{ logger *slog.Logger }

func (w slogWriter) Printf(format string, args ...any) {
	w.logger.Error("scheduler runtime event", "detail", format, "args", args)
}

var Module = fx.Module("scheduled-jobs", fx.Provide(NewRepository, NewDynamicInvoker, NewRuntimeService, NewRuntime, NewExecutionCleaner), fx.Invoke(func(*Runtime, *ExecutionCleaner) {}))
