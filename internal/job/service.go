package job

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lihongjie0209/microservice-platform-go/appaccess"
	"github.com/lihongjie0209/microservice-platform-go/principal"
	"github.com/lihongjie0209/scheduler-service/internal/apperror"
	"github.com/lihongjie0209/scheduler-service/internal/cache"
	"github.com/lihongjie0209/scheduler-service/internal/database"
	"github.com/lihongjie0209/scheduler-service/internal/idempotency"
	"github.com/lihongjie0209/scheduler-service/internal/requestid"
	"github.com/robfig/cron/v3"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Service struct {
	repository   Repository
	transactor   *database.Transactor
	invoker      Invoker
	locker       *cache.Locker
	now          func() time.Time
	changed      chan struct{}
	applications appaccess.Verifier
}

type allowAllApplications struct{}

func (allowAllApplications) Verify(context.Context, string, string) error { return nil }

func NewService(repository Repository, transactor *database.Transactor, invoker Invoker, locker *cache.Locker) *Service {
	return &Service{repository: repository, transactor: transactor, invoker: invoker, locker: locker, applications: allowAllApplications{}, now: time.Now, changed: make(chan struct{}, 1)}
}
func NewRuntimeService(repository Repository, transactor *database.Transactor, invoker Invoker, locker *cache.Locker, applications appaccess.Verifier) (*Service, error) {
	if applications == nil {
		return nil, errors.New("application verifier is required")
	}
	service := NewService(repository, transactor, invoker, locker)
	service.applications = applications
	return service, nil
}
func (s *Service) Changes() <-chan struct{} { return s.changed }
func (s *Service) signalChanged() {
	select {
	case s.changed <- struct{}{}:
	default:
	}
}

func (s *Service) Create(ctx context.Context, input Input) (Job, error) {
	actor, err := actorFromContext(ctx)
	if err != nil {
		return Job{}, err
	}
	input.TenantID, input.ApplicationID = strings.TrimSpace(input.TenantID), strings.TrimSpace(input.ApplicationID)
	if err := s.authorizeScope(ctx, input.TenantID, input.ApplicationID); err != nil {
		return Job{}, err
	}
	input, err = normalizeAndValidate(ctx, s.invoker, input)
	if err != nil {
		return Job{}, err
	}
	now := s.now()
	value := Job{ID: uuid.NewString(), TenantID: input.TenantID, ApplicationID: input.ApplicationID, Name: input.Name, CronExpression: input.CronExpression, Timezone: input.Timezone, Upstream: input.Upstream, FullMethod: input.FullMethod, RequestJSON: input.RequestJSON, TimeoutMilliseconds: input.TimeoutMilliseconds, Status: statusFromEnabled(input.Enabled), Version: 1, CreatedAt: now, UpdatedAt: now, CreatedBy: actor, UpdatedBy: actor}
	if err := s.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error { return s.repository.CreateJob(ctx, tx, value) }); err != nil {
		return Job{}, translate(err)
	}
	s.signalChanged()
	return value, nil
}
func (s *Service) Update(ctx context.Context, id string, input Input, expected int64) (Job, error) {
	if expected < 1 {
		return Job{}, apperror.Invalid("version must be positive", nil)
	}
	actor, err := actorFromContext(ctx)
	if err != nil {
		return Job{}, err
	}
	current, err := s.repository.GetJob(ctx, strings.TrimSpace(id))
	if err != nil {
		return Job{}, translate(err)
	}
	if err := s.authorizeScope(ctx, current.TenantID, current.ApplicationID); err != nil {
		return Job{}, err
	}
	input.TenantID, input.ApplicationID = current.TenantID, current.ApplicationID
	input, err = normalizeAndValidate(ctx, s.invoker, input)
	if err != nil {
		return Job{}, err
	}
	current.Name, current.CronExpression, current.Timezone, current.Upstream, current.FullMethod, current.RequestJSON, current.TimeoutMilliseconds, current.Status = input.Name, input.CronExpression, input.Timezone, input.Upstream, input.FullMethod, input.RequestJSON, input.TimeoutMilliseconds, statusFromEnabled(input.Enabled)
	current.UpdatedAt, current.UpdatedBy = s.now(), actor
	if err := s.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error { return s.repository.UpdateJob(ctx, tx, current, expected) }); err != nil {
		return Job{}, translate(err)
	}
	current.Version = expected + 1
	s.signalChanged()
	return current, nil
}
func (s *Service) Delete(ctx context.Context, id string, expected int64) error {
	if expected < 1 {
		return apperror.Invalid("version must be positive", nil)
	}
	actor, err := actorFromContext(ctx)
	if err != nil {
		return err
	}
	current, err := s.repository.GetJob(ctx, strings.TrimSpace(id))
	if err != nil {
		return translate(err)
	}
	if err := s.authorizeScope(ctx, current.TenantID, current.ApplicationID); err != nil {
		return err
	}
	err = s.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error {
		return s.repository.DeleteJob(ctx, tx, strings.TrimSpace(id), expected, timeFields{UpdatedAt: s.now(), UpdatedBy: actor})
	})
	if err == nil {
		s.signalChanged()
	}
	return translate(err)
}
func (s *Service) Get(ctx context.Context, id string) (Job, error) {
	value, err := s.repository.GetJob(ctx, strings.TrimSpace(id))
	if err != nil {
		return Job{}, translate(err)
	}
	if err := s.authorizeScope(ctx, value.TenantID, value.ApplicationID); err != nil {
		return Job{}, err
	}
	return value, nil
}
func (s *Service) List(ctx context.Context, tenantID, applicationID, statusValue string, page, pageSize int) (Page[Job], error) {
	if err := s.authorizeScope(ctx, tenantID, applicationID); err != nil {
		return Page[Job]{}, err
	}
	page, pageSize, err := pagination(page, pageSize)
	if err != nil {
		return Page[Job]{}, err
	}
	statusValue = strings.ToLower(strings.TrimSpace(statusValue))
	if statusValue != "" && statusValue != "enabled" && statusValue != "disabled" {
		return Page[Job]{}, apperror.Invalid("status must be enabled or disabled", nil)
	}
	values, total, err := s.repository.ListJobs(ctx, strings.TrimSpace(tenantID), strings.TrimSpace(applicationID), statusValue, pageSize, (page-1)*pageSize)
	return Page[Job]{Items: values, Total: total, Page: page, PageSize: pageSize}, translate(err)
}
func (s *Service) Trigger(ctx context.Context, id string) (Execution, error) {
	actor, err := actorFromContext(ctx)
	if err != nil {
		return Execution{}, err
	}
	value, err := s.repository.GetJob(ctx, strings.TrimSpace(id))
	if err != nil {
		return Execution{}, translate(err)
	}
	if err := s.authorizeScope(ctx, value.TenantID, value.ApplicationID); err != nil {
		return Execution{}, err
	}
	return s.execute(ctx, value, "manual", actor)
}
func (s *Service) ExecuteScheduled(ctx context.Context, value Job) (Execution, error) {
	if err := s.verifyApplication(ctx, value.TenantID, value.ApplicationID); err != nil {
		return Execution{}, err
	}
	return s.execute(ctx, value, "scheduled", "scheduler-service")
}
func (s *Service) execute(parent context.Context, value Job, triggerType, actor string) (Execution, error) {
	if s.locker == nil {
		return Execution{}, apperror.Unavailable("distributed scheduler lock is unavailable", nil)
	}
	timeout := time.Duration(value.TimeoutMilliseconds) * time.Millisecond
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	lock, acquired, err := s.locker.TryLock(ctx, "scheduler:job:"+value.ID, timeout+30*time.Second)
	if err != nil {
		return Execution{}, apperror.Unavailable("acquire scheduled job lock", err)
	}
	if !acquired {
		return Execution{}, apperror.Conflict("scheduled job is already running", nil)
	}
	defer func() {
		unlockCtx, unlockCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer unlockCancel()
		_ = lock.Unlock(unlockCtx)
	}()
	started := s.now()
	execution := Execution{ID: uuid.NewString(), JobID: value.ID, TenantID: value.TenantID, ApplicationID: value.ApplicationID, TriggerType: triggerType, Status: "running", StartedAt: started, Version: 1, CreatedAt: started, UpdatedAt: started, CreatedBy: actor, UpdatedBy: actor}
	if err := s.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error { return s.repository.CreateExecution(ctx, tx, execution) }); err != nil {
		return Execution{}, translate(err)
	}
	ctx = idempotency.WithContext(ctx, execution.ID)
	if _, ok := requestid.FromContext(ctx); !ok {
		ctx = requestid.WithContext(ctx, execution.ID)
	}
	ctx = metadata.AppendToOutgoingContext(ctx, "x-tenant-id", value.TenantID, "x-application-id", value.ApplicationID)
	response, invokeErr := s.invoker.Invoke(ctx, value.Upstream, value.FullMethod, value.RequestJSON)
	finished := s.now()
	execution.FinishedAt = &finished
	execution.DurationMilliseconds = finished.Sub(started).Milliseconds()
	execution.UpdatedAt = finished
	if invokeErr != nil {
		execution.Status, execution.ErrorCode, execution.ErrorMessage = "failed", status.Code(invokeErr).String(), truncate(invokeErr.Error(), 2000)
	} else {
		execution.Status, execution.ResponseJSON = "succeeded", truncate(response, 1<<20)
	}
	finishCtx, finishCancel := context.WithTimeout(context.WithoutCancel(parent), 5*time.Second)
	defer finishCancel()
	finishErr := s.transactor.Within(finishCtx, nil, func(tx *sqlx.Tx) error { return s.repository.FinishExecution(finishCtx, tx, execution) })
	if finishErr != nil {
		return Execution{}, translate(finishErr)
	}
	execution.Version++
	if invokeErr != nil {
		return execution, apperror.Unavailable("scheduled gRPC invocation failed", invokeErr)
	}
	return execution, nil
}
func (s *Service) GetExecution(ctx context.Context, id string) (Execution, error) {
	value, err := s.repository.GetExecution(ctx, strings.TrimSpace(id))
	if err != nil {
		return Execution{}, translate(err)
	}
	if err := s.authorizeScope(ctx, value.TenantID, value.ApplicationID); err != nil {
		return Execution{}, err
	}
	return value, nil
}
func (s *Service) ListExecutions(ctx context.Context, jobID string, page, pageSize int) (Page[Execution], error) {
	value, err := s.repository.GetJob(ctx, strings.TrimSpace(jobID))
	if err != nil {
		return Page[Execution]{}, translate(err)
	}
	if err := s.authorizeScope(ctx, value.TenantID, value.ApplicationID); err != nil {
		return Page[Execution]{}, err
	}
	page, pageSize, err = pagination(page, pageSize)
	if err != nil {
		return Page[Execution]{}, err
	}
	values, total, err := s.repository.ListExecutions(ctx, strings.TrimSpace(jobID), pageSize, (page-1)*pageSize)
	return Page[Execution]{Items: values, Total: total, Page: page, PageSize: pageSize}, translate(err)
}

func normalizeAndValidate(ctx context.Context, invoker Invoker, input Input) (Input, error) {
	input.TenantID, input.ApplicationID = strings.TrimSpace(input.TenantID), strings.TrimSpace(input.ApplicationID)
	input.Name, input.CronExpression, input.Timezone, input.Upstream, input.FullMethod, input.RequestJSON = strings.TrimSpace(input.Name), strings.TrimSpace(input.CronExpression), strings.TrimSpace(input.Timezone), strings.TrimSpace(input.Upstream), strings.TrimSpace(input.FullMethod), strings.TrimSpace(input.RequestJSON)
	if input.TenantID == "" || input.ApplicationID == "" || input.Name == "" || input.CronExpression == "" || input.Upstream == "" || input.FullMethod == "" {
		return Input{}, apperror.Invalid("tenant_id, application_id, name, cron_expression, upstream, and full_method are required", nil)
	}
	if input.Timezone == "" {
		input.Timezone = "Asia/Shanghai"
	}
	if _, err := time.LoadLocation(input.Timezone); err != nil {
		return Input{}, apperror.Invalid("invalid timezone", err)
	}
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	if _, err := parser.Parse(input.CronExpression); err != nil {
		return Input{}, apperror.Invalid("invalid cron_expression", err)
	}
	if input.TimeoutMilliseconds < 100 || input.TimeoutMilliseconds > int64((30*time.Minute)/time.Millisecond) {
		return Input{}, apperror.Invalid("timeout_milliseconds must be between 100 and 1800000", nil)
	}
	if input.RequestJSON == "" {
		input.RequestJSON = "{}"
	}
	if err := invoker.Validate(ctx, input.Upstream, input.FullMethod, input.RequestJSON); err != nil {
		return Input{}, apperror.Invalid("invalid dynamic gRPC target or request", err)
	}
	return input, nil
}
func actorFromContext(ctx context.Context) (string, error) {
	value, ok := principal.FromContext(ctx)
	if !ok || value.ID == "" {
		return "", apperror.Unauthorized("authenticated actor is required")
	}
	return value.ID, nil
}

func (s *Service) authorizeScope(ctx context.Context, tenantID, applicationID string) error {
	tenantID, applicationID = strings.TrimSpace(tenantID), strings.TrimSpace(applicationID)
	if tenantID == "" || applicationID == "" {
		return apperror.Invalid("tenant_id and application_id are required", nil)
	}
	identity, ok := principal.FromContext(ctx)
	if !ok || identity.ID == "" {
		return apperror.Unauthorized("authenticated actor is required")
	}
	if identity.Type == principal.TypeUser && (identity.TenantID == "" || identity.TenantID != tenantID) {
		return apperror.Forbidden("tenant access denied")
	}
	return s.verifyApplication(ctx, tenantID, applicationID)
}

func (s *Service) verifyApplication(ctx context.Context, tenantID, applicationID string) error {
	err := s.applications.Verify(ctx, strings.TrimSpace(tenantID), strings.TrimSpace(applicationID))
	if errors.Is(err, appaccess.ErrNotGranted) {
		return apperror.Forbidden("tenant application access denied")
	}
	if err != nil {
		return apperror.Unavailable("verify tenant application access", err)
	}
	return nil
}

func statusFromEnabled(enabled bool) string {
	if enabled {
		return "enabled"
	}
	return "disabled"
}
func pagination(page, pageSize int) (int, int, error) {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	if pageSize > 100 {
		return 0, 0, apperror.Invalid("page_size must not exceed 100", nil)
	}
	return page, pageSize, nil
}
func translate(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrExecutionNotFound) {
		return apperror.NotFound(err.Error())
	}
	if errors.Is(err, ErrStaleVersion) {
		return apperror.Conflict("version conflict", err)
	}
	var appErr *apperror.Error
	if errors.As(err, &appErr) {
		return appErr
	}
	return apperror.Internal(err)
}
func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}
