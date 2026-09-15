package job

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/lihongjie0209/microservice-platform-go/appaccess"
	"github.com/lihongjie0209/microservice-platform-go/distlock"
	"github.com/lihongjie0209/microservice-platform-go/operationlog"
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
	operations   operationlog.Recorder
}

type allowAllApplications struct{}

func (allowAllApplications) Verify(context.Context, string, string) error { return nil }

func NewService(repository Repository, transactor *database.Transactor, invoker Invoker, locker *cache.Locker) *Service {
	return &Service{repository: repository, transactor: transactor, invoker: invoker, locker: locker, applications: allowAllApplications{}, now: time.Now, changed: make(chan struct{}, 1)}
}
func NewRuntimeService(repository Repository, transactor *database.Transactor, invoker Invoker, locker *cache.Locker, applications appaccess.Verifier, operations operationlog.Recorder) (*Service, error) {
	if applications == nil || operations == nil {
		return nil, errors.New("application verifier and operation recorder are required")
	}
	service := NewService(repository, transactor, invoker, locker)
	service.applications = applications
	service.operations = operations
	return service, nil
}
func (s *Service) Changes() <-chan struct{} { return s.changed }
func (s *Service) signalChanged() {
	select {
	case s.changed <- struct{}{}:
	default:
	}
}

func (s *Service) Create(ctx context.Context, input Input) (result Job, err error) {
	started := s.now()
	defer func() {
		err = s.finishOperation(ctx, started, operationlog.Entry{Operation: "scheduler.job.create", ResourceType: "scheduled_job", ResourceID: result.ID, ApplicationID: input.ApplicationID, Source: "backend", Protocol: "service", Request: map[string]any{"name": input.Name, "upstream": input.Upstream, "full_method": input.FullMethod, "enabled": input.Enabled}}, err)
	}()
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
func (s *Service) Update(ctx context.Context, id string, input Input, expected int64) (result Job, err error) {
	started := s.now()
	defer func() {
		err = s.finishOperation(ctx, started, operationlog.Entry{Operation: "scheduler.job.update", ResourceType: "scheduled_job", ResourceID: strings.TrimSpace(id), ApplicationID: input.ApplicationID, Source: "backend", Protocol: "service", Request: map[string]any{"name": input.Name, "upstream": input.Upstream, "full_method": input.FullMethod, "enabled": input.Enabled, "expected_version": expected}}, err)
	}()
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
func (s *Service) Delete(ctx context.Context, id string, expected int64) (err error) {
	started := s.now()
	applicationID := ""
	defer func() {
		err = s.finishOperation(ctx, started, operationlog.Entry{Operation: "scheduler.job.delete", ResourceType: "scheduled_job", ResourceID: strings.TrimSpace(id), ApplicationID: applicationID, Source: "backend", Protocol: "service", Request: map[string]any{"expected_version": expected}}, err)
	}()
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
	applicationID = current.ApplicationID
	if err := s.authorizeScope(ctx, current.TenantID, current.ApplicationID); err != nil {
		return err
	}
	err = s.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error {
		return s.repository.DeleteJob(ctx, tx, strings.TrimSpace(id), expected, AuditFields{UpdatedAt: s.now(), UpdatedBy: actor})
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
func (s *Service) List(ctx context.Context, filter JobFilter, page, pageSize int) (Page[Job], error) {
	filter.TenantID, filter.ApplicationID = strings.TrimSpace(filter.TenantID), strings.TrimSpace(filter.ApplicationID)
	if err := s.authorizeScope(ctx, filter.TenantID, filter.ApplicationID); err != nil {
		return Page[Job]{}, err
	}
	page, pageSize, err := pagination(page, pageSize)
	if err != nil {
		return Page[Job]{}, err
	}
	filter, err = normalizeJobFilter(filter)
	if err != nil {
		return Page[Job]{}, err
	}
	values, total, err := s.repository.ListJobs(ctx, filter, pageSize, (page-1)*pageSize)
	return Page[Job]{Items: values, Total: total, Page: page, PageSize: pageSize}, translate(err)
}
func (s *Service) Trigger(ctx context.Context, id string, expected int64) (result Execution, err error) {
	started := s.now()
	applicationID := ""
	defer func() {
		err = s.finishOperation(ctx, started, operationlog.Entry{Operation: "scheduler.job.trigger", ResourceType: "scheduled_job", ResourceID: strings.TrimSpace(id), ApplicationID: applicationID, Source: "backend", Protocol: "service", Request: map[string]any{"expected_version": expected}}, err)
	}()
	if expected < 1 {
		return Execution{}, apperror.Invalid("version must be positive", nil)
	}
	actor, err := actorFromContext(ctx)
	if err != nil {
		return Execution{}, err
	}
	value, err := s.repository.GetJob(ctx, strings.TrimSpace(id))
	if err != nil {
		return Execution{}, translate(err)
	}
	applicationID = value.ApplicationID
	if err := s.authorizeScope(ctx, value.TenantID, value.ApplicationID); err != nil {
		return Execution{}, err
	}
	if err := validateManualTrigger(value, expected); err != nil {
		return Execution{}, err
	}
	return s.execute(ctx, value, "manual", actor, expected)
}

func (s *Service) finishOperation(ctx context.Context, started time.Time, entry operationlog.Entry, businessErr error) error {
	if s.operations == nil || !s.operations.Enabled() {
		return businessErr
	}
	entry.Duration = s.now().Sub(started)
	entry.Succeeded = businessErr == nil
	if businessErr != nil {
		entry.ErrorMessage = truncate(businessErr.Error(), 2000)
	}
	if id, ok := requestid.FromContext(ctx); ok {
		entry.RequestID = id
	}
	logErr := s.operations.Record(ctx, entry)
	if businessErr != nil {
		return businessErr
	}
	if logErr != nil {
		return apperror.Unavailable("enqueue operation log", logErr)
	}
	return nil
}

func validateManualTrigger(value Job, expected int64) error {
	if value.Version != expected {
		return translate(ErrStaleVersion)
	}
	if value.Status != "enabled" {
		return apperror.Conflict("scheduled job is not enabled", nil)
	}
	return nil
}
func (s *Service) ExecuteScheduled(ctx context.Context, value Job) (Execution, error) {
	ctx = principal.WithContext(ctx, principal.Principal{ID: "scheduler-service", Type: principal.TypeSystem, TenantID: value.TenantID})
	current, err := s.repository.GetJob(ctx, value.ID)
	if err != nil {
		return Execution{}, translate(err)
	}
	if current.Version != value.Version || current.Status != "enabled" {
		return Execution{}, apperror.Conflict("scheduled job definition changed before execution", ErrStaleVersion)
	}
	if err := s.verifyApplication(ctx, current.TenantID, current.ApplicationID); err != nil {
		return Execution{}, err
	}
	return s.execute(ctx, current, "scheduled", "scheduler-service", 0)
}
func (s *Service) execute(parent context.Context, value Job, triggerType, actor string, expectedVersion int64) (Execution, error) {
	if s.locker == nil {
		return Execution{}, apperror.Unavailable("distributed scheduler lock is unavailable", nil)
	}
	timeout := time.Duration(value.TimeoutMilliseconds) * time.Millisecond
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	var execution Execution
	acquired, err := distlock.TryWithLock(ctx, s.locker, "scheduler:job:"+value.ID, timeout+30*time.Second, func(leaseCtx context.Context) error {
		var executeErr error
		execution, executeErr = s.executeWithLease(leaseCtx, value, triggerType, actor, expectedVersion)
		return executeErr
	})
	if err != nil && !acquired {
		return Execution{}, apperror.Unavailable("acquire scheduled job lock", err)
	}
	if !acquired {
		return Execution{}, apperror.Conflict("scheduled job is already running", nil)
	}
	return execution, translate(err)
}

func (s *Service) executeWithLease(ctx context.Context, value Job, triggerType, actor string, expectedVersion int64) (Execution, error) {
	started := s.now()
	execution := Execution{ID: uuid.NewString(), JobID: value.ID, TenantID: value.TenantID, ApplicationID: value.ApplicationID, TriggerType: triggerType, Status: "running", StartedAt: started, Version: 1, CreatedAt: started, UpdatedAt: started, CreatedBy: actor, UpdatedBy: actor}
	if err := s.transactor.Within(ctx, nil, func(tx *sqlx.Tx) error {
		if triggerType == "manual" {
			return s.repository.CreateManualExecution(ctx, tx, execution, expectedVersion)
		}
		return s.repository.CreateExecution(ctx, tx, execution)
	}); err != nil {
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
	finishCtx, finishCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
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
func (s *Service) ListExecutions(ctx context.Context, filter ExecutionFilter, page, pageSize int) (Page[Execution], error) {
	filter.JobID = strings.TrimSpace(filter.JobID)
	value, err := s.repository.GetJob(ctx, filter.JobID)
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
	filter, err = normalizeExecutionFilter(filter)
	if err != nil {
		return Page[Execution]{}, err
	}
	values, total, err := s.repository.ListExecutions(ctx, filter, pageSize, (page-1)*pageSize)
	return Page[Execution]{Items: values, Total: total, Page: page, PageSize: pageSize}, translate(err)
}

func normalizeJobFilter(filter JobFilter) (JobFilter, error) {
	filter.Keyword = strings.TrimSpace(filter.Keyword)
	if len(filter.Keyword) > 200 {
		return JobFilter{}, apperror.Invalid("keyword must not exceed 200 bytes", nil)
	}
	var err error
	if filter.IDs, err = normalizeSet(filter.IDs, nil); err != nil {
		return JobFilter{}, err
	}
	if filter.Statuses, err = normalizeSet(filter.Statuses, map[string]struct{}{"enabled": {}, "disabled": {}}); err != nil {
		return JobFilter{}, apperror.Invalid("statuses must contain enabled or disabled", err)
	}
	if filter.Upstreams, err = normalizeSet(filter.Upstreams, nil); err != nil {
		return JobFilter{}, err
	}
	if err := validateRange(filter.CreatedFrom, filter.CreatedTo, "created"); err != nil {
		return JobFilter{}, err
	}
	return filter, nil
}

func normalizeExecutionFilter(filter ExecutionFilter) (ExecutionFilter, error) {
	filter.Keyword = strings.TrimSpace(filter.Keyword)
	if len(filter.Keyword) > 200 {
		return ExecutionFilter{}, apperror.Invalid("keyword must not exceed 200 bytes", nil)
	}
	var err error
	if filter.IDs, err = normalizeSet(filter.IDs, nil); err != nil {
		return ExecutionFilter{}, err
	}
	if filter.Statuses, err = normalizeSet(filter.Statuses, map[string]struct{}{"running": {}, "succeeded": {}, "failed": {}}); err != nil {
		return ExecutionFilter{}, apperror.Invalid("invalid execution status", err)
	}
	if filter.TriggerTypes, err = normalizeSet(filter.TriggerTypes, map[string]struct{}{"scheduled": {}, "manual": {}}); err != nil {
		return ExecutionFilter{}, apperror.Invalid("invalid trigger type", err)
	}
	if err := validateRange(filter.StartedFrom, filter.StartedTo, "started"); err != nil {
		return ExecutionFilter{}, err
	}
	if (filter.DurationMinMilliseconds != nil && *filter.DurationMinMilliseconds < 0) ||
		(filter.DurationMaxMilliseconds != nil && *filter.DurationMaxMilliseconds < 0) {
		return ExecutionFilter{}, apperror.Invalid("duration range must not be negative", nil)
	}
	if filter.DurationMinMilliseconds != nil && filter.DurationMaxMilliseconds != nil && *filter.DurationMinMilliseconds > *filter.DurationMaxMilliseconds {
		return ExecutionFilter{}, apperror.Invalid("duration minimum must not exceed maximum", nil)
	}
	return filter, nil
}

func normalizeSet(values []string, allowed map[string]struct{}) ([]string, error) {
	if len(values) > 100 {
		return nil, apperror.Invalid("filter lists must not exceed 100 values", nil)
	}
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || len(value) > 200 {
			return nil, apperror.Invalid("filter values must be non-empty and at most 200 bytes", nil)
		}
		if allowed != nil {
			value = strings.ToLower(value)
			if _, ok := allowed[value]; !ok {
				return nil, errors.New("unsupported filter value")
			}
		}
		if _, ok := seen[value]; !ok {
			seen[value] = struct{}{}
			result = append(result, value)
		}
	}
	return result, nil
}

func validateRange(from, to *time.Time, name string) error {
	if from != nil && to != nil && !from.Before(*to) {
		return apperror.Invalid(name+"_from must be before "+name+"_to", nil)
	}
	return nil
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
