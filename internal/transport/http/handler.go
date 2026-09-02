package httptransport

import (
	"log/slog"

	"github.com/gin-gonic/gin"
	"github.com/lihongjie0209/scheduler-service/internal/apperror"
	"github.com/lihongjie0209/scheduler-service/internal/buildinfo"
	"github.com/lihongjie0209/scheduler-service/internal/health"
	"github.com/lihongjie0209/scheduler-service/internal/job"
)

type Handler struct {
	logger *slog.Logger
	health *health.Service
	jobs   *job.Service
}

func NewHandler(healthService *health.Service, jobs *job.Service, logger *slog.Logger) *Handler {
	return &Handler{health: healthService, jobs: jobs, logger: logger}
}

type MeResponseBody struct {
	Subject string `json:"subject"`
}

// Login godoc
// @Summary Issue a JWT access token
// @Tags authentication
// @Accept json
// @Produce json
// @Param request body LoginRequest true "Client credentials"
// @Success 200 {object} Response{body=LoginResponseBody}
// @Failure 400 {object} Response "Code 10001: invalid request"
// @Failure 401 {object} Response "Code 20001: invalid credentials"
// @Failure 429 {object} Response "Code 10029: rate limited"

// Live godoc
// @Summary Check process liveness
// @Tags operations
// @Produce json
// @Success 200 {object} Response{body=health.Status}
// @Router /live [post]
func (h *Handler) Live(c *gin.Context) { OK(c, h.health.Live()) }

// Ready godoc
// @Summary Check database and Redis readiness
// @Tags operations
// @Produce json
// @Success 200 {object} Response{body=health.Status}
// @Failure 503 {object} Response{body=health.Status} "Code 50003: dependency unavailable"
// @Router /ready [post]
func (h *Handler) Ready(c *gin.Context) {
	status, ready := h.health.Ready(c.Request.Context())
	if !ready {
		c.JSON(503, Response{Code: apperror.CodeDependencyUnavailable, Message: "service is not ready", Body: status, RequestID: requestID(c)})
		return
	}
	OK(c, status)
}

// Me godoc
// @Summary Return the authenticated subject
// @Tags authentication
// @Produce json
// @Security Bearer
// @Success 200 {object} Response{body=MeResponseBody}
// @Failure 401 {object} Response "Code 20001: unauthorized"
// @Router /api/v1/me [post]
func (h *Handler) Me(c *gin.Context) {
	subject, _ := c.Get("subject")
	OK(c, gin.H{"subject": subject})
}

// Version godoc
// @Summary Return build and runtime version information
// @Tags operations
// @Produce json
// @Success 200 {object} Response{body=buildinfo.Info}
// @Router /api/v1/version [post]
func (h *Handler) Version(c *gin.Context) { OK(c, buildinfo.Current()) }

type JobInput struct {
	TenantID            string `json:"tenant_id"`
	ApplicationID       string `json:"application_id"`
	Name                string `json:"name" binding:"required"`
	CronExpression      string `json:"cron_expression" binding:"required"`
	Timezone            string `json:"timezone"`
	Upstream            string `json:"upstream" binding:"required"`
	FullMethod          string `json:"full_method" binding:"required"`
	RequestJSON         string `json:"request_json"`
	TimeoutMilliseconds int64  `json:"timeout_milliseconds" binding:"required"`
	Enabled             bool   `json:"enabled"`
}
type CreateJobRequest struct{ JobInput }
type UpdateJobRequest struct {
	ID      string `json:"id" binding:"required"`
	Version int64  `json:"version" binding:"required,gt=0"`
	JobInput
}
type DeleteJobRequest struct {
	ID      string `json:"id" binding:"required"`
	Version int64  `json:"version" binding:"required,gt=0"`
}
type JobIDRequest struct {
	ID string `json:"id" binding:"required"`
}
type ListJobsRequest struct {
	TenantID      string `json:"tenant_id" binding:"required"`
	ApplicationID string `json:"application_id" binding:"required"`
	Status        string `json:"status"`
	Page          int    `json:"page"`
	PageSize      int    `json:"page_size"`
}
type ListExecutionsRequest struct {
	JobID    string `json:"job_id" binding:"required"`
	Page     int    `json:"page"`
	PageSize int    `json:"page_size"`
}

func input(request JobInput) job.Input {
	return job.Input{TenantID: request.TenantID, ApplicationID: request.ApplicationID, Name: request.Name, CronExpression: request.CronExpression, Timezone: request.Timezone, Upstream: request.Upstream, FullMethod: request.FullMethod, RequestJSON: request.RequestJSON, TimeoutMilliseconds: request.TimeoutMilliseconds, Enabled: request.Enabled}
}
func (h *Handler) bind(c *gin.Context, request any) bool {
	if err := c.ShouldBindJSON(request); err != nil {
		Fail(c, h.logger, apperror.Invalid("invalid json request", err))
		return false
	}
	return true
}

// CreateJob godoc
// @Summary Create a dynamic unary gRPC scheduled job
// @Tags scheduler
// @Accept json
// @Produce json
// @Security Bearer
// @Param request body CreateJobRequest true "Job"
// @Success 200 {object} Response{body=JobBody}
// @Router /api/v1/scheduler/jobs/create [post]
func (h *Handler) CreateJob(c *gin.Context) {
	var request CreateJobRequest
	if !h.bind(c, &request) {
		return
	}
	value, err := h.jobs.Create(c.Request.Context(), input(request.JobInput))
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, jobBody(value))
}

// UpdateJob godoc
// @Summary Update a scheduled job with optimistic locking
// @Tags scheduler
// @Security Bearer
// @Accept json
// @Produce json
// @Param request body UpdateJobRequest true "Job and expected version"
// @Success 200 {object} Response{body=JobBody}
// @Router /api/v1/scheduler/jobs/update [post]
func (h *Handler) UpdateJob(c *gin.Context) {
	var request UpdateJobRequest
	if !h.bind(c, &request) {
		return
	}
	value, err := h.jobs.Update(c.Request.Context(), request.ID, input(request.JobInput), request.Version)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, jobBody(value))
}

// DeleteJob godoc
// @Summary Soft-delete a scheduled job with optimistic locking
// @Tags scheduler
// @Security Bearer
// @Accept json
// @Produce json
// @Param request body DeleteJobRequest true "Job ID and expected version"
// @Success 200 {object} Response
// @Router /api/v1/scheduler/jobs/delete [post]
func (h *Handler) DeleteJob(c *gin.Context) {
	var request DeleteJobRequest
	if !h.bind(c, &request) {
		return
	}
	if err := h.jobs.Delete(c.Request.Context(), request.ID, request.Version); err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, gin.H{"deleted": true})
}

// GetJob godoc
// @Summary Get a scheduled job
// @Tags scheduler
// @Security Bearer
// @Accept json
// @Produce json
// @Param request body JobIDRequest true "Job ID"
// @Success 200 {object} Response{body=JobBody}
// @Router /api/v1/scheduler/jobs/get [post]
func (h *Handler) GetJob(c *gin.Context) {
	var request JobIDRequest
	if !h.bind(c, &request) {
		return
	}
	value, err := h.jobs.Get(c.Request.Context(), request.ID)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, jobBody(value))
}

// ListJobs godoc
// @Summary List scheduled jobs
// @Tags scheduler
// @Security Bearer
// @Accept json
// @Produce json
// @Param request body ListJobsRequest true "Filter and pagination"
// @Success 200 {object} Response{body=JobPageBody}
// @Router /api/v1/scheduler/jobs/list [post]
func (h *Handler) ListJobs(c *gin.Context) {
	var request ListJobsRequest
	if !h.bind(c, &request) {
		return
	}
	value, err := h.jobs.List(c.Request.Context(), request.TenantID, request.ApplicationID, request.Status, request.Page, request.PageSize)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, jobPageBody(value))
}

// TriggerJob godoc
// @Summary Trigger a job immediately
// @Tags scheduler
// @Security Bearer
// @Accept json
// @Produce json
// @Param request body JobIDRequest true "Job ID"
// @Success 200 {object} Response{body=ExecutionBody}
// @Router /api/v1/scheduler/jobs/trigger [post]
func (h *Handler) TriggerJob(c *gin.Context) {
	var request JobIDRequest
	if !h.bind(c, &request) {
		return
	}
	value, err := h.jobs.Trigger(c.Request.Context(), request.ID)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, executionBody(value))
}

// GetExecution godoc
// @Summary Get a job execution
// @Tags scheduler
// @Security Bearer
// @Accept json
// @Produce json
// @Param request body JobIDRequest true "Execution ID"
// @Success 200 {object} Response{body=ExecutionBody}
// @Router /api/v1/scheduler/executions/get [post]
func (h *Handler) GetExecution(c *gin.Context) {
	var request JobIDRequest
	if !h.bind(c, &request) {
		return
	}
	value, err := h.jobs.GetExecution(c.Request.Context(), request.ID)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, executionBody(value))
}

// ListExecutions godoc
// @Summary List executions for a job
// @Tags scheduler
// @Security Bearer
// @Accept json
// @Produce json
// @Param request body ListExecutionsRequest true "Job ID and pagination"
// @Success 200 {object} Response{body=ExecutionPageBody}
// @Router /api/v1/scheduler/executions/list [post]
func (h *Handler) ListExecutions(c *gin.Context) {
	var request ListExecutionsRequest
	if !h.bind(c, &request) {
		return
	}
	value, err := h.jobs.ListExecutions(c.Request.Context(), request.JobID, request.Page, request.PageSize)
	if err != nil {
		Fail(c, h.logger, err)
		return
	}
	OK(c, executionPageBody(value))
}
