package httptransport

import (
	"time"

	"github.com/lihongjie0209/scheduler-service/internal/job"
)

// JobBody is the stable public HTTP representation of a scheduled job.
// Keep this DTO explicit so persistence-only fields cannot leak through JSON.
type JobBody struct {
	ID                  string    `json:"id"`
	TenantID            string    `json:"tenant_id"`
	ApplicationID       string    `json:"application_id"`
	Name                string    `json:"name"`
	CronExpression      string    `json:"cron_expression"`
	Timezone            string    `json:"timezone"`
	Upstream            string    `json:"upstream"`
	FullMethod          string    `json:"full_method"`
	RequestJSON         string    `json:"request_json"`
	TimeoutMilliseconds int64     `json:"timeout_milliseconds"`
	Status              string    `json:"status"`
	Version             int64     `json:"version"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
	CreatedBy           string    `json:"created_by"`
	UpdatedBy           string    `json:"updated_by"`
}

// ExecutionBody is the stable public HTTP representation of a job execution.
type ExecutionBody struct {
	ID                   string     `json:"id"`
	JobID                string     `json:"job_id"`
	TenantID             string     `json:"tenant_id"`
	ApplicationID        string     `json:"application_id"`
	TriggerType          string     `json:"trigger_type"`
	Status               string     `json:"status"`
	ResponseJSON         string     `json:"response_json"`
	ErrorCode            string     `json:"error_code"`
	ErrorMessage         string     `json:"error_message"`
	StartedAt            time.Time  `json:"started_at"`
	FinishedAt           *time.Time `json:"finished_at"`
	DurationMilliseconds int64      `json:"duration_milliseconds"`
	Version              int64      `json:"version"`
	CreatedAt            time.Time  `json:"created_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
	CreatedBy            string     `json:"created_by"`
	UpdatedBy            string     `json:"updated_by"`
}

type JobPageBody struct {
	Items    []JobBody `json:"items"`
	Total    int64     `json:"total"`
	Page     int       `json:"page"`
	PageSize int       `json:"page_size"`
}

type ExecutionPageBody struct {
	Items    []ExecutionBody `json:"items"`
	Total    int64           `json:"total"`
	Page     int             `json:"page"`
	PageSize int             `json:"page_size"`
}

func jobBody(value job.Job) JobBody {
	return JobBody{
		ID: value.ID, TenantID: value.TenantID, ApplicationID: value.ApplicationID,
		Name: value.Name, CronExpression: value.CronExpression, Timezone: value.Timezone,
		Upstream: value.Upstream, FullMethod: value.FullMethod, RequestJSON: value.RequestJSON,
		TimeoutMilliseconds: value.TimeoutMilliseconds, Status: value.Status, Version: value.Version,
		CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt, CreatedBy: value.CreatedBy, UpdatedBy: value.UpdatedBy,
	}
}

func executionBody(value job.Execution) ExecutionBody {
	return ExecutionBody{
		ID: value.ID, JobID: value.JobID, TenantID: value.TenantID, ApplicationID: value.ApplicationID,
		TriggerType: value.TriggerType, Status: value.Status, ResponseJSON: value.ResponseJSON,
		ErrorCode: value.ErrorCode, ErrorMessage: value.ErrorMessage, StartedAt: value.StartedAt,
		FinishedAt: value.FinishedAt, DurationMilliseconds: value.DurationMilliseconds, Version: value.Version,
		CreatedAt: value.CreatedAt, UpdatedAt: value.UpdatedAt, CreatedBy: value.CreatedBy, UpdatedBy: value.UpdatedBy,
	}
}

func jobPageBody(value job.Page[job.Job]) JobPageBody {
	items := make([]JobBody, len(value.Items))
	for index := range value.Items {
		items[index] = jobBody(value.Items[index])
	}
	return JobPageBody{Items: items, Total: value.Total, Page: value.Page, PageSize: value.PageSize}
}

func executionPageBody(value job.Page[job.Execution]) ExecutionPageBody {
	items := make([]ExecutionBody, len(value.Items))
	for index := range value.Items {
		items[index] = executionBody(value.Items[index])
	}
	return ExecutionPageBody{Items: items, Total: value.Total, Page: value.Page, PageSize: value.PageSize}
}
