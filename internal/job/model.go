package job

import "time"

type Job struct {
	ID                  string    `db:"id" json:"id"`
	Name                string    `db:"name" json:"name"`
	CronExpression      string    `db:"cron_expression" json:"cron_expression"`
	Timezone            string    `db:"timezone" json:"timezone"`
	Upstream            string    `db:"upstream" json:"upstream"`
	FullMethod          string    `db:"full_method" json:"full_method"`
	RequestJSON         string    `db:"request_json" json:"request_json"`
	TimeoutMilliseconds int64     `db:"timeout_milliseconds" json:"timeout_milliseconds"`
	Status              string    `db:"status" json:"status"`
	Version             int64     `db:"version" json:"version"`
	CreatedAt           time.Time `db:"created_at" json:"created_at"`
	UpdatedAt           time.Time `db:"updated_at" json:"updated_at"`
	CreatedBy           string    `db:"created_by" json:"created_by"`
	UpdatedBy           string    `db:"updated_by" json:"updated_by"`
}

type Execution struct {
	ID                   string     `db:"id" json:"id"`
	JobID                string     `db:"job_id" json:"job_id"`
	TriggerType          string     `db:"trigger_type" json:"trigger_type"`
	Status               string     `db:"status" json:"status"`
	ResponseJSON         string     `db:"response_json" json:"response_json"`
	ErrorCode            string     `db:"error_code" json:"error_code"`
	ErrorMessage         string     `db:"error_message" json:"error_message"`
	StartedAt            time.Time  `db:"started_at" json:"started_at"`
	FinishedAt           *time.Time `db:"finished_at" json:"finished_at"`
	DurationMilliseconds int64      `db:"duration_milliseconds" json:"duration_milliseconds"`
	Version              int64      `db:"version" json:"version"`
	CreatedAt            time.Time  `db:"created_at" json:"created_at"`
	UpdatedAt            time.Time  `db:"updated_at" json:"updated_at"`
	CreatedBy            string     `db:"created_by" json:"created_by"`
	UpdatedBy            string     `db:"updated_by" json:"updated_by"`
}

type Page[T any] struct {
	Items    []T   `json:"items"`
	Total    int64 `json:"total"`
	Page     int   `json:"page"`
	PageSize int   `json:"page_size"`
}

type Input struct {
	Name                string
	CronExpression      string
	Timezone            string
	Upstream            string
	FullMethod          string
	RequestJSON         string
	TimeoutMilliseconds int64
	Enabled             bool
}
