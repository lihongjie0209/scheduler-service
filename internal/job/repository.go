package job

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/jmoiron/sqlx"
)

var ErrNotFound = errors.New("scheduled job not found")
var ErrExecutionNotFound = errors.New("job execution not found")
var ErrStaleVersion = errors.New("stale scheduled job version")

type Repository interface {
	CreateJob(context.Context, sqlx.ExtContext, Job) error
	UpdateJob(context.Context, sqlx.ExtContext, Job, int64) error
	DeleteJob(context.Context, sqlx.ExtContext, string, int64, timeFields) error
	GetJob(context.Context, string) (Job, error)
	ListJobs(context.Context, string, string, string, int, int) ([]Job, int64, error)
	ListEnabled(context.Context) ([]Job, error)
	CreateExecution(context.Context, sqlx.ExtContext, Execution) error
	FinishExecution(context.Context, sqlx.ExtContext, Execution) error
	GetExecution(context.Context, string) (Execution, error)
	ListExecutions(context.Context, string, int, int) ([]Execution, int64, error)
	DeleteTerminalExecutionsBefore(context.Context, time.Time, int) (int64, error)
}

type timeFields struct {
	UpdatedAt time.Time
	UpdatedBy string
}
type SQLRepository struct{ db *sqlx.DB }

func NewRepository(db *sqlx.DB) Repository { return &SQLRepository{db: db} }

const jobColumns = `id,tenant_id,application_id,name,cron_expression,timezone,upstream,full_method,request_json,timeout_milliseconds,status,version,created_at,updated_at,created_by,updated_by`
const executionColumns = `id,job_id,tenant_id,application_id,trigger_type,status,response_json,error_code,error_message,started_at,finished_at,duration_milliseconds,version,created_at,updated_at,created_by,updated_by`

func (r *SQLRepository) CreateJob(ctx context.Context, exec sqlx.ExtContext, value Job) error {
	_, err := exec.ExecContext(ctx, r.db.Rebind(`INSERT INTO scheduled_jobs (`+jobColumns+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`), value.ID, value.TenantID, value.ApplicationID, value.Name, value.CronExpression, value.Timezone, value.Upstream, value.FullMethod, value.RequestJSON, value.TimeoutMilliseconds, value.Status, value.Version, value.CreatedAt, value.UpdatedAt, value.CreatedBy, value.UpdatedBy)
	return err
}
func (r *SQLRepository) UpdateJob(ctx context.Context, exec sqlx.ExtContext, value Job, expected int64) error {
	result, err := exec.ExecContext(ctx, r.db.Rebind(`UPDATE scheduled_jobs SET name=?,cron_expression=?,timezone=?,upstream=?,full_method=?,request_json=?,timeout_milliseconds=?,status=?,version=version+1,updated_at=?,updated_by=? WHERE id=? AND version=? AND status<>'deleted'`), value.Name, value.CronExpression, value.Timezone, value.Upstream, value.FullMethod, value.RequestJSON, value.TimeoutMilliseconds, value.Status, value.UpdatedAt, value.UpdatedBy, value.ID, expected)
	return stale(result, err)
}
func (r *SQLRepository) DeleteJob(ctx context.Context, exec sqlx.ExtContext, id string, expected int64, fields timeFields) error {
	result, err := exec.ExecContext(ctx, r.db.Rebind(`UPDATE scheduled_jobs SET status='deleted',version=version+1,updated_at=?,updated_by=? WHERE id=? AND version=? AND status<>'deleted'`), fields.UpdatedAt, fields.UpdatedBy, id, expected)
	return stale(result, err)
}
func (r *SQLRepository) GetJob(ctx context.Context, id string) (Job, error) {
	var value Job
	err := r.db.GetContext(ctx, &value, r.db.Rebind(`SELECT `+jobColumns+` FROM scheduled_jobs WHERE id=? AND status<>'deleted'`), id)
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrNotFound
	}
	return value, err
}
func (r *SQLRepository) ListJobs(ctx context.Context, tenantID, applicationID, status string, limit, offset int) ([]Job, int64, error) {
	where, args := `tenant_id=? AND application_id=? AND status<>'deleted'`, []any{tenantID, applicationID}
	if status != "" {
		where += ` AND status=?`
		args = append(args, status)
	}
	var total int64
	if err := r.db.GetContext(ctx, &total, r.db.Rebind(`SELECT COUNT(*) FROM scheduled_jobs WHERE `+where), args...); err != nil {
		return nil, 0, err
	}
	args = append(args, limit, offset)
	values := []Job{}
	err := r.db.SelectContext(ctx, &values, r.db.Rebind(`SELECT `+jobColumns+` FROM scheduled_jobs WHERE `+where+` ORDER BY created_at DESC LIMIT ? OFFSET ?`), args...)
	return values, total, err
}
func (r *SQLRepository) ListEnabled(ctx context.Context) ([]Job, error) {
	values := []Job{}
	err := r.db.SelectContext(ctx, &values, `SELECT `+jobColumns+` FROM scheduled_jobs WHERE tenant_id<>'' AND application_id<>'' AND status='enabled' ORDER BY id`)
	return values, err
}
func (r *SQLRepository) CreateExecution(ctx context.Context, exec sqlx.ExtContext, value Execution) error {
	_, err := exec.ExecContext(ctx, r.db.Rebind(`INSERT INTO job_executions (`+executionColumns+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`), value.ID, value.JobID, value.TenantID, value.ApplicationID, value.TriggerType, value.Status, value.ResponseJSON, value.ErrorCode, value.ErrorMessage, value.StartedAt, value.FinishedAt, value.DurationMilliseconds, value.Version, value.CreatedAt, value.UpdatedAt, value.CreatedBy, value.UpdatedBy)
	return err
}
func (r *SQLRepository) FinishExecution(ctx context.Context, exec sqlx.ExtContext, value Execution) error {
	result, err := exec.ExecContext(ctx, r.db.Rebind(`UPDATE job_executions SET status=?,response_json=?,error_code=?,error_message=?,finished_at=?,duration_milliseconds=?,version=version+1,updated_at=?,updated_by=? WHERE id=? AND version=?`), value.Status, value.ResponseJSON, value.ErrorCode, value.ErrorMessage, value.FinishedAt, value.DurationMilliseconds, value.UpdatedAt, value.UpdatedBy, value.ID, value.Version)
	return stale(result, err)
}
func (r *SQLRepository) GetExecution(ctx context.Context, id string) (Execution, error) {
	var value Execution
	err := r.db.GetContext(ctx, &value, r.db.Rebind(`SELECT `+executionColumns+` FROM job_executions WHERE id=?`), id)
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrExecutionNotFound
	}
	return value, err
}
func (r *SQLRepository) ListExecutions(ctx context.Context, jobID string, limit, offset int) ([]Execution, int64, error) {
	var total int64
	if err := r.db.GetContext(ctx, &total, r.db.Rebind(`SELECT COUNT(*) FROM job_executions WHERE job_id=?`), jobID); err != nil {
		return nil, 0, err
	}
	values := []Execution{}
	err := r.db.SelectContext(ctx, &values, r.db.Rebind(`SELECT `+executionColumns+` FROM job_executions WHERE job_id=? ORDER BY started_at DESC LIMIT ? OFFSET ?`), jobID, limit, offset)
	return values, total, err
}

func (r *SQLRepository) DeleteTerminalExecutionsBefore(ctx context.Context, before time.Time, limit int) (int64, error) {
	var ids []string
	query := r.db.Rebind(`SELECT id FROM job_executions WHERE status IN ('succeeded','failed') AND finished_at<? ORDER BY finished_at,id LIMIT ?`)
	if err := r.db.SelectContext(ctx, &ids, query, before, limit); err != nil || len(ids) == 0 {
		return 0, err
	}
	query, args, err := sqlx.In(`DELETE FROM job_executions WHERE id IN (?) AND status IN ('succeeded','failed') AND finished_at<?`, ids, before)
	if err != nil {
		return 0, err
	}
	result, err := r.db.ExecContext(ctx, r.db.Rebind(query), args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
func stale(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err == nil && count == 0 {
		return ErrStaleVersion
	}
	return err
}
