package job

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

var ErrNotFound = errors.New("scheduled job not found")
var ErrExecutionNotFound = errors.New("job execution not found")
var ErrStaleVersion = errors.New("stale scheduled job version")

type Repository interface {
	CreateJob(context.Context, sqlx.ExtContext, Job) error
	UpdateJob(context.Context, sqlx.ExtContext, Job, int64) error
	DeleteJob(context.Context, sqlx.ExtContext, string, int64, AuditFields) error
	GetJob(context.Context, string) (Job, error)
	ListJobs(context.Context, JobFilter, int, int) ([]Job, int64, error)
	ListEnabled(context.Context) ([]Job, error)
	CreateExecution(context.Context, sqlx.ExtContext, Execution) error
	CreateManualExecution(context.Context, sqlx.ExtContext, Execution, int64) error
	FinishExecution(context.Context, sqlx.ExtContext, Execution) error
	GetExecution(context.Context, string) (Execution, error)
	ListExecutions(context.Context, ExecutionFilter, int, int) ([]Execution, int64, error)
	SoftDeleteTerminalExecutionsBefore(context.Context, sqlx.ExtContext, time.Time, int, AuditFields) (int64, error)
}

type AuditFields struct {
	UpdatedAt time.Time
	UpdatedBy string
}
type SQLRepository struct{ db *sqlx.DB }

func NewRepository(db *sqlx.DB) Repository { return &SQLRepository{db: db} }

const jobColumns = `id,tenant_id,application_id,name,cron_expression,timezone,upstream,full_method,request_json,timeout_milliseconds,status,version,created_at,updated_at,created_by,updated_by,deleted_at,deleted_by`
const executionColumns = `id,job_id,tenant_id,application_id,trigger_type,status,response_json,error_code,error_message,started_at,finished_at,duration_milliseconds,version,created_at,updated_at,created_by,updated_by,deleted_at,deleted_by`

func (r *SQLRepository) CreateJob(ctx context.Context, exec sqlx.ExtContext, value Job) error {
	_, err := exec.ExecContext(ctx, r.db.Rebind(`INSERT INTO scheduled_jobs (`+jobColumns+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`), value.ID, value.TenantID, value.ApplicationID, value.Name, value.CronExpression, value.Timezone, value.Upstream, value.FullMethod, value.RequestJSON, value.TimeoutMilliseconds, value.Status, value.Version, value.CreatedAt, value.UpdatedAt, value.CreatedBy, value.UpdatedBy, value.DeletedAt, value.DeletedBy)
	return err
}
func (r *SQLRepository) UpdateJob(ctx context.Context, exec sqlx.ExtContext, value Job, expected int64) error {
	result, err := exec.ExecContext(ctx, r.db.Rebind(`UPDATE scheduled_jobs SET name=?,cron_expression=?,timezone=?,upstream=?,full_method=?,request_json=?,timeout_milliseconds=?,status=?,version=version+1,updated_at=?,updated_by=? WHERE id=? AND version=? AND deleted_at IS NULL`), value.Name, value.CronExpression, value.Timezone, value.Upstream, value.FullMethod, value.RequestJSON, value.TimeoutMilliseconds, value.Status, value.UpdatedAt, value.UpdatedBy, value.ID, expected)
	return stale(result, err)
}
func (r *SQLRepository) DeleteJob(ctx context.Context, exec sqlx.ExtContext, id string, expected int64, fields AuditFields) error {
	result, err := exec.ExecContext(ctx, r.db.Rebind(`UPDATE scheduled_jobs SET deleted_at=?,deleted_by=?,version=version+1,updated_at=?,updated_by=? WHERE id=? AND version=? AND deleted_at IS NULL`), fields.UpdatedAt, fields.UpdatedBy, fields.UpdatedAt, fields.UpdatedBy, id, expected)
	return stale(result, err)
}
func (r *SQLRepository) GetJob(ctx context.Context, id string) (Job, error) {
	var value Job
	err := r.db.GetContext(ctx, &value, r.db.Rebind(`SELECT `+jobColumns+` FROM scheduled_jobs WHERE id=? AND deleted_at IS NULL`), id)
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrNotFound
	}
	return value, err
}
func (r *SQLRepository) ListJobs(ctx context.Context, filter JobFilter, limit, offset int) ([]Job, int64, error) {
	where, args := `tenant_id=? AND application_id=? AND deleted_at IS NULL`, []any{filter.TenantID, filter.ApplicationID}
	where, args = appendLikeFilter(where, args, filter.Keyword, "name", "upstream", "full_method")
	var err error
	where, args, err = appendInFilter(where, args, "id", filter.IDs)
	if err != nil {
		return nil, 0, err
	}
	where, args, err = appendInFilter(where, args, "status", filter.Statuses)
	if err != nil {
		return nil, 0, err
	}
	where, args, err = appendInFilter(where, args, "upstream", filter.Upstreams)
	if err != nil {
		return nil, 0, err
	}
	where, args = appendTimeRange(where, args, "created_at", filter.CreatedFrom, filter.CreatedTo)
	var total int64
	if err := r.db.GetContext(ctx, &total, r.db.Rebind(`SELECT COUNT(*) FROM scheduled_jobs WHERE `+where), args...); err != nil {
		return nil, 0, err
	}
	args = append(args, limit, offset)
	values := []Job{}
	err = r.db.SelectContext(ctx, &values, r.db.Rebind(`SELECT `+jobColumns+` FROM scheduled_jobs WHERE `+where+` ORDER BY created_at DESC,id DESC LIMIT ? OFFSET ?`), args...)
	return values, total, err
}
func (r *SQLRepository) ListEnabled(ctx context.Context) ([]Job, error) {
	values := []Job{}
	err := r.db.SelectContext(ctx, &values, `SELECT `+jobColumns+` FROM scheduled_jobs WHERE tenant_id<>'' AND application_id<>'' AND status='enabled' AND deleted_at IS NULL ORDER BY id`)
	return values, err
}
func (r *SQLRepository) CreateExecution(ctx context.Context, exec sqlx.ExtContext, value Execution) error {
	_, err := exec.ExecContext(ctx, r.db.Rebind(`INSERT INTO job_executions (`+executionColumns+`) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`), value.ID, value.JobID, value.TenantID, value.ApplicationID, value.TriggerType, value.Status, value.ResponseJSON, value.ErrorCode, value.ErrorMessage, value.StartedAt, value.FinishedAt, value.DurationMilliseconds, value.Version, value.CreatedAt, value.UpdatedAt, value.CreatedBy, value.UpdatedBy, value.DeletedAt, value.DeletedBy)
	return err
}

func (r *SQLRepository) CreateManualExecution(
	ctx context.Context,
	exec sqlx.ExtContext,
	value Execution,
	expectedVersion int64,
) error {
	var current struct {
		Version int64  `db:"version"`
		Status  string `db:"status"`
	}
	err := sqlx.GetContext(
		ctx,
		exec,
		&current,
		r.db.Rebind(`SELECT version,status FROM scheduled_jobs WHERE id=? AND deleted_at IS NULL FOR UPDATE`),
		value.JobID,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	if current.Version != expectedVersion || current.Status != "enabled" {
		return ErrStaleVersion
	}
	return r.CreateExecution(ctx, exec, value)
}
func (r *SQLRepository) FinishExecution(ctx context.Context, exec sqlx.ExtContext, value Execution) error {
	result, err := exec.ExecContext(ctx, r.db.Rebind(`UPDATE job_executions SET status=?,response_json=?,error_code=?,error_message=?,finished_at=?,duration_milliseconds=?,version=version+1,updated_at=?,updated_by=? WHERE id=? AND version=?`), value.Status, value.ResponseJSON, value.ErrorCode, value.ErrorMessage, value.FinishedAt, value.DurationMilliseconds, value.UpdatedAt, value.UpdatedBy, value.ID, value.Version)
	return stale(result, err)
}
func (r *SQLRepository) GetExecution(ctx context.Context, id string) (Execution, error) {
	var value Execution
	err := r.db.GetContext(ctx, &value, r.db.Rebind(`SELECT `+executionColumns+` FROM job_executions WHERE id=? AND deleted_at IS NULL`), id)
	if errors.Is(err, sql.ErrNoRows) {
		err = ErrExecutionNotFound
	}
	return value, err
}

func (r *SQLRepository) ListExecutions(ctx context.Context, filter ExecutionFilter, limit, offset int) ([]Execution, int64, error) {
	where, args := `job_id=? AND deleted_at IS NULL`, []any{filter.JobID}
	where, args = appendLikeFilter(where, args, filter.Keyword, "error_code", "error_message")
	var err error
	where, args, err = appendInFilter(where, args, "id", filter.IDs)
	if err != nil {
		return nil, 0, err
	}
	where, args, err = appendInFilter(where, args, "status", filter.Statuses)
	if err != nil {
		return nil, 0, err
	}
	where, args, err = appendInFilter(where, args, "trigger_type", filter.TriggerTypes)
	if err != nil {
		return nil, 0, err
	}
	where, args = appendTimeRange(where, args, "started_at", filter.StartedFrom, filter.StartedTo)
	if filter.DurationMinMilliseconds != nil {
		where += ` AND duration_milliseconds>=?`
		args = append(args, *filter.DurationMinMilliseconds)
	}
	if filter.DurationMaxMilliseconds != nil {
		where += ` AND duration_milliseconds<=?`
		args = append(args, *filter.DurationMaxMilliseconds)
	}
	var total int64
	if err := r.db.GetContext(ctx, &total, r.db.Rebind(`SELECT COUNT(*) FROM job_executions WHERE `+where), args...); err != nil {
		return nil, 0, err
	}
	args = append(args, limit, offset)
	values := []Execution{}
	err = r.db.SelectContext(ctx, &values, r.db.Rebind(`SELECT `+executionColumns+` FROM job_executions WHERE `+where+` ORDER BY started_at DESC,id DESC LIMIT ? OFFSET ?`), args...)
	return values, total, err
}

func appendLikeFilter(where string, args []any, keyword string, columns ...string) (string, []any) {
	if keyword == "" {
		return where, args
	}
	where += ` AND (`
	for index, column := range columns {
		if index > 0 {
			where += ` OR `
		}
		where += `LOWER(` + column + `) LIKE ?`
		args = append(args, "%"+strings.ToLower(keyword)+"%")
	}
	return where + `)`, args
}

func appendInFilter(where string, args []any, column string, values []string) (string, []any, error) {
	if len(values) == 0 {
		return where, args, nil
	}
	clause, expanded, err := sqlx.In(` AND `+column+` IN (?)`, values)
	if err != nil {
		return "", nil, err
	}
	return where + clause, append(args, expanded...), nil
}

func appendTimeRange(where string, args []any, column string, from, to *time.Time) (string, []any) {
	if from != nil {
		where += ` AND ` + column + `>=?`
		args = append(args, *from)
	}
	if to != nil {
		where += ` AND ` + column + `<?`
		args = append(args, *to)
	}
	return where, args
}

func (r *SQLRepository) SoftDeleteTerminalExecutionsBefore(ctx context.Context, exec sqlx.ExtContext, before time.Time, limit int, fields AuditFields) (int64, error) {
	var ids []string
	query := r.db.Rebind(`SELECT id FROM job_executions WHERE status IN ('succeeded','failed') AND finished_at<? AND deleted_at IS NULL ORDER BY finished_at,id LIMIT ?`)
	if err := sqlx.SelectContext(ctx, exec, &ids, query, before, limit); err != nil || len(ids) == 0 {
		return 0, err
	}
	query, args, err := sqlx.In(`UPDATE job_executions SET deleted_at=?,deleted_by=?,updated_at=?,updated_by=?,version=version+1 WHERE id IN (?) AND status IN ('succeeded','failed') AND finished_at<? AND deleted_at IS NULL`, fields.UpdatedAt, fields.UpdatedBy, fields.UpdatedAt, fields.UpdatedBy, ids, before)
	if err != nil {
		return 0, err
	}
	result, err := exec.ExecContext(ctx, r.db.Rebind(query), args...)
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
