DROP TRIGGER IF EXISTS app_audit_row_trigger ON job_executions;
DROP TRIGGER IF EXISTS app_audit_row_trigger ON scheduled_jobs;
DROP INDEX IF EXISTS job_executions_active_retention_idx;
DROP INDEX IF EXISTS scheduled_jobs_active_scope_status_idx;
DELETE FROM job_executions WHERE deleted_at IS NOT NULL;
UPDATE scheduled_jobs SET status = 'deleted' WHERE deleted_at IS NOT NULL;
ALTER TABLE job_executions DROP COLUMN deleted_by;
ALTER TABLE job_executions DROP COLUMN deleted_at;
ALTER TABLE scheduled_jobs DROP COLUMN deleted_by;
ALTER TABLE scheduled_jobs DROP COLUMN deleted_at;
DROP FUNCTION IF EXISTS app_enable_audit(regclass);
DROP FUNCTION IF EXISTS app_audit_row();

