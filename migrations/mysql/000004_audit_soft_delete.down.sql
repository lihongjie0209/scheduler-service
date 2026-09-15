DROP INDEX job_executions_active_retention_idx ON job_executions;
DROP INDEX scheduled_jobs_active_scope_status_idx ON scheduled_jobs;
DELETE FROM job_executions WHERE deleted_at IS NOT NULL;
UPDATE scheduled_jobs SET status = 'deleted' WHERE deleted_at IS NOT NULL;
ALTER TABLE job_executions DROP COLUMN deleted_by, DROP COLUMN deleted_at;
ALTER TABLE scheduled_jobs DROP COLUMN deleted_by, DROP COLUMN deleted_at;

