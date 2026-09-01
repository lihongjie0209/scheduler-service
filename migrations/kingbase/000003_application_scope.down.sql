DROP INDEX job_executions_scope_job_idx;
ALTER TABLE job_executions DROP COLUMN application_id;
ALTER TABLE job_executions DROP COLUMN tenant_id;
DROP INDEX scheduled_jobs_scope_status_idx;
ALTER TABLE scheduled_jobs DROP COLUMN application_id;
ALTER TABLE scheduled_jobs DROP COLUMN tenant_id;
