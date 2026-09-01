ALTER TABLE job_executions DROP INDEX job_executions_scope_job_idx, DROP COLUMN application_id, DROP COLUMN tenant_id;
ALTER TABLE scheduled_jobs DROP INDEX scheduled_jobs_scope_status_idx, DROP COLUMN application_id, DROP COLUMN tenant_id;
