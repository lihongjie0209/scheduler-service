ALTER TABLE scheduled_jobs ADD COLUMN tenant_id TEXT NOT NULL DEFAULT '';
ALTER TABLE scheduled_jobs ADD COLUMN application_id TEXT NOT NULL DEFAULT '';
UPDATE scheduled_jobs SET status = 'disabled', version = version + 1, updated_at = CURRENT_TIMESTAMP, updated_by = 'migration:application-scope' WHERE status = 'enabled';
CREATE INDEX scheduled_jobs_scope_status_idx ON scheduled_jobs (tenant_id, application_id, status, id);
ALTER TABLE job_executions ADD COLUMN tenant_id TEXT NOT NULL DEFAULT '';
ALTER TABLE job_executions ADD COLUMN application_id TEXT NOT NULL DEFAULT '';
CREATE INDEX job_executions_scope_job_idx ON job_executions (tenant_id, application_id, job_id, started_at DESC);
