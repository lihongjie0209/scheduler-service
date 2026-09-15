ALTER TABLE scheduled_jobs
    ADD COLUMN deleted_at TIMESTAMP(6) NULL,
    ADD COLUMN deleted_by TEXT NULL;
UPDATE scheduled_jobs SET deleted_at = updated_at, deleted_by = updated_by WHERE status = 'deleted';
CREATE INDEX scheduled_jobs_active_scope_status_idx ON scheduled_jobs (tenant_id, application_id, status, deleted_at, id);

ALTER TABLE job_executions
    ADD COLUMN deleted_at TIMESTAMP(6) NULL,
    ADD COLUMN deleted_by TEXT NULL;
CREATE INDEX job_executions_active_retention_idx ON job_executions (deleted_at, status, finished_at, id);

