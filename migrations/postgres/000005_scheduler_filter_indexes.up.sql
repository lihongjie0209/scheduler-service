CREATE INDEX scheduled_jobs_scope_created_idx ON scheduled_jobs (tenant_id, application_id, created_at DESC, id DESC) WHERE deleted_at IS NULL;
CREATE INDEX scheduled_jobs_scope_upstream_created_idx ON scheduled_jobs (tenant_id, application_id, upstream, created_at DESC, id DESC) WHERE deleted_at IS NULL;
CREATE INDEX scheduled_jobs_scope_status_created_idx ON scheduled_jobs (tenant_id, application_id, status, created_at DESC, id DESC) WHERE deleted_at IS NULL;
CREATE INDEX job_executions_job_status_started_idx ON job_executions (job_id, status, trigger_type, started_at DESC, id DESC) WHERE deleted_at IS NULL;

