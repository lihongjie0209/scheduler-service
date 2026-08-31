CREATE TABLE scheduled_jobs (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    cron_expression TEXT NOT NULL,
    timezone TEXT NOT NULL,
    upstream TEXT NOT NULL,
    full_method TEXT NOT NULL,
    request_json TEXT NOT NULL,
    timeout_milliseconds BIGINT NOT NULL,
    status TEXT NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    created_by TEXT NOT NULL,
    updated_by TEXT NOT NULL,
    CONSTRAINT scheduled_jobs_status_check CHECK (status IN ('enabled', 'disabled', 'deleted')),
    CONSTRAINT scheduled_jobs_timeout_check CHECK (timeout_milliseconds BETWEEN 100 AND 1800000)
);
CREATE INDEX scheduled_jobs_status_idx ON scheduled_jobs (status, id);

CREATE TABLE job_executions (
    id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES scheduled_jobs(id),
    trigger_type TEXT NOT NULL,
    status TEXT NOT NULL,
    response_json TEXT NOT NULL DEFAULT '',
    error_code TEXT NOT NULL DEFAULT '',
    error_message TEXT NOT NULL DEFAULT '',
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    duration_milliseconds BIGINT NOT NULL DEFAULT 0,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    created_by TEXT NOT NULL,
    updated_by TEXT NOT NULL,
    CONSTRAINT job_executions_trigger_check CHECK (trigger_type IN ('scheduled', 'manual')),
    CONSTRAINT job_executions_status_check CHECK (status IN ('running', 'succeeded', 'failed'))
);
CREATE INDEX job_executions_job_started_idx ON job_executions (job_id, started_at DESC);
CREATE INDEX job_executions_retention_idx ON job_executions (finished_at, id) WHERE status IN ('succeeded', 'failed');
COMMENT ON TABLE job_executions IS 'Terminal executions default to 90-day bounded cleanup and optional pre-delete archive. Preserve global execution IDs until a time-bucket identity permits native partitioning and optional pg_partman automation.';
