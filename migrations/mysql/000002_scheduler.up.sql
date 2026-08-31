CREATE TABLE scheduled_jobs (
    id VARCHAR(64) PRIMARY KEY, name TEXT NOT NULL, cron_expression TEXT NOT NULL,
    timezone TEXT NOT NULL, upstream TEXT NOT NULL, full_method TEXT NOT NULL,
    request_json TEXT NOT NULL, timeout_milliseconds BIGINT NOT NULL,
    status VARCHAR(16) NOT NULL, version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMP(6) NOT NULL, updated_at TIMESTAMP(6) NOT NULL,
    created_by TEXT NOT NULL, updated_by TEXT NOT NULL,
    CONSTRAINT scheduled_jobs_status_check CHECK (status IN ('enabled', 'disabled', 'deleted')),
    CONSTRAINT scheduled_jobs_timeout_check CHECK (timeout_milliseconds BETWEEN 100 AND 1800000)
);
CREATE INDEX scheduled_jobs_status_idx ON scheduled_jobs (status, id);
CREATE TABLE job_executions (
    id VARCHAR(64) PRIMARY KEY, job_id VARCHAR(64) NOT NULL,
    trigger_type VARCHAR(16) NOT NULL, status VARCHAR(16) NOT NULL,
    response_json MEDIUMTEXT NOT NULL, error_code TEXT NOT NULL, error_message TEXT NOT NULL,
    started_at TIMESTAMP(6) NOT NULL, finished_at TIMESTAMP(6) NULL,
    duration_milliseconds BIGINT NOT NULL DEFAULT 0, version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMP(6) NOT NULL, updated_at TIMESTAMP(6) NOT NULL,
    created_by TEXT NOT NULL, updated_by TEXT NOT NULL,
    CONSTRAINT job_executions_job_fk FOREIGN KEY (job_id) REFERENCES scheduled_jobs(id),
    CONSTRAINT job_executions_trigger_check CHECK (trigger_type IN ('scheduled', 'manual')),
    CONSTRAINT job_executions_status_check CHECK (status IN ('running', 'succeeded', 'failed'))
);
CREATE INDEX job_executions_job_started_idx ON job_executions (job_id, started_at DESC);
CREATE INDEX job_executions_retention_idx ON job_executions (status, finished_at, id);
