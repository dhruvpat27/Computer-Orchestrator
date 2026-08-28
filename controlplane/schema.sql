CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    task_name TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    status TEXT NOT NULL DEFAULT 'PENDING',   
    retries_left INT NOT NULL DEFAULT 3,
    max_retries INT NOT NULL DEFAULT 3,
    attempt INT NOT NULL DEFAULT 0,
    result JSONB,
    error TEXT,
    next_retry_at TIMESTAMPTZ,
    worker_id TEXT,
    workflow_id UUID,
    depends_on JSONB NOT NULL DEFAULT '[]'::jsonb,  
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_retry ON jobs(status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_jobs_workflow ON jobs(workflow_id);
