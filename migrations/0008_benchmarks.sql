CREATE TABLE benchmark_runs (
    run_id TEXT PRIMARY KEY NOT NULL,
    repository_root TEXT NOT NULL,
    repository_revision TEXT NOT NULL,
    repository_fingerprint TEXT NOT NULL,
    benchmark_version TEXT NOT NULL,
    scoring_version TEXT NOT NULL,
    requested_rounds INTEGER NOT NULL CHECK (requested_rounds BETWEEN 1 AND 3),
    created_at TEXT NOT NULL
);

CREATE TABLE benchmark_rounds (
    round_id TEXT PRIMARY KEY NOT NULL,
    run_id TEXT NOT NULL REFERENCES benchmark_runs(run_id),
    provider_id TEXT NOT NULL,
    model_id TEXT,
    round_number INTEGER NOT NULL CHECK (round_number BETWEEN 0 AND 3),
    prompt_hash TEXT NOT NULL,
    response_hash TEXT,
    response_text TEXT,
    prompt_tokens INTEGER,
    completion_tokens INTEGER,
    total_tokens INTEGER,
    latency_ms INTEGER,
    task_relevance REAL,
    repository_context REAL,
    technical_correctness REAL,
    conciseness REAL,
    status TEXT NOT NULL CHECK (status IN ('succeeded', 'failed', 'unavailable')),
    failure_detail TEXT,
    created_at TEXT NOT NULL,
    CHECK (response_text IS NULL OR length(response_text) <= 16384),
    CHECK (failure_detail IS NULL OR length(failure_detail) <= 1000),
    UNIQUE(run_id, provider_id, model_id, round_number)
);

CREATE INDEX benchmark_rounds_run_idx ON benchmark_rounds(run_id, provider_id, model_id, round_number);
