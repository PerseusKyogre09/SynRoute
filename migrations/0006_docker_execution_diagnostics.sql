CREATE TABLE container_diagnostics (
    diagnostic_id TEXT PRIMARY KEY NOT NULL,
    execution_id TEXT NOT NULL REFERENCES executions(id),
    container_name TEXT NOT NULL,
    container_id TEXT,
    reason TEXT NOT NULL CHECK (reason IN ('ambiguous_create', 'cleanup_failed', 'ownership_uncertain')),
    detail TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('open', 'resolved')),
    occurred_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    resolved_at TEXT
);
CREATE INDEX container_diagnostics_open_idx
    ON container_diagnostics(status, occurred_at);
CREATE INDEX container_diagnostics_execution_idx
    ON container_diagnostics(execution_id, occurred_at);
