CREATE TABLE IF NOT EXISTS executions (
    id TEXT PRIMARY KEY NOT NULL,
    repository_root TEXT NOT NULL,
    repository_revision TEXT NOT NULL,
    task_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS execution_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT NOT NULL REFERENCES executions(id),
    state TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    payload_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS verification_evidence (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id TEXT NOT NULL REFERENCES executions(id),
    report_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS execution_events_execution_id_idx ON execution_events(execution_id);
