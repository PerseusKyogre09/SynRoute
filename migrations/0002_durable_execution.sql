ALTER TABLE executions ADD COLUMN task_description TEXT NOT NULL DEFAULT '';
ALTER TABLE executions ADD COLUMN task_metadata_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE executions ADD COLUMN repository_identity TEXT NOT NULL DEFAULT '';
ALTER TABLE executions ADD COLUMN working_directory TEXT NOT NULL DEFAULT '';
ALTER TABLE executions ADD COLUMN state TEXT NOT NULL DEFAULT 'RECEIVED';
ALTER TABLE executions ADD COLUMN updated_at TEXT NOT NULL DEFAULT '';
ALTER TABLE executions ADD COLUMN completed_at TEXT;
ALTER TABLE executions ADD COLUMN terminal_outcome TEXT;
ALTER TABLE executions ADD COLUMN failure_classification TEXT;
ALTER TABLE executions ADD COLUMN execution_config_json TEXT NOT NULL DEFAULT '{}';

ALTER TABLE execution_events ADD COLUMN sequence INTEGER NOT NULL DEFAULT 0;
ALTER TABLE execution_events ADD COLUMN event_type TEXT NOT NULL DEFAULT 'legacy_event';
UPDATE execution_events SET sequence = id WHERE sequence = 0;
CREATE UNIQUE INDEX execution_events_execution_sequence_idx ON execution_events(execution_id, sequence);
CREATE INDEX executions_state_idx ON executions(state);

CREATE TRIGGER execution_events_no_update
BEFORE UPDATE ON execution_events
BEGIN
    SELECT RAISE(ABORT, 'execution events are append-only');
END;
CREATE TRIGGER execution_events_no_delete
BEFORE DELETE ON execution_events
BEGIN
    SELECT RAISE(ABORT, 'execution events are append-only');
END;

CREATE TABLE workspaces (
    workspace_id TEXT PRIMARY KEY NOT NULL,
    execution_id TEXT NOT NULL REFERENCES executions(id),
    canonical_path TEXT NOT NULL,
    repository_identity TEXT NOT NULL,
    repository_root TEXT NOT NULL,
    repository_revision TEXT NOT NULL,
    git_admin_path TEXT,
    git_common_dir TEXT,
    sidecar_path TEXT NOT NULL,
    ownership_token TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX workspaces_execution_id_idx ON workspaces(execution_id);
CREATE INDEX workspaces_status_idx ON workspaces(status);

UPDATE executions
SET task_description = task_json,
    repository_identity = repository_root,
    working_directory = repository_root,
    updated_at = created_at
WHERE updated_at = '';
