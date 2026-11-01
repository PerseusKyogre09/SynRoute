ALTER TABLE verification_evidence
    ADD COLUMN status TEXT NOT NULL DEFAULT 'incomplete';

CREATE TRIGGER executions_completed_requires_verified_insert
BEFORE INSERT ON executions
WHEN NEW.state = 'COMPLETED'
BEGIN
    SELECT RAISE(ABORT, 'COMPLETED requires passing verification evidence');
END;

CREATE TRIGGER executions_completed_requires_verified_update
BEFORE UPDATE OF state ON executions
WHEN NEW.state = 'COMPLETED'
 AND OLD.state <> 'COMPLETED'
 AND NOT EXISTS (
    SELECT 1
      FROM verification_evidence evidence
     WHERE evidence.execution_id = NEW.id
       AND evidence.id = (
            SELECT MAX(latest.id) FROM verification_evidence latest
             WHERE latest.execution_id = NEW.id
       )
       AND evidence.status = 'passed'
       AND json_extract(evidence.report_json, '$.status') = 'passed'
       AND json_type(evidence.report_json, '$.checks') = 'array'
       AND json_array_length(evidence.report_json, '$.checks') > 0
       AND NOT EXISTS (
            SELECT 1
              FROM json_each(evidence.report_json, '$.checks') check_item
             WHERE COALESCE(json_extract(check_item.value, '$.status'), '') <> 'passed'
       )
 )
BEGIN
    SELECT RAISE(ABORT, 'COMPLETED requires passing verification evidence');
END;

CREATE TRIGGER executions_terminal_state_immutable
BEFORE UPDATE OF state ON executions
WHEN OLD.state IN (
    'COMPLETED', 'PARTIALLY_COMPLETED', 'UNVERIFIED', 'FAILED',
    'CANCELLED', 'INTERRUPTED', 'TIMED_OUT', 'RECOVERY_REQUIRED'
)
 AND NEW.state <> OLD.state
BEGIN
    SELECT RAISE(ABORT, 'terminal execution state is immutable');
END;

CREATE TABLE execution_leases (
    execution_id TEXT PRIMARY KEY NOT NULL REFERENCES executions(id),
    owner_id TEXT NOT NULL,
    owner_pid INTEGER,
    process_start TEXT,
    boot_id TEXT,
    heartbeat_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    generation INTEGER NOT NULL
);
CREATE INDEX execution_leases_expiry_idx ON execution_leases(expires_at);

CREATE TABLE recovery_cases (
    case_id TEXT PRIMARY KEY NOT NULL,
    execution_id TEXT NOT NULL REFERENCES executions(id),
    status TEXT NOT NULL CHECK (status IN ('open', 'acknowledged', 'resolved')),
    reason TEXT NOT NULL,
    details_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    last_seen_at TEXT NOT NULL,
    acknowledged_at TEXT,
    resolved_at TEXT
);
CREATE INDEX recovery_cases_execution_idx ON recovery_cases(execution_id, created_at);
CREATE INDEX recovery_cases_status_idx ON recovery_cases(status, last_seen_at);
CREATE UNIQUE INDEX recovery_cases_one_open_per_execution_idx
    ON recovery_cases(execution_id) WHERE status = 'open';

CREATE TRIGGER executions_recovery_requires_open_case
BEFORE UPDATE OF state ON executions
WHEN NEW.state = 'RECOVERY_REQUIRED'
 AND OLD.state <> 'RECOVERY_REQUIRED'
 AND NOT EXISTS (
    SELECT 1 FROM recovery_cases
     WHERE execution_id = NEW.id AND status = 'open'
 )
BEGIN
    SELECT RAISE(ABORT, 'RECOVERY_REQUIRED requires an open recovery case');
END;
