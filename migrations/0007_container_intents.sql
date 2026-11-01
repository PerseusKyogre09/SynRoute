CREATE TABLE container_execution_intents (
    intent_id TEXT PRIMARY KEY NOT NULL,
    execution_id TEXT NOT NULL REFERENCES executions(id),
    container_name TEXT NOT NULL,
    container_token TEXT NOT NULL,
    container_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('pending', 'active', 'orphan_suspected', 'resolved')),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX container_execution_intents_open_idx
    ON container_execution_intents(status, created_at);
CREATE INDEX container_execution_intents_execution_idx
    ON container_execution_intents(execution_id, created_at);

DROP TRIGGER executions_completed_requires_verified_update;
CREATE TRIGGER executions_completed_requires_verified_update
BEFORE UPDATE OF state ON executions
WHEN NEW.state = 'COMPLETED'
 AND OLD.state <> 'COMPLETED'
 AND NOT EXISTS (
    SELECT 1
      FROM verification_evidence evidence
      JOIN verification_contracts contract
        ON contract.execution_id = evidence.execution_id
       AND contract.version = evidence.contract_version
     WHERE evidence.execution_id = NEW.id
       AND evidence.id = (
            SELECT MAX(latest.id) FROM verification_evidence latest
             WHERE latest.execution_id = NEW.id
       )
       AND evidence.status = 'passed'
       AND json_extract(evidence.report_json, '$.status') = 'passed'
       AND json_extract(evidence.report_json, '$.session_cleanup.status') IN ('not_required', 'succeeded')
       AND COALESCE(json_extract(evidence.report_json, '$.session_cleanup.container_may_remain'), 1) = 0
       AND json_extract(evidence.report_json, '$.session_cleanup.inspection_error') IS NULL
       AND json_extract(evidence.report_json, '$.session_cleanup.cleanup_error') IS NULL
       AND json_array_length(contract.checks_json) > 0
       AND json_array_length(evidence.report_json, '$.checks') = json_array_length(contract.checks_json)
       AND NOT EXISTS (
            SELECT 1 FROM json_each(contract.checks_json) required
             WHERE (
                 SELECT COUNT(*) FROM json_each(evidence.report_json, '$.checks') result
                  WHERE json_extract(result.value, '$.name') = json_extract(required.value, '$.name')
                    AND json(json_extract(result.value, '$.argv')) = json(json_extract(required.value, '$.argv'))
                    AND json_extract(result.value, '$.status') = 'passed'
                    AND json_extract(result.value, '$.termination') = 'exited'
                    AND json_extract(result.value, '$.exit_code') = 0
                    AND json_extract(result.value, '$.cleanup.status') IN ('not_required', 'succeeded')
                    AND COALESCE(json_extract(result.value, '$.cleanup.container_may_remain'), 1) = 0
                    AND json_extract(result.value, '$.cleanup.inspection_error') IS NULL
                    AND json_extract(result.value, '$.cleanup.cleanup_error') IS NULL
             ) <> 1
       )
 )
BEGIN
    SELECT RAISE(ABORT, 'COMPLETED requires passing checks and confirmed cleanup');
END;
