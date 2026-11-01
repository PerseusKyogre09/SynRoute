ALTER TABLE verification_evidence
    ADD COLUMN contract_version TEXT NOT NULL DEFAULT '';

CREATE TABLE verification_contracts (
    execution_id TEXT PRIMARY KEY NOT NULL REFERENCES executions(id),
    version TEXT NOT NULL,
    checks_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

-- Existing executions had no persisted verification contract. They remain unverified
-- until a future explicit migration or policy supplies one; an empty contract cannot pass.
INSERT INTO verification_contracts (execution_id, version, checks_json, created_at)
SELECT id, '00000000-0000-0000-0000-000000000000', '[]', created_at FROM executions;
UPDATE verification_evidence
   SET contract_version = '00000000-0000-0000-0000-000000000000'
 WHERE contract_version = '';

CREATE TRIGGER verification_contracts_validate_insert
BEFORE INSERT ON verification_contracts
WHEN json_valid(NEW.checks_json) = 0
  OR json_type(NEW.checks_json) <> 'array'
  OR EXISTS (
      SELECT 1 FROM json_each(NEW.checks_json) required
       WHERE COALESCE(json_extract(required.value, '$.name'), '') = ''
          OR json_type(required.value, '$.argv') <> 'array'
          OR json_array_length(required.value, '$.argv') = 0
  )
  OR EXISTS (
      SELECT 1 FROM json_each(NEW.checks_json) left_check
      JOIN json_each(NEW.checks_json) right_check
        ON CAST(left_check.key AS INTEGER) < CAST(right_check.key AS INTEGER)
       WHERE json_extract(left_check.value, '$.name') = json_extract(right_check.value, '$.name')
  )
BEGIN
    SELECT RAISE(ABORT, 'invalid verification contract');
END;

CREATE TRIGGER verification_contracts_no_update
BEFORE UPDATE ON verification_contracts
BEGIN
    SELECT RAISE(ABORT, 'verification contracts are immutable');
END;

CREATE TRIGGER verification_contracts_no_delete
BEFORE DELETE ON verification_contracts
BEGIN
    SELECT RAISE(ABORT, 'verification contracts are immutable');
END;

CREATE TRIGGER verification_evidence_requires_verifying_contract
BEFORE INSERT ON verification_evidence
WHEN NOT EXISTS (
    SELECT 1
      FROM executions execution
      JOIN verification_contracts contract ON contract.execution_id = execution.id
     WHERE execution.id = NEW.execution_id
       AND execution.state = 'VERIFYING'
       AND contract.version = NEW.contract_version
       AND json_valid(NEW.report_json)
       AND json_type(NEW.report_json, '$.checks') = 'array'
       AND json_array_length(NEW.report_json, '$.checks') = json_array_length(contract.checks_json)
       AND NOT EXISTS (
           SELECT 1 FROM json_each(contract.checks_json) required
            WHERE (
                SELECT COUNT(*) FROM json_each(NEW.report_json, '$.checks') result
                 WHERE json_extract(result.value, '$.name') = json_extract(required.value, '$.name')
                   AND json(json_extract(result.value, '$.argv')) = json(json_extract(required.value, '$.argv'))
            ) <> 1
       )
)
BEGIN
    SELECT RAISE(ABORT, 'verification evidence must match the active persisted contract');
END;

CREATE TRIGGER verification_evidence_no_update
BEFORE UPDATE ON verification_evidence
BEGIN
    SELECT RAISE(ABORT, 'verification evidence is immutable');
END;

CREATE TRIGGER verification_evidence_no_delete
BEFORE DELETE ON verification_evidence
BEGIN
    SELECT RAISE(ABORT, 'verification evidence is immutable');
END;

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
       AND json_array_length(contract.checks_json) > 0
       AND json_array_length(evidence.report_json, '$.checks') = json_array_length(contract.checks_json)
       AND NOT EXISTS (
            SELECT 1 FROM json_each(contract.checks_json) required
             WHERE (
                 SELECT COUNT(*) FROM json_each(evidence.report_json, '$.checks') result
                  WHERE json_extract(result.value, '$.name') = json_extract(required.value, '$.name')
                    AND json(json_extract(result.value, '$.argv')) = json(json_extract(required.value, '$.argv'))
                    AND json_extract(result.value, '$.status') = 'passed'
             ) <> 1
       )
 )
BEGIN
    SELECT RAISE(ABORT, 'COMPLETED requires passing evidence for the persisted verification contract');
END;
