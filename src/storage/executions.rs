use super::*;
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use serde_json::{Value, json};
use sqlx::{Row, Sqlite, Transaction, sqlite::SqliteRow};
use std::time::Duration;
use uuid::Uuid;

#[async_trait]
pub trait ExecutionRepository: Send + Sync {
    async fn create_execution(
        &self,
        intent: ExecutionIntent,
    ) -> Result<ExecutionRecord, StorageError>;
    async fn execution(&self, id: Uuid) -> Result<ExecutionRecord, StorageError>;
    async fn list_executions(
        &self,
        limit: Option<u32>,
    ) -> Result<Vec<ExecutionRecord>, StorageError>;
    async fn transition(
        &self,
        id: Uuid,
        to: crate::core::ExecutionState,
        failure: Option<FailureClassification>,
    ) -> Result<ExecutionRecord, StorageError>;
    async fn append_event(
        &self,
        id: Uuid,
        event_type: ExecutionEventType,
        payload: Value,
    ) -> Result<ExecutionEvent, StorageError>;
    async fn events(&self, id: Uuid) -> Result<Vec<ExecutionEvent>, StorageError>;
    async fn verifications(&self, id: Uuid) -> Result<Vec<VerificationRecord>, StorageError>;
    async fn nonterminal_executions(&self) -> Result<Vec<ExecutionRecord>, StorageError>;
    async fn report(&self, id: Uuid) -> Result<ExecutionReport, StorageError>;
    async fn lease(&self, id: Uuid) -> Result<Option<ExecutionLease>, StorageError>;
    async fn renew_lease(&self, id: Uuid, ttl: Duration) -> Result<ExecutionLease, StorageError>;
}

#[derive(Debug, Clone)]
pub struct SqliteExecutionRepository {
    pool: sqlx::SqlitePool,
    owner: OwnerIdentity,
}
impl SqliteExecutionRepository {
    pub(crate) fn new(pool: sqlx::SqlitePool, owner: OwnerIdentity) -> Self {
        Self { pool, owner }
    }
}

#[async_trait]
impl ExecutionRepository for SqliteExecutionRepository {
    async fn create_execution(
        &self,
        intent: ExecutionIntent,
    ) -> Result<ExecutionRecord, StorageError> {
        ensure_originating_process(&self.owner)?;
        if intent.description.trim().is_empty()
            || intent.repository_identity.trim().is_empty()
            || intent.repository_revision.trim().is_empty()
        {
            return Err(StorageError::Invalid(
                "task description, repository identity, and revision are required".into(),
            ));
        }
        if !intent.repository_root.is_absolute() || !intent.working_directory.is_absolute() {
            return Err(StorageError::Invalid(
                "repository root and working directory must be canonical absolute paths".into(),
            ));
        }
        check_sensitive_keys(&intent.metadata)?;
        validate_verification_contract(&intent.required_verification)?;
        let now = Utc::now();
        let metadata = serde_json::to_string(&intent.metadata)?;
        let task = serde_json::to_string(
            &json!({"description": intent.description, "metadata": intent.metadata}),
        )?;
        let config = serde_json::to_string(&intent.config)?;
        let contract_version = Uuid::new_v4();
        let checks_json = serde_json::to_string(&intent.required_verification)?;
        let state = state_to_db(crate::core::ExecutionState::Received);
        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT INTO executions (id, repository_root, repository_revision, task_json, created_at, task_description, task_metadata_json, repository_identity, working_directory, state, updated_at, execution_config_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(intent.id.to_string()).bind(intent.repository_root.to_string_lossy().as_ref()).bind(&intent.repository_revision).bind(task).bind(timestamp(now))
            .bind(&intent.description).bind(metadata).bind(&intent.repository_identity).bind(intent.working_directory.to_string_lossy().as_ref()).bind(&state).bind(timestamp(now)).bind(config)
            .execute(&mut *tx).await?;
        sqlx::query("INSERT INTO verification_contracts (execution_id, version, checks_json, created_at) VALUES (?, ?, ?, ?)")
            .bind(intent.id.to_string()).bind(contract_version.to_string()).bind(checks_json).bind(timestamp(now)).execute(&mut *tx).await?;
        let lease_expiry = lease_expiry(now, DEFAULT_LEASE_TTL)?;
        sqlx::query("INSERT INTO execution_leases (execution_id, owner_id, owner_pid, process_start, boot_id, heartbeat_at, expires_at, generation) VALUES (?, ?, ?, ?, ?, ?, ?, 1)")
            .bind(intent.id.to_string()).bind(self.owner.owner_id.to_string()).bind(self.owner.process_id.map(i64::from)).bind(&self.owner.process_start).bind(&self.owner.boot_id).bind(timestamp(now)).bind(timestamp(lease_expiry)).execute(&mut *tx).await?;
        append_event_tx(
            &mut tx,
            intent.id,
            ExecutionEventType::ExecutionCreated,
            json!({"state": state}),
            now,
        )
        .await?;
        tx.commit().await?;
        self.execution(intent.id).await
    }

    async fn execution(&self, id: Uuid) -> Result<ExecutionRecord, StorageError> {
        let row = sqlx::query("SELECT * FROM executions WHERE id = ?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "execution",
                id: id.to_string(),
            })?;
        decode_execution(&row)
    }

    async fn list_executions(
        &self,
        limit: Option<u32>,
    ) -> Result<Vec<ExecutionRecord>, StorageError> {
        let rows = if let Some(limit) = limit {
            sqlx::query("SELECT * FROM executions ORDER BY created_at DESC, id DESC LIMIT ?")
                .bind(i64::from(limit))
                .fetch_all(&self.pool)
                .await?
        } else {
            sqlx::query("SELECT * FROM executions ORDER BY created_at DESC, id DESC")
                .fetch_all(&self.pool)
                .await?
        };
        rows.iter().map(decode_execution).collect()
    }

    async fn transition(
        &self,
        id: Uuid,
        to: crate::core::ExecutionState,
        failure: Option<FailureClassification>,
    ) -> Result<ExecutionRecord, StorageError> {
        ensure_originating_process(&self.owner)?;
        if to == crate::core::ExecutionState::RecoveryRequired {
            return Err(StorageError::Invalid(
                "RecoveryRequired may only be written by the atomic recovery repository operation"
                    .into(),
            ));
        }
        let mut tx = self.pool.begin().await?;
        let id_text = id.to_string();
        let now = Utc::now();
        let locked = sqlx::query("UPDATE executions SET updated_at = ? WHERE id = ?")
            .bind(timestamp(now))
            .bind(&id_text)
            .execute(&mut *tx)
            .await?;
        if locked.rows_affected() == 0 {
            return Err(StorageError::NotFound {
                entity: "execution",
                id: id_text,
            });
        }
        validate_owner_tx(&mut tx, id, &self.owner, now).await?;
        let current: String = sqlx::query_scalar("SELECT state FROM executions WHERE id = ?")
            .bind(&id_text)
            .fetch_one(&mut *tx)
            .await?;
        let from = state_from_db(&current)?;
        crate::core::ExecutionState::validate_transition(Some(from), to)
            .map_err(|e| StorageError::Invalid(e.to_string()))?;
        if to == crate::core::ExecutionState::Completed {
            let latest_status: Option<String> = sqlx::query_scalar(
                "SELECT status FROM verification_evidence WHERE execution_id = ? ORDER BY id DESC LIMIT 1",
            )
            .bind(&id_text)
            .fetch_optional(&mut *tx)
            .await?;
            if latest_status.as_deref() != Some("passed") {
                return Err(StorageError::Invalid(
                    "execution cannot complete without persisted passing verification evidence"
                        .into(),
                ));
            }
        }
        let to_db = state_to_db(to);
        let terminal = to.is_terminal();
        let completion = terminal.then(|| timestamp(now));
        let outcome = terminal.then(|| to_db.clone());
        let failure_db = failure.map(|failure| {
            match failure {
                FailureClassification::Repository => "repository",
                FailureClassification::Planning => "planning",
                FailureClassification::Agent => "agent",
                FailureClassification::Sandbox => "sandbox",
                FailureClassification::Verification => "verification",
                FailureClassification::Cancelled => "cancelled",
                FailureClassification::Interrupted => "interrupted",
                FailureClassification::Timeout => "timeout",
                FailureClassification::Unknown => "unknown",
            }
            .to_owned()
        });
        let result = sqlx::query("UPDATE executions SET state = ?, updated_at = ?, completed_at = ?, terminal_outcome = ?, failure_classification = ? WHERE id = ? AND state = ?")
            .bind(&to_db).bind(timestamp(now)).bind(completion).bind(outcome).bind(failure_db).bind(&id_text).bind(&current).execute(&mut *tx).await?;
        if result.rows_affected() != 1 {
            return Err(StorageError::Invalid(
                "execution state changed concurrently".into(),
            ));
        }
        let event = match to {
            crate::core::ExecutionState::Completed => ExecutionEventType::ExecutionCompleted,
            crate::core::ExecutionState::PartiallyCompleted => {
                ExecutionEventType::ExecutionPartiallyCompleted
            }
            crate::core::ExecutionState::Unverified => ExecutionEventType::ExecutionUnverified,
            crate::core::ExecutionState::TimedOut => ExecutionEventType::ExecutionTimedOut,
            crate::core::ExecutionState::Failed => ExecutionEventType::ExecutionFailed,
            crate::core::ExecutionState::Cancelled => ExecutionEventType::ExecutionCancelled,
            crate::core::ExecutionState::Interrupted => ExecutionEventType::ExecutionInterrupted,
            crate::core::ExecutionState::RecoveryRequired => ExecutionEventType::RecoveryRequired,
            _ => ExecutionEventType::StateTransition,
        };
        append_event_tx(
            &mut tx,
            id,
            event,
            json!({"from": current, "to": to_db}),
            now,
        )
        .await?;
        if terminal {
            sqlx::query("DELETE FROM execution_leases WHERE execution_id = ?")
                .bind(&id_text)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        self.execution(id).await
    }

    async fn append_event(
        &self,
        id: Uuid,
        event_type: ExecutionEventType,
        payload: Value,
    ) -> Result<ExecutionEvent, StorageError> {
        ensure_originating_process(&self.owner)?;
        if !event_type.is_owner_appendable() {
            return Err(StorageError::Invalid(format!(
                "event type {event_type:?} is reserved for atomic lifecycle or evidence operations"
            )));
        }
        check_sensitive_keys(&payload)?;
        let mut tx = self.pool.begin().await?;
        let now = Utc::now();
        let locked = sqlx::query("UPDATE executions SET updated_at = ? WHERE id = ?")
            .bind(timestamp(now))
            .bind(id.to_string())
            .execute(&mut *tx)
            .await?;
        if locked.rows_affected() == 0 {
            return Err(StorageError::NotFound {
                entity: "execution",
                id: id.to_string(),
            });
        }
        validate_owner_tx(&mut tx, id, &self.owner, now).await?;
        let event = append_event_tx(&mut tx, id, event_type, payload, now).await?;
        tx.commit().await?;
        Ok(event)
    }

    async fn events(&self, id: Uuid) -> Result<Vec<ExecutionEvent>, StorageError> {
        let rows = sqlx::query("SELECT id, execution_id, sequence, event_type, occurred_at, payload_json FROM execution_events WHERE execution_id = ? ORDER BY sequence ASC")
            .bind(id.to_string()).fetch_all(&self.pool).await?;
        rows.iter().map(decode_event).collect()
    }

    async fn verifications(&self, id: Uuid) -> Result<Vec<VerificationRecord>, StorageError> {
        let rows = sqlx::query("SELECT report_json, occurred_at, contract_version FROM verification_evidence WHERE execution_id = ? ORDER BY id ASC").bind(id.to_string()).fetch_all(&self.pool).await?;
        rows.iter()
            .map(|row| {
                Ok(VerificationRecord {
                    created_at: parse_timestamp(row.try_get("occurred_at")?)?,
                    contract_version: Uuid::parse_str(
                        &row.try_get::<String, _>("contract_version")?,
                    )
                    .map_err(|error| {
                        StorageError::Invalid(format!("invalid evidence contract version: {error}"))
                    })?,
                    report: serde_json::from_str(row.try_get("report_json")?)?,
                })
            })
            .collect()
    }

    async fn nonterminal_executions(&self) -> Result<Vec<ExecutionRecord>, StorageError> {
        let rows = sqlx::query("SELECT * FROM executions ORDER BY created_at ASC")
            .fetch_all(&self.pool)
            .await?;
        rows.iter()
            .map(decode_execution)
            .collect::<Result<Vec<_>, _>>()
            .map(|items| {
                items
                    .into_iter()
                    .filter(|item| !item.state.is_terminal())
                    .collect()
            })
    }

    async fn report(&self, id: Uuid) -> Result<ExecutionReport, StorageError> {
        let execution = self.execution(id).await?;
        let verification_contract = self.verification_contract(id).await?;
        let events = self.events(id).await?;
        let workspaces = SqliteWorkspaceRepository::new(self.pool.clone())
            .for_execution(id)
            .await?;
        let verifications = self.verifications(id).await?;
        let container_diagnostics = SqliteContainerDiagnosticRepository::new(self.pool.clone())
            .for_execution(id)
            .await?;
        let container_intents = SqliteContainerDiagnosticRepository::new(self.pool.clone())
            .intents_for_execution(id)
            .await?;
        Ok(ExecutionReport {
            execution,
            verification_contract,
            events,
            workspaces,
            verifications,
            container_diagnostics,
            container_intents,
        })
    }

    async fn lease(&self, id: Uuid) -> Result<Option<ExecutionLease>, StorageError> {
        let row = sqlx::query("SELECT * FROM execution_leases WHERE execution_id = ?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?;
        row.as_ref().map(super::recovery::decode_lease).transpose()
    }

    async fn renew_lease(&self, id: Uuid, ttl: Duration) -> Result<ExecutionLease, StorageError> {
        ensure_originating_process(&self.owner)?;
        let now = Utc::now();
        let expires_at = lease_expiry(now, ttl)?;
        let mut tx = self.pool.begin().await?;
        let locked = sqlx::query("UPDATE executions SET updated_at = updated_at WHERE id = ?")
            .bind(id.to_string())
            .execute(&mut *tx)
            .await?;
        if locked.rows_affected() == 0 {
            return Err(StorageError::NotFound {
                entity: "execution",
                id: id.to_string(),
            });
        }
        let state: String = sqlx::query_scalar("SELECT state FROM executions WHERE id = ?")
            .bind(id.to_string())
            .fetch_one(&mut *tx)
            .await?;
        if state_from_db(&state)?.is_terminal() {
            return Err(StorageError::Invalid(
                "terminal executions cannot renew an owner lease".into(),
            ));
        }
        let row = sqlx::query("SELECT * FROM execution_leases WHERE execution_id = ?")
            .bind(id.to_string())
            .fetch_optional(&mut *tx)
            .await?;
        let Some(row) = row else {
            return Err(StorageError::Invalid(
                "execution lease no longer exists".into(),
            ));
        };
        let current = super::recovery::decode_lease(&row)?;
        if current.owner_id != self.owner.owner_id
            || current.generation != self.owner.generation
            || current.expires_at <= now
        {
            return Err(StorageError::Invalid(
                "expired or superseded execution lease cannot be renewed".into(),
            ));
        }
        let updated = sqlx::query("UPDATE execution_leases SET heartbeat_at = ?, expires_at = ? WHERE execution_id = ? AND owner_id = ? AND generation = ? AND expires_at = ?")
            .bind(timestamp(now)).bind(timestamp(expires_at)).bind(id.to_string()).bind(self.owner.owner_id.to_string()).bind(self.owner.generation).bind(timestamp(current.expires_at)).execute(&mut *tx).await?;
        if updated.rows_affected() != 1 {
            return Err(StorageError::Invalid(
                "execution lease is not owned by this storage instance and generation".into(),
            ));
        }
        tx.commit().await?;
        self.lease(id)
            .await?
            .ok_or_else(|| StorageError::Invalid("renewed lease disappeared".into()))
    }
}

async fn validate_owner_tx(
    tx: &mut Transaction<'_, Sqlite>,
    id: Uuid,
    owner: &OwnerIdentity,
    now: chrono::DateTime<Utc>,
) -> Result<(), StorageError> {
    let row = sqlx::query(
        "SELECT owner_id, generation, expires_at FROM execution_leases WHERE execution_id = ?",
    )
    .bind(id.to_string())
    .fetch_optional(&mut **tx)
    .await?;
    let Some(row) = row else {
        return Err(StorageError::Invalid(
            "execution has no active owner lease".into(),
        ));
    };
    let owner_id: String = row.try_get("owner_id")?;
    let generation: i64 = row.try_get("generation")?;
    let expires_at = parse_timestamp(row.try_get("expires_at")?)?;
    if owner_id != owner.owner_id.to_string() || generation != owner.generation || expires_at <= now
    {
        return Err(StorageError::Invalid(
            "execution lease is expired or belongs to another owner generation".into(),
        ));
    }
    Ok(())
}

fn ensure_originating_process(owner: &OwnerIdentity) -> Result<(), StorageError> {
    if owner.is_current_process() {
        Ok(())
    } else {
        Err(StorageError::Invalid(
            "execution lease can only be used by its originating process".into(),
        ))
    }
}

fn validate_verification_contract(
    checks: &[crate::core::VerificationCommand],
) -> Result<(), StorageError> {
    let mut names = std::collections::HashSet::with_capacity(checks.len());
    for check in checks {
        if check.name.trim().is_empty()
            || check.argv.is_empty()
            || check.argv.iter().any(|part| part.is_empty())
            || !names.insert(check.name.as_str())
        {
            return Err(StorageError::Invalid(
                "verification checks require unique non-empty names and argv".into(),
            ));
        }
    }
    Ok(())
}

impl SqliteExecutionRepository {
    #[allow(dead_code)] // Called by the trusted verification path; no execution coordinator exists yet.
    pub(crate) async fn verification_contract(
        &self,
        id: Uuid,
    ) -> Result<PersistedVerificationContract, StorageError> {
        let row = sqlx::query(
            "SELECT version, checks_json FROM verification_contracts WHERE execution_id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            entity: "verification contract",
            id: id.to_string(),
        })?;
        let version: String = row.try_get("version")?;
        let checks: String = row.try_get("checks_json")?;
        Ok(PersistedVerificationContract {
            version: Uuid::parse_str(&version).map_err(|error| {
                StorageError::Invalid(format!("invalid verification contract version: {error}"))
            })?,
            required_checks: serde_json::from_str(&checks)?,
        })
    }

    #[allow(dead_code)] // Intentionally inaccessible to external callers and agent adapters.
    /// Persists only an opaque result created by the trusted verifier.
    ///
    /// ```compile_fail
    /// # async fn example(repo: &synroute::storage::SqliteExecutionRepository, id: uuid::Uuid, report: synroute::verification::VerificationReport) {
    /// repo.record_verification(id, report).await.unwrap();
    /// # }
    /// ```
    pub(crate) async fn record_verification(
        &self,
        id: Uuid,
        verified: crate::verification::ExecutedVerification,
    ) -> Result<(), StorageError> {
        ensure_originating_process(&self.owner)?;
        let report = verified.report();
        let contract_version = verified.contract_version();
        validate_verification_report(report)?;
        let report_json = serde_json::to_value(report)?;
        check_sensitive_keys(&report_json)?;
        let mut tx = self.pool.begin().await?;
        let now = Utc::now();
        let locked = sqlx::query("UPDATE executions SET updated_at = ? WHERE id = ?")
            .bind(timestamp(now))
            .bind(id.to_string())
            .execute(&mut *tx)
            .await?;
        if locked.rows_affected() == 0 {
            return Err(StorageError::NotFound {
                entity: "execution",
                id: id.to_string(),
            });
        }
        validate_owner_tx(&mut tx, id, &self.owner, now).await?;
        let state: String = sqlx::query_scalar("SELECT state FROM executions WHERE id = ?")
            .bind(id.to_string())
            .fetch_one(&mut *tx)
            .await?;
        if state_from_db(&state)? != crate::core::ExecutionState::Verifying {
            return Err(StorageError::Invalid(
                "verification evidence can only be recorded while execution is verifying".into(),
            ));
        }
        let contract_row = sqlx::query(
            "SELECT version, checks_json FROM verification_contracts WHERE execution_id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            entity: "verification contract",
            id: id.to_string(),
        })?;
        let persisted_version: String = contract_row.try_get("version")?;
        if persisted_version != contract_version.to_string() {
            return Err(StorageError::Invalid(
                "verification result is bound to a stale contract version".into(),
            ));
        }
        let required: Vec<crate::core::VerificationCommand> =
            serde_json::from_str(&contract_row.try_get::<String, _>("checks_json")?)?;
        validate_report_matches_contract(report, &required)?;
        sqlx::query("INSERT INTO verification_evidence (execution_id, report_json, occurred_at, status, contract_version) VALUES (?, ?, ?, ?, ?)")
            .bind(id.to_string()).bind(serde_json::to_string(&report_json)?).bind(timestamp(now)).bind(verification_status_str(report.status)).bind(contract_version.to_string()).execute(&mut *tx).await?;
        append_event_tx(
            &mut tx,
            id,
            ExecutionEventType::VerificationCompleted,
            json!({"status": report.status, "contract_version": contract_version}),
            now,
        )
        .await?;
        tx.commit().await?;
        Ok(())
    }
}

fn lease_expiry(now: DateTime<Utc>, ttl: Duration) -> Result<DateTime<Utc>, StorageError> {
    if ttl.is_zero() || ttl > Duration::from_secs(300) {
        return Err(StorageError::Invalid(
            "execution lease TTL must be between 1 and 300 seconds".into(),
        ));
    }
    let ttl = ChronoDuration::from_std(ttl)
        .map_err(|error| StorageError::Invalid(format!("invalid lease TTL: {error}")))?;
    now.checked_add_signed(ttl)
        .ok_or_else(|| StorageError::Invalid("execution lease expiry overflows timestamp".into()))
}

#[allow(dead_code)]
fn validate_verification_report(
    report: &crate::verification::VerificationReport,
) -> Result<(), StorageError> {
    let all_checks_passed = !report.checks.is_empty()
        && report.session_cleanup.is_successful()
        && report.checks.iter().all(|check| {
            check.status == crate::verification::CheckStatus::Passed
                && check.termination == crate::sandbox::Termination::Exited
                && check.exit_code == Some(0)
                && check.cleanup.is_successful()
        });
    if (report.status == crate::verification::VerificationStatus::Passed) != all_checks_passed {
        return Err(StorageError::Invalid(
            "verification status must be passed exactly when all checks and session cleanup succeeded"
                .into(),
        ));
    }
    Ok(())
}

#[allow(dead_code)]
fn validate_report_matches_contract(
    report: &crate::verification::VerificationReport,
    required: &[crate::core::VerificationCommand],
) -> Result<(), StorageError> {
    if report.checks.len() != required.len()
        || required.iter().any(|expected| {
            report
                .checks
                .iter()
                .filter(|actual| actual.name == expected.name && actual.argv == expected.argv)
                .count()
                != 1
        })
    {
        return Err(StorageError::Invalid(
            "verification results do not exactly match all required contract checks".into(),
        ));
    }
    Ok(())
}

#[allow(dead_code)]
fn verification_status_str(status: crate::verification::VerificationStatus) -> &'static str {
    match status {
        crate::verification::VerificationStatus::Passed => "passed",
        crate::verification::VerificationStatus::Failed => "failed",
        crate::verification::VerificationStatus::TimedOut => "timed_out",
        crate::verification::VerificationStatus::Cancelled => "cancelled",
        crate::verification::VerificationStatus::Incomplete => "incomplete",
    }
}

pub(super) async fn append_event_tx(
    tx: &mut Transaction<'_, Sqlite>,
    execution_id: Uuid,
    event_type: ExecutionEventType,
    payload: Value,
    at: DateTime<Utc>,
) -> Result<ExecutionEvent, StorageError> {
    check_sensitive_keys(&payload)?;
    let sequence: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(sequence), 0) + 1 FROM execution_events WHERE execution_id = ?",
    )
    .bind(execution_id.to_string())
    .fetch_one(&mut **tx)
    .await?;
    let result = sqlx::query("INSERT INTO execution_events (execution_id, state, occurred_at, payload_json, sequence, event_type) VALUES (?, '', ?, ?, ?, ?)")
        .bind(execution_id.to_string()).bind(timestamp(at)).bind(serde_json::to_string(&payload)?).bind(sequence).bind(event_type.as_str()).execute(&mut **tx).await?;
    Ok(ExecutionEvent {
        id: result.last_insert_rowid(),
        execution_id,
        sequence,
        event_type,
        occurred_at: at,
        payload,
    })
}

pub(super) fn decode_execution(row: &SqliteRow) -> Result<ExecutionRecord, StorageError> {
    let id: String = row.try_get("id")?;
    let metadata: String = row.try_get("task_metadata_json")?;
    let config: String = row.try_get("execution_config_json")?;
    let outcome: Option<String> = row.try_get("terminal_outcome")?;
    let failure: Option<String> = row.try_get("failure_classification")?;
    Ok(ExecutionRecord {
        id: Uuid::parse_str(&id)
            .map_err(|e| StorageError::Invalid(format!("invalid execution ID in database: {e}")))?,
        description: row.try_get("task_description")?,
        metadata: serde_json::from_str(&metadata)?,
        repository_identity: row.try_get("repository_identity")?,
        repository_root: PathBuf::from(row.try_get::<String, _>("repository_root")?),
        repository_revision: row.try_get("repository_revision")?,
        working_directory: PathBuf::from(row.try_get::<String, _>("working_directory")?),
        state: state_from_db(row.try_get("state")?)?,
        created_at: parse_timestamp(row.try_get("created_at")?)?,
        updated_at: parse_timestamp(row.try_get("updated_at")?)?,
        completed_at: row
            .try_get::<Option<String>, _>("completed_at")?
            .as_deref()
            .map(parse_timestamp)
            .transpose()?,
        terminal_outcome: outcome.as_deref().map(state_from_db).transpose()?,
        failure_classification: failure
            .as_deref()
            .map(|f| {
                serde_json::from_value(Value::String(f.to_owned())).map_err(StorageError::from)
            })
            .transpose()?,
        config: serde_json::from_str(&config)?,
    })
}

fn decode_event(row: &SqliteRow) -> Result<ExecutionEvent, StorageError> {
    let execution_id: String = row.try_get("execution_id")?;
    let event_type: String = row.try_get("event_type")?;
    Ok(ExecutionEvent {
        id: row.try_get("id")?,
        sequence: row.try_get("sequence")?,
        execution_id: Uuid::parse_str(&execution_id)
            .map_err(|e| StorageError::Invalid(format!("invalid event execution ID: {e}")))?,
        event_type: serde_json::from_value(Value::String(event_type))
            .map_err(StorageError::from)?,
        occurred_at: parse_timestamp(row.try_get("occurred_at")?)?,
        payload: serde_json::from_str(row.try_get("payload_json")?)?,
    })
}
