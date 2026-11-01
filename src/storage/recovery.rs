use super::*;
use chrono::Utc;
use serde_json::{Value, json};
use sqlx::{Row, Sqlite, Transaction, sqlite::SqliteRow};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryCaseStatus {
    Open,
    Acknowledged,
    Resolved,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecoveryCaseRecord {
    pub case_id: Uuid,
    pub execution_id: Uuid,
    pub status: RecoveryCaseStatus,
    pub reason: String,
    pub details: Value,
    pub created_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub acknowledged_at: Option<DateTime<Utc>>,
    pub resolved_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct SqliteRecoveryRepository {
    pool: SqlitePool,
}

impl SqliteRecoveryRepository {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn open_cases(&self) -> Result<Vec<RecoveryCaseRecord>, StorageError> {
        let rows = sqlx::query(
            "SELECT * FROM recovery_cases WHERE status = 'open' ORDER BY created_at, case_id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(decode_case).collect()
    }

    pub async fn cases_for_execution(
        &self,
        execution_id: Uuid,
    ) -> Result<Vec<RecoveryCaseRecord>, StorageError> {
        let rows = sqlx::query(
            "SELECT * FROM recovery_cases WHERE execution_id = ? ORDER BY created_at, case_id",
        )
        .bind(execution_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(decode_case).collect()
    }

    /// Acknowledgement suppresses the open case while preserving its history.
    pub async fn acknowledge(&self, case_id: Uuid) -> Result<RecoveryCaseRecord, StorageError> {
        self.set_case_status(case_id, RecoveryCaseStatus::Acknowledged)
            .await
    }

    /// Resolution is explicit and append-only history remains queryable.
    pub async fn resolve(&self, case_id: Uuid) -> Result<RecoveryCaseRecord, StorageError> {
        self.set_case_status(case_id, RecoveryCaseStatus::Resolved)
            .await
    }

    async fn set_case_status(
        &self,
        case_id: Uuid,
        status: RecoveryCaseStatus,
    ) -> Result<RecoveryCaseRecord, StorageError> {
        let now = timestamp(Utc::now());
        let result = match status {
            RecoveryCaseStatus::Acknowledged => sqlx::query("UPDATE recovery_cases SET status = 'acknowledged', acknowledged_at = COALESCE(acknowledged_at, ?), last_seen_at = ? WHERE case_id = ? AND status = 'open'")
                .bind(&now).bind(&now).bind(case_id.to_string()).execute(&self.pool).await?,
            RecoveryCaseStatus::Resolved => sqlx::query("UPDATE recovery_cases SET status = 'resolved', resolved_at = COALESCE(resolved_at, ?), last_seen_at = ? WHERE case_id = ? AND status IN ('open', 'acknowledged')")
                .bind(&now).bind(&now).bind(case_id.to_string()).execute(&self.pool).await?,
            RecoveryCaseStatus::Open => unreachable!("open cases are created by recovery inspection"),
        };
        if result.rows_affected() == 0 {
            return Err(StorageError::Invalid(format!(
                "recovery case {case_id} is missing or cannot transition to {status:?}"
            )));
        }
        self.case(case_id).await
    }

    async fn case(&self, case_id: Uuid) -> Result<RecoveryCaseRecord, StorageError> {
        let row = sqlx::query("SELECT * FROM recovery_cases WHERE case_id = ?")
            .bind(case_id.to_string())
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "recovery case",
                id: case_id.to_string(),
            })?;
        decode_case(&row)
    }

    /// Mutates an execution only when the observed lease still matches, has expired,
    /// and its owner was independently classified inactive by the caller.
    pub(crate) async fn recover_expired_lease(
        &self,
        observed: &ExecutionLease,
        now: DateTime<Utc>,
        details: Value,
    ) -> Result<Option<ExecutionRecord>, StorageError> {
        let id = observed.execution_id;
        let id_text = id.to_string();
        let mut tx = self.pool.begin().await?;
        let locked = sqlx::query("UPDATE executions SET updated_at = updated_at WHERE id = ?")
            .bind(&id_text)
            .execute(&mut *tx)
            .await?;
        if locked.rows_affected() == 0 {
            return Ok(None);
        }
        let row = sqlx::query("SELECT * FROM execution_leases WHERE execution_id = ?")
            .bind(&id_text)
            .fetch_optional(&mut *tx)
            .await?;
        let Some(row) = row else {
            tx.rollback().await?;
            return Ok(None);
        };
        let lease = decode_lease(&row)?;
        if lease.owner_id != observed.owner_id
            || lease.generation != observed.generation
            || lease.expires_at > now
        {
            tx.rollback().await?;
            return Ok(None);
        }
        let current: String = sqlx::query_scalar("SELECT state FROM executions WHERE id = ?")
            .bind(&id_text)
            .fetch_one(&mut *tx)
            .await?;
        let from = state_from_db(&current)?;
        if from.is_terminal() {
            tx.rollback().await?;
            return Ok(None);
        }
        crate::core::ExecutionState::validate_transition(
            Some(from),
            crate::core::ExecutionState::RecoveryRequired,
        )
        .map_err(|error| StorageError::Invalid(error.to_string()))?;
        let at = timestamp(now);
        let recovery_state = state_to_db(crate::core::ExecutionState::RecoveryRequired);
        insert_open_case_tx(&mut tx, id, "expired_lease_owner_inactive", details, now).await?;
        let update = sqlx::query("UPDATE executions SET state = ?, updated_at = ?, completed_at = ?, terminal_outcome = ?, failure_classification = 'interrupted' WHERE id = ? AND state = ?")
            .bind(&recovery_state).bind(&at).bind(&at).bind(&recovery_state).bind(&id_text).bind(&current).execute(&mut *tx).await?;
        if update.rows_affected() != 1 {
            tx.rollback().await?;
            return Ok(None);
        }
        sqlx::query("DELETE FROM execution_leases WHERE execution_id = ? AND owner_id = ? AND generation = ?")
            .bind(&id_text).bind(lease.owner_id.to_string()).bind(lease.generation).execute(&mut *tx).await?;
        super::executions::append_event_tx(
            &mut tx,
            id,
            ExecutionEventType::RecoveryAttempted,
            json!({"owner_id": lease.owner_id, "lease_generation": lease.generation, "owner_liveness": "inactive", "automatic_resume": false}),
            now,
        )
        .await?;
        super::executions::append_event_tx(
            &mut tx,
            id,
            ExecutionEventType::RecoveryRequired,
            json!({"from": current, "to": recovery_state}),
            now,
        )
        .await?;
        tx.commit().await?;
        let row = sqlx::query("SELECT * FROM executions WHERE id = ?")
            .bind(id_text)
            .fetch_one(&self.pool)
            .await?;
        super::executions::decode_execution(&row).map(Some)
    }

    /// Records liveness uncertainty without changing execution state or lease ownership.
    pub(crate) async fn record_uncertainty(
        &self,
        execution_id: Uuid,
        reason: &str,
        details: Value,
        now: DateTime<Utc>,
    ) -> Result<RecoveryCaseRecord, StorageError> {
        let mut tx = self.pool.begin().await?;
        let locked = sqlx::query("UPDATE executions SET updated_at = updated_at WHERE id = ?")
            .bind(execution_id.to_string())
            .execute(&mut *tx)
            .await?;
        if locked.rows_affected() == 0 {
            return Err(StorageError::NotFound {
                entity: "execution",
                id: execution_id.to_string(),
            });
        }
        let existing = sqlx::query("SELECT case_id, status, reason FROM recovery_cases WHERE execution_id = ? ORDER BY created_at DESC, case_id DESC LIMIT 1")
            .bind(execution_id.to_string()).fetch_optional(&mut *tx).await?;
        let case_id = if let Some(row) = existing {
            let existing_id: String = row.try_get("case_id")?;
            let status: String = row.try_get("status")?;
            let old_reason: String = row.try_get("reason")?;
            if status == "resolved" || (status == "acknowledged" && old_reason != reason) {
                insert_open_case_tx(&mut tx, execution_id, reason, details, now).await?
            } else {
                let id = Uuid::parse_str(&existing_id)
                    .map_err(|e| StorageError::Invalid(e.to_string()))?;
                sqlx::query("UPDATE recovery_cases SET reason = ?, last_seen_at = ?, details_json = ? WHERE case_id = ?")
                    .bind(reason).bind(timestamp(now)).bind(serde_json::to_string(&details)?).bind(id.to_string()).execute(&mut *tx).await?;
                id
            }
        } else {
            insert_open_case_tx(&mut tx, execution_id, reason, details, now).await?
        };
        tx.commit().await?;
        self.case(case_id).await
    }
}

async fn insert_open_case_tx(
    tx: &mut Transaction<'_, Sqlite>,
    execution_id: Uuid,
    reason: &str,
    details: Value,
    now: DateTime<Utc>,
) -> Result<Uuid, StorageError> {
    let case_id = Uuid::new_v4();
    let now = timestamp(now);
    sqlx::query("INSERT INTO recovery_cases (case_id, execution_id, status, reason, details_json, created_at, last_seen_at) VALUES (?, ?, 'open', ?, ?, ?, ?) ON CONFLICT DO NOTHING")
        .bind(case_id.to_string()).bind(execution_id.to_string()).bind(reason).bind(serde_json::to_string(&details)?).bind(&now).bind(&now).execute(&mut **tx).await?;
    let existing_id: String = sqlx::query_scalar(
        "SELECT case_id FROM recovery_cases WHERE execution_id = ? AND status = 'open'",
    )
    .bind(execution_id.to_string())
    .fetch_one(&mut **tx)
    .await?;
    let existing_id =
        Uuid::parse_str(&existing_id).map_err(|e| StorageError::Invalid(e.to_string()))?;
    sqlx::query("UPDATE recovery_cases SET reason = ?, last_seen_at = ?, details_json = ? WHERE case_id = ?")
        .bind(reason).bind(now).bind(serde_json::to_string(&details)?).bind(existing_id.to_string()).execute(&mut **tx).await?;
    Ok(existing_id)
}

pub(crate) fn decode_lease(row: &SqliteRow) -> Result<ExecutionLease, StorageError> {
    let execution_id: String = row.try_get("execution_id")?;
    let owner_id: String = row.try_get("owner_id")?;
    Ok(ExecutionLease {
        execution_id: Uuid::parse_str(&execution_id)
            .map_err(|e| StorageError::Invalid(e.to_string()))?,
        owner_id: Uuid::parse_str(&owner_id).map_err(|e| StorageError::Invalid(e.to_string()))?,
        process_id: row
            .try_get::<Option<i64>, _>("owner_pid")?
            .map(|pid| pid as u32),
        process_start: row.try_get("process_start")?,
        boot_id: row.try_get("boot_id")?,
        heartbeat_at: parse_timestamp(row.try_get("heartbeat_at")?)?,
        expires_at: parse_timestamp(row.try_get("expires_at")?)?,
        generation: row.try_get("generation")?,
    })
}

fn decode_case(row: &SqliteRow) -> Result<RecoveryCaseRecord, StorageError> {
    let uuid = |name: &str| -> Result<Uuid, StorageError> {
        let value: String = row.try_get(name)?;
        Uuid::parse_str(&value).map_err(|error| StorageError::Invalid(error.to_string()))
    };
    let status = match row.try_get::<String, _>("status")?.as_str() {
        "open" => RecoveryCaseStatus::Open,
        "acknowledged" => RecoveryCaseStatus::Acknowledged,
        "resolved" => RecoveryCaseStatus::Resolved,
        other => {
            return Err(StorageError::Invalid(format!(
                "unknown recovery case status '{other}'"
            )));
        }
    };
    let optional_time = |name: &str| -> Result<Option<DateTime<Utc>>, StorageError> {
        row.try_get::<Option<String>, _>(name)?
            .as_deref()
            .map(parse_timestamp)
            .transpose()
    };
    Ok(RecoveryCaseRecord {
        case_id: uuid("case_id")?,
        execution_id: uuid("execution_id")?,
        status,
        reason: row.try_get("reason")?,
        details: serde_json::from_str(&row.try_get::<String, _>("details_json")?)?,
        created_at: parse_timestamp(row.try_get("created_at")?)?,
        last_seen_at: parse_timestamp(row.try_get("last_seen_at")?)?,
        acknowledged_at: optional_time("acknowledged_at")?,
        resolved_at: optional_time("resolved_at")?,
    })
}
