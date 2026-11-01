use super::*;
use chrono::Utc;
use sqlx::Row;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContainerDiagnosticReason {
    AmbiguousCreate,
    CleanupFailed,
    OwnershipUncertain,
}

impl ContainerDiagnosticReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AmbiguousCreate => "ambiguous_create",
            Self::CleanupFailed => "cleanup_failed",
            Self::OwnershipUncertain => "ownership_uncertain",
        }
    }

    pub fn safe_detail(self) -> &'static str {
        match self {
            Self::AmbiguousCreate => {
                "container creation may have succeeded without a confirmed response"
            }
            Self::CleanupFailed => "Synroute could not confirm container removal",
            Self::OwnershipUncertain => {
                "container ownership labels did not match; automatic removal was refused"
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContainerDiagnosticStatus {
    Open,
    Resolved,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ContainerIntentStatus {
    Pending,
    Active,
    OrphanSuspected,
    Resolved,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerIntentRecord {
    pub intent_id: Uuid,
    pub execution_id: Uuid,
    pub container_name: String,
    pub container_token: Uuid,
    pub container_id: Option<String>,
    pub status: ContainerIntentStatus,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContainerDiagnosticRecord {
    pub diagnostic_id: Uuid,
    pub execution_id: Uuid,
    pub container_name: String,
    pub container_id: Option<String>,
    pub reason: String,
    pub detail: String,
    pub status: ContainerDiagnosticStatus,
    pub occurred_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub resolved_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
pub struct SqliteContainerDiagnosticRepository {
    pool: SqlitePool,
}

impl SqliteContainerDiagnosticRepository {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub(crate) async fn reserve_intent(
        &self,
        execution_id: Uuid,
        container_name: &str,
        token: Uuid,
    ) -> Result<ContainerIntentRecord, StorageError> {
        if !valid_managed_name(container_name, execution_id) {
            return Err(StorageError::Invalid(
                "container intent name is invalid".into(),
            ));
        }
        let id = Uuid::new_v4();
        let now = Utc::now();
        sqlx::query("INSERT INTO container_execution_intents (intent_id, execution_id, container_name, container_token, status, created_at, updated_at) VALUES (?, ?, ?, ?, 'pending', ?, ?)")
            .bind(id.to_string())
            .bind(execution_id.to_string())
            .bind(container_name)
            .bind(token.to_string())
            .bind(timestamp(now))
            .bind(timestamp(now))
            .execute(&self.pool).await?;
        self.intent(id).await
    }

    pub(crate) async fn mark_active(
        &self,
        intent_id: Uuid,
        container_id: &str,
    ) -> Result<(), StorageError> {
        if !valid_container_id(container_id) {
            return Err(StorageError::Invalid("container ID is invalid".into()));
        }
        self.update_intent(intent_id, "active", Some(container_id))
            .await
    }

    pub(crate) async fn mark_orphan_suspected(
        &self,
        intent_id: Uuid,
        container_id: Option<&str>,
    ) -> Result<(), StorageError> {
        if container_id.is_some_and(|id| !valid_container_id(id)) {
            return Err(StorageError::Invalid("container ID is invalid".into()));
        }
        self.update_intent(intent_id, "orphan_suspected", container_id)
            .await
    }

    pub(crate) async fn resolve_intent(&self, intent_id: Uuid) -> Result<(), StorageError> {
        self.update_intent(intent_id, "resolved", None).await
    }

    pub async fn unresolved_intents(&self) -> Result<Vec<ContainerIntentRecord>, StorageError> {
        let rows = sqlx::query("SELECT * FROM container_execution_intents WHERE status != 'resolved' ORDER BY created_at, intent_id")
            .fetch_all(&self.pool).await?;
        rows.iter().map(decode_intent).collect()
    }

    pub async fn intents_for_execution(
        &self,
        execution_id: Uuid,
    ) -> Result<Vec<ContainerIntentRecord>, StorageError> {
        let rows = sqlx::query("SELECT * FROM container_execution_intents WHERE execution_id = ? ORDER BY created_at, intent_id")
            .bind(execution_id.to_string()).fetch_all(&self.pool).await?;
        rows.iter().map(decode_intent).collect()
    }

    async fn update_intent(
        &self,
        id: Uuid,
        status: &str,
        container_id: Option<&str>,
    ) -> Result<(), StorageError> {
        let result = sqlx::query("UPDATE container_execution_intents SET status = ?, container_id = COALESCE(?, container_id), updated_at = ? WHERE intent_id = ? AND status != 'resolved'")
            .bind(status).bind(container_id).bind(timestamp(Utc::now())).bind(id.to_string()).execute(&self.pool).await?;
        if result.rows_affected() != 1 {
            return Err(StorageError::Invalid(format!(
                "container intent {id} is missing or resolved"
            )));
        }
        Ok(())
    }

    async fn intent(&self, id: Uuid) -> Result<ContainerIntentRecord, StorageError> {
        let row = sqlx::query("SELECT * FROM container_execution_intents WHERE intent_id = ?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "container intent",
                id: id.to_string(),
            })?;
        decode_intent(&row)
    }

    /// Stores only container identity and a fixed diagnostic; Docker output is never persisted here.
    pub(crate) async fn record(
        &self,
        execution_id: Uuid,
        container_name: &str,
        container_id: Option<&str>,
        reason: ContainerDiagnosticReason,
    ) -> Result<ContainerDiagnosticRecord, StorageError> {
        if !valid_managed_name(container_name, execution_id)
            || container_id.is_some_and(|id| !valid_container_id(id))
        {
            return Err(StorageError::Invalid(
                "container diagnostic identity is invalid".into(),
            ));
        }
        let id = Uuid::new_v4();
        let now = Utc::now();
        sqlx::query("INSERT INTO container_diagnostics (diagnostic_id, execution_id, container_name, container_id, reason, detail, status, occurred_at, last_seen_at) VALUES (?, ?, ?, ?, ?, ?, 'open', ?, ?)")
            .bind(id.to_string())
            .bind(execution_id.to_string())
            .bind(container_name)
            .bind(container_id)
            .bind(reason.as_str())
            .bind(reason.safe_detail())
            .bind(timestamp(now))
            .bind(timestamp(now))
            .execute(&self.pool)
            .await?;
        self.get(id).await
    }

    pub async fn open(&self) -> Result<Vec<ContainerDiagnosticRecord>, StorageError> {
        let rows = sqlx::query("SELECT * FROM container_diagnostics WHERE status = 'open' ORDER BY occurred_at, diagnostic_id")
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(decode).collect()
    }

    pub async fn for_execution(
        &self,
        execution_id: Uuid,
    ) -> Result<Vec<ContainerDiagnosticRecord>, StorageError> {
        let rows = sqlx::query("SELECT * FROM container_diagnostics WHERE execution_id = ? ORDER BY occurred_at, diagnostic_id")
            .bind(execution_id.to_string())
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(decode).collect()
    }

    /// Mark a fixed container diagnostic as manually reviewed/resolved. This does not remove any container.
    pub async fn resolve(&self, id: Uuid) -> Result<ContainerDiagnosticRecord, StorageError> {
        let now = timestamp(Utc::now());
        let result = sqlx::query("UPDATE container_diagnostics SET status = 'resolved', resolved_at = ?, last_seen_at = ? WHERE diagnostic_id = ? AND status = 'open'")
            .bind(&now)
            .bind(&now)
            .bind(id.to_string())
            .execute(&self.pool)
            .await?;
        if result.rows_affected() != 1 {
            return Err(StorageError::Invalid(format!(
                "container diagnostic {id} is missing or already resolved"
            )));
        }
        self.get(id).await
    }

    async fn get(&self, id: Uuid) -> Result<ContainerDiagnosticRecord, StorageError> {
        let row = sqlx::query("SELECT * FROM container_diagnostics WHERE diagnostic_id = ?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "container diagnostic",
                id: id.to_string(),
            })?;
        decode(&row)
    }
}

fn decode_intent(row: &sqlx::sqlite::SqliteRow) -> Result<ContainerIntentRecord, StorageError> {
    let uuid = |column: &str| -> Result<Uuid, StorageError> {
        let value: String = row.try_get(column)?;
        Uuid::parse_str(&value).map_err(|error| StorageError::Invalid(error.to_string()))
    };
    let status = match row.try_get::<String, _>("status")?.as_str() {
        "pending" => ContainerIntentStatus::Pending,
        "active" => ContainerIntentStatus::Active,
        "orphan_suspected" => ContainerIntentStatus::OrphanSuspected,
        "resolved" => ContainerIntentStatus::Resolved,
        other => {
            return Err(StorageError::Invalid(format!(
                "unknown container intent status '{other}'"
            )));
        }
    };
    Ok(ContainerIntentRecord {
        intent_id: uuid("intent_id")?,
        execution_id: uuid("execution_id")?,
        container_name: row.try_get("container_name")?,
        container_token: uuid("container_token")?,
        container_id: row.try_get("container_id")?,
        status,
        created_at: parse_timestamp(row.try_get("created_at")?)?,
        updated_at: parse_timestamp(row.try_get("updated_at")?)?,
    })
}

fn valid_managed_name(name: &str, execution_id: Uuid) -> bool {
    let prefix = format!("synroute-{execution_id}-");
    name.strip_prefix(&prefix)
        .is_some_and(|suffix| suffix.parse::<Uuid>().is_ok())
}

fn valid_container_id(id: &str) -> bool {
    (12..=64).contains(&id.len()) && id.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn decode(row: &sqlx::sqlite::SqliteRow) -> Result<ContainerDiagnosticRecord, StorageError> {
    let uuid = |column: &str| -> Result<Uuid, StorageError> {
        let value: String = row.try_get(column)?;
        Uuid::parse_str(&value).map_err(|error| StorageError::Invalid(error.to_string()))
    };
    let status = match row.try_get::<String, _>("status")?.as_str() {
        "open" => ContainerDiagnosticStatus::Open,
        "resolved" => ContainerDiagnosticStatus::Resolved,
        other => {
            return Err(StorageError::Invalid(format!(
                "unknown container diagnostic status '{other}'"
            )));
        }
    };
    Ok(ContainerDiagnosticRecord {
        diagnostic_id: uuid("diagnostic_id")?,
        execution_id: uuid("execution_id")?,
        container_name: row.try_get("container_name")?,
        container_id: row.try_get("container_id")?,
        reason: row.try_get("reason")?,
        detail: row.try_get("detail")?,
        status,
        occurred_at: parse_timestamp(row.try_get("occurred_at")?)?,
        last_seen_at: parse_timestamp(row.try_get("last_seen_at")?)?,
        resolved_at: row
            .try_get::<Option<String>, _>("resolved_at")?
            .as_deref()
            .map(parse_timestamp)
            .transpose()?,
    })
}
