use super::*;
use async_trait::async_trait;
use chrono::Utc;
use serde_json::json;
use sqlx::Row;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceStatus {
    Preparing,
    Registered,
    Removing,
    Removed,
    Uncertain,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WorkspaceIssueCode {
    ExistingSidecar,
    SidecarMissing,
    SidecarInvalid,
    SidecarMismatch,
    RepositoryUnavailable,
    WorkspaceUnavailable,
    PathMismatch,
    RegistrationMissing,
    GitMetadataMismatch,
    GitInspectionFailed,
    InterruptedCreation,
    InterruptedRemoval,
    GitOperationFailed,
    SidecarUpdateFailed,
}

impl WorkspaceIssueCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ExistingSidecar => "existing_sidecar",
            Self::SidecarMissing => "sidecar_missing",
            Self::SidecarInvalid => "sidecar_invalid",
            Self::SidecarMismatch => "sidecar_mismatch",
            Self::RepositoryUnavailable => "repository_unavailable",
            Self::WorkspaceUnavailable => "workspace_unavailable",
            Self::PathMismatch => "workspace_path_mismatch",
            Self::RegistrationMissing => "git_registration_missing",
            Self::GitMetadataMismatch => "git_metadata_mismatch",
            Self::GitInspectionFailed => "git_inspection_failed",
            Self::InterruptedCreation => "creation_interrupted_or_incomplete",
            Self::InterruptedRemoval => "removal_interrupted_or_incomplete",
            Self::GitOperationFailed => "git_operation_failed",
            Self::SidecarUpdateFailed => "sidecar_update_failed",
        }
    }

    pub fn safe_detail(self) -> &'static str {
        match self {
            Self::ExistingSidecar => "the reserved sidecar destination already existed",
            Self::SidecarMissing => "the ownership sidecar is missing or inaccessible",
            Self::SidecarInvalid => "the ownership sidecar is not valid regular-file metadata",
            Self::SidecarMismatch => "the sidecar does not match the persisted owner record",
            Self::RepositoryUnavailable => "the repository root is missing or inaccessible",
            Self::WorkspaceUnavailable => "the workspace path is missing or inaccessible",
            Self::PathMismatch => "the workspace path does not match the persisted canonical path",
            Self::RegistrationMissing => "the worktree is not registered at the persisted path",
            Self::GitMetadataMismatch => {
                "Git administrative metadata differs from the owner record"
            }
            Self::GitInspectionFailed => {
                "isolated Git inspection could not establish workspace ownership"
            }
            Self::InterruptedCreation => {
                "workspace creation did not reach a confirmed registered state"
            }
            Self::InterruptedRemoval => "workspace removal did not reach a confirmed removed state",
            Self::GitOperationFailed => "an isolated Git worktree operation failed",
            Self::SidecarUpdateFailed => "ownership metadata update did not complete",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkspaceDiagnostic {
    pub code: WorkspaceIssueCode,
}

impl WorkspaceDiagnostic {
    pub fn new(code: WorkspaceIssueCode) -> Self {
        Self { code }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkspaceOwnershipRecord {
    pub workspace_id: Uuid,
    pub execution_id: Uuid,
    pub canonical_path: PathBuf,
    pub repository_identity: String,
    pub repository_root: PathBuf,
    pub repository_revision: String,
    pub git_admin_path: Option<PathBuf>,
    pub git_common_dir: Option<PathBuf>,
    pub sidecar_path: PathBuf,
    pub ownership_token: Uuid,
    pub status: WorkspaceStatus,
    pub uncertainty_code: Option<String>,
    pub uncertainty_detail: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[async_trait]
pub trait WorkspaceRepository: Send + Sync {
    async fn reserve(&self, record: &WorkspaceOwnershipRecord) -> Result<(), StorageError>;
    async fn workspace(&self, id: Uuid) -> Result<WorkspaceOwnershipRecord, StorageError>;
    async fn for_execution(
        &self,
        execution_id: Uuid,
    ) -> Result<Vec<WorkspaceOwnershipRecord>, StorageError>;
    async fn inspectable(&self) -> Result<Vec<WorkspaceOwnershipRecord>, StorageError>;
    async fn set_status(
        &self,
        id: Uuid,
        status: WorkspaceStatus,
        git_admin: Option<&Path>,
        git_common: Option<&Path>,
        diagnostic: Option<WorkspaceDiagnostic>,
    ) -> Result<WorkspaceOwnershipRecord, StorageError>;
}

#[derive(Debug, Clone)]
pub struct SqliteWorkspaceRepository {
    pool: sqlx::SqlitePool,
}
impl SqliteWorkspaceRepository {
    pub(crate) fn new(pool: sqlx::SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl WorkspaceRepository for SqliteWorkspaceRepository {
    async fn reserve(&self, record: &WorkspaceOwnershipRecord) -> Result<(), StorageError> {
        if !record.canonical_path.is_absolute()
            || !record.repository_root.is_absolute()
            || !record.sidecar_path.is_absolute()
        {
            return Err(StorageError::Invalid(
                "workspace paths must be absolute canonical paths".into(),
            ));
        }
        if record.status != WorkspaceStatus::Preparing {
            return Err(StorageError::Invalid(
                "workspace reservation must start in preparing state".into(),
            ));
        }
        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT INTO workspaces (workspace_id, execution_id, canonical_path, repository_identity, repository_root, repository_revision, git_admin_path, git_common_dir, sidecar_path, ownership_token, status, created_at, updated_at, uncertainty_code, uncertainty_detail) VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, ?, ?, ?, ?, ?, NULL, NULL)")
            .bind(record.workspace_id.to_string()).bind(record.execution_id.to_string()).bind(record.canonical_path.to_string_lossy().as_ref()).bind(&record.repository_identity)
            .bind(record.repository_root.to_string_lossy().as_ref()).bind(&record.repository_revision).bind(record.sidecar_path.to_string_lossy().as_ref())
            .bind(record.ownership_token.to_string()).bind(status_str(record.status)).bind(timestamp(record.created_at)).bind(timestamp(record.updated_at)).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn workspace(&self, id: Uuid) -> Result<WorkspaceOwnershipRecord, StorageError> {
        let row = sqlx::query("SELECT * FROM workspaces WHERE workspace_id = ?")
            .bind(id.to_string())
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| StorageError::NotFound {
                entity: "workspace",
                id: id.to_string(),
            })?;
        decode_workspace(&row)
    }

    async fn for_execution(
        &self,
        execution_id: Uuid,
    ) -> Result<Vec<WorkspaceOwnershipRecord>, StorageError> {
        let rows = sqlx::query(
            "SELECT * FROM workspaces WHERE execution_id = ? ORDER BY created_at, workspace_id",
        )
        .bind(execution_id.to_string())
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(decode_workspace).collect()
    }

    async fn inspectable(&self) -> Result<Vec<WorkspaceOwnershipRecord>, StorageError> {
        let rows = sqlx::query(
            "SELECT * FROM workspaces WHERE status <> 'removed' ORDER BY created_at, workspace_id",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.iter().map(decode_workspace).collect()
    }

    async fn set_status(
        &self,
        id: Uuid,
        status: WorkspaceStatus,
        git_admin: Option<&Path>,
        git_common: Option<&Path>,
        diagnostic: Option<WorkspaceDiagnostic>,
    ) -> Result<WorkspaceOwnershipRecord, StorageError> {
        let mut tx = self.pool.begin().await?;
        let locked =
            sqlx::query("UPDATE workspaces SET updated_at = updated_at WHERE workspace_id = ?")
                .bind(id.to_string())
                .execute(&mut *tx)
                .await?;
        if locked.rows_affected() == 0 {
            return Err(StorageError::NotFound {
                entity: "workspace",
                id: id.to_string(),
            });
        }
        let previous = sqlx::query(
            "SELECT execution_id, status, canonical_path FROM workspaces WHERE workspace_id = ?",
        )
        .bind(id.to_string())
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| StorageError::NotFound {
            entity: "workspace",
            id: id.to_string(),
        })?;
        let execution_id: String = previous.try_get("execution_id")?;
        let old = parse_status(previous.try_get("status")?)?;
        let path: String = previous.try_get("canonical_path")?;
        let allowed = matches!(
            (old, status),
            (
                WorkspaceStatus::Preparing,
                WorkspaceStatus::Registered | WorkspaceStatus::Uncertain | WorkspaceStatus::Removed
            ) | (
                WorkspaceStatus::Registered,
                WorkspaceStatus::Removing | WorkspaceStatus::Uncertain
            ) | (
                WorkspaceStatus::Removing,
                WorkspaceStatus::Removed | WorkspaceStatus::Registered | WorkspaceStatus::Uncertain
            ) | (WorkspaceStatus::Uncertain, WorkspaceStatus::Uncertain)
                | (WorkspaceStatus::Removed, WorkspaceStatus::Uncertain)
        );
        if !allowed {
            return Err(StorageError::Invalid(format!(
                "invalid workspace status transition: {old:?} -> {status:?}"
            )));
        }
        let now = Utc::now();
        let (issue_code, issue_detail) = if status == WorkspaceStatus::Uncertain {
            let diagnostic = diagnostic.ok_or_else(|| {
                StorageError::Invalid(
                    "uncertain workspace status requires a safe diagnostic code".into(),
                )
            })?;
            (
                Some(diagnostic.code.as_str()),
                Some(diagnostic.code.safe_detail()),
            )
        } else {
            (None, None)
        };
        sqlx::query("UPDATE workspaces SET status = ?, git_admin_path = COALESCE(?, git_admin_path), git_common_dir = COALESCE(?, git_common_dir), uncertainty_code = ?, uncertainty_detail = ?, updated_at = ? WHERE workspace_id = ?")
            .bind(status_str(status)).bind(git_admin.map(|p| p.to_string_lossy().into_owned())).bind(git_common.map(|p| p.to_string_lossy().into_owned())).bind(issue_code).bind(issue_detail).bind(timestamp(now)).bind(id.to_string()).execute(&mut *tx).await?;
        if matches!(
            status,
            WorkspaceStatus::Registered | WorkspaceStatus::Removed | WorkspaceStatus::Uncertain
        ) {
            let event_type = match status {
                WorkspaceStatus::Registered => ExecutionEventType::WorkspacePrepared,
                WorkspaceStatus::Removed => ExecutionEventType::WorkspaceRemoved,
                _ => ExecutionEventType::WorkspaceOwnershipUncertain,
            };
            super::executions::append_event_tx(
                &mut tx,
                Uuid::parse_str(&execution_id).map_err(|e| StorageError::Invalid(e.to_string()))?,
                event_type,
                json!({"workspace_id": id, "status": status, "path": path, "uncertainty_code": issue_code, "uncertainty_detail": issue_detail}),
                now,
            )
            .await?;
        }
        tx.commit().await?;
        self.workspace(id).await
    }
}

fn status_str(status: WorkspaceStatus) -> &'static str {
    match status {
        WorkspaceStatus::Preparing => "preparing",
        WorkspaceStatus::Registered => "registered",
        WorkspaceStatus::Removing => "removing",
        WorkspaceStatus::Removed => "removed",
        WorkspaceStatus::Uncertain => "uncertain",
    }
}
fn parse_status(value: &str) -> Result<WorkspaceStatus, StorageError> {
    match value {
        "preparing" => Ok(WorkspaceStatus::Preparing),
        "registered" => Ok(WorkspaceStatus::Registered),
        "removing" => Ok(WorkspaceStatus::Removing),
        "removed" => Ok(WorkspaceStatus::Removed),
        "uncertain" => Ok(WorkspaceStatus::Uncertain),
        other => Err(StorageError::Invalid(format!(
            "unknown workspace status '{other}'"
        ))),
    }
}
fn decode_workspace(
    row: &sqlx::sqlite::SqliteRow,
) -> Result<WorkspaceOwnershipRecord, StorageError> {
    let uuid = |column: &str| -> Result<Uuid, StorageError> {
        let text: String = row.try_get(column)?;
        Uuid::parse_str(&text).map_err(|e| StorageError::Invalid(format!("invalid {column}: {e}")))
    };
    Ok(WorkspaceOwnershipRecord {
        workspace_id: uuid("workspace_id")?,
        execution_id: uuid("execution_id")?,
        canonical_path: PathBuf::from(row.try_get::<String, _>("canonical_path")?),
        repository_identity: row.try_get("repository_identity")?,
        repository_root: PathBuf::from(row.try_get::<String, _>("repository_root")?),
        repository_revision: row.try_get("repository_revision")?,
        git_admin_path: row
            .try_get::<Option<String>, _>("git_admin_path")?
            .map(PathBuf::from),
        git_common_dir: row
            .try_get::<Option<String>, _>("git_common_dir")?
            .map(PathBuf::from),
        sidecar_path: PathBuf::from(row.try_get::<String, _>("sidecar_path")?),
        ownership_token: uuid("ownership_token")?,
        status: parse_status(row.try_get("status")?)?,
        uncertainty_code: row.try_get("uncertainty_code")?,
        uncertainty_detail: row.try_get("uncertainty_detail")?,
        created_at: parse_timestamp(row.try_get("created_at")?)?,
        updated_at: parse_timestamp(row.try_get("updated_at")?)?,
    })
}
