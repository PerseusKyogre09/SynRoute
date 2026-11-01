use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};
use std::{
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};
use thiserror::Error;
use uuid::Uuid;

mod containers;
mod executions;
mod recovery;
mod workspaces;
pub use containers::*;
pub use executions::*;
pub use recovery::*;
pub use workspaces::*;

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    #[error("database migration failed: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),
    #[error("storage serialization failed: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("{entity} {id} was not found")]
    NotFound { entity: &'static str, id: String },
    #[error("invalid storage request: {0}")]
    Invalid(String),
    #[error("refusing to persist sensitive field '{0}'")]
    SensitiveField(String),
    #[error("filesystem operation failed: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone)]
pub struct SqliteStorage {
    pool: SqlitePool,
    owner: OwnerIdentity,
}

pub const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(45);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OwnerIdentity {
    pub owner_id: Uuid,
    pub process_id: Option<u32>,
    pub process_start: Option<String>,
    pub boot_id: Option<String>,
    /// This implementation has create-only ownership; it never transfers leases.
    pub generation: i64,
}

impl OwnerIdentity {
    fn current() -> Self {
        let process_id = std::process::id();
        let (process_start, boot_id) = process_identity(process_id)
            .map(|(start, boot)| (Some(start), Some(boot)))
            .unwrap_or((None, None));
        Self {
            owner_id: Uuid::new_v4(),
            process_id: Some(process_id),
            process_start,
            boot_id,
            generation: 1,
        }
    }

    pub(crate) fn is_current_process(&self) -> bool {
        let current = Self::current();
        self.process_id == current.process_id
            && self.process_start == current.process_start
            && self.boot_id == current.boot_id
    }
}

#[cfg(target_os = "linux")]
fn process_identity(pid: u32) -> Option<(String, String)> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let fields = stat
        .rsplit_once(')')?
        .1
        .split_whitespace()
        .collect::<Vec<_>>();
    // The suffix begins with field 3; starttime is field 22.
    let start = fields.get(19)?.to_string();
    let boot_id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id")
        .ok()?
        .trim()
        .to_owned();
    Some((start, boot_id))
}

#[cfg(not(target_os = "linux"))]
fn process_identity(_: u32) -> Option<(String, String)> {
    None
}

impl SqliteStorage {
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref();
        if let Some(parent) = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            tokio::fs::create_dir_all(parent).await?;
        }
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(10));
        Self::connect(options).await
    }

    pub async fn from_url(url: &str) -> Result<Self, StorageError> {
        let options = SqliteConnectOptions::from_str(url)
            .map_err(|error| StorageError::Invalid(format!("invalid SQLite URL: {error}")))?
            .create_if_missing(true)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(10));
        Self::connect(options).await
    }

    async fn connect(options: SqliteConnectOptions) -> Result<Self, StorageError> {
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(10))
            .connect_with(options)
            .await?;
        MIGRATOR.run(&pool).await?;
        Ok(Self {
            pool,
            owner: OwnerIdentity::current(),
        })
    }

    #[cfg(test)]
    pub(crate) fn pool(&self) -> &SqlitePool {
        &self.pool
    }
    pub fn executions(&self) -> SqliteExecutionRepository {
        SqliteExecutionRepository::new(self.pool.clone(), self.owner.clone())
    }
    pub fn workspaces(&self) -> SqliteWorkspaceRepository {
        SqliteWorkspaceRepository::new(self.pool.clone())
    }
    pub fn recovery(&self) -> SqliteRecoveryRepository {
        SqliteRecoveryRepository::new(self.pool.clone())
    }
    pub fn containers(&self) -> SqliteContainerDiagnosticRepository {
        SqliteContainerDiagnosticRepository::new(self.pool.clone())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedExecutionConfig {
    pub timeout_seconds: u64,
    pub max_recovery_attempts: u8,
    pub memory_bytes: u64,
    pub cpu_millis: u32,
    pub process_limit: u32,
    pub output_limit_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionIntent {
    pub id: Uuid,
    pub description: String,
    pub metadata: Value,
    pub repository_identity: String,
    pub repository_root: PathBuf,
    pub repository_revision: String,
    pub working_directory: PathBuf,
    pub config: PersistedExecutionConfig,
    pub required_verification: Vec<crate::core::VerificationCommand>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedVerificationContract {
    pub version: Uuid,
    pub required_checks: Vec<crate::core::VerificationCommand>,
}

impl PersistedVerificationContract {
    #[allow(dead_code)] // Used by the trusted verification path; execution orchestration is future scope.
    pub(crate) fn version(&self) -> Uuid {
        self.version
    }
    #[allow(dead_code)]
    pub(crate) fn required_checks(&self) -> &[crate::core::VerificationCommand] {
        &self.required_checks
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRecord {
    pub id: Uuid,
    pub description: String,
    pub metadata: Value,
    pub repository_identity: String,
    pub repository_root: PathBuf,
    pub repository_revision: String,
    pub working_directory: PathBuf,
    pub state: crate::core::ExecutionState,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub terminal_outcome: Option<crate::core::ExecutionState>,
    pub failure_classification: Option<FailureClassification>,
    pub config: PersistedExecutionConfig,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FailureClassification {
    Repository,
    Planning,
    Agent,
    Sandbox,
    Verification,
    Cancelled,
    Interrupted,
    Timeout,
    Unknown,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionEventType {
    ExecutionCreated,
    RepositoryAnalyzed,
    PlanningStarted,
    PlanningCompleted,
    WorkspacePrepared,
    WorkspaceRemoved,
    AgentExecutionStarted,
    AgentExecutionCompleted,
    AgentActivity,
    VerificationStarted,
    VerificationCompleted,
    RecoveryAttempted,
    StateTransition,
    ExecutionCompleted,
    ExecutionPartiallyCompleted,
    ExecutionUnverified,
    ExecutionTimedOut,
    ExecutionFailed,
    ExecutionCancelled,
    ExecutionInterrupted,
    RecoveryRequired,
    WorkspaceOwnershipUncertain,
    LegacyEvent,
}

impl ExecutionEventType {
    pub(crate) fn is_owner_appendable(self) -> bool {
        matches!(
            self,
            Self::RepositoryAnalyzed
                | Self::PlanningStarted
                | Self::PlanningCompleted
                | Self::WorkspacePrepared
                | Self::WorkspaceRemoved
                | Self::WorkspaceOwnershipUncertain
                | Self::AgentActivity
        )
    }

    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::ExecutionCreated => "execution_created",
            Self::RepositoryAnalyzed => "repository_analyzed",
            Self::PlanningStarted => "planning_started",
            Self::PlanningCompleted => "planning_completed",
            Self::WorkspacePrepared => "workspace_prepared",
            Self::AgentExecutionStarted => "agent_execution_started",
            Self::WorkspaceRemoved => "workspace_removed",
            Self::AgentExecutionCompleted => "agent_execution_completed",
            Self::AgentActivity => "agent_activity",
            Self::VerificationStarted => "verification_started",
            Self::VerificationCompleted => "verification_completed",
            Self::RecoveryAttempted => "recovery_attempted",
            Self::StateTransition => "state_transition",
            Self::ExecutionCompleted => "execution_completed",
            Self::ExecutionPartiallyCompleted => "execution_partially_completed",
            Self::ExecutionUnverified => "execution_unverified",
            Self::ExecutionTimedOut => "execution_timed_out",
            Self::ExecutionFailed => "execution_failed",
            Self::ExecutionCancelled => "execution_cancelled",
            Self::ExecutionInterrupted => "execution_interrupted",
            Self::RecoveryRequired => "recovery_required",
            Self::WorkspaceOwnershipUncertain => "workspace_ownership_uncertain",
            Self::LegacyEvent => "legacy_event",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionEvent {
    pub id: i64,
    pub execution_id: Uuid,
    pub sequence: i64,
    pub event_type: ExecutionEventType,
    pub occurred_at: DateTime<Utc>,
    pub payload: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ExecutionLease {
    pub execution_id: Uuid,
    pub owner_id: Uuid,
    pub process_id: Option<u32>,
    pub process_start: Option<String>,
    pub boot_id: Option<String>,
    pub heartbeat_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub generation: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationRecord {
    pub created_at: DateTime<Utc>,
    pub contract_version: Uuid,
    pub report: crate::verification::VerificationReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionReport {
    pub execution: ExecutionRecord,
    pub verification_contract: PersistedVerificationContract,
    pub events: Vec<ExecutionEvent>,
    pub workspaces: Vec<WorkspaceOwnershipRecord>,
    pub verifications: Vec<VerificationRecord>,
    pub container_diagnostics: Vec<ContainerDiagnosticRecord>,
    pub container_intents: Vec<ContainerIntentRecord>,
}

pub(crate) fn state_to_db(state: crate::core::ExecutionState) -> String {
    serde_json::to_value(state)
        .expect("serializing ExecutionState is infallible")
        .as_str()
        .unwrap()
        .to_owned()
}
pub(crate) fn state_from_db(value: &str) -> Result<crate::core::ExecutionState, StorageError> {
    Ok(serde_json::from_value(Value::String(value.to_owned()))?)
}
pub(crate) fn timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339()
}
pub(crate) fn parse_timestamp(value: &str) -> Result<DateTime<Utc>, StorageError> {
    Ok(DateTime::parse_from_rfc3339(value)
        .map_err(|error| StorageError::Invalid(format!("invalid stored timestamp: {error}")))?
        .with_timezone(&Utc))
}
pub(crate) fn check_sensitive_keys(value: &Value) -> Result<(), StorageError> {
    const BLOCKED: [&str; 9] = [
        "api_key",
        "apikey",
        "token",
        "secret",
        "password",
        "credential",
        "authorization",
        "private_key",
        "access_key",
    ];
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                let normalized: String = key
                    .chars()
                    .filter(|c| c.is_ascii_alphanumeric())
                    .flat_map(char::to_lowercase)
                    .collect();
                if BLOCKED.iter().any(|blocked| normalized.contains(blocked)) {
                    return Err(StorageError::SensitiveField(key.clone()));
                }
                check_sensitive_keys(value)?;
            }
        }
        Value::Array(items) => {
            for item in items {
                check_sensitive_keys(item)?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        core::ExecutionState,
        verification::{
            CheckEvidence, CheckStatus, ExecutedVerification, VerificationReport,
            VerificationStatus,
        },
    };
    use serde_json::json;

    async fn fixture() -> (tempfile::TempDir, SqliteStorage, ExecutionIntent) {
        let dir = tempfile::tempdir().unwrap();
        let db = SqliteStorage::open(dir.path().join("state.sqlite"))
            .await
            .unwrap();
        let intent = ExecutionIntent {
            id: Uuid::new_v4(),
            description: "fix a checked fixture".into(),
            metadata: json!({"labels": ["unit"]}),
            repository_identity: "file:///fixture/repository".into(),
            repository_root: dir.path().canonicalize().unwrap(),
            repository_revision: "abcdef0123456789".into(),
            working_directory: dir.path().canonicalize().unwrap(),
            config: PersistedExecutionConfig {
                timeout_seconds: 60,
                max_recovery_attempts: 1,
                memory_bytes: 1024,
                cpu_millis: 500,
                process_limit: 16,
                output_limit_bytes: 8192,
            },
            required_verification: vec![crate::core::VerificationCommand {
                name: "unit tests".into(),
                argv: vec!["cargo".into(), "test".into()],
            }],
        };
        (dir, db, intent)
    }

    #[tokio::test]
    async fn initializes_sqlite_wal_foreign_keys_and_versioned_migrations() {
        let (_dir, db, _) = fixture().await;
        let mut conn = db.pool().acquire().await.unwrap();
        assert_eq!(
            sqlx::query_scalar::<_, String>("PRAGMA journal_mode")
                .fetch_one(&mut *conn)
                .await
                .unwrap()
                .to_lowercase(),
            "wal"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("PRAGMA foreign_keys")
                .fetch_one(&mut *db.pool().acquire().await.unwrap())
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM _sqlx_migrations WHERE success = 1")
                .fetch_one(db.pool())
                .await
                .unwrap(),
            7
        );
    }

    #[tokio::test]
    async fn lifecycle_rejects_completion_without_passing_verification() {
        let (_dir, db, intent) = fixture().await;
        let repo = db.executions();
        let id = intent.id;
        let created = repo.create_execution(intent).await.unwrap();
        assert_eq!(created.state, ExecutionState::Received);
        assert_eq!(
            repo.events(id).await.unwrap()[0].event_type,
            ExecutionEventType::ExecutionCreated
        );
        assert!(
            record_test_report(&repo, id, passing_report())
                .await
                .is_err()
        );
        assert!(
            repo.transition(id, ExecutionState::Completed, None)
                .await
                .is_err()
        );
        assert_eq!(
            repo.execution(id).await.unwrap().state,
            ExecutionState::Received
        );
        for (next, failure) in [
            (ExecutionState::RepositoryAnalysis, None),
            (ExecutionState::Planning, None),
            (ExecutionState::PlanValidation, None),
            (ExecutionState::WorkspacePreparation, None),
            (ExecutionState::Executing, None),
            (ExecutionState::Verifying, None),
        ] {
            repo.transition(id, next, failure).await.unwrap();
        }
        assert!(
            repo.transition(id, ExecutionState::Completed, None)
                .await
                .is_err()
        );
        assert_eq!(
            repo.execution(id).await.unwrap().state,
            ExecutionState::Verifying
        );
        record_test_report(&repo, id, passing_report())
            .await
            .unwrap();
        repo.transition(id, ExecutionState::Completed, None)
            .await
            .unwrap();
        let done = repo.execution(id).await.unwrap();
        assert_eq!(done.terminal_outcome, Some(ExecutionState::Completed));
        assert!(done.completed_at.is_some());
        assert!(
            repo.transition(
                id,
                ExecutionState::Failed,
                Some(FailureClassification::Unknown)
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn foreign_storage_instance_cannot_mutate_or_emit_lifecycle_events() {
        let (dir, owner, intent) = fixture().await;
        let id = intent.id;
        let repo = owner.executions();
        repo.create_execution(intent).await.unwrap();
        let foreign = SqliteStorage::open(dir.path().join("state.sqlite"))
            .await
            .unwrap();
        assert!(
            foreign
                .executions()
                .transition(id, ExecutionState::RepositoryAnalysis, None)
                .await
                .is_err()
        );
        for forbidden in [
            ExecutionEventType::AgentExecutionCompleted,
            ExecutionEventType::VerificationCompleted,
            ExecutionEventType::RecoveryRequired,
            ExecutionEventType::ExecutionFailed,
        ] {
            assert!(
                repo.append_event(id, forbidden, serde_json::json!({}))
                    .await
                    .is_err()
            );
        }
        assert!(
            foreign
                .executions()
                .append_event(
                    id,
                    ExecutionEventType::ExecutionCompleted,
                    serde_json::json!({})
                )
                .await
                .is_err()
        );
        assert!(
            foreign
                .executions()
                .append_event(
                    id,
                    ExecutionEventType::RepositoryAnalyzed,
                    serde_json::json!({})
                )
                .await
                .is_err()
        );
        repo.transition(id, ExecutionState::RepositoryAnalysis, None)
            .await
            .unwrap();
        for state in [
            ExecutionState::Planning,
            ExecutionState::PlanValidation,
            ExecutionState::WorkspacePreparation,
            ExecutionState::Executing,
            ExecutionState::Verifying,
        ] {
            repo.transition(id, state, None).await.unwrap();
        }
        assert!(
            record_test_report(&foreign.executions(), id, passing_report())
                .await
                .is_err()
        );
        assert_eq!(
            repo.execution(id).await.unwrap().state,
            ExecutionState::Verifying
        );
    }

    #[tokio::test]
    async fn old_lease_generation_cannot_renew_transition_or_append_events() {
        let (_dir, db, intent) = fixture().await;
        let id = intent.id;
        let repo = db.executions();
        advance_to_verifying(&repo, intent).await;
        sqlx::query("UPDATE execution_leases SET generation = 2 WHERE execution_id = ?")
            .bind(id.to_string())
            .execute(db.pool())
            .await
            .unwrap();
        assert!(repo.renew_lease(id, Duration::from_secs(30)).await.is_err());
        assert!(
            repo.transition(id, ExecutionState::Completed, None)
                .await
                .is_err()
        );
        assert!(
            repo.append_event(
                id,
                ExecutionEventType::RepositoryAnalyzed,
                serde_json::json!({})
            )
            .await
            .is_err()
        );
        assert!(
            record_test_report(&repo, id, passing_report())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn duplicate_verification_contract_check_names_are_rejected() {
        let (_dir, db, mut intent) = fixture().await;
        intent
            .required_verification
            .push(intent.required_verification[0].clone());
        assert!(db.executions().create_execution(intent).await.is_err());
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM executions")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(count, 0);
    }

    fn passing_report() -> VerificationReport {
        VerificationReport {
            checks: vec![CheckEvidence {
                name: "unit tests".into(),
                argv: vec!["cargo".into(), "test".into()],
                status: CheckStatus::Passed,
                exit_code: Some(0),
                stdout: b"ok".to_vec(),
                stderr: Vec::new(),
                stdout_truncated: false,
                stderr_truncated: false,
                duration: Duration::from_millis(1),
                detail: None,
                termination: crate::sandbox::Termination::Exited,
                execution_error: None,
                cleanup: crate::sandbox::CleanupOutcome::not_required(),
            }],
            status: VerificationStatus::Passed,
            session_cleanup: crate::sandbox::CleanupOutcome::not_required(),
        }
    }

    async fn record_test_report(
        repo: &SqliteExecutionRepository,
        id: Uuid,
        report: VerificationReport,
    ) -> Result<(), StorageError> {
        let contract = repo.verification_contract(id).await?;
        repo.record_verification(
            id,
            ExecutedVerification::test_only(contract.version(), report),
        )
        .await
    }

    async fn advance_to_verifying(
        repo: &SqliteExecutionRepository,
        intent: ExecutionIntent,
    ) -> Uuid {
        let id = intent.id;
        repo.create_execution(intent).await.unwrap();
        for state in [
            ExecutionState::RepositoryAnalysis,
            ExecutionState::Planning,
            ExecutionState::PlanValidation,
            ExecutionState::WorkspacePreparation,
            ExecutionState::Executing,
            ExecutionState::Verifying,
        ] {
            repo.transition(id, state, None).await.unwrap();
        }
        id
    }

    #[tokio::test]
    async fn failed_and_incomplete_verification_cannot_complete() {
        let (_dir, db, intent) = fixture().await;
        let repo = db.executions();
        let id = advance_to_verifying(&repo, intent).await;
        let failed = VerificationReport {
            checks: vec![CheckEvidence {
                name: "unit tests".into(),
                argv: vec!["cargo".into(), "test".into()],
                status: CheckStatus::Failed,
                exit_code: Some(1),
                stdout: Vec::new(),
                stderr: b"failure".to_vec(),
                stdout_truncated: false,
                stderr_truncated: false,
                duration: Duration::from_millis(1),
                detail: None,
                termination: crate::sandbox::Termination::Exited,
                execution_error: None,
                cleanup: crate::sandbox::CleanupOutcome::not_required(),
            }],
            status: VerificationStatus::Failed,
            session_cleanup: crate::sandbox::CleanupOutcome::not_required(),
        };
        record_test_report(&repo, id, failed.clone()).await.unwrap();
        assert!(
            repo.transition(id, ExecutionState::Completed, None)
                .await
                .is_err()
        );

        assert!(
            record_test_report(
                &repo,
                id,
                VerificationReport {
                    checks: Vec::new(),
                    status: VerificationStatus::Incomplete,
                    session_cleanup: crate::sandbox::CleanupOutcome::not_required(),
                },
            )
            .await
            .is_err()
        );
        assert!(
            repo.transition(id, ExecutionState::Completed, None)
                .await
                .is_err()
        );
        assert_eq!(
            repo.execution(id).await.unwrap().state,
            ExecutionState::Verifying
        );
        for (check_status, report_status) in [
            (CheckStatus::TimedOut, VerificationStatus::TimedOut),
            (CheckStatus::Cancelled, VerificationStatus::Cancelled),
            (CheckStatus::Skipped, VerificationStatus::Incomplete),
        ] {
            record_test_report(
                &repo,
                id,
                VerificationReport {
                    checks: vec![CheckEvidence {
                        name: "unit tests".into(),
                        argv: vec!["cargo".into(), "test".into()],
                        status: check_status,
                        exit_code: None,
                        stdout: Vec::new(),
                        stderr: Vec::new(),
                        stdout_truncated: false,
                        stderr_truncated: false,
                        duration: Duration::ZERO,
                        detail: None,
                        termination: match check_status {
                            CheckStatus::TimedOut => crate::sandbox::Termination::TimedOut,
                            CheckStatus::Cancelled => crate::sandbox::Termination::Cancelled,
                            _ => crate::sandbox::Termination::Failed,
                        },
                        execution_error: None,
                        cleanup: crate::sandbox::CleanupOutcome::not_required(),
                    }],
                    status: report_status,
                    session_cleanup: crate::sandbox::CleanupOutcome::not_required(),
                },
            )
            .await
            .unwrap();
            assert!(
                repo.transition(id, ExecutionState::Completed, None)
                    .await
                    .is_err()
            );
        }
        record_test_report(&repo, id, passing_report())
            .await
            .unwrap();
        record_test_report(&repo, id, failed).await.unwrap();
        assert!(
            repo.transition(id, ExecutionState::Completed, None)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn cleanup_failure_cannot_be_persisted_as_verified_completion() {
        let (_dir, db, intent) = fixture().await;
        let repo = db.executions();
        let id = advance_to_verifying(&repo, intent).await;
        let mut report = passing_report();
        report.status = VerificationStatus::Failed;
        report.session_cleanup = crate::sandbox::CleanupOutcome::failed(
            crate::sandbox::CleanupStatus::Failed,
            Some("synroute-test-container".into()),
            None,
            "cleanup failure".into(),
        );
        record_test_report(&repo, id, report).await.unwrap();
        assert!(
            repo.transition(id, ExecutionState::Completed, None)
                .await
                .is_err()
        );
        assert_eq!(
            repo.execution(id).await.unwrap().state,
            ExecutionState::Verifying
        );
    }

    #[tokio::test]
    async fn completion_requires_exited_zero_status_in_persisted_checks() {
        let (_dir, db, intent) = fixture().await;
        let repo = db.executions();
        let id = advance_to_verifying(&repo, intent).await;
        let mut report = passing_report();
        report.checks[0].exit_code = Some(1);
        assert!(record_test_report(&repo, id, report).await.is_err());
    }

    #[tokio::test]
    async fn passing_evidence_and_completion_racing_cannot_bypass_gate() {
        let (_dir, db, intent) = fixture().await;
        let repo = db.executions();
        let id = advance_to_verifying(&repo, intent).await;
        let verify_repo = repo.clone();
        let transition_repo = repo.clone();
        let (verification, completion) = tokio::join!(
            record_test_report(&verify_repo, id, passing_report()),
            transition_repo.transition(id, ExecutionState::Completed, None),
        );
        verification.unwrap();
        if completion.is_err() {
            repo.transition(id, ExecutionState::Completed, None)
                .await
                .unwrap();
        }
        let persisted = repo.execution(id).await.unwrap();
        assert_eq!(persisted.state, ExecutionState::Completed);
        assert!(repo.verifications(id).await.unwrap().iter().any(|item| {
            item.report.status == VerificationStatus::Passed
                && !item.report.checks.is_empty()
                && item
                    .report
                    .checks
                    .iter()
                    .all(|check| check.status == CheckStatus::Passed)
        }));
    }

    #[tokio::test]
    async fn sqlite_trigger_rejects_direct_unverified_completion_and_terminal_overwrite() {
        let (_dir, db, intent) = fixture().await;
        let repo = db.executions();
        let id = advance_to_verifying(&repo, intent).await;
        let direct = sqlx::query("UPDATE executions SET state = 'COMPLETED' WHERE id = ?")
            .bind(id.to_string())
            .execute(db.pool())
            .await;
        assert!(direct.is_err());
        record_test_report(&repo, id, passing_report())
            .await
            .unwrap();
        repo.transition(id, ExecutionState::Completed, None)
            .await
            .unwrap();
        assert!(
            repo.transition(id, ExecutionState::Failed, None)
                .await
                .is_err()
        );
        let overwrite = sqlx::query("UPDATE executions SET state = 'FAILED' WHERE id = ?")
            .bind(id.to_string())
            .execute(db.pool())
            .await;
        assert!(overwrite.is_err());
    }

    #[tokio::test]
    async fn sqlite_rejects_evidence_outside_verifying_or_outside_contract_and_freezes_it() {
        let (_dir, db, intent) = fixture().await;
        let id = intent.id;
        let repo = db.executions();
        repo.create_execution(intent).await.unwrap();
        let contract_version: String =
            sqlx::query_scalar("SELECT version FROM verification_contracts WHERE execution_id = ?")
                .bind(id.to_string())
                .fetch_one(db.pool())
                .await
                .unwrap();
        let passing = serde_json::json!({
            "status": "passed",
            "checks": [{"name":"unit tests", "argv":["cargo","test"], "status":"passed"}]
        });
        let insert_evidence = |report: serde_json::Value, version: String, status: &'static str| {
            let pool = db.pool().clone();
            async move {
                sqlx::query("INSERT INTO verification_evidence (execution_id, report_json, occurred_at, status, contract_version) VALUES (?, ?, ?, ?, ?)")
                    .bind(id.to_string())
                    .bind(report.to_string())
                    .bind(timestamp(Utc::now()))
                    .bind(status)
                    .bind(version)
                    .execute(&pool)
                    .await
            }
        };
        assert!(
            insert_evidence(passing.clone(), contract_version.clone(), "passed")
                .await
                .is_err()
        );
        for state in [
            ExecutionState::RepositoryAnalysis,
            ExecutionState::Planning,
            ExecutionState::PlanValidation,
            ExecutionState::WorkspacePreparation,
            ExecutionState::Executing,
            ExecutionState::Verifying,
        ] {
            repo.transition(id, state, None).await.unwrap();
        }
        let wrong_argv = serde_json::json!({
            "status":"passed",
            "checks":[{"name":"unit tests", "argv":["cargo","check"], "status":"passed"}]
        });
        assert!(
            insert_evidence(wrong_argv, contract_version.clone(), "passed")
                .await
                .is_err()
        );
        let missing = serde_json::json!({"status":"incomplete", "checks":[]});
        assert!(
            insert_evidence(missing, contract_version.clone(), "incomplete")
                .await
                .is_err()
        );
        let duplicate = serde_json::json!({
            "status":"passed",
            "checks":[
                {"name":"unit tests", "argv":["cargo","test"], "status":"passed"},
                {"name":"unit tests", "argv":["cargo","test"], "status":"passed"}
            ]
        });
        assert!(
            insert_evidence(duplicate, contract_version.clone(), "passed")
                .await
                .is_err()
        );
        assert!(
            insert_evidence(passing, Uuid::new_v4().to_string(), "passed")
                .await
                .is_err()
        );

        record_test_report(&repo, id, passing_report())
            .await
            .unwrap();
        repo.transition(id, ExecutionState::Completed, None)
            .await
            .unwrap();
        let id_text = id.to_string();
        let update = sqlx::query(
            "UPDATE verification_evidence SET status = 'failed' WHERE execution_id = ?",
        )
        .bind(&id_text)
        .execute(db.pool())
        .await;
        assert!(update.is_err());
        let delete = sqlx::query("DELETE FROM verification_evidence WHERE execution_id = ?")
            .bind(&id_text)
            .execute(db.pool())
            .await;
        assert!(delete.is_err());
        assert!(insert_evidence(
            serde_json::json!({"status":"passed", "checks":[{"name":"unit tests", "argv":["cargo","test"], "status":"passed"}]}),
            contract_version,
            "passed"
        ).await.is_err());
    }

    #[tokio::test]
    async fn event_history_is_append_only_and_sequences_are_concurrency_safe() {
        let (_dir, db, intent) = fixture().await;
        let id = intent.id;
        let repo = db.executions();
        advance_to_verifying(&repo, intent).await;
        let mut tasks = Vec::new();
        for index in 0..32 {
            let repo = repo.clone();
            tasks.push(tokio::spawn(async move {
                repo.append_event(
                    id,
                    ExecutionEventType::RepositoryAnalyzed,
                    json!({"attempt": index}),
                )
                .await
                .unwrap()
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        let events = repo.events(id).await.unwrap();
        assert_eq!(events.len(), 39);
        assert_eq!(
            events
                .iter()
                .map(|event| event.sequence)
                .collect::<Vec<_>>(),
            (1..=39).collect::<Vec<_>>()
        );
        let update =
            sqlx::query("UPDATE execution_events SET payload_json = '{}' WHERE execution_id = ?")
                .bind(id.to_string())
                .execute(db.pool())
                .await;
        assert!(update.is_err());
        let delete = sqlx::query("DELETE FROM execution_events WHERE execution_id = ?")
            .bind(id.to_string())
            .execute(db.pool())
            .await;
        assert!(delete.is_err());
    }

    #[tokio::test]
    async fn persists_verification_and_reconstructs_a_structured_report() {
        let (_dir, db, intent) = fixture().await;
        let id = intent.id;
        let repo = db.executions();
        repo.create_execution(intent).await.unwrap();
        repo.append_event(
            id,
            ExecutionEventType::RepositoryAnalyzed,
            json!({"language": "Rust"}),
        )
        .await
        .unwrap();
        for state in [
            ExecutionState::RepositoryAnalysis,
            ExecutionState::Planning,
            ExecutionState::PlanValidation,
            ExecutionState::WorkspacePreparation,
            ExecutionState::Executing,
            ExecutionState::Verifying,
        ] {
            repo.transition(id, state, None).await.unwrap();
        }
        record_test_report(&repo, id, passing_report())
            .await
            .unwrap();
        let report = repo.report(id).await.unwrap();
        assert_eq!(report.execution.description, "fix a checked fixture");
        assert_eq!(
            report.verification_contract.required_checks[0].name,
            "unit tests"
        );
        assert_eq!(
            report.verifications[0].contract_version,
            report.verification_contract.version
        );
        assert_eq!(report.events.len(), 9);
        assert_eq!(
            report.verifications[0].report.status,
            VerificationStatus::Passed
        );
        let json = serde_json::to_string(&report).unwrap();
        assert!(json.contains("unit tests"));
    }

    #[tokio::test]
    async fn refuses_sensitive_metadata_keys() {
        let (_dir, db, mut intent) = fixture().await;
        intent.metadata = json!({"nested": {"api_token": "must-not-persist"}});
        assert!(matches!(
            db.executions().create_execution(intent).await,
            Err(StorageError::SensitiveField(_))
        ));
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM executions")
            .fetch_one(db.pool())
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
}
