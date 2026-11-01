//! Read-only, SQLite-backed CLI views of persisted execution history.
use crate::{
    core::ExecutionState,
    storage::{
        ExecutionRecord, ExecutionReport, ExecutionRepository, RecoveryCaseRecord,
        RecoveryCaseStatus, SqliteStorage, StorageError, WorkspaceRepository, WorkspaceStatus,
    },
};
use serde::Serialize;
use std::collections::BTreeMap;
use uuid::Uuid;

#[derive(Debug, Serialize)]
pub struct StatusSnapshot {
    pub report: ExecutionReport,
    pub unresolved_recovery: Vec<RecoveryCaseRecord>,
}

pub async fn status(storage: &SqliteStorage, id: Uuid) -> Result<StatusSnapshot, StorageError> {
    let report = storage.executions().report(id).await?;
    let unresolved_recovery = storage
        .recovery()
        .cases_for_execution(id)
        .await?
        .into_iter()
        .filter(|case| case.status == RecoveryCaseStatus::Open)
        .collect();
    Ok(StatusSnapshot {
        report,
        unresolved_recovery,
    })
}

#[derive(Debug, Default, Serialize)]
pub struct Stats {
    pub total_executions: usize,
    pub completed: usize,
    pub failed: usize,
    pub interrupted: usize,
    pub cancelled: usize,
    pub timed_out: usize,
    pub recovery_required: usize,
    pub verification_pass_rate: Option<f64>,
    /// End-to-end verification wall time is not persisted, so this remains absent.
    pub average_verification_duration_ms: Option<f64>,
    pub workspace_cleanup_succeeded: usize,
    pub workspace_cleanup_failed_or_uncertain: usize,
    pub repositories: BTreeMap<String, RepositoryStats>,
    pub timing_note: &'static str,
}

#[derive(Debug, Default, Serialize)]
pub struct RepositoryStats {
    pub executions: usize,
    pub completed: usize,
    pub failed: usize,
    pub interrupted: usize,
    pub cancelled: usize,
    pub timed_out: usize,
    pub recovery_required: usize,
    pub verifications_passed: usize,
    pub verifications_recorded: usize,
}

pub async fn stats(storage: &SqliteStorage) -> Result<Stats, StorageError> {
    let repository = storage.executions();
    let executions = repository.list_executions(None).await?;
    let mut result = Stats {
        timing_note: "Unavailable: persisted evidence records per-check durations, not total verification wall time.",
        ..Stats::default()
    };
    result.total_executions = executions.len();
    let mut passed = 0usize;
    let mut recorded = 0usize;
    for execution in executions {
        match execution.state {
            ExecutionState::Completed => result.completed += 1,
            ExecutionState::Failed => result.failed += 1,
            ExecutionState::Interrupted => result.interrupted += 1,
            ExecutionState::Cancelled => result.cancelled += 1,
            ExecutionState::TimedOut => result.timed_out += 1,
            ExecutionState::RecoveryRequired => result.recovery_required += 1,
            _ => {}
        }
        let group = result
            .repositories
            .entry(execution.repository_identity.clone())
            .or_default();
        group.executions += 1;
        match execution.state {
            ExecutionState::Completed => group.completed += 1,
            ExecutionState::Failed => group.failed += 1,
            ExecutionState::Interrupted => group.interrupted += 1,
            ExecutionState::Cancelled => group.cancelled += 1,
            ExecutionState::TimedOut => group.timed_out += 1,
            ExecutionState::RecoveryRequired => group.recovery_required += 1,
            _ => {}
        }
        for workspace in storage.workspaces().for_execution(execution.id).await? {
            match workspace.status {
                WorkspaceStatus::Removed => result.workspace_cleanup_succeeded += 1,
                WorkspaceStatus::Uncertain | WorkspaceStatus::Removing => {
                    result.workspace_cleanup_failed_or_uncertain += 1
                }
                WorkspaceStatus::Preparing | WorkspaceStatus::Registered => {}
            }
        }
        let reports = repository.verifications(execution.id).await?;
        if let Some(latest) = reports.last() {
            recorded += 1;
            group.verifications_recorded += 1;
            if latest.report.status == crate::verification::VerificationStatus::Passed {
                passed += 1;
                group.verifications_passed += 1;
            }
        }
    }
    result.verification_pass_rate = (recorded > 0).then_some(passed as f64 / recorded as f64);
    Ok(result)
}

pub fn history_line(record: &ExecutionRecord) -> HistoryLine {
    HistoryLine {
        id: record.id,
        repository: record.repository_identity.clone(),
        state: record.state,
        created_at: record.created_at,
        final_outcome: record.terminal_outcome,
    }
}

#[derive(Debug, Serialize)]
pub struct HistoryLine {
    pub id: Uuid,
    pub repository: String,
    pub state: ExecutionState,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub final_outcome: Option<ExecutionState>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        core::{ExecutionState, VerificationCommand},
        sandbox::{CleanupOutcome, Termination},
        storage::{ExecutionIntent, ExecutionRepository, PersistedExecutionConfig},
        verification::{
            CheckEvidence, CheckStatus, ExecutedVerification, VerificationReport,
            VerificationStatus,
        },
    };
    use serde_json::json;
    use std::time::Duration;

    async fn setup() -> (tempfile::TempDir, SqliteStorage) {
        let dir = tempfile::tempdir().unwrap();
        let db = SqliteStorage::open(dir.path().join("history.sqlite"))
            .await
            .unwrap();
        (dir, db)
    }

    async fn execution(db: &SqliteStorage, root: &std::path::Path, repo: &str) -> Uuid {
        let root = root.canonicalize().unwrap();
        let intent = ExecutionIntent {
            id: Uuid::new_v4(),
            description: "history fixture".into(),
            metadata: json!({}),
            repository_identity: repo.into(),
            repository_root: root.clone(),
            repository_revision: "0123456789abcdef".into(),
            working_directory: root,
            config: PersistedExecutionConfig {
                timeout_seconds: 10,
                max_recovery_attempts: 0,
                memory_bytes: 1024,
                cpu_millis: 100,
                process_limit: 8,
                output_limit_bytes: 1024,
            },
            required_verification: vec![VerificationCommand {
                name: "check".into(),
                argv: vec!["true".into()],
            }],
        };
        db.executions().create_execution(intent).await.unwrap().id
    }

    async fn finish_success(db: &SqliteStorage, id: Uuid) {
        let repo = db.executions();
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
        let contract = repo.report(id).await.unwrap().verification_contract;
        let report = VerificationReport {
            checks: vec![CheckEvidence {
                name: "check".into(),
                argv: vec!["true".into()],
                status: CheckStatus::Passed,
                exit_code: Some(0),
                stdout: b"ok".to_vec(),
                stderr: Vec::new(),
                stdout_truncated: false,
                stderr_truncated: false,
                duration: Duration::from_millis(12),
                detail: None,
                termination: Termination::Exited,
                execution_error: None,
                cleanup: CleanupOutcome::not_required(),
            }],
            status: VerificationStatus::Passed,
            session_cleanup: CleanupOutcome::not_required(),
        };
        repo.record_verification(
            id,
            ExecutedVerification::test_only(contract.version, report),
        )
        .await
        .unwrap();
        repo.transition(id, ExecutionState::Completed, None)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn empty_database_has_zero_counts_and_no_rate_or_timing() {
        let (_dir, db) = setup().await;
        assert!(
            db.executions()
                .list_executions(Some(20))
                .await
                .unwrap()
                .is_empty()
        );
        let report = stats(&db).await.unwrap();
        assert_eq!(report.total_executions, 0);
        assert_eq!(report.verification_pass_rate, None);
        assert_eq!(report.average_verification_duration_ms, None);
        assert!(report.repositories.is_empty());
    }

    #[tokio::test]
    async fn mixed_outcomes_derive_counts_from_records_and_ignore_missing_evidence() {
        let (dir, db) = setup().await;
        let passed = execution(&db, dir.path(), "repo-a").await;
        finish_success(&db, passed).await;
        let failed = execution(&db, dir.path(), "repo-a").await;
        db.executions()
            .transition(failed, ExecutionState::Failed, None)
            .await
            .unwrap();
        let interrupted = execution(&db, dir.path(), "repo-b").await;
        db.executions()
            .transition(interrupted, ExecutionState::Interrupted, None)
            .await
            .unwrap();
        let recovery_required = execution(&db, dir.path(), "repo-c").await;
        let expired = chrono::Utc::now() - chrono::Duration::seconds(1);
        sqlx::query("UPDATE execution_leases SET expires_at = ? WHERE execution_id = ?")
            .bind(expired.to_rfc3339())
            .bind(recovery_required.to_string())
            .execute(db.pool())
            .await
            .unwrap();
        let observed_lease = db
            .executions()
            .lease(recovery_required)
            .await
            .unwrap()
            .unwrap();
        db.recovery()
            .recover_expired_lease(
                &observed_lease,
                ExecutionState::Received,
                chrono::Utc::now(),
                json!({"test_observation": "inactive owner"}),
            )
            .await
            .unwrap();
        let stats = stats(&db).await.unwrap();
        assert_eq!(stats.total_executions, 4);
        assert_eq!(stats.completed, 1);
        assert_eq!(stats.failed, 1);
        assert_eq!(stats.interrupted, 1);
        assert_eq!(stats.recovery_required, 1);
        assert_eq!(stats.verification_pass_rate, Some(1.0));
        assert_eq!(stats.average_verification_duration_ms, None);
        assert_eq!(stats.repositories["repo-a"].executions, 2);
    }

    #[tokio::test]
    async fn status_includes_open_recovery_and_workspace_cleanup_uncertainty() {
        let (dir, db) = setup().await;
        let id = execution(&db, dir.path(), "repo").await;
        let now = chrono::Utc::now();
        db.recovery()
            .record_uncertainty(id, "owner_liveness_unknown", json!({"source":"test"}), now)
            .await
            .unwrap();
        let path = dir.path().join("workspace");
        tokio::fs::create_dir(&path).await.unwrap();
        let root = dir.path().canonicalize().unwrap();
        let record = crate::storage::WorkspaceOwnershipRecord {
            workspace_id: Uuid::new_v4(),
            execution_id: id,
            canonical_path: path.canonicalize().unwrap(),
            repository_identity: "repo".into(),
            repository_root: root,
            repository_revision: "0123456789abcdef".into(),
            git_admin_path: None,
            git_common_dir: None,
            sidecar_path: dir.path().join("sidecar.json"),
            ownership_token: Uuid::new_v4(),
            status: WorkspaceStatus::Preparing,
            uncertainty_code: None,
            uncertainty_detail: None,
            created_at: now,
            updated_at: now,
        };
        db.workspaces().reserve(&record).await.unwrap();
        db.workspaces()
            .set_status(
                record.workspace_id,
                WorkspaceStatus::Uncertain,
                None,
                None,
                Some(crate::storage::WorkspaceDiagnostic::new(
                    crate::storage::WorkspaceIssueCode::GitOperationFailed,
                )),
            )
            .await
            .unwrap();
        let snapshot = status(&db, id).await.unwrap();
        assert_eq!(snapshot.unresolved_recovery.len(), 1);
        assert_eq!(
            snapshot.report.workspaces[0].status,
            WorkspaceStatus::Uncertain
        );
        assert!(snapshot.report.verifications.is_empty());
        let stats = stats(&db).await.unwrap();
        assert_eq!(stats.workspace_cleanup_failed_or_uncertain, 1);
    }

    #[tokio::test]
    async fn history_is_newest_first_and_limit_is_applied_in_sqlite() {
        let (dir, db) = setup().await;
        let older = execution(&db, dir.path(), "old").await;
        tokio::time::sleep(Duration::from_millis(2)).await;
        let newer = execution(&db, dir.path(), "new").await;
        let list = db.executions().list_executions(Some(1)).await.unwrap();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, newer);
        assert_ne!(older, newer);
    }
}
