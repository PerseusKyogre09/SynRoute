use crate::{
    sandbox::{RuntimeContainerPresence, RuntimeInspector},
    storage::{
        ContainerDiagnosticRecord, ContainerIntentRecord, ExecutionLease, ExecutionRecord,
        ExecutionRepository, RecoveryCaseRecord, SqliteStorage, StorageError,
        WorkspaceOwnershipRecord, WorkspaceRepository, WorkspaceStatus,
    },
    workspace::persistent::PersistentWorkspaceManager,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
#[cfg(target_os = "linux")]
use std::fs;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RuntimeInspection {
    Available {
        containers: Vec<RuntimeContainerPresence>,
    },
    Unavailable {
        reason: String,
    },
    NotConfigured,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum OwnerLiveness {
    Active,
    Inactive,
    Unknown,
}

/// Uses Linux boot ID, PID, and `/proc/<pid>/stat` start ticks. A PID alone is never
/// considered an owner identity; unsupported or unreadable process identity is Unknown.
trait OwnerLivenessCheck: Send + Sync {
    fn check(&self, lease: &ExecutionLease) -> OwnerLiveness;
}

#[derive(Debug, Default)]
struct SystemOwnerLiveness;

impl OwnerLivenessCheck for SystemOwnerLiveness {
    fn check(&self, lease: &ExecutionLease) -> OwnerLiveness {
        let (Some(pid), Some(expected_start), Some(expected_boot)) = (
            lease.process_id,
            lease.process_start.as_ref(),
            lease.boot_id.as_ref(),
        ) else {
            return OwnerLiveness::Unknown;
        };
        #[cfg(target_os = "linux")]
        {
            let Ok(boot) = fs::read_to_string("/proc/sys/kernel/random/boot_id") else {
                return OwnerLiveness::Unknown;
            };
            if boot.trim() != expected_boot {
                return OwnerLiveness::Inactive;
            }
            match fs::read_to_string(format!("/proc/{pid}/stat")) {
                Ok(stat) => match process_start_ticks(&stat) {
                    Some(actual) if &actual == expected_start => OwnerLiveness::Active,
                    Some(_) => OwnerLiveness::Inactive,
                    None => OwnerLiveness::Unknown,
                },
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    OwnerLiveness::Inactive
                }
                Err(_) => OwnerLiveness::Unknown,
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = (pid, expected_start, expected_boot);
            OwnerLiveness::Unknown
        }
    }
}

#[cfg(target_os = "linux")]
fn process_start_ticks(stat: &str) -> Option<String> {
    // `comm` is parenthesized and may contain spaces or `)`, so split at its final `)`.
    // The remaining fields start at field 3; starttime is field 22 (index 19).
    stat.rsplit_once(')')?
        .1
        .split_whitespace()
        .nth(19)
        .map(str::to_owned)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryAssessment {
    OwnerActive,
    OwnerActiveLeaseExpired,
    OwnerInactiveLeaseFresh,
    OwnerInactiveLeaseExpired,
    OwnerLivenessUnknown,
    LeaseMissing,
    Recovered,
    ChangedSinceInspection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceInspection {
    workspace: WorkspaceOwnershipRecord,
    validation_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionInspection {
    execution: ExecutionRecord,
    lease: Option<ExecutionLease>,
    assessment: RecoveryAssessment,
    runtime: RuntimeInspection,
    workspaces: Vec<WorkspaceInspection>,
    process_liveness_unknown: bool,
}

impl ExecutionInspection {
    pub fn execution(&self) -> &ExecutionRecord {
        &self.execution
    }
    pub fn assessment(&self) -> &RecoveryAssessment {
        &self.assessment
    }
    pub fn runtime(&self) -> &RuntimeInspection {
        &self.runtime
    }
    pub fn workspaces(&self) -> &[WorkspaceInspection] {
        &self.workspaces
    }
    pub fn process_liveness_unknown(&self) -> bool {
        self.process_liveness_unknown
    }
}

impl WorkspaceInspection {
    pub fn workspace(&self) -> &WorkspaceOwnershipRecord {
        &self.workspace
    }
    pub fn validation_error(&self) -> Option<&str> {
        self.validation_error.as_deref()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryReport {
    inspected_at: DateTime<Utc>,
    executions: Vec<ExecutionInspection>,
    /// Workspace states requiring attention, including those attached to terminal executions.
    workspace_issues: Vec<WorkspaceInspection>,
    /// Open cases are repeated in each report until acknowledged or resolved.
    unresolved_cases: Vec<RecoveryCaseRecord>,
    unresolved_container_intents: Vec<ContainerIntentRecord>,
    container_diagnostics: Vec<ContainerDiagnosticRecord>,
}

impl RecoveryReport {
    pub fn requires_attention(&self) -> bool {
        !self.unresolved_cases.is_empty()
            || !self.workspace_issues.is_empty()
            || !self.unresolved_container_intents.is_empty()
            || !self.container_diagnostics.is_empty()
    }
    pub fn inspected_at(&self) -> DateTime<Utc> {
        self.inspected_at
    }
    pub fn executions(&self) -> &[ExecutionInspection] {
        &self.executions
    }
    pub fn unresolved_cases(&self) -> &[RecoveryCaseRecord] {
        &self.unresolved_cases
    }
    pub fn workspace_issues(&self) -> &[WorkspaceInspection] {
        &self.workspace_issues
    }
    pub fn unresolved_container_intents(&self) -> &[ContainerIntentRecord] {
        &self.unresolved_container_intents
    }
    pub fn container_diagnostics(&self) -> &[ContainerDiagnosticRecord] {
        &self.container_diagnostics
    }
}

#[derive(Debug, Clone)]
pub struct RecoveryService {
    storage: SqliteStorage,
}

impl RecoveryService {
    pub fn new(storage: SqliteStorage) -> Self {
        Self { storage }
    }

    /// Read-only inspection. It does not alter lifecycle state, leases, workspaces, or containers.
    pub async fn inspect_startup(
        &self,
        runtime_inspector: Option<&dyn RuntimeInspector>,
        now: DateTime<Utc>,
    ) -> Result<RecoveryReport, StorageError> {
        self.inspect_with_liveness(runtime_inspector, &SystemOwnerLiveness, now)
            .await
    }

    async fn inspect_with_liveness(
        &self,
        runtime_inspector: Option<&dyn RuntimeInspector>,
        liveness: &dyn OwnerLivenessCheck,
        now: DateTime<Utc>,
    ) -> Result<RecoveryReport, StorageError> {
        let executions = self.storage.executions().nonterminal_executions().await?;
        let mut workspace_issues = Vec::new();
        for workspace in self.storage.workspaces().inspectable().await? {
            let validation_error = PersistentWorkspaceManager::diagnose_record(&workspace)
                .await
                .err()
                .map(|issue| format!("{}: {}", issue.code.as_str(), issue.code.safe_detail()));
            if workspace.status != WorkspaceStatus::Registered || validation_error.is_some() {
                workspace_issues.push(WorkspaceInspection {
                    workspace,
                    validation_error,
                });
            }
        }
        let mut inspections = Vec::with_capacity(executions.len());
        for execution in executions {
            let lease = self.storage.executions().lease(execution.id).await?;
            let assessment = assess_lease(lease.as_ref(), now, liveness);
            let runtime = match runtime_inspector {
                Some(inspector) => match inspector.inspect_execution(execution.id).await {
                    Ok(containers) => RuntimeInspection::Available { containers },
                    Err(error) => RuntimeInspection::Unavailable {
                        reason: error.to_string(),
                    },
                },
                None => RuntimeInspection::NotConfigured,
            };
            let mut workspaces = Vec::new();
            for workspace in self
                .storage
                .workspaces()
                .for_execution(execution.id)
                .await?
            {
                let validation_error = PersistentWorkspaceManager::diagnose_record(&workspace)
                    .await
                    .err()
                    .map(|issue| format!("{}: {}", issue.code.as_str(), issue.code.safe_detail()));
                workspaces.push(WorkspaceInspection {
                    workspace,
                    validation_error,
                });
            }
            inspections.push(ExecutionInspection {
                execution,
                lease,
                process_liveness_unknown: assessment == RecoveryAssessment::OwnerLivenessUnknown
                    || assessment == RecoveryAssessment::LeaseMissing,
                assessment,
                runtime,
                workspaces,
            });
        }
        let unresolved_cases = self.storage.recovery().open_cases().await?;
        let unresolved_container_intents = self.storage.containers().unresolved_intents().await?;
        let container_diagnostics = self.storage.containers().open().await?;
        Ok(RecoveryReport {
            inspected_at: now,
            executions: inspections,
            workspace_issues,
            unresolved_cases,
            unresolved_container_intents,
            container_diagnostics,
        })
    }

    /// Applies only decisions whose owner was confirmed inactive and lease expired. The lease,
    /// lifecycle transition, events, and recovery case are changed in one SQLite transaction.
    /// Unknown ownership becomes a persisted case while the execution itself remains untouched.
    pub async fn apply_safe_recovery(
        &self,
        report: &mut RecoveryReport,
    ) -> Result<(), StorageError> {
        self.apply_with_liveness(report, &SystemOwnerLiveness).await
    }

    async fn apply_with_liveness(
        &self,
        report: &mut RecoveryReport,
        liveness: &dyn OwnerLivenessCheck,
    ) -> Result<(), StorageError> {
        let recovery = self.storage.recovery();
        let workspace_store = self.storage.workspaces();
        let mut workspace_issues = Vec::new();
        for mut workspace in workspace_store.inspectable().await? {
            let validation_error =
                match PersistentWorkspaceManager::diagnose_record(&workspace).await {
                    Ok(()) => None,
                    Err(issue) => {
                        let code = issue.code.as_str();
                        let detail = issue.code.safe_detail();
                        if workspace.status != WorkspaceStatus::Uncertain
                            || workspace.uncertainty_code.as_deref() != Some(code)
                            || workspace.uncertainty_detail.as_deref() != Some(detail)
                        {
                            workspace = workspace_store
                                .set_status(
                                    workspace.workspace_id,
                                    WorkspaceStatus::Uncertain,
                                    None,
                                    None,
                                    Some(issue),
                                )
                                .await?;
                        }
                        Some(format!("{code}: {detail}"))
                    }
                };
            if workspace.status != WorkspaceStatus::Registered || validation_error.is_some() {
                workspace_issues.push(WorkspaceInspection {
                    workspace,
                    validation_error,
                });
            }
        }
        report.workspace_issues = workspace_issues;
        // The report is a display artifact. Use only its execution IDs; all authorization
        // inputs are reloaded from SQLite before checking liveness.
        for inspection in &mut report.executions {
            let execution_id = inspection.execution.id;
            let mut workspace_inspections = Vec::new();
            for workspace in self
                .storage
                .workspaces()
                .for_execution(execution_id)
                .await?
            {
                let validation_error = PersistentWorkspaceManager::diagnose_record(&workspace)
                    .await
                    .err()
                    .map(|issue| format!("{}: {}", issue.code.as_str(), issue.code.safe_detail()));
                workspace_inspections.push(WorkspaceInspection {
                    workspace,
                    validation_error,
                });
            }
            inspection.workspaces = workspace_inspections;
            let authoritative = match self.storage.executions().execution(execution_id).await {
                Ok(execution) => execution,
                Err(StorageError::NotFound { .. }) => continue,
                Err(error) => return Err(error),
            };
            if authoritative.state.is_terminal() {
                inspection.execution = authoritative;
                continue;
            }
            let lease = self.storage.executions().lease(execution_id).await?;
            let assessment = assess_lease(lease.as_ref(), Utc::now(), liveness);
            inspection.execution = authoritative.clone();
            inspection.lease = lease.clone();
            inspection.assessment = assessment.clone();
            inspection.process_liveness_unknown = matches!(
                assessment,
                RecoveryAssessment::OwnerLivenessUnknown | RecoveryAssessment::LeaseMissing
            );
            match inspection.assessment {
                RecoveryAssessment::OwnerInactiveLeaseExpired => {
                    let Some(lease) = lease.as_ref() else {
                        continue;
                    };
                    match liveness.check(lease) {
                        OwnerLiveness::Inactive => {
                            let details = json!({
                                "assessment": inspection.assessment,
                                "owner_id": lease.owner_id,
                                "lease_generation": lease.generation,
                                "automatic_resume": false,
                                "resource_cleanup": false
                            });
                            if let Some(execution) = recovery
                                .recover_expired_lease(
                                    lease,
                                    authoritative.state,
                                    Utc::now(),
                                    details,
                                )
                                .await?
                            {
                                inspection.execution = execution;
                                inspection.assessment = RecoveryAssessment::Recovered;
                            } else {
                                inspection.assessment = RecoveryAssessment::ChangedSinceInspection;
                            }
                        }
                        OwnerLiveness::Active => {
                            inspection.assessment = RecoveryAssessment::OwnerActiveLeaseExpired;
                            recovery
                                .record_uncertainty(
                                    inspection.execution.id,
                                    "owner_active_but_heartbeat_expired",
                                    json!({"automatic_resume": false, "resource_cleanup": false}),
                                    Utc::now(),
                                )
                                .await?;
                        }
                        OwnerLiveness::Unknown => {
                            inspection.assessment = RecoveryAssessment::OwnerLivenessUnknown;
                            recovery
                                .record_uncertainty(
                                    inspection.execution.id,
                                    "execution_owner_liveness_unknown",
                                    json!({"automatic_resume": false, "resource_cleanup": false}),
                                    Utc::now(),
                                )
                                .await?;
                        }
                    }
                }
                RecoveryAssessment::OwnerLivenessUnknown
                | RecoveryAssessment::LeaseMissing
                | RecoveryAssessment::OwnerInactiveLeaseFresh
                | RecoveryAssessment::OwnerActiveLeaseExpired => {
                    let reason = match inspection.assessment {
                        RecoveryAssessment::LeaseMissing => "execution_owner_lease_missing",
                        RecoveryAssessment::OwnerInactiveLeaseFresh => {
                            "owner_inactive_lease_not_expired"
                        }
                        RecoveryAssessment::OwnerActiveLeaseExpired => {
                            "owner_active_but_heartbeat_expired"
                        }
                        _ => "execution_owner_liveness_unknown",
                    };
                    let details = json!({
                        "assessment": inspection.assessment,
                        "owner_id": lease.as_ref().map(|lease| lease.owner_id),
                        "lease_generation": lease.as_ref().map(|lease| lease.generation),
                        "automatic_resume": false,
                        "resource_cleanup": false
                    });
                    recovery
                        .record_uncertainty(
                            inspection.execution.id,
                            reason,
                            details,
                            report.inspected_at,
                        )
                        .await?;
                }
                _ => {}
            }
            let workspace_records = self
                .storage
                .workspaces()
                .for_execution(execution_id)
                .await?;
            let mut workspace_issues = Vec::new();
            for workspace in workspace_records {
                if workspace.status != WorkspaceStatus::Removed
                    && let Err(error) =
                        PersistentWorkspaceManager::validate_record(&workspace).await
                {
                    workspace_issues.push(error.to_string());
                }
            }
            if !workspace_issues.is_empty() {
                recovery
                    .record_uncertainty(
                        inspection.execution.id,
                        "workspace_ownership_uncertain",
                        json!({
                            "workspace_issues": workspace_issues,
                            "automatic_resume": false,
                            "resource_cleanup": false
                        }),
                        report.inspected_at,
                    )
                    .await?;
            }
        }
        report.unresolved_cases = recovery.open_cases().await?;
        Ok(())
    }
}

fn assess_lease(
    lease: Option<&ExecutionLease>,
    now: DateTime<Utc>,
    liveness: &dyn OwnerLivenessCheck,
) -> RecoveryAssessment {
    let Some(lease) = lease else {
        return RecoveryAssessment::LeaseMissing;
    };
    let owner = liveness.check(lease);
    let expired = lease.expires_at <= now;
    match (owner, expired) {
        (OwnerLiveness::Active, false) => RecoveryAssessment::OwnerActive,
        (OwnerLiveness::Active, true) => RecoveryAssessment::OwnerActiveLeaseExpired,
        (OwnerLiveness::Inactive, false) => RecoveryAssessment::OwnerInactiveLeaseFresh,
        (OwnerLiveness::Inactive, true) => RecoveryAssessment::OwnerInactiveLeaseExpired,
        (OwnerLiveness::Unknown, _) => RecoveryAssessment::OwnerLivenessUnknown,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        core::ExecutionState,
        sandbox::SandboxError,
        storage::{
            ExecutionEventType, ExecutionIntent, ExecutionRepository, PersistedExecutionConfig,
            RecoveryCaseStatus, SqliteStorage, WorkspaceOwnershipRecord, WorkspaceRepository,
            WorkspaceStatus,
        },
    };
    use async_trait::async_trait;
    use std::{
        collections::HashMap,
        process::Command,
        sync::atomic::{AtomicUsize, Ordering},
    };
    use uuid::Uuid;

    struct FakeRuntime {
        inspections: AtomicUsize,
        containers: Vec<RuntimeContainerPresence>,
    }
    #[async_trait]
    impl RuntimeInspector for FakeRuntime {
        async fn inspect_execution(
            &self,
            _: Uuid,
        ) -> Result<Vec<RuntimeContainerPresence>, SandboxError> {
            self.inspections.fetch_add(1, Ordering::SeqCst);
            Ok(self.containers.clone())
        }
    }

    struct FakeLiveness(HashMap<Uuid, OwnerLiveness>);
    impl OwnerLivenessCheck for FakeLiveness {
        fn check(&self, lease: &ExecutionLease) -> OwnerLiveness {
            self.0
                .get(&lease.owner_id)
                .copied()
                .unwrap_or(OwnerLiveness::Unknown)
        }
    }

    async fn fixture() -> (tempfile::TempDir, SqliteStorage, Uuid) {
        let dir = tempfile::tempdir().unwrap();
        let storage = SqliteStorage::open(dir.path().join("recovery.sqlite"))
            .await
            .unwrap();
        let id = Uuid::new_v4();
        let root = dir.path().canonicalize().unwrap();
        storage
            .executions()
            .create_execution(ExecutionIntent {
                id,
                description: "durable task".into(),
                metadata: json!({"kind":"test"}),
                repository_identity: "local-test".into(),
                repository_root: root.clone(),
                repository_revision: "abc123".into(),
                working_directory: root,
                config: PersistedExecutionConfig {
                    timeout_seconds: 30,
                    max_recovery_attempts: 1,
                    memory_bytes: 1024,
                    cpu_millis: 200,
                    process_limit: 8,
                    output_limit_bytes: 4096,
                },
                required_verification: Vec::new(),
            })
            .await
            .unwrap();
        (dir, storage, id)
    }

    #[tokio::test]
    async fn subprocess_entrypoint() {
        let Ok(mode) = std::env::var("SYNROUTE_TEST_CHILD_MODE") else {
            return;
        };
        let db_path = std::env::var("SYNROUTE_TEST_CHILD_DB").unwrap();
        let storage = SqliteStorage::open(&db_path).await.unwrap();
        match mode.as_str() {
            "mutate" => {
                let id = Uuid::parse_str(&std::env::var("SYNROUTE_TEST_CHILD_EXECUTION").unwrap())
                    .unwrap();
                assert!(
                    storage
                        .executions()
                        .transition(id, ExecutionState::RepositoryAnalysis, None)
                        .await
                        .is_err()
                );
            }
            "create" => {
                let root = std::path::PathBuf::from(&db_path)
                    .parent()
                    .unwrap()
                    .canonicalize()
                    .unwrap();
                let id = Uuid::new_v4();
                storage
                    .executions()
                    .create_execution(ExecutionIntent {
                        id,
                        description: "subprocess lease fixture".into(),
                        metadata: json!({}),
                        repository_identity: "subprocess-test".into(),
                        repository_root: root.clone(),
                        repository_revision: "fixture-revision".into(),
                        working_directory: root,
                        config: PersistedExecutionConfig {
                            timeout_seconds: 10,
                            max_recovery_attempts: 0,
                            memory_bytes: 1024,
                            cpu_millis: 100,
                            process_limit: 4,
                            output_limit_bytes: 1024,
                        },
                        required_verification: Vec::new(),
                    })
                    .await
                    .unwrap();
                std::fs::write(
                    std::env::var("SYNROUTE_TEST_CHILD_OUTPUT").unwrap(),
                    id.to_string(),
                )
                .unwrap();
            }
            "create_wait_renew" => {
                let root = std::path::PathBuf::from(&db_path)
                    .parent()
                    .unwrap()
                    .canonicalize()
                    .unwrap();
                let id = Uuid::new_v4();
                storage
                    .executions()
                    .create_execution(ExecutionIntent {
                        id,
                        description: "subprocess lease race".into(),
                        metadata: json!({}),
                        repository_identity: "subprocess-test".into(),
                        repository_root: root.clone(),
                        repository_revision: "fixture-revision".into(),
                        working_directory: root,
                        config: PersistedExecutionConfig {
                            timeout_seconds: 10,
                            max_recovery_attempts: 0,
                            memory_bytes: 1024,
                            cpu_millis: 100,
                            process_limit: 4,
                            output_limit_bytes: 1024,
                        },
                        required_verification: Vec::new(),
                    })
                    .await
                    .unwrap();
                std::fs::write(
                    std::env::var("SYNROUTE_TEST_CHILD_OUTPUT").unwrap(),
                    id.to_string(),
                )
                .unwrap();
                let start =
                    std::path::PathBuf::from(std::env::var("SYNROUTE_TEST_CHILD_START").unwrap());
                tokio::time::timeout(std::time::Duration::from_secs(10), async {
                    while !start.exists() {
                        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                    }
                })
                .await
                .unwrap();
                let result = storage
                    .executions()
                    .renew_lease(id, std::time::Duration::from_secs(60))
                    .await;
                std::fs::write(
                    std::env::var("SYNROUTE_TEST_CHILD_RESULT").unwrap(),
                    if result.is_ok() {
                        "renewed"
                    } else {
                        "rejected"
                    },
                )
                .unwrap();
            }
            other => panic!("unknown subprocess test mode {other}"),
        }
    }

    fn run_test_subprocess(mode: &str, db_path: &std::path::Path, args: &[(&str, String)]) {
        let output = spawn_test_subprocess(mode, db_path, args)
            .wait_with_output()
            .unwrap();
        assert!(
            output.status.success(),
            "child failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn spawn_test_subprocess(
        mode: &str,
        db_path: &std::path::Path,
        args: &[(&str, String)],
    ) -> std::process::Child {
        let executable = std::env::current_exe().unwrap();
        let mut command = Command::new(executable);
        command
            .args([
                "--exact",
                "recovery::tests::subprocess_entrypoint",
                "--nocapture",
            ])
            .env("SYNROUTE_TEST_CHILD_MODE", mode)
            .env("SYNROUTE_TEST_CHILD_DB", db_path);
        for (key, value) in args {
            command.env(key, value);
        }
        command.spawn().unwrap()
    }

    #[tokio::test]
    async fn separate_os_process_cannot_mutate_another_process_execution() {
        let (_dir, storage, id) = fixture().await;
        let db_path = _dir.path().join("recovery.sqlite");
        run_test_subprocess(
            "mutate",
            &db_path,
            &[("SYNROUTE_TEST_CHILD_EXECUTION", id.to_string())],
        );
        assert_eq!(
            storage.executions().execution(id).await.unwrap().state,
            ExecutionState::Received
        );
    }

    #[tokio::test]
    async fn exited_subprocess_lease_requires_expiry_and_authoritative_dead_owner_check() {
        let (dir, storage, _) = fixture().await;
        let db_path = dir.path().join("recovery.sqlite");
        let id_file = dir.path().join("child-execution-id");
        run_test_subprocess(
            "create",
            &db_path,
            &[(
                "SYNROUTE_TEST_CHILD_OUTPUT",
                id_file.to_string_lossy().into_owned(),
            )],
        );
        let id = Uuid::parse_str(&std::fs::read_to_string(&id_file).unwrap()).unwrap();
        let service = RecoveryService::new(storage.clone());
        let mut fresh = service.inspect_startup(None, Utc::now()).await.unwrap();
        let fresh_item = fresh
            .executions
            .iter()
            .find(|item| item.execution.id == id)
            .unwrap();
        assert_eq!(
            fresh_item.assessment,
            RecoveryAssessment::OwnerInactiveLeaseFresh
        );
        service.apply_safe_recovery(&mut fresh).await.unwrap();
        assert_eq!(
            storage.executions().execution(id).await.unwrap().state,
            ExecutionState::Received
        );

        expire_lease(&storage, id).await;
        let mut expired = service.inspect_startup(None, Utc::now()).await.unwrap();
        assert_eq!(
            expired
                .executions
                .iter()
                .find(|item| item.execution.id == id)
                .unwrap()
                .assessment,
            RecoveryAssessment::OwnerInactiveLeaseExpired
        );
        service.apply_safe_recovery(&mut expired).await.unwrap();
        assert_eq!(
            storage.executions().execution(id).await.unwrap().state,
            ExecutionState::RecoveryRequired
        );
    }

    #[tokio::test]
    async fn subprocess_renewal_and_recovery_race_has_single_winner() {
        let (dir, storage, _) = fixture().await;
        let db_path = dir.path().join("recovery.sqlite");
        let id_file = dir.path().join("race-id");
        let start_file = dir.path().join("race-start");
        let result_file = dir.path().join("race-result");
        let mut child = spawn_test_subprocess(
            "create_wait_renew",
            &db_path,
            &[
                (
                    "SYNROUTE_TEST_CHILD_OUTPUT",
                    id_file.to_string_lossy().into_owned(),
                ),
                (
                    "SYNROUTE_TEST_CHILD_START",
                    start_file.to_string_lossy().into_owned(),
                ),
                (
                    "SYNROUTE_TEST_CHILD_RESULT",
                    result_file.to_string_lossy().into_owned(),
                ),
            ],
        );
        tokio::time::timeout(std::time::Duration::from_secs(10), async {
            while !id_file.exists() {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let id = Uuid::parse_str(&std::fs::read_to_string(&id_file).unwrap()).unwrap();
        expire_lease(&storage, id).await;
        let lease = storage.executions().lease(id).await.unwrap().unwrap();
        let dead = FakeLiveness(HashMap::from([(lease.owner_id, OwnerLiveness::Inactive)]));
        let service = RecoveryService::new(storage.clone());
        let mut report = service
            .inspect_with_liveness(None, &dead, Utc::now())
            .await
            .unwrap();
        std::fs::write(&start_file, "go").unwrap();
        service
            .apply_with_liveness(&mut report, &dead)
            .await
            .unwrap();
        let status = tokio::time::timeout(std::time::Duration::from_secs(10), async move {
            tokio::task::spawn_blocking(move || child.wait())
                .await
                .unwrap()
                .unwrap()
        })
        .await
        .unwrap();
        assert!(status.success());
        let outcome = std::fs::read_to_string(&result_file).unwrap();
        let execution = storage.executions().execution(id).await.unwrap();
        let lease = storage.executions().lease(id).await.unwrap();
        if outcome == "renewed" {
            assert_ne!(execution.state, ExecutionState::RecoveryRequired);
            assert!(lease.is_some());
        } else {
            assert_eq!(outcome, "rejected");
            assert_eq!(execution.state, ExecutionState::RecoveryRequired);
            assert!(lease.is_none());
        }
    }

    async fn expire_lease(storage: &SqliteStorage, id: Uuid) {
        let expired = (Utc::now() - chrono::Duration::seconds(5)).to_rfc3339();
        sqlx::query("UPDATE execution_leases SET expires_at = ? WHERE execution_id = ?")
            .bind(expired)
            .bind(id.to_string())
            .execute(storage.pool())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn second_process_inspection_does_not_interrupt_an_active_lease() {
        let (dir, first, id) = fixture().await;
        let second = SqliteStorage::open(dir.path().join("recovery.sqlite"))
            .await
            .unwrap();
        let lease = first.executions().lease(id).await.unwrap().unwrap();
        let live = FakeLiveness(HashMap::from([(lease.owner_id, OwnerLiveness::Active)]));
        let mut report = RecoveryService::new(second.clone())
            .inspect_with_liveness(None, &live, Utc::now())
            .await
            .unwrap();
        RecoveryService::new(second.clone())
            .apply_with_liveness(&mut report, &live)
            .await
            .unwrap();
        assert_eq!(
            second.executions().execution(id).await.unwrap().state,
            ExecutionState::Received
        );
        assert!(second.recovery().open_cases().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn forged_inspection_cannot_authorize_recovery_of_active_persisted_owner() {
        let (_dir, storage, id) = fixture().await;
        expire_lease(&storage, id).await;
        let authoritative_owner = storage
            .executions()
            .lease(id)
            .await
            .unwrap()
            .unwrap()
            .owner_id;
        let service = RecoveryService::new(storage.clone());
        let mut report = service.inspect_startup(None, Utc::now()).await.unwrap();
        let inspection = &mut report.executions[0];
        assert_eq!(
            inspection.assessment,
            RecoveryAssessment::OwnerActiveLeaseExpired
        );
        let lease = inspection.lease.as_mut().unwrap();
        lease.process_id = Some(u32::MAX);
        lease.process_start = Some("forged-start-time".into());
        inspection.assessment = RecoveryAssessment::OwnerInactiveLeaseExpired;
        service.apply_safe_recovery(&mut report).await.unwrap();
        assert_eq!(
            storage.executions().execution(id).await.unwrap().state,
            ExecutionState::Received
        );
        assert_eq!(
            storage
                .executions()
                .lease(id)
                .await
                .unwrap()
                .unwrap()
                .owner_id,
            authoritative_owner
        );
        assert_eq!(
            report.executions[0].assessment,
            RecoveryAssessment::OwnerActiveLeaseExpired
        );
        assert!(
            report
                .unresolved_cases
                .iter()
                .any(|case| case.reason == "owner_active_but_heartbeat_expired")
        );
    }

    #[tokio::test]
    async fn changed_lease_snapshot_cannot_be_recovered_from_stale_observation() {
        let (_dir, storage, id) = fixture().await;
        expire_lease(&storage, id).await;
        let observed = storage.executions().lease(id).await.unwrap().unwrap();
        sqlx::query(
            "UPDATE execution_leases SET generation = generation + 1 WHERE execution_id = ?",
        )
        .bind(id.to_string())
        .execute(storage.pool())
        .await
        .unwrap();
        let result = storage
            .recovery()
            .recover_expired_lease(&observed, ExecutionState::Received, Utc::now(), json!({}))
            .await
            .unwrap();
        assert!(result.is_none());
        assert_eq!(
            storage.executions().execution(id).await.unwrap().state,
            ExecutionState::Received
        );
    }

    #[tokio::test]
    async fn renewal_and_recovery_race_cannot_both_win() {
        let (_dir, storage, id) = fixture().await;
        expire_lease(&storage, id).await;
        let lease = storage.executions().lease(id).await.unwrap().unwrap();
        let dead = FakeLiveness(HashMap::from([(lease.owner_id, OwnerLiveness::Inactive)]));
        let service = RecoveryService::new(storage.clone());
        let mut report = service
            .inspect_with_liveness(None, &dead, Utc::now())
            .await
            .unwrap();
        let renewal_repository = storage.executions();
        let (renewal, recovery) = tokio::join!(
            renewal_repository.renew_lease(id, std::time::Duration::from_secs(60)),
            service.apply_with_liveness(&mut report, &dead),
        );
        recovery.unwrap();
        let current = storage.executions().execution(id).await.unwrap();
        let current_lease = storage.executions().lease(id).await.unwrap();
        if renewal.is_ok() {
            assert_ne!(current.state, ExecutionState::RecoveryRequired);
            assert!(current_lease.is_some());
        } else {
            assert_eq!(current.state, ExecutionState::RecoveryRequired);
            assert!(current_lease.is_none());
        }
    }

    #[tokio::test]
    async fn expired_dead_owner_is_recovered_and_remains_reported() {
        let (dir, storage, id) = fixture().await;
        expire_lease(&storage, id).await;
        let workspace_path = dir.path().join("workspace-preserve-marker");
        std::fs::create_dir(&workspace_path).unwrap();
        std::fs::write(workspace_path.join("keep.txt"), "preserve").unwrap();
        let root = dir.path().canonicalize().unwrap();
        storage
            .workspaces()
            .reserve(&WorkspaceOwnershipRecord {
                workspace_id: Uuid::new_v4(),
                execution_id: id,
                canonical_path: workspace_path.canonicalize().unwrap(),
                repository_identity: "local-test".into(),
                repository_root: root,
                repository_revision: "abc123".into(),
                git_admin_path: None,
                git_common_dir: None,
                sidecar_path: dir.path().join("missing-sidecar.json"),
                ownership_token: Uuid::new_v4(),
                status: WorkspaceStatus::Preparing,
                uncertainty_code: None,
                uncertainty_detail: None,
                created_at: Utc::now(),
                updated_at: Utc::now(),
            })
            .await
            .unwrap();
        let lease = storage.executions().lease(id).await.unwrap().unwrap();
        let dead = FakeLiveness(HashMap::from([(lease.owner_id, OwnerLiveness::Inactive)]));
        let runtime = FakeRuntime {
            inspections: AtomicUsize::new(0),
            containers: vec![RuntimeContainerPresence {
                id: "deadbeef1234".into(),
                name: "synroute-fixture".into(),
                state: "running".into(),
                ownership_labels_verified: true,
            }],
        };
        let mut report = RecoveryService::new(storage.clone())
            .inspect_with_liveness(Some(&runtime), &dead, Utc::now())
            .await
            .unwrap();
        assert_eq!(
            report.executions[0].assessment,
            RecoveryAssessment::OwnerInactiveLeaseExpired
        );
        assert_eq!(
            storage.executions().execution(id).await.unwrap().state,
            ExecutionState::Received,
            "inspection alone must not mutate lifecycle state"
        );
        assert!(
            storage
                .executions()
                .events(id)
                .await
                .unwrap()
                .iter()
                .all(|event| event.event_type != ExecutionEventType::RecoveryRequired)
        );
        RecoveryService::new(storage.clone())
            .apply_with_liveness(&mut report, &dead)
            .await
            .unwrap();
        assert_eq!(
            storage.executions().execution(id).await.unwrap().state,
            ExecutionState::RecoveryRequired
        );
        let former_owner = storage.executions();
        assert!(
            former_owner
                .transition(id, ExecutionState::Failed, None)
                .await
                .is_err()
        );
        assert!(
            former_owner
                .append_event(id, ExecutionEventType::RepositoryAnalyzed, json!({}))
                .await
                .is_err()
        );
        assert!(
            former_owner
                .renew_lease(id, std::time::Duration::from_secs(30))
                .await
                .is_err()
        );
        let contract = former_owner.verification_contract(id).await.unwrap();
        let fabricated = crate::verification::ExecutedVerification::test_only(
            contract.version(),
            crate::verification::VerificationReport {
                checks: Vec::new(),
                status: crate::verification::VerificationStatus::Incomplete,
                session_cleanup: crate::sandbox::CleanupOutcome::not_required(),
            },
        );
        assert!(
            former_owner
                .record_verification(id, fabricated)
                .await
                .is_err()
        );
        assert!(storage.executions().lease(id).await.unwrap().is_none());
        assert_eq!(runtime.inspections.load(Ordering::SeqCst), 1);
        assert_eq!(report.unresolved_cases.len(), 1);
        assert_eq!(report.unresolved_cases[0].status, RecoveryCaseStatus::Open);
        assert_eq!(
            std::fs::read_to_string(workspace_path.join("keep.txt")).unwrap(),
            "preserve"
        );

        let repeated = RecoveryService::new(storage.clone())
            .inspect_with_liveness(None, &dead, Utc::now())
            .await
            .unwrap();
        assert_eq!(repeated.executions.len(), 0);
        assert_eq!(repeated.unresolved_cases.len(), 1);
        let events = storage.executions().events(id).await.unwrap();
        assert!(
            events
                .iter()
                .any(|event| event.event_type == ExecutionEventType::RecoveryRequired)
        );
    }

    #[tokio::test]
    async fn lease_renewal_extends_expiry_for_its_owner_only() {
        let (dir, storage, id) = fixture().await;
        let before = storage.executions().lease(id).await.unwrap().unwrap();
        let renewed = storage
            .executions()
            .renew_lease(id, std::time::Duration::from_secs(120))
            .await
            .unwrap();
        assert_eq!(renewed.owner_id, before.owner_id);
        assert!(renewed.expires_at > before.expires_at);

        let other_owner = SqliteStorage::open(dir.path().join("recovery.sqlite"))
            .await
            .unwrap();
        assert!(
            other_owner
                .executions()
                .renew_lease(id, std::time::Duration::from_secs(60))
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn expired_lease_cannot_be_renewed_without_reacquisition() {
        let (_dir, storage, id) = fixture().await;
        expire_lease(&storage, id).await;
        assert!(
            storage
                .executions()
                .renew_lease(id, std::time::Duration::from_secs(60))
                .await
                .is_err()
        );
        assert_eq!(
            storage.executions().execution(id).await.unwrap().state,
            ExecutionState::Received
        );
    }

    #[tokio::test]
    async fn unknown_liveness_is_reported_without_mutating_execution() {
        let (_dir, storage, id) = fixture().await;
        let unknown = FakeLiveness(HashMap::new());
        let mut report = RecoveryService::new(storage.clone())
            .inspect_with_liveness(None, &unknown, Utc::now())
            .await
            .unwrap();
        assert_eq!(
            report.executions[0].assessment,
            RecoveryAssessment::OwnerLivenessUnknown
        );
        RecoveryService::new(storage.clone())
            .apply_with_liveness(&mut report, &unknown)
            .await
            .unwrap();
        assert_eq!(
            storage.executions().execution(id).await.unwrap().state,
            ExecutionState::Received
        );
        assert_eq!(report.unresolved_cases.len(), 1);
        assert!(report.executions[0].process_liveness_unknown);
    }

    #[tokio::test]
    async fn concurrent_recovery_attempts_change_execution_once() {
        let (_dir, storage, id) = fixture().await;
        expire_lease(&storage, id).await;
        let lease = storage.executions().lease(id).await.unwrap().unwrap();
        let dead = FakeLiveness(HashMap::from([(lease.owner_id, OwnerLiveness::Inactive)]));
        let service1 = RecoveryService::new(storage.clone());
        let service2 = RecoveryService::new(storage.clone());
        let mut report1 = service1
            .inspect_with_liveness(None, &dead, Utc::now())
            .await
            .unwrap();
        let mut report2 = service2
            .inspect_with_liveness(None, &dead, Utc::now())
            .await
            .unwrap();
        let (first, second) = tokio::join!(
            service1.apply_with_liveness(&mut report1, &dead),
            service2.apply_with_liveness(&mut report2, &dead)
        );
        first.unwrap();
        second.unwrap();
        assert_eq!(
            storage.executions().execution(id).await.unwrap().state,
            ExecutionState::RecoveryRequired
        );
        let terminal_events = storage
            .executions()
            .events(id)
            .await
            .unwrap()
            .into_iter()
            .filter(|event| event.event_type == ExecutionEventType::RecoveryRequired)
            .count();
        assert_eq!(terminal_events, 1);
        assert_eq!(storage.recovery().open_cases().await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn recovery_case_acknowledgement_and_resolution_are_durable() {
        let (_dir, storage, id) = fixture().await;
        let unknown = FakeLiveness(HashMap::new());
        let mut report = RecoveryService::new(storage.clone())
            .inspect_with_liveness(None, &unknown, Utc::now())
            .await
            .unwrap();
        RecoveryService::new(storage.clone())
            .apply_with_liveness(&mut report, &unknown)
            .await
            .unwrap();
        let case = report.unresolved_cases[0].clone();
        storage.recovery().acknowledge(case.case_id).await.unwrap();
        assert!(storage.recovery().open_cases().await.unwrap().is_empty());
        let history = storage.recovery().cases_for_execution(id).await.unwrap();
        assert_eq!(history[0].status, RecoveryCaseStatus::Acknowledged);
        let mut repeated = RecoveryService::new(storage.clone())
            .inspect_with_liveness(None, &unknown, Utc::now())
            .await
            .unwrap();
        RecoveryService::new(storage.clone())
            .apply_with_liveness(&mut repeated, &unknown)
            .await
            .unwrap();
        assert!(repeated.unresolved_cases.is_empty());
        assert_eq!(
            storage.recovery().cases_for_execution(id).await.unwrap()[0].status,
            RecoveryCaseStatus::Acknowledged
        );
        storage.recovery().resolve(case.case_id).await.unwrap();
        assert_eq!(
            storage.recovery().cases_for_execution(id).await.unwrap()[0].status,
            RecoveryCaseStatus::Resolved
        );
    }

    #[tokio::test]
    async fn expired_lease_waits_until_owner_is_confirmed_inactive() {
        let (_dir, storage, id) = fixture().await;
        expire_lease(&storage, id).await;
        let lease = storage.executions().lease(id).await.unwrap().unwrap();
        let active = FakeLiveness(HashMap::from([(lease.owner_id, OwnerLiveness::Active)]));
        let mut report = RecoveryService::new(storage.clone())
            .inspect_with_liveness(None, &active, Utc::now())
            .await
            .unwrap();
        RecoveryService::new(storage.clone())
            .apply_with_liveness(&mut report, &active)
            .await
            .unwrap();
        assert_eq!(
            storage.executions().execution(id).await.unwrap().state,
            ExecutionState::Received
        );
        assert_eq!(storage.recovery().open_cases().await.unwrap().len(), 1);
    }

    #[test]
    fn linux_process_start_parser_uses_field_22_after_final_comm_paren() {
        let fields = std::iter::once("S")
            .chain((4..=21).map(|_| "x"))
            .chain(std::iter::once("start-ticks"))
            .collect::<Vec<_>>()
            .join(" ");
        let stat = format!("42 (process ) has parens) {fields}");
        #[cfg(target_os = "linux")]
        assert_eq!(process_start_ticks(&stat).as_deref(), Some("start-ticks"));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn system_liveness_checks_boot_and_process_start_identity_not_pid_alone() {
        let (_dir, storage, id) = fixture().await;
        let lease = storage.executions().lease(id).await.unwrap().unwrap();
        assert_eq!(SystemOwnerLiveness.check(&lease), OwnerLiveness::Active);
        let mut reused_pid = lease.clone();
        reused_pid.process_start = Some("not-the-current-start-time".into());
        assert_eq!(
            SystemOwnerLiveness.check(&reused_pid),
            OwnerLiveness::Inactive
        );
        let mut different_boot = lease;
        different_boot.boot_id = Some("different-boot-id".into());
        assert_eq!(
            SystemOwnerLiveness.check(&different_boot),
            OwnerLiveness::Inactive
        );
    }
}
