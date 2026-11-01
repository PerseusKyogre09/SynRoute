use crate::{
    sandbox::{RuntimeContainerPresence, RuntimeInspector},
    storage::{
        ExecutionLease, ExecutionRecord, ExecutionRepository, RecoveryCaseRecord, SqliteStorage,
        StorageError, WorkspaceOwnershipRecord, WorkspaceRepository, WorkspaceStatus,
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
    pub workspace: WorkspaceOwnershipRecord,
    pub validation_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionInspection {
    pub execution: ExecutionRecord,
    pub lease: Option<ExecutionLease>,
    pub assessment: RecoveryAssessment,
    pub runtime: RuntimeInspection,
    pub workspaces: Vec<WorkspaceInspection>,
    pub process_liveness_unknown: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryReport {
    pub inspected_at: DateTime<Utc>,
    pub executions: Vec<ExecutionInspection>,
    /// Open cases are repeated in each report until acknowledged or resolved.
    pub unresolved_cases: Vec<RecoveryCaseRecord>,
}

impl RecoveryReport {
    pub fn requires_attention(&self) -> bool {
        !self.unresolved_cases.is_empty()
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
                let validation_error = if workspace.status == WorkspaceStatus::Removed {
                    None
                } else {
                    PersistentWorkspaceManager::validate_record(&workspace)
                        .await
                        .err()
                };
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
        Ok(RecoveryReport {
            inspected_at: now,
            executions: inspections,
            unresolved_cases,
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
        for inspection in &mut report.executions {
            match inspection.assessment {
                RecoveryAssessment::OwnerInactiveLeaseExpired => {
                    let Some(lease) = inspection.lease.as_ref() else {
                        continue;
                    };
                    match liveness.check(lease) {
                        OwnerLiveness::Inactive => {
                            let workspace_issues = inspection
                                .workspaces
                                .iter()
                                .filter_map(|workspace| workspace.validation_error.as_ref())
                                .collect::<Vec<_>>();
                            let details = json!({
                                "assessment": inspection.assessment,
                                "runtime": inspection.runtime,
                                "workspace_issues": workspace_issues,
                                "automatic_resume": false,
                                "resource_cleanup": false
                            });
                            if let Some(execution) = recovery
                                .recover_expired_lease(lease, Utc::now(), details)
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
                        "lease": inspection.lease,
                        "runtime": inspection.runtime,
                        "workspace_issues": inspection.workspaces.iter()
                            .filter_map(|workspace| workspace.validation_error.as_ref())
                            .collect::<Vec<_>>(),
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
            let workspace_issues = inspection
                .workspaces
                .iter()
                .filter_map(|workspace| workspace.validation_error.as_ref())
                .collect::<Vec<_>>();
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
            })
            .await
            .unwrap();
        (dir, storage, id)
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
