use crate::storage::{SqliteStorage, StorageError};
use crate::{
    core::VerificationCommand,
    sandbox::{
        CleanupOutcome, ExecutionResult, ResourceLimits, SandboxCommand, SandboxConfig,
        SandboxError, SandboxProvider, Termination,
        authorization::{Authorization, ExecutionPolicy},
    },
};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckStatus {
    Passed,
    Failed,
    TimedOut,
    Cancelled,
    Skipped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VerificationStatus {
    Passed,
    Failed,
    TimedOut,
    Cancelled,
    Incomplete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckEvidence {
    pub name: String,
    pub argv: Vec<String>,
    pub status: CheckStatus,
    pub exit_code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub duration: Duration,
    pub detail: Option<String>,
    #[serde(default)]
    pub termination: Termination,
    #[serde(default)]
    pub execution_error: Option<String>,
    #[serde(default)]
    pub cleanup: CleanupOutcome,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationReport {
    pub checks: Vec<CheckEvidence>,
    pub status: VerificationStatus,
    #[serde(default)]
    pub session_cleanup: CleanupOutcome,
}

#[derive(Debug, Error)]
pub enum VerificationError {
    #[error("verification requires a configured sandbox; refusing host execution")]
    SandboxUnavailable,
    #[error("sandbox configuration is missing")]
    ConfigMissing,
    #[error("sandbox configuration belongs to another execution")]
    ConfigExecutionMismatch,
    #[error(transparent)]
    Sandbox(#[from] SandboxError),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

/// Opaque in-process result issued only by the repository-bound verification path below.
#[allow(dead_code)] // Constructed only by the trusted persistence path below.
pub(crate) struct ExecutedVerification {
    contract_version: uuid::Uuid,
    report: VerificationReport,
}

impl ExecutedVerification {
    pub(crate) fn report(&self) -> &VerificationReport {
        &self.report
    }
    pub(crate) fn contract_version(&self) -> uuid::Uuid {
        self.contract_version
    }

    #[cfg(test)]
    pub(crate) fn test_only(contract_version: uuid::Uuid, report: VerificationReport) -> Self {
        Self {
            contract_version,
            report,
        }
    }
}

/// Run the exact contract stored for this execution and atomically persist its results.
/// The caller cannot supply a report; evidence is constructed from sandbox command results.
#[allow(dead_code)] // Kept crate-private until an execution coordinator is implemented.
pub(crate) async fn run_for_execution(
    storage: &SqliteStorage,
    execution_id: uuid::Uuid,
    provider: Option<&dyn SandboxProvider>,
    config: Option<SandboxConfig>,
    policy: &ExecutionPolicy,
    approvals: &BTreeSet<String>,
    global_timeout: Duration,
    per_command_limits: ResourceLimits,
    cancellation: watch::Receiver<bool>,
) -> Result<VerificationReport, VerificationError> {
    if config
        .as_ref()
        .is_some_and(|config| config.execution_id != execution_id)
    {
        return Err(VerificationError::ConfigExecutionMismatch);
    }
    let repository = storage.executions();
    let contract = repository.verification_contract(execution_id).await?;
    let report = run(
        provider,
        config,
        policy,
        approvals,
        contract.required_checks(),
        global_timeout,
        per_command_limits,
        cancellation,
    )
    .await?;
    repository
        .record_verification(
            execution_id,
            ExecutedVerification {
                contract_version: contract.version(),
                report: report.clone(),
            },
        )
        .await?;
    Ok(report)
}

/// Run only explicitly authorized argv commands through a sandbox. Empty contracts are incomplete.
pub async fn run(
    provider: Option<&dyn SandboxProvider>,
    config: Option<SandboxConfig>,
    policy: &ExecutionPolicy,
    approvals: &BTreeSet<String>,
    commands: &[VerificationCommand],
    global_timeout: Duration,
    per_command_limits: ResourceLimits,
    cancellation: watch::Receiver<bool>,
) -> Result<VerificationReport, VerificationError> {
    if commands.is_empty() {
        return Ok(VerificationReport {
            checks: Vec::new(),
            status: VerificationStatus::Incomplete,
            session_cleanup: CleanupOutcome::not_required(),
        });
    }
    let mut checks = Vec::with_capacity(commands.len());
    let authorized: Vec<_> = commands
        .iter()
        .map(|check| {
            if check.argv.is_empty() {
                return Authorization::Unsupported;
            }
            policy.authorize(
                &SandboxCommand {
                    executable: check.argv[0].clone(),
                    args: check.argv[1..].to_vec(),
                },
                approvals,
            )
        })
        .collect();
    let has_allowed = authorized.contains(&Authorization::Allowed);
    let mut session = if has_allowed {
        let provider = provider.ok_or(VerificationError::SandboxUnavailable)?;
        let config = config.ok_or(VerificationError::ConfigMissing)?;
        Some(provider.create(config).await?)
    } else {
        None
    };
    let started = Instant::now();
    let mut stopped = false;
    for (check, decision) in commands.iter().zip(authorized) {
        if stopped {
            checks.push(skipped(check, "not run because verification stopped"));
            continue;
        }
        match decision {
            Authorization::Denied => {
                checks.push(skipped(check, "denied by execution policy"));
                continue;
            }
            Authorization::ApprovalRequired => {
                checks.push(skipped(check, "explicit approval required"));
                continue;
            }
            Authorization::Unsupported => {
                checks.push(skipped(check, "command unsupported by execution policy"));
                continue;
            }
            Authorization::Allowed => {}
        }
        if *cancellation.borrow() {
            checks.push(CheckEvidence {
                name: check.name.clone(),
                argv: check.argv.clone(),
                status: CheckStatus::Cancelled,
                exit_code: None,
                stdout: Vec::new(),
                stderr: Vec::new(),
                stdout_truncated: false,
                stderr_truncated: false,
                duration: Duration::ZERO,
                detail: Some("cancelled before command start".into()),
                termination: Termination::Cancelled,
                execution_error: None,
                cleanup: CleanupOutcome::not_required(),
            });
            stopped = true;
            continue;
        }
        let remaining = global_timeout.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            checks.push(CheckEvidence {
                name: check.name.clone(),
                argv: check.argv.clone(),
                status: CheckStatus::TimedOut,
                exit_code: None,
                stdout: Vec::new(),
                stderr: Vec::new(),
                stdout_truncated: false,
                stderr_truncated: false,
                duration: Duration::ZERO,
                detail: Some("global verification timeout elapsed".into()),
                termination: Termination::TimedOut,
                execution_error: None,
                cleanup: CleanupOutcome::not_required(),
            });
            stopped = true;
            continue;
        }
        let mut limits = per_command_limits.clone();
        limits.timeout = limits.timeout.min(remaining);
        let argv = &check.argv;
        let command = SandboxCommand {
            executable: argv[0].clone(),
            args: argv[1..].to_vec(),
        };
        let result = match session
            .as_mut()
            .expect("authorized command creates session")
            .execute(command, limits, cancellation.clone())
            .await
        {
            Ok(result) => result,
            Err(error) => {
                checks.push(CheckEvidence {
                    name: check.name.clone(),
                    argv: check.argv.clone(),
                    status: CheckStatus::Failed,
                    exit_code: None,
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                    stdout_truncated: false,
                    stderr_truncated: false,
                    duration: Duration::ZERO,
                    detail: Some("sandbox execution failed".into()),
                    termination: Termination::Failed,
                    execution_error: Some(error.to_string()),
                    cleanup: CleanupOutcome::not_required(),
                });
                stopped = true;
                continue;
            }
        };
        let mut evidence = evidence(check, result);
        // A command cannot count as passing if its own container may still exist.
        if !evidence.cleanup.is_successful() && evidence.status == CheckStatus::Passed {
            evidence.status = CheckStatus::Failed;
            evidence.detail = Some("command passed but container cleanup was not confirmed".into());
        }
        stopped = matches!(
            evidence.status,
            CheckStatus::TimedOut | CheckStatus::Cancelled
        );
        checks.push(evidence);
        if started.elapsed() >= global_timeout {
            stopped = true;
        }
    }
    let session_cleanup = if let Some(mut session) = session {
        match session.destroy().await {
            Ok(outcome) => outcome,
            Err(error) => CleanupOutcome::failed(
                crate::sandbox::CleanupStatus::Failed,
                None,
                None,
                error.to_string(),
            ),
        }
    } else {
        CleanupOutcome::not_required()
    };
    let mut status = summarize(&checks);
    if status == VerificationStatus::Passed && !session_cleanup.is_successful() {
        status = VerificationStatus::Failed;
    }
    Ok(VerificationReport {
        checks,
        status,
        session_cleanup,
    })
}

fn skipped(check: &VerificationCommand, detail: &str) -> CheckEvidence {
    CheckEvidence {
        name: check.name.clone(),
        argv: check.argv.clone(),
        status: CheckStatus::Skipped,
        exit_code: None,
        stdout: Vec::new(),
        stderr: Vec::new(),
        stdout_truncated: false,
        stderr_truncated: false,
        duration: Duration::ZERO,
        detail: Some(detail.into()),
        termination: Termination::Failed,
        execution_error: None,
        cleanup: CleanupOutcome::not_required(),
    }
}

fn evidence(check: &VerificationCommand, result: ExecutionResult) -> CheckEvidence {
    let status = match result.termination {
        Termination::TimedOut => CheckStatus::TimedOut,
        Termination::Cancelled => CheckStatus::Cancelled,
        Termination::Exited if result.exit_code == Some(0) => CheckStatus::Passed,
        Termination::Exited | Termination::Failed => CheckStatus::Failed,
    };
    CheckEvidence {
        name: check.name.clone(),
        argv: check.argv.clone(),
        status,
        exit_code: result.exit_code,
        stdout: result.stdout,
        stderr: result.stderr,
        stdout_truncated: result.stdout_truncated,
        stderr_truncated: result.stderr_truncated,
        duration: result.duration,
        detail: result.execution_error.clone(),
        termination: result.termination,
        execution_error: result.execution_error,
        cleanup: result.cleanup,
    }
}

fn summarize(checks: &[CheckEvidence]) -> VerificationStatus {
    if checks.is_empty() {
        return VerificationStatus::Incomplete;
    }
    if checks.iter().any(|c| c.status == CheckStatus::Cancelled) {
        return VerificationStatus::Cancelled;
    }
    if checks.iter().any(|c| c.status == CheckStatus::TimedOut) {
        return VerificationStatus::TimedOut;
    }
    if checks.iter().any(|c| c.status == CheckStatus::Failed) {
        return VerificationStatus::Failed;
    }
    if checks.iter().any(|c| c.status == CheckStatus::Skipped) {
        return VerificationStatus::Incomplete;
    }
    if checks.iter().all(|c| c.status == CheckStatus::Passed) {
        VerificationStatus::Passed
    } else {
        VerificationStatus::Incomplete
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        core::{ExecutionState, VerificationCommand},
        sandbox::SandboxSession,
        storage::{ExecutionIntent, ExecutionRepository, PersistedExecutionConfig, SqliteStorage},
    };
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    #[derive(Clone)]
    struct Fake {
        result: ExecutionResult,
        calls: Arc<Mutex<usize>>,
        delay: Duration,
        destroy_failed: bool,
    }
    #[async_trait]
    impl SandboxProvider for Fake {
        async fn create(&self, _: SandboxConfig) -> Result<Box<dyn SandboxSession>, SandboxError> {
            Ok(Box::new(self.clone()))
        }
    }
    #[async_trait]
    impl SandboxSession for Fake {
        async fn execute(
            &mut self,
            _: SandboxCommand,
            limits: ResourceLimits,
            _: watch::Receiver<bool>,
        ) -> Result<ExecutionResult, SandboxError> {
            *self.calls.lock().unwrap() += 1;
            if !self.delay.is_zero()
                && tokio::time::timeout(limits.timeout, tokio::time::sleep(self.delay))
                    .await
                    .is_err()
            {
                let mut result = self.result.clone();
                result.termination = Termination::TimedOut;
                result.exit_code = None;
                return Ok(result);
            }
            Ok(self.result.clone())
        }
        async fn destroy(&mut self) -> Result<CleanupOutcome, SandboxError> {
            if self.destroy_failed {
                Err(SandboxError::Provider(
                    "mock session destruction failure".into(),
                ))
            } else {
                Ok(CleanupOutcome::not_required())
            }
        }
    }
    fn setup() -> (Fake, SandboxConfig, ExecutionPolicy, ResourceLimits) {
        let id = Uuid::new_v4();
        let fake = Fake {
            result: ExecutionResult {
                execution_id: id,
                exit_code: Some(0),
                stdout: b"ok".to_vec(),
                stderr: vec![],
                stdout_truncated: false,
                stderr_truncated: false,
                termination: Termination::Exited,
                duration: Duration::from_millis(1),
                execution_error: None,
                cleanup: CleanupOutcome::not_required(),
            },
            calls: Arc::new(Mutex::new(0)),
            delay: Duration::ZERO,
            destroy_failed: false,
        };
        let limits = ResourceLimits {
            timeout: Duration::from_secs(1),
            memory_bytes: 1024,
            cpu_count: 1.0,
            pids: 10,
            stdout_bytes: 100,
            stderr_bytes: 100,
        };
        (
            fake,
            SandboxConfig {
                execution_id: id,
                workspace: ".".into(),
                image: "fake".into(),
                uid: 1000,
                gid: 1000,
                environment: Default::default(),
                limits: limits.clone(),
            },
            ExecutionPolicy::new([(
                "cargo".into(),
                super::super::sandbox::authorization::CommandRule::Allow,
            )]),
            limits,
        )
    }
    fn check(argv: &[&str]) -> VerificationCommand {
        VerificationCommand {
            name: "check".into(),
            argv: argv.iter().map(|v| (*v).into()).collect(),
        }
    }

    #[tokio::test]
    async fn empty_contract_is_incomplete() {
        let report = run(
            None,
            None,
            &ExecutionPolicy::default(),
            &BTreeSet::new(),
            &[],
            Duration::from_secs(1),
            setup().3,
            watch::channel(false).1,
        )
        .await
        .unwrap();
        assert_eq!(report.status, VerificationStatus::Incomplete);
    }

    #[test]
    fn legacy_evidence_without_cleanup_proof_deserializes_conservatively() {
        let report: VerificationReport = serde_json::from_value(serde_json::json!({
            "checks": [{
                "name": "legacy check",
                "argv": ["test"],
                "status": "passed",
                "exit_code": 0,
                "stdout": [],
                "stderr": [],
                "stdout_truncated": false,
                "stderr_truncated": false,
                "duration": {"secs": 0, "nanos": 0},
                "detail": null
            }],
            "status": "passed"
        }))
        .unwrap();
        assert_eq!(report.checks[0].termination, Termination::Failed);
        assert!(!report.checks[0].cleanup.is_successful());
        assert!(!report.session_cleanup.is_successful());
    }
    #[tokio::test]
    async fn missing_sandbox_fails_closed() {
        let (_, _, policy, limits) = setup();
        assert!(matches!(
            run(
                None,
                None,
                &policy,
                &BTreeSet::new(),
                &[check(&["cargo", "test"])],
                Duration::from_secs(1),
                limits,
                watch::channel(false).1
            )
            .await,
            Err(VerificationError::SandboxUnavailable)
        ));
    }
    #[tokio::test]
    async fn denied_and_approval_commands_are_skipped_incomplete() {
        let (fake, config, _, limits) = setup();
        let policy = ExecutionPolicy::new([
            (
                "blocked".into(),
                super::super::sandbox::authorization::CommandRule::Deny,
            ),
            (
                "publish".into(),
                super::super::sandbox::authorization::CommandRule::RequireApproval,
            ),
        ]);
        let report = run(
            Some(&fake),
            Some(config),
            &policy,
            &BTreeSet::new(),
            &[check(&["blocked"]), check(&["publish"])],
            Duration::from_secs(1),
            limits,
            watch::channel(false).1,
        )
        .await
        .unwrap();
        assert_eq!(report.status, VerificationStatus::Incomplete);
        assert_eq!(*fake.calls.lock().unwrap(), 0);
    }
    #[tokio::test]
    async fn global_timeout_bounds_each_sandbox_command() {
        let (mut fake, config, policy, mut limits) = setup();
        fake.delay = Duration::from_secs(2);
        limits.timeout = Duration::from_secs(10);
        let report = run(
            Some(&fake),
            Some(config),
            &policy,
            &BTreeSet::new(),
            &[check(&["cargo", "test"])],
            Duration::from_millis(20),
            limits,
            watch::channel(false).1,
        )
        .await
        .unwrap();
        assert_eq!(report.status, VerificationStatus::TimedOut);
        assert_eq!(report.checks[0].status, CheckStatus::TimedOut);
        assert!(report.checks[0].duration < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn verification_failure_and_session_destruction_failure_remain_separate() {
        let (mut fake, config, policy, limits) = setup();
        fake.destroy_failed = true;
        fake.result.exit_code = Some(1);
        fake.result.stderr = b"test failure".to_vec();
        let report = run(
            Some(&fake),
            Some(config),
            &policy,
            &BTreeSet::new(),
            &[check(&["cargo", "test"])],
            Duration::from_secs(1),
            limits,
            watch::channel(false).1,
        )
        .await
        .unwrap();
        assert_eq!(report.checks[0].exit_code, Some(1));
        assert_eq!(report.checks[0].stdout, b"ok");
        assert_eq!(report.checks[0].status, CheckStatus::Failed);
        assert_eq!(report.status, VerificationStatus::Failed);
        assert_eq!(
            report.session_cleanup.status,
            crate::sandbox::CleanupStatus::Failed
        );
        assert!(report.session_cleanup.container_may_remain);
    }

    #[tokio::test]
    async fn command_success_with_cleanup_failure_is_not_verified() {
        let (mut fake, config, policy, limits) = setup();
        fake.result.cleanup = CleanupOutcome::failed(
            crate::sandbox::CleanupStatus::Failed,
            Some("synroute-container".into()),
            Some("0123456789ab".into()),
            "mock cleanup failure".into(),
        );
        let report = run(
            Some(&fake),
            Some(config),
            &policy,
            &BTreeSet::new(),
            &[check(&["cargo", "test"])],
            Duration::from_secs(1),
            limits,
            watch::channel(false).1,
        )
        .await
        .unwrap();
        assert_eq!(report.checks[0].termination, Termination::Exited);
        assert_eq!(report.checks[0].exit_code, Some(0));
        assert_eq!(report.checks[0].status, CheckStatus::Failed);
        assert!(report.checks[0].cleanup.cleanup_error.is_some());
        assert_eq!(report.status, VerificationStatus::Failed);
    }

    #[test]
    fn evidence_and_summary_preserve_timeout_cancellation_failure_and_skips() {
        let check = check(&["cargo", "test"]);
        let make_result = |termination, code| ExecutionResult {
            execution_id: Uuid::new_v4(),
            exit_code: code,
            stdout: vec![],
            stderr: vec![],
            stdout_truncated: false,
            stderr_truncated: false,
            termination,
            duration: Duration::ZERO,
            execution_error: None,
            cleanup: CleanupOutcome::not_required(),
        };
        for (termination, code, expected) in [
            (Termination::Exited, Some(0), CheckStatus::Passed),
            (Termination::Exited, Some(2), CheckStatus::Failed),
            (Termination::TimedOut, None, CheckStatus::TimedOut),
            (Termination::Cancelled, None, CheckStatus::Cancelled),
        ] {
            assert_eq!(
                evidence(&check, make_result(termination, code)).status,
                expected
            );
        }
        let timed = evidence(&check, make_result(Termination::TimedOut, None));
        assert_eq!(
            summarize(&[timed, skipped(&check, "later check not run")]),
            VerificationStatus::TimedOut
        );
        assert_eq!(
            summarize(&[skipped(&check, "approval required")]),
            VerificationStatus::Incomplete
        );
    }

    #[tokio::test]
    async fn approved_check_uses_sandbox_and_records_evidence() {
        let (fake, config, policy, limits) = setup();
        let report = run(
            Some(&fake),
            Some(config),
            &policy,
            &BTreeSet::new(),
            &[check(&["cargo", "test"])],
            Duration::from_secs(1),
            limits,
            watch::channel(false).1,
        )
        .await
        .unwrap();
        assert_eq!(report.status, VerificationStatus::Passed);
        assert_eq!(report.checks[0].stdout, b"ok");
        assert_eq!(*fake.calls.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn persisted_execution_verification_uses_its_contract_and_runner_results() {
        let dir = tempfile::tempdir().unwrap();
        let storage = SqliteStorage::open(dir.path().join("verified.sqlite"))
            .await
            .unwrap();
        let id = Uuid::new_v4();
        let root = dir.path().canonicalize().unwrap();
        let contract_check = VerificationCommand {
            name: "unit tests".into(),
            argv: vec!["cargo".into(), "test".into()],
        };
        let repository = storage.executions();
        repository
            .create_execution(ExecutionIntent {
                id,
                description: "verified fixture".into(),
                metadata: serde_json::json!({}),
                repository_identity: "test".into(),
                repository_root: root.clone(),
                repository_revision: "deadbeef".into(),
                working_directory: root,
                config: PersistedExecutionConfig {
                    timeout_seconds: 10,
                    max_recovery_attempts: 0,
                    memory_bytes: 1024,
                    cpu_millis: 100,
                    process_limit: 4,
                    output_limit_bytes: 1024,
                },
                required_verification: vec![contract_check],
            })
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
            repository.transition(id, state, None).await.unwrap();
        }
        let (fake, mut config, policy, limits) = setup();
        config.execution_id = id;
        let report = run_for_execution(
            &storage,
            id,
            Some(&fake),
            Some(config),
            &policy,
            &BTreeSet::new(),
            Duration::from_secs(3),
            limits,
            watch::channel(false).1,
        )
        .await
        .unwrap();
        assert_eq!(report.status, VerificationStatus::Passed);
        assert_eq!(*fake.calls.lock().unwrap(), 1);
        assert_eq!(
            repository.verifications(id).await.unwrap()[0].report.checks[0].argv,
            vec!["cargo", "test"]
        );
        repository
            .transition(id, ExecutionState::Completed, None)
            .await
            .unwrap();
    }
}
