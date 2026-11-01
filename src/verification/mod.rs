use crate::{
    core::VerificationCommand,
    sandbox::{
        ExecutionResult, ResourceLimits, SandboxCommand, SandboxConfig, SandboxError,
        SandboxProvider, Termination,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationReport {
    pub checks: Vec<CheckEvidence>,
    pub status: VerificationStatus,
}

#[derive(Debug, Error)]
pub enum VerificationError {
    #[error("verification requires a configured sandbox; refusing host execution")]
    SandboxUnavailable,
    #[error("sandbox configuration is missing")]
    ConfigMissing,
    #[error(transparent)]
    Sandbox(#[from] SandboxError),
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
    let mut execution_error = None;
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
                execution_error = Some(error);
                stopped = true;
                continue;
            }
        };
        let evidence = evidence(check, result);
        stopped = matches!(
            evidence.status,
            CheckStatus::TimedOut | CheckStatus::Cancelled
        );
        checks.push(evidence);
        if started.elapsed() >= global_timeout {
            stopped = true;
        }
    }
    if let Some(mut session) = session {
        session.destroy().await?;
    }
    if let Some(error) = execution_error {
        return Err(VerificationError::Sandbox(error));
    }
    let status = summarize(&checks);
    Ok(VerificationReport { checks, status })
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
    }
}

fn evidence(check: &VerificationCommand, result: ExecutionResult) -> CheckEvidence {
    let status = match result.termination {
        Termination::TimedOut => CheckStatus::TimedOut,
        Termination::Cancelled => CheckStatus::Cancelled,
        Termination::Exited if result.exit_code == Some(0) => CheckStatus::Passed,
        Termination::Exited => CheckStatus::Failed,
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
        detail: None,
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
    use crate::sandbox::SandboxSession;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};
    use uuid::Uuid;

    #[derive(Clone)]
    struct Fake {
        result: ExecutionResult,
        calls: Arc<Mutex<usize>>,
        delay: Duration,
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
        async fn destroy(&mut self) -> Result<(), SandboxError> {
            Ok(())
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
            },
            calls: Arc::new(Mutex::new(0)),
            delay: Duration::ZERO,
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
}
