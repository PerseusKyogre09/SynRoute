use crate::{
    agent::{AgentRequest, CodingAgent},
    core::{ExecutionState, VerificationCommand},
    repository,
    storage::{
        ExecutionEventType, ExecutionIntent, ExecutionRepository, FailureClassification,
        PersistedExecutionConfig, SqliteStorage, WorkspaceRepository,
    },
    workspace::persistent::PersistentWorkspaceManager,
};
use anyhow::{Context, Result};
use serde::Serialize;
use serde_json::json;
use std::{path::Path, time::Duration};
use tokio::sync::{mpsc, watch};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize)]
pub struct ExecutionSummary {
    pub execution_id: Uuid,
    pub state: ExecutionState,
    pub provider: String,
    pub workspace: Option<std::path::PathBuf>,
    pub primary_error: Option<String>,
    pub cleanup_error: Option<String>,
}

/// Creates the durable intent, prepares an owned worktree, and invokes an adapter only if
/// it explicitly reports isolated execution support. Verification remains a separate
/// trusted runner operation; adapter completion never transitions to Completed.
pub async fn run(
    storage: SqliteStorage,
    repository_path: &Path,
    task: &str,
    timeout: Duration,
    agent: &dyn CodingAgent,
) -> Result<ExecutionSummary> {
    anyhow::ensure!(!task.trim().is_empty(), "task description cannot be empty");
    anyhow::ensure!(!timeout.is_zero(), "execution timeout must be positive");
    anyhow::ensure!(
        agent.capabilities().isolated_execution,
        "agent {} does not provide a proven controlled execution boundary",
        agent.id()
    );
    let profile = repository::discover(repository_path).await?;
    let checks = load_contract(&profile.root).await?;
    anyhow::ensure!(
        !checks.is_empty(),
        "repository has no valid required verification contract; refusing execution"
    );
    let id = Uuid::new_v4();
    let repository_identity = profile.root.to_string_lossy().into_owned();
    let executions = storage.executions();
    executions
        .create_execution(ExecutionIntent {
            id,
            description: task.to_owned(),
            metadata: json!({"agent": agent.id(), "coordinator": "phase4"}),
            repository_identity: repository_identity.clone(),
            repository_root: profile.root.clone(),
            repository_revision: profile.revision.clone(),
            working_directory: profile.working_directory.clone(),
            config: PersistedExecutionConfig {
                timeout_seconds: timeout.as_secs().max(1),
                max_recovery_attempts: 0,
                memory_bytes: 2 * 1024 * 1024 * 1024,
                cpu_millis: 2000,
                process_limit: 256,
                output_limit_bytes: 1024 * 1024,
            },
            required_verification: checks,
        })
        .await?;

    let mut state = ExecutionState::Received;
    let mut workspace = None;
    let mut primary_error = None;
    let mut cleanup_error = None;
    let attempt: Result<()> = async {
        advance(
            &executions,
            id,
            &mut state,
            ExecutionState::RepositoryAnalysis,
        )
        .await?;
        executions
            .append_event(
                id,
                ExecutionEventType::RepositoryAnalyzed,
                json!({"root": profile.root, "revision": profile.revision}),
            )
            .await?;
        advance(&executions, id, &mut state, ExecutionState::Planning).await?;
        advance(&executions, id, &mut state, ExecutionState::PlanValidation).await?;
        advance(
            &executions,
            id,
            &mut state,
            ExecutionState::WorkspacePreparation,
        )
        .await?;
        executions.renew_lease(id, Duration::from_secs(120)).await?;
        let workspace_id = Uuid::new_v4();
        let base = std::env::temp_dir().join("synroute-workspaces");
        let mut manager = PersistentWorkspaceManager::new(storage.clone());
        let path = manager
            .create(
                id,
                workspace_id,
                repository_identity,
                &profile.root,
                &base,
                &profile.revision,
            )
            .await?;
        workspace = Some(path.clone());
        executions
            .append_event(
                id,
                ExecutionEventType::WorkspacePrepared,
                json!({"workspace_id": workspace_id, "path": path}),
            )
            .await?;
        advance(&executions, id, &mut state, ExecutionState::Executing).await?;

        let (event_tx, mut event_rx) = mpsc::channel(128);
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let execution = tokio::time::timeout(
            timeout,
            agent.execute(
                AgentRequest {
                    prompt: task.to_owned(),
                    workspace: path.clone(),
                    timeout,
                },
                event_tx,
                cancel_rx,
            ),
        )
        .await;
        drop(cancel_tx);
        while let Ok(event) = event_rx.try_recv() {
            executions
                .append_event(
                    id,
                    ExecutionEventType::AgentActivity,
                    json!({"kind": event.kind, "message": event.message}),
                )
                .await?;
        }
        let outcome = execution.map_err(|_| {
            anyhow::anyhow!("coding agent timed out after {} seconds", timeout.as_secs())
        })??;
        executions
            .append_event(
                id,
                ExecutionEventType::AgentActivity,
                json!({
                    "provider": outcome.provider,
                    "model": outcome.model,
                    "provider_thread_id": outcome.provider_thread_id,
                    "exit_status": outcome.exit_status,
                    "completed": outcome.completed,
                    "failure": outcome.failure,
                    "stdout": outcome.stdout,
                    "stderr": outcome.stderr,
                    "output_truncated": outcome.output_truncated,
                    "duration": outcome.duration
                }),
            )
            .await?;
        anyhow::bail!(
            "agent invocation returned; independent verification coordinator is not configured"
        )
    }
    .await;

    if let Err(error) = attempt {
        primary_error = Some(format!("{error:#}"));
    }
    // Preserve changed worktrees and refuse cleanup; users must inspect or commit the changes.
    if let Some(path) = &workspace {
        if let Err(error) = cleanup_diagnostics(&storage, id, path).await {
            cleanup_error = Some(format!("{error:#}"));
        }
    }
    if !state.is_terminal() {
        let failure = if primary_error
            .as_deref()
            .is_some_and(|e| e.contains("timeout"))
        {
            FailureClassification::Timeout
        } else if primary_error
            .as_deref()
            .is_some_and(|e| e.contains("agent"))
        {
            FailureClassification::Agent
        } else {
            FailureClassification::Sandbox
        };
        match executions
            .transition(id, ExecutionState::Failed, Some(failure))
            .await
        {
            Ok(_) => state = ExecutionState::Failed,
            Err(error) => {
                primary_error = Some(format!(
                    "{}; failed to persist terminal failure: {error:#}",
                    primary_error.unwrap_or_default()
                ));
            }
        }
    }
    Ok(ExecutionSummary {
        execution_id: id,
        state,
        provider: agent.id().to_owned(),
        workspace,
        primary_error,
        cleanup_error,
    })
}

async fn load_contract(root: &Path) -> Result<Vec<VerificationCommand>> {
    let path = root.join("synroute-verification.json");
    let bytes = tokio::fs::read(&path)
        .await
        .with_context(|| format!("read required verification contract '{}'", path.display()))?;
    #[derive(serde::Deserialize)]
    struct Contract {
        checks: Vec<VerificationCommand>,
    }
    let contract: Contract =
        serde_json::from_slice(&bytes).context("decode verification contract")?;
    anyhow::ensure!(
        contract
            .checks
            .iter()
            .all(|check| !check.name.trim().is_empty() && !check.argv.is_empty()),
        "verification checks need non-empty names and argv"
    );
    Ok(contract.checks)
}

async fn advance(
    executions: &impl ExecutionRepository,
    id: Uuid,
    current: &mut ExecutionState,
    next: ExecutionState,
) -> Result<()> {
    executions.transition(id, next, None).await?;
    *current = next;
    Ok(())
}

async fn cleanup_diagnostics(storage: &SqliteStorage, id: Uuid, path: &Path) -> Result<()> {
    let workspaces = storage.workspaces().for_execution(id).await?;
    let Some(workspace) = workspaces.iter().find(|item| item.canonical_path == path) else {
        anyhow::bail!("workspace ownership record not found; cleanup refused")
    };
    anyhow::ensure!(
        workspace.status == crate::storage::WorkspaceStatus::Registered,
        "workspace ownership is uncertain; cleanup refused"
    );
    anyhow::bail!(
        "workspace contains task state; retained for inspection at {}",
        path.display()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent::{AgentCapabilities, AgentEvent, AgentOutcome};
    use async_trait::async_trait;

    struct Unsupported;
    #[async_trait]
    impl CodingAgent for Unsupported {
        fn id(&self) -> &'static str {
            "unsupported-test"
        }
        fn capabilities(&self) -> AgentCapabilities {
            AgentCapabilities {
                cancellation: false,
                structured_events: false,
                model_metadata: false,
                isolated_execution: false,
            }
        }
        async fn execute(
            &self,
            _: AgentRequest,
            _: mpsc::Sender<AgentEvent>,
            _: watch::Receiver<bool>,
        ) -> Result<AgentOutcome> {
            anyhow::bail!("must not be called")
        }
    }

    #[tokio::test]
    async fn unsupported_agent_fails_before_creating_execution_or_workspace() {
        let dir = tempfile::tempdir().unwrap();
        let storage = SqliteStorage::open(dir.path().join("coordinator.sqlite"))
            .await
            .unwrap();
        let error = run(
            storage.clone(),
            Path::new("/path/that/does/not/exist"),
            "change something",
            Duration::from_secs(1),
            &Unsupported,
        )
        .await
        .unwrap_err();
        assert!(
            error
                .to_string()
                .contains("proven controlled execution boundary")
        );
        assert!(
            storage
                .executions()
                .list_executions(None)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn requires_repository_contract_before_execution_intent() {
        let dir = tempfile::tempdir().unwrap();
        let repo = tempfile::tempdir().unwrap();
        std::process::Command::new("git")
            .args(["init", "-q"])
            .current_dir(repo.path())
            .status()
            .unwrap();
        std::process::Command::new("git")
            .args([
                "-c",
                "user.email=test@example.com",
                "-c",
                "user.name=Test",
                "commit",
                "--allow-empty",
                "-qm",
                "initial",
            ])
            .current_dir(repo.path())
            .status()
            .unwrap();
        let storage = SqliteStorage::open(dir.path().join("contract.sqlite"))
            .await
            .unwrap();
        let error = run(
            storage.clone(),
            repo.path(),
            "change something",
            Duration::from_secs(1),
            &TestSupported,
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("synroute-verification.json"));
        assert!(
            storage
                .executions()
                .list_executions(None)
                .await
                .unwrap()
                .is_empty()
        );
    }

    struct TestSupported;
    #[async_trait]
    impl CodingAgent for TestSupported {
        fn id(&self) -> &'static str {
            "test-supported"
        }
        fn capabilities(&self) -> AgentCapabilities {
            AgentCapabilities {
                cancellation: true,
                structured_events: true,
                model_metadata: true,
                isolated_execution: true,
            }
        }
        async fn execute(
            &self,
            _: AgentRequest,
            _: mpsc::Sender<AgentEvent>,
            _: watch::Receiver<bool>,
        ) -> Result<AgentOutcome> {
            anyhow::bail!("test adapter must not execute")
        }
    }
}
