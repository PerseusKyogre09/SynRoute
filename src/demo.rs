//! Small, explicit local demonstration workflow. This is not an agent coordinator.
use crate::{
    agent::{AgentStrategy, CodexAppServer},
    benchmark,
    core::{ExecutionState, VerificationCommand},
    inference::{self, InferenceResponse},
    models::RegistryConfig,
    repository::{self, RepositoryProfile},
    sandbox::{
        ResourceLimits, SandboxConfig,
        authorization::{CommandRule, ExecutionPolicy},
        docker::DockerSandbox,
    },
    selector::SelectionReport,
    storage::{
        ExecutionEventType, ExecutionIntent, ExecutionRepository, FailureClassification,
        PersistedExecutionConfig, SqliteStorage, WorkspaceRepository,
    },
    verification::{self, VerificationReport},
    workspace::persistent::PersistentWorkspaceManager,
};
use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::json;
use std::{collections::BTreeSet, path::Path, time::Duration};
use tokio::sync::watch;
use uuid::Uuid;

#[derive(Debug)]
pub struct DemoResult {
    pub profile: RepositoryProfile,
    pub execution_id: Uuid,
    pub workspace: Option<std::path::PathBuf>,
    pub sandbox_status: String,
    pub verification: Option<VerificationReport>,
    pub cleanup: String,
    pub state: ExecutionState,
}

#[derive(Debug, serde::Serialize)]
pub struct TaskDemoReport {
    pub repository: TaskDemoRepository,
    pub selection: SelectionReport,
    pub inference_response: Option<InferenceResponse>,
    pub inference_error: Option<String>,
    pub inference_notice: &'static str,
    pub execution_status: String,
    pub execution_summary: Option<crate::execution::coordinator::ExecutionSummary>,
    pub execution_error: Option<String>,
    pub verification_status: String,
    pub verification_evidence: Vec<crate::storage::VerificationRecord>,
}

#[derive(Debug, serde::Serialize)]
pub struct TaskDemoRepository {
    pub name: String,
    pub root: std::path::PathBuf,
    pub revision: String,
    pub fingerprint: String,
    pub project_type: String,
    pub languages: Vec<String>,
}

/// Unified repository analysis, model selection, metadata-only inference, and fail-closed
/// coding-agent orchestration demonstration. Repository source files are never sent.
pub async fn run_task(
    repository: &Path,
    task: &str,
    timeout: Duration,
    config: &RegistryConfig,
    storage: SqliteStorage,
) -> Result<TaskDemoReport> {
    anyhow::ensure!(!task.trim().is_empty(), "--task must not be empty");
    let profile = crate::repository::profiler::profile(
        repository,
        &crate::repository::profiler::ProfileOptions {
            provider: None,
            model: None,
            allow_remote: false,
        },
    )
    .await?;
    let fingerprint = benchmark::repository_fingerprint(&profile)?;
    let repository_info = TaskDemoRepository {
        name: profile.repository.name.clone(),
        root: profile.repository.root.clone(),
        revision: profile.repository.revision.clone(),
        fingerprint,
        project_type: profile.project.project_type.clone(),
        languages: profile.languages.iter().map(|l| l.name.clone()).collect(),
    };
    let ask = inference::ask_with_profile(&profile, task, config, &storage).await?;
    let selection = ask.selection;
    let mut execution_status = "not_started_no_available_model".to_owned();
    let mut execution_summary = None;
    let mut execution_error = None;
    let mut verification_status = "not_run".to_owned();
    let mut verification_evidence = Vec::new();

    if let Some(selected) = selection.selected.as_ref() {
        let strategy = AgentStrategy {
            task_category: format!("{:?}", selection.task_category).to_ascii_lowercase(),
            selection_mode: inference::selection_mode_name(selection.mode).to_owned(),
            provider_id: selected.provider_id.clone(),
            model_id: selected.model_id.clone(),
            repository_fingerprint: repository_info.fingerprint.clone(),
            repository_name: repository_info.name.clone(),
            repository_revision: repository_info.revision.clone(),
            project_type: repository_info.project_type.clone(),
            selection_explanation: selection.explanation.clone(),
        };
        match crate::execution::coordinator::run_with_strategy(
            storage.clone(),
            repository,
            task,
            timeout,
            &CodexAppServer,
            Some(strategy),
        )
        .await
        {
            Ok(summary) => {
                execution_status = format!("{:?}", summary.state).to_ascii_lowercase();
                match storage.executions().report(summary.execution_id).await {
                    Ok(report) => {
                        verification_evidence = report.verifications;
                        if !verification_evidence.is_empty() {
                            verification_status = verification_evidence
                                .last()
                                .map(|v| format!("{:?}", v.report.status).to_ascii_lowercase())
                                .unwrap_or_else(|| "not_run".into());
                        }
                    }
                    Err(error) => verification_status = format!("evidence_unavailable: {error}"),
                }
                execution_error = summary.primary_error.clone();
                execution_summary = Some(summary);
            }
            Err(error) => {
                let message = format!("{error:#}");
                execution_status = "not_started_or_unsupported".into();
                execution_error = Some(message);
            }
        }
    }

    Ok(TaskDemoReport {
        repository: repository_info,
        selection,
        inference_response: ask.inference,
        inference_error: ask.inference_error,
        inference_notice: "Inference is an answer based on repository profile metadata; it does not modify code or constitute verification.",
        execution_status,
        execution_summary,
        execution_error,
        verification_status,
        verification_evidence,
    })
}

/// Runs the fixed, persisted pytest contract in a Docker sandbox. The only executable
/// authorized here is `python3`; repository discovery hints are never executed.
pub async fn run(repository: &Path, storage: SqliteStorage) -> Result<DemoResult> {
    let profile = repository::discover(repository).await?;
    let id = Uuid::new_v4();
    let supported = vec![VerificationCommand {
        name: "pytest".into(),
        argv: vec![
            "python3".into(),
            "-m".into(),
            "pytest".into(),
            "-q".into(),
            "-p".into(),
            "no:cacheprovider".into(),
        ],
    }];
    #[derive(Deserialize)]
    struct DemoContract {
        checks: Vec<VerificationCommand>,
    }
    let contract_path = profile.root.join("synroute-verification.json");
    let contract: DemoContract =
        serde_json::from_slice(&tokio::fs::read(&contract_path).await.with_context(|| {
            format!("read explicit demo contract '{}'", contract_path.display())
        })?)
        .context("decode explicit demo contract")?;
    anyhow::ensure!(
        contract.checks.len() == 1
            && contract.checks[0].name == supported[0].name
            && contract.checks[0].argv == supported[0].argv,
        "demo contract must exactly match the supported pytest argv: python3 -m pytest -q -p no:cacheprovider"
    );
    let checks = contract.checks;
    let intent = ExecutionIntent {
        id,
        description: "Run the explicit Synroute demo verification contract".into(),
        metadata: json!({"kind":"local_demo","agent_activity":false}),
        repository_identity: profile.root.to_string_lossy().into_owned(),
        repository_root: profile.root.clone(),
        repository_revision: profile.revision.clone(),
        working_directory: profile.working_directory.clone(),
        config: PersistedExecutionConfig {
            timeout_seconds: 120,
            max_recovery_attempts: 0,
            memory_bytes: 1024 * 1024 * 1024,
            cpu_millis: 1000,
            process_limit: 128,
            output_limit_bytes: 1024 * 1024,
        },
        required_verification: checks,
    };
    let executions = storage.executions();
    let mut record = executions.create_execution(intent).await?; // also acquires the durable lease
    let mut verification = None;
    let mut workspace = None;
    let mut cleanup = "not attempted".to_owned();
    let mut sandbox_status = "not started".to_owned();
    let mut primary_error: Option<anyhow::Error> = None;

    macro_rules! transition {
        ($state:expr, $failure:expr) => {
            record = executions.transition(id, $state, $failure).await?;
        };
    }
    let attempt = async {
        transition!(ExecutionState::RepositoryAnalysis, None);
        executions.append_event(id, ExecutionEventType::RepositoryAnalyzed, json!({
            "root": profile.root, "revision": profile.revision, "languages": profile.languages
        })).await?;
        transition!(ExecutionState::Planning, None);
        transition!(ExecutionState::PlanValidation, None);
        transition!(ExecutionState::WorkspacePreparation, None);
        executions.renew_lease(id, Duration::from_secs(120)).await?;
        let base = std::env::temp_dir().join("synroute-workspaces");
        let workspace_id = Uuid::new_v4();
        let mut manager = PersistentWorkspaceManager::new(storage.clone());
        let path = manager
            .create(
                id,
                workspace_id,
                profile.root.to_string_lossy().into_owned(),
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
                json!({"workspace_id":workspace_id,"path":path}),
            )
            .await?;
        transition!(ExecutionState::Executing, None);
        transition!(ExecutionState::Verifying, None);
        let provider = DockerSandbox::default().with_storage(storage.clone());
        let config = SandboxConfig {
            execution_id: id,
            workspace: path.clone(),
            image: "synroute-demo-python:3.12.8-pytest-8.3.5".into(),
            uid: 1000,
            gid: 1000,
            environment: [("PYTHONDONTWRITEBYTECODE".into(), "1".into())].into(),
            limits: limits(),
        };
        sandbox_status = "Docker sandbox configured".into();
        let policy = ExecutionPolicy::new([("python3".into(), CommandRule::Allow)]);
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let report = verification::run_for_execution(
            &storage,
            id,
            Some(&provider),
            Some(config),
            &policy,
            &BTreeSet::new(),
            Duration::from_secs(120),
            limits(),
            cancel_rx,
        )
        .await
        .context("trusted verification failed")?;
        drop(cancel_tx);
        sandbox_status = "Docker execution attempted".into();
        verification = Some(report.clone());
        let workspaces = storage.workspaces();
        let owned = workspaces.for_execution(id).await?;
        if let Some(item) = owned
            .iter()
            .find(|item| item.status == crate::storage::WorkspaceStatus::Registered)
        {
            match manager.remove(id, item.workspace_id, &path).await {
                Ok(()) => {
                    cleanup =
                        "workspace removed safely; Docker cleanup recorded in verification".into();
                    executions
                        .append_event(
                            id,
                            ExecutionEventType::WorkspaceRemoved,
                            json!({"workspace_id":item.workspace_id}),
                        )
                        .await?;
                }
                Err(error) => {
                    cleanup = format!("workspace cleanup failed: {error:#}");
                    primary_error = Some(anyhow::anyhow!("workspace cleanup failed"));
                }
            }
        }
        if primary_error.is_none() {
            if report.status == verification::VerificationStatus::Passed {
                transition!(ExecutionState::Completed, None);
            } else {
                let failure = if report.status == verification::VerificationStatus::TimedOut {
                    FailureClassification::Timeout
                } else {
                    FailureClassification::Verification
                };
                transition!(ExecutionState::Failed, Some(failure));
            }
        }
        Ok::<(), anyhow::Error>(())
    }
    .await;

    if let Err(error) = attempt {
        if primary_error.is_none() {
            primary_error = Some(error);
        }
    }
    if !record.state.is_terminal() {
        let terminal = ExecutionState::Failed;
        let failure = if sandbox_status == "not started" {
            FailureClassification::Sandbox
        } else {
            FailureClassification::Verification
        };
        // Preserve the original error if lifecycle recording itself also fails.
        match executions.transition(id, terminal, Some(failure)).await {
            Ok(updated) => record = updated,
            Err(state_error) => {
                if let Some(error) = primary_error.take() {
                    return Err(error.context(format!(
                        "also could not record terminal failure state: {state_error:#}"
                    )));
                }
                return Err(state_error.into());
            }
        }
    }
    if let Some(error) = primary_error {
        tracing::warn!(execution_id=%id, error=%error, "demo execution ended with a failure");
    }
    Ok(DemoResult {
        profile,
        execution_id: id,
        workspace,
        sandbox_status,
        verification,
        cleanup,
        state: record.state,
    })
}

fn limits() -> ResourceLimits {
    ResourceLimits {
        timeout: Duration::from_secs(120),
        memory_bytes: 1024 * 1024 * 1024,
        cpu_count: 1.0,
        pids: 128,
        stdout_bytes: 1024 * 1024,
        stderr_bytes: 1024 * 1024,
    }
}
