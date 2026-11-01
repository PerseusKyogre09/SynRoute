use super::{
    CleanupOutcome, CleanupStatus, ExecutionResult, ResourceLimits, RuntimeContainerPresence,
    RuntimeInspector, SandboxCommand, SandboxConfig, SandboxError, SandboxProvider, SandboxSession,
    Termination,
};
use crate::storage::{
    ContainerDiagnosticReason, SqliteContainerDiagnosticRepository, SqliteStorage,
};
use async_trait::async_trait;
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant},
};
use tokio::sync::watch;
use tokio::{io::AsyncReadExt, process::Command, time::timeout};
use uuid::Uuid;

const MANAGED: &str = "com.synroute.managed";
const EXECUTION: &str = "com.synroute.execution-id";
const CONTAINER_TOKEN: &str = "com.synroute.container-token";
const CLEANUP_GRACE: Duration = Duration::from_secs(15);

#[derive(Debug, Clone)]
pub struct DockerSandbox {
    binary: PathBuf,
    storage: Option<SqliteStorage>,
    cleanup_grace: Duration,
}
impl Default for DockerSandbox {
    fn default() -> Self {
        Self {
            binary: "docker".into(),
            storage: None,
            cleanup_grace: CLEANUP_GRACE,
        }
    }
}
impl DockerSandbox {
    pub fn new(binary: PathBuf) -> Self {
        Self {
            binary,
            storage: None,
            cleanup_grace: CLEANUP_GRACE,
        }
    }

    /// Configure durable orphan diagnostics. Command execution is refused without this store.
    pub fn with_storage(mut self, storage: SqliteStorage) -> Self {
        self.storage = Some(storage);
        self
    }

    #[cfg(test)]
    fn with_cleanup_grace(mut self, cleanup_grace: Duration) -> Self {
        self.cleanup_grace = cleanup_grace;
        self
    }
}

#[async_trait]
impl SandboxProvider for DockerSandbox {
    async fn create(&self, config: SandboxConfig) -> Result<Box<dyn SandboxSession>, SandboxError> {
        validate_config(&config)?;
        let diagnostics = self.storage.as_ref().ok_or_else(|| {
            SandboxError::InvalidConfig(
                "Docker execution requires SQLite storage for durable container diagnostics".into(),
            )
        })?;
        let workspace = tokio::fs::canonicalize(&config.workspace)
            .await
            .map_err(|e| {
                SandboxError::InvalidConfig(format!("workspace must exist and be accessible: {e}"))
            })?;
        Ok(Box::new(DockerSession {
            docker: self.binary.clone(),
            config,
            workspace,
            containers: Vec::new(),
            diagnostics: diagnostics.containers(),
            cleanup_grace: self.cleanup_grace,
        }))
    }
}

#[async_trait]
impl RuntimeInspector for DockerSandbox {
    async fn inspect_execution(
        &self,
        execution_id: Uuid,
    ) -> Result<Vec<RuntimeContainerPresence>, SandboxError> {
        let args = vec![
            "ps".into(),
            "--all".into(),
            "--filter".into(),
            format!("label={MANAGED}=true"),
            "--filter".into(),
            format!("label={EXECUTION}={execution_id}"),
            "--format".into(),
            "{{.ID}}|{{.Names}}|{{.State}}".into(),
        ];
        let output = run_capture(&self.binary, &args, Duration::from_secs(10)).await?;
        if output.exit_code != Some(0) {
            return Err(SandboxError::Provider(format!(
                "Docker runtime inspection failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        let mut containers = Vec::new();
        for line in String::from_utf8_lossy(&output.stdout).lines() {
            let mut columns = line.split('|');
            let (Some(id), Some(name), Some(state)) =
                (columns.next(), columns.next(), columns.next())
            else {
                continue;
            };
            if !valid_id(id) {
                continue;
            }
            let ownership_labels_verified =
                verify_container_labels(&self.binary, id, execution_id).await?;
            containers.push(RuntimeContainerPresence {
                id: id.to_owned(),
                name: name.to_owned(),
                state: state.to_owned(),
                ownership_labels_verified,
            });
        }
        Ok(containers)
    }
}

struct DockerSession {
    docker: PathBuf,
    config: SandboxConfig,
    workspace: PathBuf,
    containers: Vec<ContainerIdentity>,
    diagnostics: SqliteContainerDiagnosticRepository,
    cleanup_grace: Duration,
}

/// Constructed only after `docker create` returns success and a valid container ID.
struct CreatedContainer {
    id: String,
    name: String,
    token: Uuid,
    intent_id: Uuid,
}

#[derive(Debug, Clone)]
struct ContainerIdentity {
    id: String,
    name: String,
    token: Uuid,
    intent_id: Uuid,
}

#[async_trait]
impl SandboxSession for DockerSession {
    async fn execute(
        &mut self,
        command: SandboxCommand,
        limits: ResourceLimits,
        cancellation: watch::Receiver<bool>,
    ) -> Result<ExecutionResult, SandboxError> {
        limits.validate()?;
        validate_command(&command)?;
        let started = Instant::now();
        let deadline = started + limits.timeout;
        let (created, mut output, mut execution_error) = self
            .create_container(&command, &limits, deadline, cancellation.clone())
            .await;
        let Some(created) = created else {
            return Ok(ExecutionResult {
                execution_id: self.config.execution_id,
                exit_code: output.exit_code,
                stdout: output.stdout,
                stderr: output.stderr,
                stdout_truncated: output.stdout_truncated,
                stderr_truncated: output.stderr_truncated,
                termination: output.termination,
                duration: started.elapsed(),
                execution_error,
                cleanup: output.cleanup,
            });
        };
        self.containers.push(ContainerIdentity {
            id: created.id.clone(),
            name: created.name.clone(),
            token: created.token,
            intent_id: created.intent_id,
        });
        // `docker create` control output is the container ID, never command output.
        output.exit_code = None;
        output.stdout.clear();
        output.stderr.clear();
        output.stdout_truncated = false;
        output.stderr_truncated = false;
        output.termination = Termination::Exited;
        if execution_error.is_none() && output.termination == Termination::Exited {
            match run_until(
                &self.docker,
                &["start".into(), "--attach".into(), created.id.clone()],
                deadline,
                limits.stdout_bytes,
                limits.stderr_bytes,
                cancellation,
            )
            .await
            {
                Ok(started_output) => output = started_output,
                Err(error) => {
                    output.termination = match error {
                        DockerCliError::DeadlineBeforeStart => Termination::TimedOut,
                        DockerCliError::CancelledBeforeStart => Termination::Cancelled,
                        _ => Termination::Failed,
                    };
                    execution_error = Some(error.to_string());
                }
            }
        }
        let identity = ContainerIdentity {
            id: created.id.clone(),
            name: created.name.clone(),
            token: created.token,
            intent_id: created.intent_id,
        };
        let cleanup = self
            .remove_owned(&identity, output.termination != Termination::Exited)
            .await;
        if cleanup.is_successful() {
            self.containers
                .retain(|container| container.id != created.id);
        }
        Ok(ExecutionResult {
            execution_id: self.config.execution_id,
            exit_code: output.exit_code,
            stdout: output.stdout,
            stderr: output.stderr,
            stdout_truncated: output.stdout_truncated,
            stderr_truncated: output.stderr_truncated,
            termination: output.termination,
            duration: started.elapsed(),
            execution_error,
            cleanup,
        })
    }

    async fn destroy(&mut self) -> Result<CleanupOutcome, SandboxError> {
        let mut failures = Vec::new();
        let cleanup_deadline = Instant::now() + self.cleanup_grace;
        for container in self.containers.clone() {
            let result = self
                .remove_owned_until(&container, true, cleanup_deadline)
                .await;
            if result.is_successful() {
                self.containers.retain(|entry| entry.id != container.id);
            } else if let Some(detail) = result.diagnostic {
                failures.push(detail);
            }
        }
        if failures.is_empty() {
            Ok(CleanupOutcome::not_required())
        } else {
            Ok(CleanupOutcome::failed(
                CleanupStatus::Failed,
                None,
                None,
                failures.join("; "),
            ))
        }
    }
}

impl DockerSession {
    async fn create_container(
        &self,
        command: &SandboxCommand,
        limits: &ResourceLimits,
        deadline: Instant,
        cancellation: watch::Receiver<bool>,
    ) -> (Option<CreatedContainer>, BoundedOutput, Option<String>) {
        let token = Uuid::new_v4();
        let workspace = self.workspace.to_str().unwrap_or_default();
        let name = format!("synroute-{}-{token}", self.config.execution_id);
        if workspace.is_empty() {
            let message = "workspace path is not UTF-8".to_owned();
            return (None, failed_output(&message), Some(message));
        }
        let mount = match workspace_mount_arg(workspace) {
            Ok(mount) => mount,
            Err(error) => {
                let message = error.to_string();
                return (None, failed_output(&message), Some(message));
            }
        };
        let mut args = vec![
            "create".to_owned(),
            "--name".into(),
            name.clone(),
            "--network=none".into(),
            "--cap-drop=ALL".into(),
            "--security-opt=no-new-privileges=true".into(),
            "--read-only".into(),
            "--init".into(),
            format!("--user={}:{}", self.config.uid, self.config.gid),
            format!("--memory={}", limits.memory_bytes),
            format!("--cpus={}", limits.cpu_count),
            format!("--pids-limit={}", limits.pids),
            mount,
            tmpfs_mount_arg(),
            "--workdir=/workspace".into(),
            format!("--label={MANAGED}=true"),
            format!("--label={EXECUTION}={}", self.config.execution_id),
            format!("--label={CONTAINER_TOKEN}={token}"),
        ];
        for (key, value) in &self.config.environment {
            if let Err(error) = validate_env(key, value) {
                let message = error.to_string();
                return (None, failed_output(&message), Some(message));
            }
            args.extend(["--env".into(), format!("{key}={value}")]);
        }
        args.push(self.config.image.clone());
        args.push(command.executable.clone());
        args.extend(command.args.clone());
        let intent = match self
            .diagnostics
            .reserve_intent(self.config.execution_id, &name, token)
            .await
        {
            Ok(intent) => intent,
            Err(error) => {
                let message = format!("container intent persistence failed: {error}");
                return (None, failed_output(&message), Some(message));
            }
        };
        let output = match run_until(
            &self.docker,
            &args,
            deadline,
            64 * 1024,
            64 * 1024,
            cancellation,
        )
        .await
        {
            Err(error) => {
                let primary = error.to_string();
                let cleanup = match &error {
                    DockerCliError::NotStarted(_)
                    | DockerCliError::DeadlineBeforeStart
                    | DockerCliError::CancelledBeforeStart => {
                        match self.diagnostics.resolve_intent(intent.intent_id).await {
                            Ok(()) => CleanupOutcome::not_required(),
                            Err(persist_error) => CleanupOutcome {
                                status: CleanupStatus::Failed,
                                container_name: Some(name.clone()),
                                container_id: None,
                                container_may_remain: false,
                                diagnostic: Some(format!(
                                    "Docker was not started; intent resolution failed: {persist_error}"
                                )),
                                inspection_error: None,
                                cleanup_error: Some(format!(
                                    "intent resolution failed: {persist_error}"
                                )),
                            },
                        }
                    }
                    DockerCliError::Operation(_) => {
                        self.cleanup_ambiguous_creation(&name, token, intent.intent_id)
                            .await
                    }
                };
                let mut result = failed_output(&primary);
                result.termination = match &error {
                    DockerCliError::DeadlineBeforeStart => Termination::TimedOut,
                    DockerCliError::CancelledBeforeStart => Termination::Cancelled,
                    _ => Termination::Failed,
                };
                result.cleanup = cleanup;
                return (None, result, Some(primary));
            }
            Ok(output) => output,
        };
        if output.termination == Termination::Exited && output.exit_code == Some(0) {
            let id = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            if valid_id(&id) {
                match self.diagnostics.mark_active(intent.intent_id, &id).await {
                    Ok(()) => {
                        return (
                            Some(CreatedContainer {
                                id,
                                name,
                                token,
                                intent_id: intent.intent_id,
                            }),
                            output,
                            None,
                        );
                    }
                    Err(error) => {
                        let primary = format!(
                            "container created but durable identity update failed: {error}"
                        );
                        let identity = ContainerIdentity {
                            id: id.clone(),
                            name: name.clone(),
                            token,
                            intent_id: intent.intent_id,
                        };
                        let cleanup = self.remove_owned(&identity, true).await;
                        return (
                            None,
                            BoundedOutput {
                                cleanup,
                                ..failed_output(&primary)
                            },
                            Some(primary),
                        );
                    }
                }
            }
            let primary = "docker create returned an invalid container ID".to_owned();
            let cleanup = self
                .cleanup_ambiguous_creation(&name, token, intent.intent_id)
                .await;
            let mut output = failed_output(&primary);
            output.cleanup = cleanup;
            return (None, output, Some(primary));
        }
        let diagnostic = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        let primary = if diagnostic.is_empty() {
            format!(
                "docker create ended with {:?} (exit code {:?})",
                output.termination, output.exit_code
            )
        } else {
            diagnostic.clone()
        };
        let ambiguous = output.termination != Termination::Exited
            || output.exit_code.is_none()
            || ambiguous_creation_failure(&diagnostic);
        if ambiguous {
            let cleanup = self
                .cleanup_ambiguous_creation(&name, token, intent.intent_id)
                .await;
            let primary = primary.clone();
            let mut output = output;
            output.cleanup = cleanup;
            return (None, output, Some(primary));
        }
        let mut output = output;
        if let Err(error) = self.diagnostics.resolve_intent(intent.intent_id).await {
            output.cleanup = CleanupOutcome {
                status: CleanupStatus::Failed,
                container_name: Some(name),
                container_id: None,
                container_may_remain: false,
                diagnostic: Some(format!(
                    "definite create failure preserved; durable intent resolution failed: {error}"
                )),
                inspection_error: None,
                cleanup_error: Some(format!("durable intent resolution failed: {error}")),
            };
        }
        (None, output, Some(primary))
    }

    async fn cleanup_ambiguous_creation(
        &self,
        generated_name: &str,
        token: Uuid,
        intent_id: Uuid,
    ) -> CleanupOutcome {
        let cleanup_deadline = Instant::now() + self.cleanup_grace;
        if !valid_container_reference(generated_name, self.config.execution_id) {
            return CleanupOutcome::failed(
                CleanupStatus::OwnershipUncertain,
                Some(generated_name.into()),
                None,
                "generated container name failed validation".into(),
            );
        }
        let args = vec![
            "ps".into(),
            "--all".into(),
            "--filter".into(),
            format!("name={generated_name}"),
            "--format".into(),
            "{{.ID}}|{{.Names}}".into(),
        ];
        let listing = match run_capture_until(&self.docker, &args, cleanup_deadline).await {
            Ok(output) if output.exit_code == Some(0) => output,
            Ok(_) => {
                return self
                    .persist_cleanup_failure(
                        generated_name,
                        None,
                        intent_id,
                        ContainerDiagnosticReason::AmbiguousCreate,
                        "container listing failed",
                    )
                    .await;
            }
            Err(_) => {
                return self
                    .persist_cleanup_failure(
                        generated_name,
                        None,
                        intent_id,
                        ContainerDiagnosticReason::AmbiguousCreate,
                        "container listing could not be completed",
                    )
                    .await;
            }
        };
        if listing.stdout_truncated {
            return self
                .persist_cleanup_failure(
                    generated_name,
                    None,
                    intent_id,
                    ContainerDiagnosticReason::OwnershipUncertain,
                    "container listing was truncated; ownership could not be established",
                )
                .await;
        }
        let mut matches = Vec::new();
        for row in String::from_utf8_lossy(&listing.stdout).lines() {
            let Some((id, name)) = row.split_once('|') else {
                return self
                    .persist_cleanup_failure(
                        generated_name,
                        None,
                        intent_id,
                        ContainerDiagnosticReason::OwnershipUncertain,
                        "container listing returned an unparseable identity",
                    )
                    .await;
            };
            if name == generated_name {
                if !valid_id(id) {
                    return self
                        .persist_cleanup_failure(
                            generated_name,
                            None,
                            intent_id,
                            ContainerDiagnosticReason::OwnershipUncertain,
                            "matching container name had an invalid ID",
                        )
                        .await;
                }
                matches.push(id.to_owned());
            }
        }
        if matches.len() > 1 {
            return self
                .persist_cleanup_failure(
                    generated_name,
                    None,
                    intent_id,
                    ContainerDiagnosticReason::OwnershipUncertain,
                    "multiple containers matched the unique generated name",
                )
                .await;
        }
        let Some(id) = matches.pop() else {
            return match self.diagnostics.resolve_intent(intent_id).await {
                Ok(()) => CleanupOutcome {
                    status: CleanupStatus::Succeeded,
                    container_name: Some(generated_name.into()),
                    container_id: None,
                    container_may_remain: false,
                    diagnostic: None,
                    inspection_error: None,
                    cleanup_error: None,
                },
                Err(error) => CleanupOutcome {
                    status: CleanupStatus::Failed,
                    container_name: Some(generated_name.into()),
                    container_id: None,
                    container_may_remain: false,
                    diagnostic: Some(format!(
                        "no matching container was found but its intent could not be resolved: {error}"
                    )),
                    inspection_error: None,
                    cleanup_error: Some(format!("intent resolution failed: {error}")),
                },
            };
        };
        let identity = ContainerIdentity {
            id: id.clone(),
            name: generated_name.into(),
            token,
            intent_id,
        };
        if let Err(error) = self.verify_identity(&identity, cleanup_deadline).await {
            return self
                .persist_cleanup_failure(
                    generated_name,
                    Some(&id),
                    intent_id,
                    ContainerDiagnosticReason::OwnershipUncertain,
                    &format!("ownership inspection failed: {error}"),
                )
                .await;
        }
        self.remove_owned_until(&identity, true, cleanup_deadline)
            .await
    }

    async fn remove_owned(&self, identity: &ContainerIdentity, force: bool) -> CleanupOutcome {
        self.remove_owned_until(identity, force, Instant::now() + self.cleanup_grace)
            .await
    }

    async fn remove_owned_until(
        &self,
        identity: &ContainerIdentity,
        force: bool,
        deadline: Instant,
    ) -> CleanupOutcome {
        if let Err(error) = self.verify_identity(identity, deadline).await {
            return self
                .persist_cleanup_failure(
                    &identity.name,
                    Some(&identity.id),
                    identity.intent_id,
                    ContainerDiagnosticReason::OwnershipUncertain,
                    &error.to_string(),
                )
                .await;
        }
        let mut args = vec!["rm".to_owned()];
        if force {
            args.push("--force".into());
        }
        args.push("--volumes".into());
        args.push(identity.id.clone());
        let output = match run_capture_until(&self.docker, &args, deadline).await {
            Ok(output) => output,
            Err(error) => {
                return self
                    .persist_cleanup_failure(
                        &identity.name,
                        Some(&identity.id),
                        identity.intent_id,
                        ContainerDiagnosticReason::CleanupFailed,
                        &error.to_string(),
                    )
                    .await;
            }
        };
        if output.exit_code != Some(0) {
            return self
                .persist_cleanup_failure(
                    &identity.name,
                    Some(&identity.id),
                    identity.intent_id,
                    ContainerDiagnosticReason::CleanupFailed,
                    "docker rm returned an unsuccessful status",
                )
                .await;
        }
        if let Err(error) = self.diagnostics.resolve_intent(identity.intent_id).await {
            return CleanupOutcome {
                status: CleanupStatus::Failed,
                container_name: Some(identity.name.clone()),
                container_id: Some(identity.id.clone()),
                container_may_remain: false,
                diagnostic: Some(format!(
                    "container was removed but its durable intent could not be resolved: {error}"
                )),
                inspection_error: None,
                cleanup_error: Some(format!("intent resolution failed: {error}")),
            };
        }
        CleanupOutcome::succeeded(identity.name.clone(), identity.id.clone())
    }

    async fn verify_identity(
        &self,
        identity: &ContainerIdentity,
        deadline: Instant,
    ) -> Result<(), String> {
        if !valid_id(&identity.id)
            || !valid_container_reference(&identity.name, self.config.execution_id)
        {
            return Err("container identity is invalid".into());
        }
        let format = format!(
            "{{{{.Name}}}}|{{{{ index .Config.Labels \"{MANAGED}\" }}}}|{{{{ index .Config.Labels \"{EXECUTION}\" }}}}|{{{{ index .Config.Labels \"{CONTAINER_TOKEN}\" }}}}"
        );
        let output = run_capture_until(
            &self.docker,
            &[
                "inspect".into(),
                "--format".into(),
                format,
                identity.id.clone(),
            ],
            deadline,
        )
        .await
        .map_err(|e| e.to_string())?;
        let expected = format!(
            "/{}|true|{}|{}",
            identity.name, self.config.execution_id, identity.token
        );
        if output.exit_code == Some(0) && String::from_utf8_lossy(&output.stdout).trim() == expected
        {
            Ok(())
        } else {
            Err("container identity or ownership labels did not match".into())
        }
    }

    async fn persist_cleanup_failure(
        &self,
        name: &str,
        id: Option<&str>,
        intent_id: Uuid,
        reason: ContainerDiagnosticReason,
        diagnostic: &str,
    ) -> CleanupOutcome {
        let status = if matches!(
            reason,
            ContainerDiagnosticReason::OwnershipUncertain
                | ContainerDiagnosticReason::AmbiguousCreate
        ) {
            CleanupStatus::OwnershipUncertain
        } else {
            CleanupStatus::Failed
        };
        let diagnostic = diagnostic.chars().take(512).collect::<String>();
        let mut safe = CleanupOutcome::failed(
            status,
            Some(name.into()),
            id.map(str::to_owned),
            diagnostic.clone(),
        );
        if matches!(
            reason,
            ContainerDiagnosticReason::OwnershipUncertain
                | ContainerDiagnosticReason::AmbiguousCreate
        ) {
            safe.inspection_error = Some(diagnostic.clone());
            safe.cleanup_error = None;
        }
        let ownership_persisted = self.diagnostics.mark_orphan_suspected(intent_id, id).await;
        let persisted = self
            .diagnostics
            .record(self.config.execution_id, name, id, reason)
            .await;
        if let Err(error) = ownership_persisted.and(persisted.map(|_| ())) {
            return CleanupOutcome {
                diagnostic: Some(format!(
                    "{}; durable diagnostic persistence failed: {error}",
                    safe.diagnostic.as_deref().unwrap_or("cleanup uncertain")
                )),
                ..safe
            };
        }
        safe
    }
}

async fn verify_container_labels(
    docker: &Path,
    id: &str,
    execution_id: Uuid,
) -> Result<bool, SandboxError> {
    let format = format!(
        "{{{{ index .Config.Labels \"{MANAGED}\" }}}}|{{{{ index .Config.Labels \"{EXECUTION}\" }}}}"
    );
    let output = run_capture(
        docker,
        &["inspect".into(), "--format".into(), format, id.into()],
        Duration::from_secs(10),
    )
    .await?;
    Ok(output.exit_code == Some(0)
        && String::from_utf8_lossy(&output.stdout).trim() == format!("true|{execution_id}"))
}

fn valid_id(value: &str) -> bool {
    (12..=64).contains(&value.len()) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}
fn workspace_mount_arg(workspace: &str) -> Result<String, SandboxError> {
    if workspace.contains(',') || workspace.chars().any(char::is_control) {
        return Err(SandboxError::InvalidConfig(
            "workspace path contains characters unsupported by Docker --mount syntax".into(),
        ));
    }
    // Bind mounts are read-write by default; Docker --mount accepts key=value options only.
    Ok(format!("--mount=type=bind,src={workspace},dst=/workspace"))
}
fn tmpfs_mount_arg() -> String {
    // Docker's --tmpfs uses path:options syntax, where rw is valid.
    "--tmpfs=/tmp:rw,nosuid,nodev,noexec,size=67108864".into()
}
fn ambiguous_creation_failure(diagnostic: &str) -> bool {
    let diagnostic = diagnostic.to_ascii_lowercase();
    [
        "connection reset",
        "unexpected eof",
        "broken pipe",
        "context deadline exceeded",
        "client.timeout",
        "client timeout",
        "error during connect",
    ]
    .iter()
    .any(|marker| diagnostic.contains(marker))
}
fn valid_container_reference(value: &str, execution_id: Uuid) -> bool {
    if valid_id(value) {
        return true;
    }
    let prefix = format!("synroute-{execution_id}-");
    value
        .strip_prefix(&prefix)
        .is_some_and(|suffix| suffix.parse::<Uuid>().is_ok())
}
fn validate_config(config: &SandboxConfig) -> Result<(), SandboxError> {
    config.limits.validate()?;
    if config.uid == 0 || config.gid == 0 {
        return Err(SandboxError::InvalidConfig(
            "container UID and GID must be non-root".into(),
        ));
    }
    if config.image.is_empty() || config.image.starts_with('-') || config.image.contains('\0') {
        return Err(SandboxError::InvalidConfig(
            "invalid image reference".into(),
        ));
    }
    for (key, value) in &config.environment {
        validate_env(key, value)?;
    }
    Ok(())
}
fn validate_env(key: &str, value: &str) -> Result<(), SandboxError> {
    if key.is_empty()
        || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        || value.contains('\0')
    {
        return Err(SandboxError::InvalidConfig(format!(
            "invalid environment variable '{key}'"
        )));
    }
    Ok(())
}
fn validate_command(command: &SandboxCommand) -> Result<(), SandboxError> {
    if command.executable.is_empty()
        || command.executable.starts_with('-')
        || command.executable.contains('\0')
        || command.args.iter().any(|arg| arg.contains('\0'))
    {
        return Err(SandboxError::InvalidConfig("invalid command argv".into()));
    }
    Ok(())
}

struct BoundedOutput {
    exit_code: Option<i32>,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    stdout_truncated: bool,
    stderr_truncated: bool,
    termination: Termination,
    cleanup: CleanupOutcome,
}
#[derive(Debug)]
enum DockerCliError {
    NotStarted(String),
    Operation(String),
    DeadlineBeforeStart,
    CancelledBeforeStart,
}
impl std::fmt::Display for DockerCliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotStarted(error) | Self::Operation(error) => f.write_str(error),
            Self::DeadlineBeforeStart => {
                f.write_str("Docker deadline elapsed before operation start")
            }
            Self::CancelledBeforeStart => {
                f.write_str("Docker operation was cancelled before start")
            }
        }
    }
}

fn failed_output(message: &str) -> BoundedOutput {
    BoundedOutput {
        exit_code: None,
        stdout: Vec::new(),
        stderr: message.as_bytes().to_vec(),
        stdout_truncated: false,
        stderr_truncated: false,
        termination: Termination::Failed,
        cleanup: CleanupOutcome::not_required(),
    }
}

async fn run_capture(
    docker: &Path,
    args: &[String],
    limit: Duration,
) -> Result<BoundedOutput, SandboxError> {
    run_bounded(
        docker,
        args,
        limit,
        64 * 1024,
        64 * 1024,
        watch::channel(false).1,
    )
    .await
    .map_err(|error| SandboxError::Provider(error.to_string()))
}

async fn run_capture_until(
    docker: &Path,
    args: &[String],
    deadline: Instant,
) -> Result<BoundedOutput, SandboxError> {
    run_until(
        docker,
        args,
        deadline,
        64 * 1024,
        64 * 1024,
        watch::channel(false).1,
    )
    .await
    .map_err(|error| SandboxError::Provider(error.to_string()))
}
async fn run_bounded(
    docker: &Path,
    args: &[String],
    limit: Duration,
    out_max: usize,
    err_max: usize,
    cancellation: watch::Receiver<bool>,
) -> Result<BoundedOutput, DockerCliError> {
    run_until(
        docker,
        args,
        Instant::now() + limit,
        out_max,
        err_max,
        cancellation,
    )
    .await
}

async fn run_until(
    docker: &Path,
    args: &[String],
    deadline: Instant,
    out_max: usize,
    err_max: usize,
    mut cancellation: watch::Receiver<bool>,
) -> Result<BoundedOutput, DockerCliError> {
    if *cancellation.borrow() {
        return Err(DockerCliError::CancelledBeforeStart);
    }
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(DockerCliError::DeadlineBeforeStart);
    }
    let mut child = Command::new(docker)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| DockerCliError::NotStarted(format!("start Docker CLI: {e}")))?;
    let stdout = tokio::spawn(read_bounded(child.stdout.take().unwrap(), out_max));
    let stderr = tokio::spawn(read_bounded(child.stderr.take().unwrap(), err_max));
    let mut cancellation_open = true;
    let (termination, status) = {
        let wait = timeout(remaining, child.wait());
        tokio::pin!(wait);
        loop {
            tokio::select! {
                result = &mut wait => break match result {
                    Ok(Ok(status)) => (Termination::Exited, Some(status)),
                    Ok(Err(error)) => return Err(DockerCliError::Operation(format!("wait for Docker CLI: {error}"))),
                    Err(_) => (Termination::TimedOut, None),
                },
                changed = cancellation.changed(), if cancellation_open => match changed {
                    Ok(()) if *cancellation.borrow() => break (Termination::Cancelled, None),
                    Err(_) => cancellation_open = false,
                    _ => {},
                },
            }
        }
    };
    if termination != Termination::Exited {
        let _ = child.kill().await;
        let _ = child.wait().await;
    }
    let (stdout, stdout_truncated) = stdout
        .await
        .map_err(|e| DockerCliError::Operation(e.to_string()))?
        .map_err(|e| DockerCliError::Operation(format!("read Docker stdout: {e}")))?;
    let (stderr, stderr_truncated) = stderr
        .await
        .map_err(|e| DockerCliError::Operation(e.to_string()))?
        .map_err(|e| DockerCliError::Operation(format!("read Docker stderr: {e}")))?;
    Ok(BoundedOutput {
        exit_code: status.and_then(|s| s.code()),
        stdout,
        stderr,
        stdout_truncated,
        stderr_truncated,
        termination,
        cleanup: CleanupOutcome::not_required(),
    })
}
async fn read_bounded<R: tokio::io::AsyncRead + Unpin>(
    mut reader: R,
    max: usize,
) -> std::io::Result<(Vec<u8>, bool)> {
    let mut kept = Vec::with_capacity(max.min(8192));
    let mut truncated = false;
    let mut buf = [0u8; 8192];
    loop {
        match reader.read(&mut buf).await? {
            0 => break,
            n => {
                let count = n.min(max.saturating_sub(kept.len()));
                kept.extend_from_slice(&buf[..count]);
                truncated |= count < n;
            }
        }
    }
    Ok((kept, truncated))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::ExecutionRepository;

    async fn test_storage(root: &Path, execution_id: Uuid) -> SqliteStorage {
        use crate::storage::{ExecutionIntent, ExecutionRepository, PersistedExecutionConfig};
        let storage = SqliteStorage::open(root.join("state.sqlite"))
            .await
            .unwrap();
        storage
            .executions()
            .create_execution(ExecutionIntent {
                id: execution_id,
                description: "docker sandbox test".into(),
                metadata: serde_json::json!({}),
                repository_identity: "file:///docker-test".into(),
                repository_root: root.to_path_buf(),
                repository_revision: "test-revision".into(),
                working_directory: root.to_path_buf(),
                config: PersistedExecutionConfig {
                    timeout_seconds: 30,
                    max_recovery_attempts: 0,
                    memory_bytes: 1024 * 1024,
                    cpu_millis: 1000,
                    process_limit: 16,
                    output_limit_bytes: 4096,
                },
                required_verification: vec![],
            })
            .await
            .unwrap();
        storage
    }

    fn mock_body(log: &Path, id: &str, create_action: &str, start_action: &str) -> String {
        mock_body_with_rm(log, id, create_action, start_action, "exit 0")
    }

    fn mock_body_with_rm(
        log: &Path,
        id: &str,
        create_action: &str,
        start_action: &str,
        rm_action: &str,
    ) -> String {
        format!(
            r#"#!/bin/sh
printf '%s\n' "$*" >> '{log}'
case "$1" in
 create)
   shift; name= token= execid= managed=
   while [ $# -gt 0 ]; do
     case "$1" in
       --name) name="$2"; shift 2 ;;
       --label=com.synroute.execution-id=*) execid="${{1##*=}}"; shift ;;
       --label=com.synroute.container-token=*) token="${{1##*=}}"; shift ;;
       --label=com.synroute.managed=*) managed="${{1##*=}}"; shift ;;
       *) shift ;;
     esac
   done
   printf '/%s|%s|%s|%s\n' "$name" "$managed" "$execid" "$token" > "$0.identity"
   echo '{id}' > "$0.cid"
   printf '{id}\n'
   {create_action}
   ;;
 inspect) cat "$0.identity" ;;
 ps) id=$(cat "$0.cid" 2>/dev/null); name=; for arg in "$@"; do case "$arg" in name=*) name=${{arg#name=}} ;; esac; done; printf '%s|%s\n' "$id" "$name" ;;
 start) {start_action} ;;
 rm) {rm_action} ;;
esac
"#,
            log = log.display(),
            id = id,
            create_action = create_action,
            start_action = start_action,
            rm_action = rm_action
        )
    }
    #[test]
    fn validates_non_root_config_resources_and_argv() {
        let mut config = SandboxConfig {
            execution_id: Uuid::new_v4(),
            workspace: ".".into(),
            image: "image:tag".into(),
            uid: 1000,
            gid: 1000,
            environment: Default::default(),
            limits: ResourceLimits {
                timeout: Duration::from_secs(1),
                memory_bytes: 1024,
                cpu_count: 1.0,
                pids: 2,
                stdout_bytes: 10,
                stderr_bytes: 10,
            },
        };
        assert!(validate_config(&config).is_ok());
        let mut excessive_output = config.limits.clone();
        excessive_output.stdout_bytes = usize::MAX;
        assert!(excessive_output.validate().is_err());
        config.uid = 0;
        assert!(validate_config(&config).is_err());
        config.uid = 1000;
        config.environment.insert("BAD=KEY".into(), "x".into());
        assert!(validate_config(&config).is_err());
        assert!(
            validate_command(&SandboxCommand {
                executable: "-x".into(),
                args: vec![]
            })
            .is_err()
        );
    }

    #[test]
    fn workspace_and_tmpfs_mounts_use_their_documented_syntax() {
        assert_eq!(
            workspace_mount_arg("/tmp/work space").unwrap(),
            "--mount=type=bind,src=/tmp/work space,dst=/workspace"
        );
        assert_eq!(
            workspace_mount_arg("/tmp/$literal;semi'quote").unwrap(),
            "--mount=type=bind,src=/tmp/$literal;semi'quote,dst=/workspace"
        );
        assert!(
            !workspace_mount_arg("/tmp/work space")
                .unwrap()
                .ends_with(",rw")
        );
        assert!(workspace_mount_arg("/tmp/a,b").is_err());
        assert!(workspace_mount_arg("/tmp/a\nwork").is_err());
        assert_eq!(
            tmpfs_mount_arg(),
            "--tmpfs=/tmp:rw,nosuid,nodev,noexec,size=67108864"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn bounded_reader_records_truncation_and_run_honors_timeout_and_cancel() {
        let out = run_bounded(
            Path::new("/bin/sh"),
            &["-c".into(), "printf 123456789".into()],
            Duration::from_secs(1),
            4,
            10,
            watch::channel(false).1,
        )
        .await
        .unwrap();
        assert_eq!(out.stdout, b"1234");
        assert!(out.stdout_truncated);
        let timed = run_bounded(
            Path::new("/bin/sh"),
            &["-c".into(), "sleep 3".into()],
            Duration::from_millis(30),
            10,
            10,
            watch::channel(false).1,
        )
        .await
        .unwrap();
        assert_eq!(timed.termination, Termination::TimedOut);
        let (tx, rx) = watch::channel(false);
        let task = tokio::spawn(async {
            run_bounded(
                Path::new("/bin/sh"),
                &["-c".into(), "sleep 5".into()],
                Duration::from_secs(10),
                10,
                10,
                rx,
            )
            .await
            .unwrap()
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        tx.send(true).unwrap();
        assert_eq!(task.await.unwrap().termination, Termination::Cancelled);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn mock_docker_receives_security_limits_and_only_owned_cleanup() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let script = root.path().join("docker-mock");
        let log = root.path().join("calls.log");
        let execution = Uuid::new_v4();
        let id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let body = mock_body(&log, id, ":", "echo mock-output");
        std::fs::write(&script, body).unwrap();
        let mut permissions = std::fs::metadata(&script).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&script, permissions).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let limits = ResourceLimits {
            timeout: Duration::from_secs(1),
            memory_bytes: 64 * 1024 * 1024,
            cpu_count: 0.5,
            pids: 12,
            stdout_bytes: 20,
            stderr_bytes: 30,
        };
        let config = SandboxConfig {
            execution_id: execution,
            workspace: workspace.path().into(),
            image: "image:fixed".into(),
            uid: 1001,
            gid: 1002,
            environment: [("TASK_MODE".into(), "verify".into())].into(),
            limits: limits.clone(),
        };
        let storage = test_storage(root.path(), execution).await;
        let mut session = DockerSandbox::new(script.clone())
            .with_storage(storage)
            .create(config)
            .await
            .unwrap();
        let result = session
            .execute(
                SandboxCommand {
                    executable: "cargo".into(),
                    args: vec!["test".into()],
                },
                limits,
                watch::channel(false).1,
            )
            .await
            .unwrap();
        assert_eq!(result.stdout, b"mock-output\n");
        assert!(result.cleanup.is_successful(), "{:?}", result.cleanup);
        let calls = std::fs::read_to_string(log).unwrap();
        assert!(calls.contains("--network=none"));
        assert!(calls.contains("--cap-drop=ALL"));
        assert!(calls.contains("--security-opt=no-new-privileges=true"));
        assert!(calls.contains("--read-only"));
        assert!(calls.contains("--memory=67108864"));
        assert!(calls.contains("--cpus=0.5"));
        assert!(calls.contains("--pids-limit=12"));
        assert!(calls.contains("--user=1001:1002"));
        assert!(calls.contains("TASK_MODE=verify"));
        assert!(calls.contains("--mount=type=bind,src="));
        assert!(calls.contains("dst=/workspace"));
        assert!(calls.contains("--tmpfs=/tmp:rw,nosuid,nodev,noexec,size=67108864"));
        assert!(!calls.contains("docker.sock"));
        assert!(!calls.contains("--privileged"));
        assert!(calls.contains(&format!("rm --volumes {id}")), "{calls}");
        assert!(!calls.contains(&format!("rm --force --volumes {id}")));
        session.destroy().await.unwrap();
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn definite_create_failure_does_not_attempt_inspection_or_cleanup() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let script = root.path().join("docker-mock");
        let log = root.path().join("calls.log");
        std::fs::write(&script, format!("#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\nif [ \"$1\" = create ]; then echo 'invalid field rw must be a key=value pair' >&2; exit 1; fi\n", log.display())).unwrap();
        let mut permissions = std::fs::metadata(&script).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&script, permissions).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let id = Uuid::new_v4();
        let limits = ResourceLimits {
            timeout: Duration::from_secs(1),
            memory_bytes: 1024,
            cpu_count: 1.0,
            pids: 4,
            stdout_bytes: 100,
            stderr_bytes: 100,
        };
        let config = SandboxConfig {
            execution_id: id,
            workspace: workspace.path().into(),
            image: "fixture".into(),
            uid: 1001,
            gid: 1001,
            environment: Default::default(),
            limits: limits.clone(),
        };
        let storage = test_storage(root.path(), id).await;
        let mut session = DockerSandbox::new(script)
            .with_storage(storage)
            .create(config)
            .await
            .unwrap();
        let result = session
            .execute(
                SandboxCommand {
                    executable: "sh".into(),
                    args: vec![],
                },
                limits,
                watch::channel(false).1,
            )
            .await
            .unwrap();
        assert!(
            result
                .execution_error
                .as_deref()
                .unwrap()
                .contains("invalid field rw")
        );
        assert_eq!(result.termination, Termination::Exited);
        assert_eq!(result.exit_code, Some(1));
        let calls = std::fs::read_to_string(log).unwrap();
        assert_eq!(calls.lines().count(), 1);
        assert!(calls.lines().next().unwrap().starts_with("create "));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn ambiguous_creation_inspects_labels_and_only_removes_verified_owner() {
        use std::os::unix::fs::PermissionsExt;
        for mismatched_labels in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let script = root.path().join("docker-mock");
            let log = root.path().join("calls.log");
            let execution = Uuid::new_v4();
            let create_action = if mismatched_labels {
                "sed -i 's/|true|/|false|/' \"$0.identity\"; echo 'connection reset by peer' >&2; exit 1"
            } else {
                "echo 'connection reset by peer' >&2; exit 1"
            };
            let body = mock_body(
                &log,
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                create_action,
                ":",
            );
            std::fs::write(&script, body).unwrap();
            let mut permissions = std::fs::metadata(&script).unwrap().permissions();
            permissions.set_mode(0o700);
            std::fs::set_permissions(&script, permissions).unwrap();
            let workspace = tempfile::tempdir().unwrap();
            let limits = ResourceLimits {
                timeout: Duration::from_secs(1),
                memory_bytes: 1024,
                cpu_count: 1.0,
                pids: 4,
                stdout_bytes: 100,
                stderr_bytes: 100,
            };
            let config = SandboxConfig {
                execution_id: execution,
                workspace: workspace.path().into(),
                image: "fixture".into(),
                uid: 1001,
                gid: 1001,
                environment: Default::default(),
                limits: limits.clone(),
            };
            let storage = test_storage(root.path(), execution).await;
            let mut session = DockerSandbox::new(script)
                .with_storage(storage)
                .create(config)
                .await
                .unwrap();
            let result = session
                .execute(
                    SandboxCommand {
                        executable: "task".into(),
                        args: vec![],
                    },
                    limits,
                    watch::channel(false).1,
                )
                .await
                .unwrap();
            assert!(
                result
                    .execution_error
                    .as_deref()
                    .unwrap()
                    .contains("connection reset")
            );
            assert_eq!(
                result.cleanup.status == CleanupStatus::OwnershipUncertain,
                mismatched_labels
            );
            let calls = std::fs::read_to_string(log).unwrap();
            assert!(calls.contains("ps --all --filter name=synroute-"));
            assert!(calls.contains("inspect --format"), "{calls}");
            assert_eq!(calls.contains("rm --force"), !mismatched_labels);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn daemon_unavailability_is_preserved_without_claiming_a_container_exists() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let script = root.path().join("docker-mock");
        let log = root.path().join("calls.log");
        let execution = Uuid::new_v4();
        std::fs::write(&script, format!("#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\nif [ \"$1\" = create ]; then echo 'Cannot connect to the Docker daemon' >&2; exit 1; fi\n", log.display())).unwrap();
        let mut permissions = std::fs::metadata(&script).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&script, permissions).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let limits = ResourceLimits {
            timeout: Duration::from_secs(1),
            memory_bytes: 1024,
            cpu_count: 1.0,
            pids: 4,
            stdout_bytes: 32,
            stderr_bytes: 128,
        };
        let config = SandboxConfig {
            execution_id: execution,
            workspace: workspace.path().into(),
            image: "fixture".into(),
            uid: 1001,
            gid: 1001,
            environment: Default::default(),
            limits: limits.clone(),
        };
        let storage = test_storage(root.path(), execution).await;
        let mut session = DockerSandbox::new(script)
            .with_storage(storage)
            .create(config)
            .await
            .unwrap();
        let result = session
            .execute(
                SandboxCommand {
                    executable: "test".into(),
                    args: vec![],
                },
                limits,
                watch::channel(false).1,
            )
            .await
            .unwrap();
        assert!(
            result
                .execution_error
                .as_deref()
                .unwrap()
                .contains("Docker daemon")
        );
        assert_eq!(result.exit_code, Some(1));
        assert_eq!(result.cleanup.status, CleanupStatus::NotRequired);
        let calls = std::fs::read_to_string(log).unwrap();
        assert_eq!(calls.lines().count(), 1, "{calls}");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_ambiguous_create_inspection_is_reported_without_removal() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let script = root.path().join("docker-mock");
        let log = root.path().join("calls.log");
        let execution = Uuid::new_v4();
        let id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let body = mock_body(&log, id, "echo 'connection reset by peer' >&2; exit 1", ":").replace(
            "inspect) cat \"$0.identity\" ;;",
            "inspect) echo inspect-failed >&2; exit 1 ;;",
        );
        std::fs::write(&script, body).unwrap();
        let mut permissions = std::fs::metadata(&script).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&script, permissions).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let limits = ResourceLimits {
            timeout: Duration::from_secs(1),
            memory_bytes: 1024,
            cpu_count: 1.0,
            pids: 4,
            stdout_bytes: 32,
            stderr_bytes: 128,
        };
        let config = SandboxConfig {
            execution_id: execution,
            workspace: workspace.path().into(),
            image: "fixture".into(),
            uid: 1001,
            gid: 1001,
            environment: Default::default(),
            limits: limits.clone(),
        };
        let storage = test_storage(root.path(), execution).await;
        let mut session = DockerSandbox::new(script)
            .with_storage(storage.clone())
            .create(config)
            .await
            .unwrap();
        let result = session
            .execute(
                SandboxCommand {
                    executable: "test".into(),
                    args: vec![],
                },
                limits,
                watch::channel(false).1,
            )
            .await
            .unwrap();
        assert!(
            result
                .execution_error
                .as_deref()
                .unwrap()
                .contains("connection reset")
        );
        assert_eq!(result.cleanup.status, CleanupStatus::OwnershipUncertain);
        assert!(result.cleanup.inspection_error.is_some());
        assert!(result.cleanup.cleanup_error.is_none());
        let calls = std::fs::read_to_string(log).unwrap();
        assert!(
            !calls.contains("rm --"),
            "unverified container must not be removed: {calls}"
        );
        assert_eq!(
            storage
                .containers()
                .unresolved_intents()
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn mock_docker_force_removes_only_owned_container_after_cancellation() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let script = root.path().join("docker-mock");
        let log = root.path().join("calls.log");
        let execution = Uuid::new_v4();
        let id = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        let body = mock_body(&log, id, ":", "exec /bin/sleep 10");
        std::fs::write(&script, body).unwrap();
        let mut permissions = std::fs::metadata(&script).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&script, permissions).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let limits = ResourceLimits {
            timeout: Duration::from_secs(5),
            memory_bytes: 1024 * 1024,
            cpu_count: 1.0,
            pids: 4,
            stdout_bytes: 10,
            stderr_bytes: 10,
        };
        let config = SandboxConfig {
            execution_id: execution,
            workspace: workspace.path().into(),
            image: "fixture".into(),
            uid: 1001,
            gid: 1001,
            environment: Default::default(),
            limits: limits.clone(),
        };
        let storage = test_storage(root.path(), execution).await;
        let mut session = DockerSandbox::new(script)
            .with_storage(storage)
            .create(config)
            .await
            .unwrap();
        let (tx, rx) = watch::channel(false);
        let execution_task = tokio::spawn(async move {
            session
                .execute(
                    SandboxCommand {
                        executable: "task".into(),
                        args: vec![],
                    },
                    limits,
                    rx,
                )
                .await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        let _ = tx.send(true);
        let result = execution_task.await.unwrap().unwrap();
        assert_eq!(result.termination, Termination::Cancelled);
        let calls = std::fs::read_to_string(log).unwrap();
        assert!(calls.contains(&format!("rm --force --volumes {id}")));
        assert!(!calls.contains("rm --force unrelated"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn command_failure_and_cleanup_failure_are_reported_independently() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let script = root.path().join("docker-mock");
        let log = root.path().join("calls.log");
        let execution = Uuid::new_v4();
        let id = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        std::fs::write(
            &script,
            mock_body_with_rm(
                &log,
                id,
                ":",
                "echo test-failure >&2; exit 7",
                "echo cleanup-failure >&2; exit 1",
            ),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&script).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&script, permissions).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let limits = ResourceLimits {
            timeout: Duration::from_secs(1),
            memory_bytes: 1024,
            cpu_count: 1.0,
            pids: 4,
            stdout_bytes: 128,
            stderr_bytes: 128,
        };
        let config = SandboxConfig {
            execution_id: execution,
            workspace: workspace.path().into(),
            image: "fixture".into(),
            uid: 1001,
            gid: 1001,
            environment: Default::default(),
            limits: limits.clone(),
        };
        let storage = test_storage(root.path(), execution).await;
        let mut session = DockerSandbox::new(script)
            .with_storage(storage.clone())
            .create(config)
            .await
            .unwrap();
        let result = session
            .execute(
                SandboxCommand {
                    executable: "test".into(),
                    args: vec![],
                },
                limits,
                watch::channel(false).1,
            )
            .await
            .unwrap();
        assert_eq!(result.termination, Termination::Exited);
        assert_eq!(result.exit_code, Some(7));
        assert!(String::from_utf8_lossy(&result.stderr).contains("test-failure"));
        assert_eq!(result.cleanup.status, CleanupStatus::Failed);
        assert!(result.cleanup.container_may_remain);
        let report = storage.executions().report(execution).await.unwrap();
        assert_eq!(report.container_diagnostics.len(), 1);
        assert_eq!(report.container_intents.len(), 1);
        assert_eq!(
            report.container_intents[0].status,
            crate::storage::ContainerIntentStatus::OrphanSuspected
        );
        assert!(
            !report.container_diagnostics[0]
                .detail
                .contains("test-failure")
        );
        let startup = crate::recovery::RecoveryService::new(storage.clone())
            .inspect_startup(None, chrono::Utc::now())
            .await
            .unwrap();
        assert!(startup.requires_attention());
        assert_eq!(startup.unresolved_container_intents().len(), 1);
        let reopened = SqliteStorage::open(root.path().join("state.sqlite"))
            .await
            .unwrap();
        assert_eq!(
            reopened
                .containers()
                .unresolved_intents()
                .await
                .unwrap()
                .len(),
            1
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn failed_command_with_successful_cleanup_keeps_exit_evidence() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let script = root.path().join("docker-mock");
        let log = root.path().join("calls.log");
        let execution = Uuid::new_v4();
        let id = "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321";
        std::fs::write(
            &script,
            mock_body(&log, id, ":", "echo regression >&2; exit 3"),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&script).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&script, permissions).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let limits = ResourceLimits {
            timeout: Duration::from_secs(1),
            memory_bytes: 1024,
            cpu_count: 1.0,
            pids: 4,
            stdout_bytes: 64,
            stderr_bytes: 64,
        };
        let config = SandboxConfig {
            execution_id: execution,
            workspace: workspace.path().into(),
            image: "fixture".into(),
            uid: 1001,
            gid: 1001,
            environment: Default::default(),
            limits: limits.clone(),
        };
        let storage = test_storage(root.path(), execution).await;
        let mut session = DockerSandbox::new(script)
            .with_storage(storage)
            .create(config)
            .await
            .unwrap();
        let result = session
            .execute(
                SandboxCommand {
                    executable: "test".into(),
                    args: vec![],
                },
                limits,
                watch::channel(false).1,
            )
            .await
            .unwrap();
        assert_eq!(result.termination, Termination::Exited);
        assert_eq!(result.exit_code, Some(3));
        assert!(String::from_utf8_lossy(&result.stderr).contains("regression"));
        assert!(result.cleanup.is_successful());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn timeout_and_cancellation_survive_cleanup_failure() {
        use std::os::unix::fs::PermissionsExt;
        for cancel in [false, true] {
            let root = tempfile::tempdir().unwrap();
            let script = root.path().join("docker-mock");
            let log = root.path().join("calls.log");
            let execution = Uuid::new_v4();
            let id = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
            std::fs::write(
                &script,
                mock_body_with_rm(&log, id, ":", "exec /bin/sleep 5", "exit 1"),
            )
            .unwrap();
            let mut permissions = std::fs::metadata(&script).unwrap().permissions();
            permissions.set_mode(0o700);
            std::fs::set_permissions(&script, permissions).unwrap();
            let workspace = tempfile::tempdir().unwrap();
            let limits = ResourceLimits {
                timeout: Duration::from_millis(70),
                memory_bytes: 1024,
                cpu_count: 1.0,
                pids: 4,
                stdout_bytes: 32,
                stderr_bytes: 32,
            };
            let config = SandboxConfig {
                execution_id: execution,
                workspace: workspace.path().into(),
                image: "fixture".into(),
                uid: 1001,
                gid: 1001,
                environment: Default::default(),
                limits: limits.clone(),
            };
            let storage = test_storage(root.path(), execution).await;
            let mut session = DockerSandbox::new(script)
                .with_storage(storage)
                .create(config)
                .await
                .unwrap();
            let (tx, rx) = watch::channel(false);
            let task = tokio::spawn(async move {
                session
                    .execute(
                        SandboxCommand {
                            executable: "test".into(),
                            args: vec![],
                        },
                        limits,
                        rx,
                    )
                    .await
                    .unwrap()
            });
            if cancel {
                tokio::time::sleep(Duration::from_millis(20)).await;
                let _ = tx.send(true);
            }
            let result = task.await.unwrap();
            assert_eq!(
                result.termination,
                if cancel {
                    Termination::Cancelled
                } else {
                    Termination::TimedOut
                }
            );
            assert_eq!(result.cleanup.status, CleanupStatus::Failed);
            assert!(result.cleanup.container_may_remain);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn creation_cancellation_is_forwarded_and_owned_candidate_is_removed() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let script = root.path().join("docker-mock");
        let log = root.path().join("calls.log");
        let execution = Uuid::new_v4();
        let id = "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321";
        std::fs::write(&script, mock_body(&log, id, "exec /bin/sleep 5", ":")).unwrap();
        let mut permissions = std::fs::metadata(&script).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&script, permissions).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let limits = ResourceLimits {
            timeout: Duration::from_secs(2),
            memory_bytes: 1024,
            cpu_count: 1.0,
            pids: 4,
            stdout_bytes: 32,
            stderr_bytes: 32,
        };
        let config = SandboxConfig {
            execution_id: execution,
            workspace: workspace.path().into(),
            image: "fixture".into(),
            uid: 1001,
            gid: 1001,
            environment: Default::default(),
            limits: limits.clone(),
        };
        let storage = test_storage(root.path(), execution).await;
        let mut session = DockerSandbox::new(script.clone())
            .with_storage(storage)
            .create(config)
            .await
            .unwrap();
        let (tx, rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            session
                .execute(
                    SandboxCommand {
                        executable: "test".into(),
                        args: vec![],
                    },
                    limits,
                    rx,
                )
                .await
                .unwrap()
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        let _ = tx.send(true);
        let result = task.await.unwrap();
        assert_eq!(result.termination, Termination::Cancelled);
        assert!(result.cleanup.is_successful());
        let calls = std::fs::read_to_string(log).unwrap();
        assert!(calls.contains("inspect --format"));
        assert!(calls.contains(&format!("rm --force --volumes {id}")));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn creation_and_start_share_one_absolute_deadline() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let script = root.path().join("docker-mock");
        let log = root.path().join("calls.log");
        let execution = Uuid::new_v4();
        let id = "fedcba0987654321fedcba0987654321fedcba0987654321fedcba0987654321";
        std::fs::write(
            &script,
            mock_body(&log, id, "sleep 0.05", "exec /bin/sleep 5"),
        )
        .unwrap();
        let mut permissions = std::fs::metadata(&script).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&script, permissions).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let limits = ResourceLimits {
            timeout: Duration::from_millis(90),
            memory_bytes: 1024,
            cpu_count: 1.0,
            pids: 4,
            stdout_bytes: 32,
            stderr_bytes: 32,
        };
        let config = SandboxConfig {
            execution_id: execution,
            workspace: workspace.path().into(),
            image: "fixture".into(),
            uid: 1001,
            gid: 1001,
            environment: Default::default(),
            limits: limits.clone(),
        };
        let storage = test_storage(root.path(), execution).await;
        let mut session = DockerSandbox::new(script)
            .with_storage(storage)
            .create(config)
            .await
            .unwrap();
        let started = Instant::now();
        let result = session
            .execute(
                SandboxCommand {
                    executable: "test".into(),
                    args: vec![],
                },
                limits,
                watch::channel(false).1,
            )
            .await
            .unwrap();
        assert_eq!(result.termination, Termination::TimedOut);
        assert!(
            started.elapsed() < Duration::from_millis(300),
            "deadline was reset: {:?}",
            started.elapsed()
        );
        assert!(result.cleanup.is_successful());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cleanup_inspection_timeout_is_bounded_and_preserves_primary_success() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let script = root.path().join("docker-mock");
        let log = root.path().join("calls.log");
        let execution = Uuid::new_v4();
        let id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let body = mock_body(&log, id, ":", "echo successful-task").replace(
            "inspect) cat \"$0.identity\" ;;",
            "inspect) exec /bin/sleep 5 ;;",
        );
        std::fs::write(&script, body).unwrap();
        let mut permissions = std::fs::metadata(&script).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&script, permissions).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let limits = ResourceLimits {
            timeout: Duration::from_secs(1),
            memory_bytes: 1024,
            cpu_count: 1.0,
            pids: 4,
            stdout_bytes: 64,
            stderr_bytes: 64,
        };
        let config = SandboxConfig {
            execution_id: execution,
            workspace: workspace.path().into(),
            image: "fixture".into(),
            uid: 1001,
            gid: 1001,
            environment: Default::default(),
            limits: limits.clone(),
        };
        let storage = test_storage(root.path(), execution).await;
        let mut session = DockerSandbox::new(script)
            .with_storage(storage)
            .with_cleanup_grace(Duration::from_millis(30))
            .create(config)
            .await
            .unwrap();
        let started = Instant::now();
        let result = session
            .execute(
                SandboxCommand {
                    executable: "test".into(),
                    args: vec![],
                },
                limits,
                watch::channel(false).1,
            )
            .await
            .unwrap();
        assert_eq!(result.termination, Termination::Exited);
        assert_eq!(result.exit_code, Some(0));
        assert_eq!(result.cleanup.status, CleanupStatus::OwnershipUncertain);
        assert!(result.cleanup.container_may_remain);
        assert!(result.cleanup.inspection_error.is_some());
        assert!(started.elapsed() < Duration::from_millis(300));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cleans_up_by_verified_generated_name_when_create_times_out() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let script = root.path().join("docker-mock");
        let log = root.path().join("calls.log");
        let execution = Uuid::new_v4();
        let id = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        let body = mock_body(&log, id, "exec /bin/sleep 5", ":");
        std::fs::write(&script, body).unwrap();
        let mut permissions = std::fs::metadata(&script).unwrap().permissions();
        permissions.set_mode(0o700);
        std::fs::set_permissions(&script, permissions).unwrap();
        let workspace = tempfile::tempdir().unwrap();
        let limits = ResourceLimits {
            timeout: Duration::from_millis(30),
            memory_bytes: 1024,
            cpu_count: 1.0,
            pids: 4,
            stdout_bytes: 32,
            stderr_bytes: 32,
        };
        let config = SandboxConfig {
            execution_id: execution,
            workspace: workspace.path().into(),
            image: "fixture".into(),
            uid: 1001,
            gid: 1001,
            environment: Default::default(),
            limits: limits.clone(),
        };
        let storage = test_storage(root.path(), execution).await;
        let mut session = DockerSandbox::new(script)
            .with_storage(storage)
            .create(config)
            .await
            .unwrap();
        let result = session
            .execute(
                SandboxCommand {
                    executable: "task".into(),
                    args: vec![],
                },
                limits,
                watch::channel(false).1,
            )
            .await
            .unwrap();
        assert_eq!(result.termination, Termination::TimedOut);
        assert!(
            result
                .execution_error
                .as_deref()
                .unwrap()
                .contains("TimedOut")
        );
        let calls = std::fs::read_to_string(log).unwrap();
        assert!(calls.contains("inspect --format"), "{calls}");
        assert!(calls.contains(&format!("rm --force --volumes {id}")));
        assert!(valid_container_reference(
            &format!("synroute-{execution}-{}", Uuid::new_v4()),
            execution
        ));
        assert!(!valid_container_reference("unrelated", execution));
    }

    /// Opt in using `SYNROUTE_DEMO_DOCKER_IMAGE=... cargo test docker_integration -- --ignored --nocapture`.
    #[tokio::test]
    #[ignore = "requires explicitly configured Docker daemon and image"]
    async fn docker_integration_runs_demo_pytest_fixture_with_pinned_image() {
        let image = std::env::var("SYNROUTE_DEMO_DOCKER_IMAGE")
            .unwrap_or_else(|_| "synroute-demo-python:3.12.8-pytest-8.3.5".into());
        let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("examples/demo-project");
        let limits = ResourceLimits {
            timeout: Duration::from_secs(60),
            memory_bytes: 512 * 1024 * 1024,
            cpu_count: 1.0,
            pids: 32,
            stdout_bytes: 1024,
            stderr_bytes: 1024,
        };
        let config = SandboxConfig {
            execution_id: Uuid::new_v4(),
            workspace,
            image,
            uid: 1000,
            gid: 1000,
            environment: [("PYTHONDONTWRITEBYTECODE".into(), "1".into())].into(),
            limits: limits.clone(),
        };
        let root = tempfile::tempdir().unwrap();
        let storage = test_storage(root.path(), config.execution_id).await;
        let mut session = DockerSandbox::default()
            .with_storage(storage)
            .create(config)
            .await
            .unwrap();
        let output = session
            .execute(
                SandboxCommand {
                    executable: "python3".into(),
                    args: vec![
                        "-m".into(),
                        "pytest".into(),
                        "-q".into(),
                        "-p".into(),
                        "no:cacheprovider".into(),
                    ],
                },
                limits,
                watch::channel(false).1,
            )
            .await
            .unwrap();
        assert!(
            String::from_utf8_lossy(&output.stdout).contains("1 passed"),
            "Docker execution result: {output:?}"
        );
        assert_eq!(output.exit_code, Some(0));
        session.destroy().await.unwrap();
    }
}
