use super::{
    ExecutionResult, ResourceLimits, RuntimeContainerPresence, RuntimeInspector, SandboxCommand,
    SandboxConfig, SandboxError, SandboxProvider, SandboxSession, Termination,
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

#[derive(Debug, Clone)]
pub struct DockerSandbox {
    binary: PathBuf,
}
impl Default for DockerSandbox {
    fn default() -> Self {
        Self {
            binary: "docker".into(),
        }
    }
}
impl DockerSandbox {
    pub fn new(binary: PathBuf) -> Self {
        Self { binary }
    }
}

#[async_trait]
impl SandboxProvider for DockerSandbox {
    async fn create(&self, config: SandboxConfig) -> Result<Box<dyn SandboxSession>, SandboxError> {
        validate_config(&config)?;
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
    containers: Vec<String>,
}

/// Constructed only after `docker create` returns success and a valid container ID.
struct CreatedContainer {
    id: String,
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
        let created = self.create_container(&command, &limits).await?;
        let id = created.id;
        self.containers.push(id.clone());
        let output = match run_bounded(
            &self.docker,
            &["start".into(), "--attach".into(), id.clone()],
            limits.timeout,
            limits.stdout_bytes,
            limits.stderr_bytes,
            cancellation,
        )
        .await
        {
            Ok(out) => out,
            Err(error) => {
                return match self.remove_owned(&id, true).await {
                    Ok(()) => Err(SandboxError::Provider(error.to_string())),
                    Err(cleanup) => Err(SandboxError::Ownership(format!(
                        "{error}; cleanup failed: {cleanup}"
                    ))),
                };
            }
        };
        self.remove_owned(&id, output.termination != Termination::Exited)
            .await?;
        self.containers.retain(|container| container != &id);
        Ok(ExecutionResult {
            execution_id: self.config.execution_id,
            exit_code: output.exit_code,
            stdout: output.stdout,
            stderr: output.stderr,
            stdout_truncated: output.stdout_truncated,
            stderr_truncated: output.stderr_truncated,
            termination: output.termination,
            duration: started.elapsed(),
        })
    }

    async fn destroy(&mut self) -> Result<(), SandboxError> {
        for id in self.containers.clone() {
            self.remove_owned(&id, true).await?;
            self.containers.retain(|entry| entry != &id);
        }
        Ok(())
    }
}

impl DockerSession {
    async fn create_container(
        &self,
        command: &SandboxCommand,
        limits: &ResourceLimits,
    ) -> Result<CreatedContainer, SandboxError> {
        let workspace = self
            .workspace
            .to_str()
            .ok_or_else(|| SandboxError::InvalidConfig("workspace path is not UTF-8".into()))?;
        let name = format!("synroute-{}-{}", self.config.execution_id, Uuid::new_v4());
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
            workspace_mount_arg(workspace)?,
            tmpfs_mount_arg(),
            "--workdir=/workspace".into(),
            format!("--label={MANAGED}=true"),
            format!("--label={EXECUTION}={}", self.config.execution_id),
        ];
        for (key, value) in &self.config.environment {
            validate_env(key, value)?;
            args.extend(["--env".into(), format!("{key}={value}")]);
        }
        args.push(self.config.image.clone());
        args.push(command.executable.clone());
        args.extend(command.args.clone());
        let output = match run_bounded(
            &self.docker,
            &args,
            limits.timeout.min(Duration::from_secs(30)),
            64 * 1024,
            64 * 1024,
            watch::channel(false).1,
        )
        .await
        {
            Err(DockerCliError::NotStarted(error)) => {
                return Err(SandboxError::Creation {
                    primary: error,
                    cleanup: None,
                });
            }
            Err(DockerCliError::Operation(error)) => {
                return Err(self.creation_error(error, &name, true).await);
            }
            Ok(output) => output,
        };
        if output.termination == Termination::Exited && output.exit_code == Some(0) {
            let id = String::from_utf8_lossy(&output.stdout).trim().to_owned();
            if valid_id(&id) {
                return Ok(CreatedContainer { id });
            }
            return Err(self
                .creation_error(
                    "docker create returned an invalid container ID".into(),
                    &name,
                    true,
                )
                .await);
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
        Err(self.creation_error(primary, &name, ambiguous).await)
    }

    async fn creation_error(
        &self,
        primary: String,
        generated_name: &str,
        inspect_possible_container: bool,
    ) -> SandboxError {
        let cleanup = if inspect_possible_container {
            self.cleanup_ambiguous_creation(generated_name)
                .await
                .err()
                .map(|error| error.to_string())
        } else {
            None
        };
        SandboxError::Creation { primary, cleanup }
    }

    async fn cleanup_ambiguous_creation(&self, generated_name: &str) -> Result<(), SandboxError> {
        if !valid_container_reference(generated_name, self.config.execution_id) {
            return Err(SandboxError::Ownership(
                "generated container name failed validation".into(),
            ));
        }
        let args = vec![
            "ps".into(),
            "--all".into(),
            "--filter".into(),
            format!("name={generated_name}"),
            "--format".into(),
            "{{.Names}}".into(),
        ];
        let listing = run_capture(&self.docker, &args, Duration::from_secs(10)).await?;
        if listing.exit_code != Some(0) {
            return Err(SandboxError::Provider(format!(
                "could not inspect ambiguous container creation: {}",
                String::from_utf8_lossy(&listing.stderr).trim()
            )));
        }
        let exists = String::from_utf8_lossy(&listing.stdout)
            .lines()
            .any(|name| name == generated_name);
        if !exists {
            return Ok(());
        }
        self.remove_owned(generated_name, true).await
    }

    async fn remove_owned(&self, reference: &str, force: bool) -> Result<(), SandboxError> {
        if !valid_container_reference(reference, self.config.execution_id)
            || !self.owns_container(reference).await?
        {
            return Err(SandboxError::Ownership(format!(
                "container reference {reference} is invalid or labels do not match this execution"
            )));
        }
        let mut args = vec!["rm".to_owned()];
        if force {
            args.push("--force".into());
        }
        args.push("--volumes".into());
        args.push(reference.into());
        let output = run_capture(&self.docker, &args, Duration::from_secs(15)).await?;
        if output.exit_code != Some(0) {
            return Err(SandboxError::Provider(format!(
                "docker rm failed: {}",
                String::from_utf8_lossy(&output.stderr)
            )));
        }
        Ok(())
    }

    async fn owns_container(&self, id: &str) -> Result<bool, SandboxError> {
        if !valid_container_reference(id, self.config.execution_id) {
            return Ok(false);
        }
        verify_container_labels(&self.docker, id, self.config.execution_id).await
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
}
#[derive(Debug)]
enum DockerCliError {
    NotStarted(String),
    Operation(String),
}
impl std::fmt::Display for DockerCliError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotStarted(error) | Self::Operation(error) => f.write_str(error),
        }
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
async fn run_bounded(
    docker: &Path,
    args: &[String],
    limit: Duration,
    out_max: usize,
    err_max: usize,
    mut cancellation: watch::Receiver<bool>,
) -> Result<BoundedOutput, DockerCliError> {
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
        let wait = timeout(limit, child.wait());
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
        let body = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\ncase \"$1\" in\n create) echo {id} ;;\n start) echo mock-output ;;\n inspect) echo 'true|{execution}' ;;\n rm) exit 0 ;;\nesac\n",
            log.display()
        );
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
        let mut session = DockerSandbox::new(script).create(config).await.unwrap();
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
        assert!(calls.contains(&format!("rm --volumes {id}")));
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
        let mut session = DockerSandbox::new(script).create(config).await.unwrap();
        let error = session
            .execute(
                SandboxCommand {
                    executable: "sh".into(),
                    args: vec![],
                },
                limits,
                watch::channel(false).1,
            )
            .await
            .unwrap_err();
        match error {
            SandboxError::Creation { primary, cleanup } => {
                assert!(primary.contains("invalid field rw"));
                assert_eq!(cleanup, None);
            }
            other => panic!("unexpected error: {other}"),
        }
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
            let labels = if mismatched_labels {
                "false|other"
            } else {
                "true|EXECUTION_ID"
            };
            let labels = labels.replace("EXECUTION_ID", &execution.to_string());
            let body = format!(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\ncase \"$1\" in\n create) echo 'connection reset by peer' >&2; exit 1 ;;\n ps) for arg in \"$@\"; do case \"$arg\" in name=*) printf '%s\\n' \"${{arg#name=}}\" ;; esac; done ;;\n inspect) echo '{}' ;;\n rm) exit 0 ;;\nesac\n",
                log.display(),
                labels
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
            let mut session = DockerSandbox::new(script).create(config).await.unwrap();
            let error = session
                .execute(
                    SandboxCommand {
                        executable: "task".into(),
                        args: vec![],
                    },
                    limits,
                    watch::channel(false).1,
                )
                .await
                .unwrap_err();
            match error {
                SandboxError::Creation { primary, cleanup } => {
                    assert!(primary.contains("connection reset"));
                    assert_eq!(cleanup.is_some(), mismatched_labels);
                    if let Some(error) = cleanup {
                        assert!(error.contains("labels do not match"), "{error}");
                    }
                }
                other => panic!("unexpected error: {other}"),
            }
            let calls = std::fs::read_to_string(log).unwrap();
            assert!(calls.contains("ps --all --filter name=synroute-"));
            assert!(calls.contains("inspect --format"));
            assert_eq!(calls.contains("rm --force"), !mismatched_labels);
        }
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
        let body = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\ncase \"$1\" in\n create) echo {id} ;;\n start) exec /bin/sleep 10 ;;\n inspect) echo 'true|{execution}' ;;\n rm) exit 0 ;;\nesac\n",
            log.display()
        );
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
        let mut session = DockerSandbox::new(script).create(config).await.unwrap();
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
    async fn cleans_up_by_verified_generated_name_when_create_times_out() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let script = root.path().join("docker-mock");
        let log = root.path().join("calls.log");
        let execution = Uuid::new_v4();
        let id = "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef";
        let body = format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\ncase \"$1\" in\n create) echo {id}; exec /bin/sleep 5 ;;\n ps) for arg in \"$@\"; do case \"$arg\" in name=*) printf '%s\\n' \"${{arg#name=}}\" ;; esac; done ;;\n inspect) echo 'true|{execution}' ;;\n rm) exit 0 ;;\nesac\n",
            log.display()
        );
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
        let mut session = DockerSandbox::new(script).create(config).await.unwrap();
        let error = session
            .execute(
                SandboxCommand {
                    executable: "task".into(),
                    args: vec![],
                },
                limits,
                watch::channel(false).1,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, SandboxError::Creation { primary, cleanup: None } if primary.contains("TimedOut"))
        );
        let calls = std::fs::read_to_string(log).unwrap();
        assert!(calls.contains("inspect --format"));
        assert!(calls.contains("rm --force --volumes synroute-"));
        assert!(valid_container_reference(
            &format!("synroute-{execution}-{}", Uuid::new_v4()),
            execution
        ));
        assert!(!valid_container_reference("unrelated", execution));
    }

    /// Opt in using `SYNROUTE_DOCKER_IMAGE=... cargo test docker_integration -- --ignored --nocapture`.
    #[tokio::test]
    #[ignore = "requires explicitly configured Docker daemon and image"]
    async fn docker_integration_executes_and_removes_labeled_container() {
        let image = std::env::var("SYNROUTE_DOCKER_IMAGE").expect("set SYNROUTE_DOCKER_IMAGE");
        let workspace = tempfile::tempdir().unwrap();
        let limits = ResourceLimits {
            timeout: Duration::from_secs(10),
            memory_bytes: 128 * 1024 * 1024,
            cpu_count: 1.0,
            pids: 32,
            stdout_bytes: 1024,
            stderr_bytes: 1024,
        };
        let config = SandboxConfig {
            execution_id: Uuid::new_v4(),
            workspace: workspace.path().into(),
            image,
            uid: 1000,
            gid: 1000,
            environment: Default::default(),
            limits: limits.clone(),
        };
        let mut session = DockerSandbox::default().create(config).await.unwrap();
        let output = session
            .execute(
                SandboxCommand {
                    executable: "sh".into(),
                    args: vec!["-c".into(), "printf sandbox-ok".into()],
                },
                limits,
                watch::channel(false).1,
            )
            .await
            .unwrap();
        assert_eq!(output.stdout, b"sandbox-ok");
        assert_eq!(output.exit_code, Some(0));
        session.destroy().await.unwrap();
    }
}
