use super::{
    ExecutionResult, ResourceLimits, SandboxCommand, SandboxConfig, SandboxError, SandboxProvider,
    SandboxSession, Termination,
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

struct DockerSession {
    docker: PathBuf,
    config: SandboxConfig,
    workspace: PathBuf,
    containers: Vec<String>,
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
        let id = self.create_container(&command, &limits).await?;
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
                    Ok(()) => Err(error),
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
    ) -> Result<String, SandboxError> {
        let workspace = self
            .workspace
            .to_str()
            .ok_or_else(|| SandboxError::InvalidConfig("workspace path is not UTF-8".into()))?;
        if workspace.contains(',') {
            return Err(SandboxError::InvalidConfig(
                "Docker mount paths containing commas are unsupported".into(),
            ));
        }
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
            format!("--mount=type=bind,src={workspace},dst=/workspace,rw"),
            "--tmpfs=/tmp:rw,nosuid,nodev,noexec,size=67108864".into(),
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
        let output = match run_capture(
            &self.docker,
            &args,
            limits.timeout.min(Duration::from_secs(30)),
        )
        .await
        {
            Ok(output) => output,
            Err(error) => {
                return match self.remove_owned(&name, true).await {
                    Ok(()) => Err(error),
                    Err(cleanup) => Err(SandboxError::Ownership(format!(
                        "docker create failed ({error}); possible container cleanup failed ({cleanup})"
                    ))),
                };
            }
        };
        if output.termination != Termination::Exited || output.exit_code != Some(0) {
            let error = SandboxError::Provider(format!(
                "docker create failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
            return match self.remove_owned(&name, true).await {
                Ok(()) => Err(error),
                Err(cleanup) => Err(SandboxError::Ownership(format!(
                    "{error}; possible container cleanup failed ({cleanup})"
                ))),
            };
        }
        let id = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        if !valid_id(&id) {
            return Err(SandboxError::Provider(
                "docker create returned invalid container ID".into(),
            ));
        }
        Ok(id)
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
        let format = format!(
            "{{{{ index .Config.Labels \"{MANAGED}\" }}}}|{{{{ index .Config.Labels \"{EXECUTION}\" }}}}"
        );
        let output = run_capture(
            &self.docker,
            &["inspect".into(), "--format".into(), format, id.into()],
            Duration::from_secs(10),
        )
        .await?;
        Ok(output.exit_code == Some(0)
            && String::from_utf8_lossy(&output.stdout).trim()
                == format!("true|{}", self.config.execution_id))
    }
}

fn valid_id(value: &str) -> bool {
    (12..=64).contains(&value.len()) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
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
}
async fn run_bounded(
    docker: &Path,
    args: &[String],
    limit: Duration,
    out_max: usize,
    err_max: usize,
    mut cancellation: watch::Receiver<bool>,
) -> Result<BoundedOutput, SandboxError> {
    let mut child = Command::new(docker)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| SandboxError::Provider(format!("start Docker CLI: {e}")))?;
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
                    Ok(Err(error)) => return Err(SandboxError::Provider(format!("wait for Docker CLI: {error}"))),
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
        .map_err(|e| SandboxError::Provider(e.to_string()))?
        .map_err(|e| SandboxError::Provider(format!("read Docker stdout: {e}")))?;
    let (stderr, stderr_truncated) = stderr
        .await
        .map_err(|e| SandboxError::Provider(e.to_string()))?
        .map_err(|e| SandboxError::Provider(format!("read Docker stderr: {e}")))?;
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
        assert!(!calls.contains("docker.sock"));
        assert!(!calls.contains("--privileged"));
        assert!(calls.contains(&format!("rm --volumes {id}")));
        assert!(!calls.contains(&format!("rm --force --volumes {id}")));
        session.destroy().await.unwrap();
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
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\ncase \"$1\" in\n create) echo {id}; exec /bin/sleep 5 ;;\n inspect) echo 'true|{execution}' ;;\n rm) exit 0 ;;\nesac\n",
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
        assert!(error.to_string().contains("docker create failed"));
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
