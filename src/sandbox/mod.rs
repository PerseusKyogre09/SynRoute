use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::PathBuf, time::Duration};
use thiserror::Error;
use tokio::sync::watch;
use uuid::Uuid;

/// Hard ceiling so even a caller-supplied limit cannot make host-side capture unbounded.
pub const MAX_CAPTURE_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceLimits {
    pub timeout: Duration,
    pub memory_bytes: u64,
    pub cpu_count: f64,
    pub pids: u64,
    pub stdout_bytes: usize,
    pub stderr_bytes: usize,
}

impl ResourceLimits {
    pub fn validate(&self) -> Result<(), SandboxError> {
        if self.timeout.is_zero()
            || self.memory_bytes == 0
            || !self.cpu_count.is_finite()
            || self.cpu_count <= 0.0
            || self.pids == 0
            || self.stdout_bytes > MAX_CAPTURE_BYTES
            || self.stderr_bytes > MAX_CAPTURE_BYTES
        {
            return Err(SandboxError::InvalidConfig(
                "resource limits must be positive and output caps must not exceed 16 MiB per stream".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxConfig {
    pub execution_id: Uuid,
    pub workspace: PathBuf,
    pub image: String,
    /// Numeric, non-root UID:GID inside the container.
    pub uid: u32,
    pub gid: u32,
    /// Explicit allowlist; host environment variables are never inherited.
    pub environment: BTreeMap<String, String>,
    pub limits: ResourceLimits,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandboxCommand {
    pub executable: String,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Termination {
    Exited,
    TimedOut,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionResult {
    pub execution_id: Uuid,
    pub exit_code: Option<i32>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
    pub termination: Termination,
    pub duration: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeContainerPresence {
    pub id: String,
    pub name: String,
    pub state: String,
    /// This validates Synroute labels only; it does not prove who created the container.
    pub ownership_labels_verified: bool,
}

#[derive(Debug, Error)]
pub enum SandboxError {
    #[error("sandbox configuration is invalid: {0}")]
    InvalidConfig(String),
    #[error("sandbox provider error: {0}")]
    Provider(String),
    #[error("sandbox ownership could not be verified; refusing cleanup: {0}")]
    Ownership(String),
    #[error("Docker container creation failed: {primary}; cleanup result: {cleanup:?}")]
    Creation {
        primary: String,
        cleanup: Option<String>,
    },
}

#[async_trait]
pub trait SandboxProvider: Send + Sync {
    async fn create(&self, config: SandboxConfig) -> Result<Box<dyn SandboxSession>, SandboxError>;
}

#[async_trait]
pub trait SandboxSession: Send {
    async fn execute(
        &mut self,
        command: SandboxCommand,
        limits: ResourceLimits,
        cancellation: watch::Receiver<bool>,
    ) -> Result<ExecutionResult, SandboxError>;
    async fn destroy(&mut self) -> Result<(), SandboxError>;
}

#[async_trait]
pub trait RuntimeInspector: Send + Sync {
    async fn inspect_execution(
        &self,
        execution_id: Uuid,
    ) -> Result<Vec<RuntimeContainerPresence>, SandboxError>;
}

pub mod authorization;
pub mod docker;
