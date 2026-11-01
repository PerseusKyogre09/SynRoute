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

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Termination {
    Exited,
    TimedOut,
    Cancelled,
    #[default]
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CleanupStatus {
    NotRequired,
    Succeeded,
    Failed,
    OwnershipUncertain,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CleanupOutcome {
    pub status: CleanupStatus,
    pub container_name: Option<String>,
    pub container_id: Option<String>,
    pub container_may_remain: bool,
    /// Bounded diagnostic text for the caller; durable orphan records use fixed reason codes.
    pub diagnostic: Option<String>,
    pub inspection_error: Option<String>,
    pub cleanup_error: Option<String>,
}

impl Default for CleanupOutcome {
    fn default() -> Self {
        // Legacy evidence has no cleanup proof. Treat it conservatively.
        Self {
            status: CleanupStatus::OwnershipUncertain,
            container_name: None,
            container_id: None,
            container_may_remain: true,
            diagnostic: Some("legacy evidence did not record container cleanup".into()),
            inspection_error: None,
            cleanup_error: None,
        }
    }
}

impl CleanupOutcome {
    pub fn not_required() -> Self {
        Self {
            status: CleanupStatus::NotRequired,
            container_name: None,
            container_id: None,
            container_may_remain: false,
            diagnostic: None,
            inspection_error: None,
            cleanup_error: None,
        }
    }

    pub fn succeeded(container_name: String, container_id: String) -> Self {
        Self {
            status: CleanupStatus::Succeeded,
            container_name: Some(container_name),
            container_id: Some(container_id),
            container_may_remain: false,
            diagnostic: None,
            inspection_error: None,
            cleanup_error: None,
        }
    }

    pub fn failed(
        status: CleanupStatus,
        container_name: Option<String>,
        container_id: Option<String>,
        diagnostic: String,
    ) -> Self {
        Self {
            status,
            container_name,
            container_id,
            container_may_remain: true,
            diagnostic: Some(diagnostic.clone()),
            inspection_error: None,
            cleanup_error: Some(diagnostic),
        }
    }

    pub fn is_successful(&self) -> bool {
        matches!(
            self.status,
            CleanupStatus::NotRequired | CleanupStatus::Succeeded
        ) && !self.container_may_remain
            && self.inspection_error.is_none()
            && self.cleanup_error.is_none()
    }
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
    pub execution_error: Option<String>,
    pub cleanup: CleanupOutcome,
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
    async fn destroy(&mut self) -> Result<CleanupOutcome, SandboxError>;
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
