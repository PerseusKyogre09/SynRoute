//! Provider-independent coding agent contracts and explicit provider support status.
//!
//! No adapter currently executes repository-modifying tools. In particular, the installed
//! Codex App Server is not safe to launch on the host: it can run tools and subprocesses
//! outside Synroute's Docker boundary.
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, time::Duration};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentCapabilities {
    pub cancellation: bool,
    pub structured_events: bool,
    pub model_metadata: bool,
    pub isolated_execution: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentEvent {
    pub kind: String,
    pub message: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct AgentRequest {
    pub prompt: String,
    pub workspace: PathBuf,
    pub timeout: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentOutcome {
    pub provider: String,
    pub model: Option<String>,
    pub provider_thread_id: Option<String>,
    pub exit_status: Option<i32>,
    pub completed: bool,
    pub failure: Option<String>,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub output_truncated: bool,
    pub duration: Duration,
}

#[async_trait]
pub trait CodingAgent: Send + Sync {
    fn id(&self) -> &'static str;
    fn capabilities(&self) -> AgentCapabilities;

    async fn execute(
        &self,
        request: AgentRequest,
        events: mpsc::Sender<AgentEvent>,
        cancellation: tokio::sync::watch::Receiver<bool>,
    ) -> anyhow::Result<AgentOutcome>;
}

/// Codex's documented App Server is present locally, but it is deliberately unavailable as
/// an execution adapter until the process and all tool subprocesses can be confined to the
/// Synroute-controlled workload boundary.
#[derive(Debug, Default, Clone, Copy)]
pub struct CodexAppServer;

impl CodexAppServer {
    pub fn support_status(&self) -> &'static str {
        "unsupported: persistent Codex stdio transport is not implemented in the Docker runtime, and no scoped credential broker is configured; App Server tokens would be readable by agent tool subprocesses"
    }
}

#[async_trait]
impl CodingAgent for CodexAppServer {
    fn id(&self) -> &'static str {
        "codex-app-server"
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
        _request: AgentRequest,
        _events: mpsc::Sender<AgentEvent>,
        _cancellation: tokio::sync::watch::Receiver<bool>,
    ) -> anyhow::Result<AgentOutcome> {
        anyhow::bail!("{}", self.support_status())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn codex_adapter_fails_closed_until_execution_boundary_is_proven() {
        let adapter = CodexAppServer;
        let (_tx, rx) = mpsc::channel(1);
        let cancel = tokio::sync::watch::channel(false).1;
        let error = adapter
            .execute(
                AgentRequest {
                    prompt: "task".into(),
                    workspace: PathBuf::from("/tmp/work"),
                    timeout: Duration::from_secs(1),
                },
                _tx,
                cancel,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("unsupported"));
        assert!(!adapter.capabilities().isolated_execution);
        drop(rx);
    }
}
