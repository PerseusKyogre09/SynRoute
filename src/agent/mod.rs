//! Provider boundaries. The App Server adapter will be implemented against OpenAI's documented JSON-RPC protocol.
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentEvent {
    pub kind: String,
    pub message: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct AgentRequest {
    pub prompt: String,
    pub workspace: std::path::PathBuf,
    pub timeout_seconds: u64,
}

#[async_trait]
pub trait CodingAgent: Send + Sync {
    async fn execute(
        &self,
        request: AgentRequest,
        events: mpsc::Sender<AgentEvent>,
    ) -> anyhow::Result<AgentOutcome>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentOutcome {
    pub provider: String,
    pub provider_thread_id: Option<String>,
    pub completed: bool,
}
