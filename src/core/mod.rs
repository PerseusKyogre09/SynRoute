use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskContract {
    pub id: Uuid,
    pub repository: String,
    pub revision: String,
    pub prompt: String,
    pub verification: Vec<VerificationCommand>,
    pub timeout_seconds: u64,
    pub max_recovery_attempts: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationCommand {
    pub name: String,
    pub argv: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecutionState {
    Received,
    RepositoryAnalysis,
    Planning,
    PlanValidation,
    WorkspacePreparation,
    Executing,
    Verifying,
    Recovering,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateTransition {
    pub execution_id: Uuid,
    pub from: Option<ExecutionState>,
    pub to: ExecutionState,
    pub at: DateTime<Utc>,
    pub detail: Option<String>,
}

impl StateTransition {
    pub fn new(execution_id: Uuid, from: Option<ExecutionState>, to: ExecutionState) -> Self {
        Self {
            execution_id,
            from,
            to,
            at: Utc::now(),
            detail: None,
        }
    }
}
