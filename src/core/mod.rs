use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
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

/// `Completed` is reserved for independently verified success. Agent completion is an
/// observation that leads to verification, not a terminal execution state.
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
    PartiallyCompleted,
    Unverified,
    Failed,
    Cancelled,
    Interrupted,
    TimedOut,
    /// Execution ownership or liveness is uncertain after restart; manual review is required.
    RecoveryRequired,
}

impl ExecutionState {
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed
                | Self::PartiallyCompleted
                | Self::Unverified
                | Self::Failed
                | Self::Cancelled
                | Self::Interrupted
                | Self::TimedOut
                | Self::RecoveryRequired
        )
    }

    /// Validate one lifecycle edge. `None` is valid only for initial RECEIVED.
    pub fn validate_transition(from: Option<Self>, to: Self) -> Result<(), TransitionError> {
        let allowed = match from {
            None => to == Self::Received,
            Some(current) if current.is_terminal() => false,
            Some(Self::Received) => to == Self::RepositoryAnalysis,
            Some(Self::RepositoryAnalysis) => to == Self::Planning,
            Some(Self::Planning) => to == Self::PlanValidation,
            Some(Self::PlanValidation) => to == Self::WorkspacePreparation,
            Some(Self::WorkspacePreparation) => to == Self::Executing,
            Some(Self::Executing) => to == Self::Verifying,
            Some(Self::Verifying) => matches!(
                to,
                Self::Completed
                    | Self::PartiallyCompleted
                    | Self::Unverified
                    | Self::Recovering
                    | Self::Failed
            ),
            Some(Self::Recovering) => {
                matches!(to, Self::Executing | Self::Verifying | Self::Failed)
            }
            _ => false,
        } || (from.is_some_and(|state| !state.is_terminal())
            && matches!(
                to,
                Self::Failed
                    | Self::Cancelled
                    | Self::Interrupted
                    | Self::TimedOut
                    | Self::RecoveryRequired
            ));
        if allowed {
            Ok(())
        } else {
            Err(TransitionError { from, to })
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[error("invalid execution state transition: {from:?} -> {to:?}")]
pub struct TransitionError {
    pub from: Option<ExecutionState>,
    pub to: ExecutionState,
}

/// Provider observation only. It must never be used as Synroute's verified outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentCompletion {
    ReportedComplete,
    ReportedIncomplete,
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
    pub fn new(
        execution_id: Uuid,
        from: Option<ExecutionState>,
        to: ExecutionState,
    ) -> Result<Self, TransitionError> {
        ExecutionState::validate_transition(from, to)?;
        Ok(Self {
            execution_id,
            from,
            to,
            at: Utc::now(),
            detail: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_initial_and_happy_path_transitions() {
        let path = [
            (None, ExecutionState::Received),
            (
                Some(ExecutionState::Received),
                ExecutionState::RepositoryAnalysis,
            ),
            (
                Some(ExecutionState::RepositoryAnalysis),
                ExecutionState::Planning,
            ),
            (
                Some(ExecutionState::Planning),
                ExecutionState::PlanValidation,
            ),
            (
                Some(ExecutionState::PlanValidation),
                ExecutionState::WorkspacePreparation,
            ),
            (
                Some(ExecutionState::WorkspacePreparation),
                ExecutionState::Executing,
            ),
            (Some(ExecutionState::Executing), ExecutionState::Verifying),
            (Some(ExecutionState::Verifying), ExecutionState::Completed),
        ];
        for (from, to) in path {
            assert!(
                ExecutionState::validate_transition(from, to).is_ok(),
                "{from:?} -> {to:?}"
            );
        }
    }

    #[test]
    fn allows_bounded_recovery_and_terminal_classifications() {
        assert!(
            ExecutionState::validate_transition(
                Some(ExecutionState::Verifying),
                ExecutionState::Recovering
            )
            .is_ok()
        );
        assert!(
            ExecutionState::validate_transition(
                Some(ExecutionState::Recovering),
                ExecutionState::Executing
            )
            .is_ok()
        );
        for to in [
            ExecutionState::Failed,
            ExecutionState::Cancelled,
            ExecutionState::Interrupted,
            ExecutionState::TimedOut,
            ExecutionState::RecoveryRequired,
        ] {
            assert!(
                ExecutionState::validate_transition(Some(ExecutionState::Executing), to).is_ok()
            );
        }
        for to in [
            ExecutionState::PartiallyCompleted,
            ExecutionState::Unverified,
            ExecutionState::Failed,
        ] {
            assert!(
                ExecutionState::validate_transition(Some(ExecutionState::Verifying), to).is_ok()
            );
        }
    }

    #[test]
    fn rejects_invalid_skipped_and_repeated_transitions() {
        for (from, to) in [
            (None, ExecutionState::Executing),
            (Some(ExecutionState::Received), ExecutionState::Executing),
            (Some(ExecutionState::Executing), ExecutionState::Completed),
            (Some(ExecutionState::Verifying), ExecutionState::Planning),
            (Some(ExecutionState::Recovering), ExecutionState::Completed),
        ] {
            assert!(
                ExecutionState::validate_transition(from, to).is_err(),
                "{from:?} -> {to:?}"
            );
        }
    }

    #[test]
    fn terminal_states_cannot_be_overwritten() {
        for terminal in [
            ExecutionState::Completed,
            ExecutionState::PartiallyCompleted,
            ExecutionState::Unverified,
            ExecutionState::Failed,
            ExecutionState::Cancelled,
            ExecutionState::Interrupted,
            ExecutionState::TimedOut,
            ExecutionState::RecoveryRequired,
        ] {
            assert!(terminal.is_terminal());
            assert!(
                ExecutionState::validate_transition(Some(terminal), ExecutionState::Executing)
                    .is_err()
            );
            assert!(
                ExecutionState::validate_transition(Some(terminal), ExecutionState::Failed)
                    .is_err()
            );
        }
    }

    #[test]
    fn constructor_validates_edges_and_agent_completion_is_separate() {
        let id = Uuid::new_v4();
        assert!(StateTransition::new(id, None, ExecutionState::Received).is_ok());
        assert!(
            StateTransition::new(
                id,
                Some(ExecutionState::Executing),
                ExecutionState::Completed
            )
            .is_err()
        );
        assert_eq!(
            AgentCompletion::ReportedComplete,
            AgentCompletion::ReportedComplete
        );
    }
}
