//! Lifecycle contracts used by the future execution coordinator.
pub mod coordinator;
pub use crate::core::{
    AgentCompletion, ExecutionState, StateTransition, TaskContract, TransitionError,
};
