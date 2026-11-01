use crate::sandbox::SandboxCommand;
use std::collections::{BTreeMap, BTreeSet};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommandRule {
    Allow,
    Deny,
    RequireApproval,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Authorization {
    Allowed,
    Denied,
    ApprovalRequired,
    Unsupported,
}

#[derive(Debug, Clone, Default)]
pub struct ExecutionPolicy {
    rules: BTreeMap<String, CommandRule>,
}
impl ExecutionPolicy {
    pub fn new(rules: impl IntoIterator<Item = (String, CommandRule)>) -> Self {
        Self {
            rules: rules.into_iter().collect(),
        }
    }
    /// Exact executable-name matching; accepts argv only, never shell text.
    pub fn authorize(
        &self,
        command: &SandboxCommand,
        approvals: &BTreeSet<String>,
    ) -> Authorization {
        match self.rules.get(&command.executable).copied() {
            Some(CommandRule::Allow) => Authorization::Allowed,
            Some(CommandRule::Deny) => Authorization::Denied,
            Some(CommandRule::RequireApproval) if approvals.contains(&command.executable) => {
                Authorization::Allowed
            }
            Some(CommandRule::RequireApproval) => Authorization::ApprovalRequired,
            None => Authorization::Unsupported,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AuthorizationError {
    #[error("command is denied by execution policy")]
    Denied,
    #[error("command requires explicit approval")]
    ApprovalRequired,
    #[error("command is not supported by the execution policy")]
    Unsupported,
}

impl TryFrom<Authorization> for () {
    type Error = AuthorizationError;
    fn try_from(value: Authorization) -> Result<Self, Self::Error> {
        match value {
            Authorization::Allowed => Ok(()),
            Authorization::Denied => Err(AuthorizationError::Denied),
            Authorization::ApprovalRequired => Err(AuthorizationError::ApprovalRequired),
            Authorization::Unsupported => Err(AuthorizationError::Unsupported),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn cmd(executable: &str) -> SandboxCommand {
        SandboxCommand {
            executable: executable.into(),
            args: vec!["arg".into()],
        }
    }
    #[test]
    fn policy_decisions_fail_closed_and_require_explicit_approval() {
        let policy = ExecutionPolicy::new([
            ("cargo".into(), CommandRule::Allow),
            ("danger".into(), CommandRule::Deny),
            ("publish".into(), CommandRule::RequireApproval),
        ]);
        assert_eq!(
            policy.authorize(&cmd("cargo"), &BTreeSet::new()),
            Authorization::Allowed
        );
        assert_eq!(
            policy.authorize(&cmd("danger"), &BTreeSet::new()),
            Authorization::Denied
        );
        assert_eq!(
            policy.authorize(&cmd("publish"), &BTreeSet::new()),
            Authorization::ApprovalRequired
        );
        assert_eq!(
            policy.authorize(&cmd("publish"), &BTreeSet::from(["publish".into()])),
            Authorization::Allowed
        );
        assert_eq!(
            policy.authorize(&cmd("sh"), &BTreeSet::new()),
            Authorization::Unsupported
        );
        assert_eq!(
            policy.authorize(&cmd("/usr/bin/cargo"), &BTreeSet::new()),
            Authorization::Unsupported
        );
    }
}
