use crate::core::VerificationCommand;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::{path::Path, process::Stdio, time::Duration};
use tokio::{process::Command, time::timeout};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckEvidence {
    pub name: String,
    pub argv: Vec<String>,
    pub exit_code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
    pub timed_out: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VerificationReport {
    pub checks: Vec<CheckEvidence>,
    pub passed: bool,
}

pub async fn run(
    workspace: &Path,
    commands: &[VerificationCommand],
    limit: Duration,
) -> Result<VerificationReport> {
    let mut checks = Vec::with_capacity(commands.len());
    for check in commands {
        let Some((program, args)) = check.argv.split_first() else {
            checks.push(CheckEvidence {
                name: check.name.clone(),
                argv: check.argv.clone(),
                exit_code: None,
                stdout: String::new(),
                stderr: "empty command".into(),
                timed_out: false,
            });
            continue;
        };
        let child = Command::new(program)
            .args(args)
            .current_dir(workspace)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .output();
        let (exit_code, stdout, stderr, timed_out) = match timeout(limit, child).await {
            Ok(Ok(out)) => (
                out.status.code(),
                String::from_utf8_lossy(&out.stdout).into_owned(),
                String::from_utf8_lossy(&out.stderr).into_owned(),
                false,
            ),
            Ok(Err(err)) => (None, String::new(), err.to_string(), false),
            Err(_) => (None, String::new(), String::new(), true),
        };
        checks.push(CheckEvidence {
            name: check.name.clone(),
            argv: check.argv.clone(),
            exit_code,
            stdout,
            stderr,
            timed_out,
        });
    }
    let passed = !checks.is_empty()
        && checks
            .iter()
            .all(|c| c.exit_code == Some(0) && !c.timed_out);
    Ok(VerificationReport { checks, passed })
}
