use anyhow::{Context, Result, bail};
use std::path::{Path, PathBuf};
use tokio::process::Command;
use uuid::Uuid;

/// Creates an isolated Git worktree. This is source isolation only, not a security sandbox.
pub async fn create_worktree(
    repository: &Path,
    base: &Path,
    execution_id: Uuid,
    revision: &str,
) -> Result<PathBuf> {
    tokio::fs::create_dir_all(base).await?;
    let target = base.join(execution_id.to_string());
    let output = Command::new("git")
        .args(["worktree", "add", "--detach"])
        .arg(&target)
        .arg(revision)
        .current_dir(repository)
        .output()
        .await?;
    if !output.status.success() {
        bail!(
            "git worktree add failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(target)
}

pub async fn remove_worktree(repository: &Path, target: &Path) -> Result<()> {
    let output = Command::new("git")
        .args(["worktree", "remove", "--force"])
        .arg(target)
        .current_dir(repository)
        .output()
        .await
        .context("remove worktree")?;
    if !output.status.success() {
        bail!(
            "git worktree remove failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(())
}
