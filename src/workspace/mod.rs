use anyhow::{Context, Result, bail};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Stdio,
};
use tokio::process::Command;
use uuid::Uuid;

/// Tracks worktrees created by this manager. The registry is intentionally process-local;
/// after restart, ownership cannot be established and cleanup is refused.
#[derive(Debug, Default)]
pub struct WorkspaceManager {
    owned: HashMap<Uuid, (PathBuf, PathBuf)>, // id -> (canonical repository, canonical workspace)
}

impl WorkspaceManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a detached Git worktree. A Git worktree is source isolation, not a sandbox.
    pub async fn create(
        &mut self,
        repository: &Path,
        base: &Path,
        id: Uuid,
        revision: &str,
    ) -> Result<PathBuf> {
        if self.owned.contains_key(&id) {
            bail!("workspace ID {id} is already managed");
        }
        let repository = tokio::fs::canonicalize(repository)
            .await
            .context("canonicalize Git repository")?;
        tokio::fs::create_dir_all(base)
            .await
            .context("create workspace base")?;
        let base = tokio::fs::canonicalize(base)
            .await
            .context("canonicalize workspace base")?;
        let target = base.join(id.to_string());
        if tokio::fs::try_exists(&target).await? {
            bail!("workspace path '{}' already exists", target.display());
        }
        let output = Command::new("git")
            .args(["worktree", "add", "--detach"])
            .arg(&target)
            .arg(revision)
            .current_dir(&repository)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .context("could not start Git worktree add")?;
        if !output.status.success() {
            bail!(
                "git worktree add failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        let workspace = tokio::fs::canonicalize(&target)
            .await
            .context("canonicalize created worktree")?;
        if !is_registered(&repository, &workspace).await? {
            bail!(
                "Git created workspace '{}' but did not register it",
                workspace.display()
            );
        }
        self.owned.insert(id, (repository, workspace.clone()));
        Ok(workspace)
    }

    /// Removes only a registered, clean worktree created by this manager.
    pub async fn remove(&mut self, id: Uuid, target: &Path) -> Result<()> {
        let Some((repository, owned_path)) = self.owned.get(&id).cloned() else {
            bail!("workspace ID {id} is not managed by this process");
        };
        let requested = tokio::fs::canonicalize(target).await.with_context(|| {
            format!(
                "managed workspace '{}' is missing or inaccessible",
                target.display()
            )
        })?;
        if requested != owned_path {
            bail!(
                "path '{}' does not match managed workspace for ID {id}",
                requested.display()
            );
        }
        if !is_registered(&repository, &owned_path).await? {
            bail!(
                "workspace '{}' is not registered with Git; refusing cleanup",
                owned_path.display()
            );
        }
        let status = Command::new("git")
            .args(["status", "--porcelain", "--untracked-files=all"])
            .current_dir(&owned_path)
            .output()
            .await
            .context("check worktree changes")?;
        if !status.status.success() {
            bail!(
                "could not inspect workspace changes: {}",
                String::from_utf8_lossy(&status.stderr).trim()
            );
        }
        if !status.stdout.is_empty() {
            bail!(
                "workspace '{}' has uncommitted or untracked changes; refusing cleanup",
                owned_path.display()
            );
        }
        let output = Command::new("git")
            .args(["worktree", "remove"])
            .arg(&owned_path)
            .current_dir(&repository)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .output()
            .await
            .context("could not start Git worktree remove")?;
        if !output.status.success() {
            bail!(
                "git worktree remove failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            );
        }
        self.owned.remove(&id);
        Ok(())
    }
}

async fn is_registered(repository: &Path, workspace: &Path) -> Result<bool> {
    let output = Command::new("git")
        .args(["worktree", "list", "--porcelain"])
        .current_dir(repository)
        .output()
        .await
        .context("list Git worktrees")?;
    if !output.status.success() {
        bail!(
            "git worktree list failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let expected = workspace.to_string_lossy();
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .any(|line| line.strip_prefix("worktree ") == Some(expected.as_ref())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command as StdCommand;
    use tempfile::TempDir;

    fn repo() -> TempDir {
        let dir = tempfile::tempdir().unwrap();
        for args in [
            vec!["init", "-q"],
            vec!["config", "user.email", "test@example.com"],
            vec!["config", "user.name", "Test"],
        ] {
            assert!(
                StdCommand::new("git")
                    .args(args)
                    .current_dir(dir.path())
                    .status()
                    .unwrap()
                    .success()
            );
        }
        std::fs::write(dir.path().join("README.md"), "fixture\n").unwrap();
        assert!(
            StdCommand::new("git")
                .args(["add", "."])
                .current_dir(dir.path())
                .status()
                .unwrap()
                .success()
        );
        assert!(
            StdCommand::new("git")
                .args(["commit", "-q", "-m", "fixture"])
                .current_dir(dir.path())
                .status()
                .unwrap()
                .success()
        );
        dir
    }
    async fn create(manager: &mut WorkspaceManager, repo: &Path, base: &Path, id: Uuid) -> PathBuf {
        let revision = String::from_utf8(
            StdCommand::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(repo)
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap();
        manager
            .create(repo, base, id, revision.trim())
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn creates_registers_and_cleans_owned_worktree() {
        let repo = repo();
        let base = tempfile::tempdir().unwrap();
        let id = Uuid::new_v4();
        let mut manager = WorkspaceManager::new();
        let path = create(&mut manager, repo.path(), base.path(), id).await;
        assert!(is_registered(repo.path(), &path).await.unwrap());
        manager.remove(id, &path).await.unwrap();
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn rejects_unknown_workspace() {
        let repo = repo();
        let mut manager = WorkspaceManager::new();
        let error = manager
            .remove(Uuid::new_v4(), repo.path())
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("not managed"));
    }

    #[tokio::test]
    async fn refuses_uncommitted_changes_and_keeps_ownership_for_retry() {
        let repo = repo();
        let base = tempfile::tempdir().unwrap();
        let id = Uuid::new_v4();
        let mut manager = WorkspaceManager::new();
        let path = create(&mut manager, repo.path(), base.path(), id).await;
        std::fs::write(path.join("new.txt"), "keep me").unwrap();
        assert!(
            manager
                .remove(id, &path)
                .await
                .unwrap_err()
                .to_string()
                .contains("uncommitted or untracked")
        );
        assert!(path.join("new.txt").exists());
    }

    #[tokio::test]
    async fn rejects_missing_workspace_directory() {
        let repo = repo();
        let base = tempfile::tempdir().unwrap();
        let id = Uuid::new_v4();
        let mut manager = WorkspaceManager::new();
        let path = create(&mut manager, repo.path(), base.path(), id).await;
        std::fs::remove_dir_all(&path).unwrap();
        assert!(
            manager
                .remove(id, &path)
                .await
                .unwrap_err()
                .to_string()
                .contains("missing or inaccessible")
        );
    }

    #[tokio::test]
    async fn refuses_cleanup_when_git_registration_is_missing() {
        let repo = repo();
        let base = tempfile::tempdir().unwrap();
        let id = Uuid::new_v4();
        let mut manager = WorkspaceManager::new();
        let path = create(&mut manager, repo.path(), base.path(), id).await;
        assert!(
            StdCommand::new("git")
                .args(["worktree", "remove", "--force"])
                .arg(&path)
                .current_dir(repo.path())
                .status()
                .unwrap()
                .success()
        );
        std::fs::create_dir(&path).unwrap();
        assert!(
            manager
                .remove(id, &path)
                .await
                .unwrap_err()
                .to_string()
                .contains("not registered")
        );
    }
}
