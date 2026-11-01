use super::{WorkspaceManager, is_registered};
use crate::storage::{
    SqliteStorage, WorkspaceOwnershipRecord, WorkspaceRepository, WorkspaceStatus,
};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
};
use tokio::{io::AsyncWriteExt, process::Command};
use uuid::Uuid;

#[derive(Debug)]
pub struct PersistentWorkspaceManager {
    storage: SqliteStorage,
    volatile: WorkspaceManager,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OwnershipSidecar {
    workspace_id: Uuid,
    execution_id: Uuid,
    canonical_path: PathBuf,
    repository_identity: String,
    repository_root: PathBuf,
    repository_revision: String,
    git_admin_path: Option<PathBuf>,
    git_common_dir: Option<PathBuf>,
    ownership_token: Uuid,
    status: WorkspaceStatus,
}

impl PersistentWorkspaceManager {
    pub fn new(storage: SqliteStorage) -> Self {
        Self {
            storage,
            volatile: WorkspaceManager::new(),
        }
    }

    /// Reserves ownership durably before `git worktree add`; a crash gap leaves an uncertain record, never implicit ownership.
    pub async fn create(
        &mut self,
        execution_id: Uuid,
        workspace_id: Uuid,
        repository_identity: String,
        repository: &Path,
        base: &Path,
        revision: &str,
    ) -> Result<PathBuf> {
        let repository = tokio::fs::canonicalize(repository)
            .await
            .context("canonicalize repository")?;
        let repository = git_path(&repository, &["rev-parse", "--show-toplevel"]).await?;
        tokio::fs::create_dir_all(base)
            .await
            .context("create workspace base")?;
        let base = tokio::fs::canonicalize(base)
            .await
            .context("canonicalize workspace base")?;
        let target = base.join(workspace_id.to_string());
        if tokio::fs::try_exists(&target).await? {
            bail!("workspace path '{}' already exists", target.display());
        }
        let record = WorkspaceOwnershipRecord {
            workspace_id,
            execution_id,
            canonical_path: target,
            repository_identity,
            repository_root: repository.clone(),
            repository_revision: revision.to_owned(),
            git_admin_path: None,
            git_common_dir: None,
            sidecar_path: base.join(format!(".synroute-workspace-{workspace_id}.json")),
            ownership_token: Uuid::new_v4(),
            status: WorkspaceStatus::Preparing,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        self.storage
            .workspaces()
            .reserve(&record)
            .await
            .context("reserve durable workspace ownership")?;
        write_sidecar(&record)
            .await
            .context("write initial workspace ownership metadata")?;

        let path = self
            .volatile
            .create(&repository, &base, workspace_id, revision)
            .await
            .context("create Git worktree")?;
        let git_admin = canonical_git_dir(&path, "--git-dir").await?;
        let git_common = canonical_git_dir(&path, "--git-common-dir").await?;
        if !is_registered(&repository, &path).await? {
            bail!("created worktree is not registered with Git");
        }
        let registered = self
            .storage
            .workspaces()
            .set_status(
                workspace_id,
                WorkspaceStatus::Registered,
                Some(&git_admin),
                Some(&git_common),
            )
            .await?;
        if let Err(error) = write_sidecar(&registered).await {
            let uncertain = self
                .storage
                .workspaces()
                .set_status(workspace_id, WorkspaceStatus::Uncertain, None, None)
                .await;
            return match uncertain { Ok(_) => Err(error).context("workspace registry sidecar update failed; ownership marked uncertain"), Err(mark_error) => Err(error).context(format!("workspace registry sidecar update failed and status update failed: {mark_error}")) };
        }
        Ok(path)
    }

    /// Removes a durable workspace only after database, sidecar, and Git registration agree.
    pub async fn remove(
        &mut self,
        execution_id: Uuid,
        workspace_id: Uuid,
        requested_path: &Path,
    ) -> Result<()> {
        let store = self.storage.workspaces();
        let record = store
            .workspace(workspace_id)
            .await
            .context("load durable workspace ownership")?;
        if record.execution_id != execution_id {
            bail!("workspace execution ID does not match persisted owner");
        }
        if record.status != WorkspaceStatus::Registered {
            bail!(
                "workspace status {:?} is not eligible for cleanup",
                record.status
            );
        }
        let requested = tokio::fs::canonicalize(requested_path)
            .await
            .context("workspace missing or inaccessible")?;
        if requested != record.canonical_path {
            bail!("requested path does not match persisted workspace path");
        }
        verify_sidecar(&record).await?;
        let repository = tokio::fs::canonicalize(&record.repository_root)
            .await
            .context("repository missing or inaccessible")?;
        if repository != record.repository_root {
            bail!("repository canonical path changed; refusing cleanup");
        }
        if !is_registered(&repository, &record.canonical_path).await? {
            bail!("Git worktree registration is missing; refusing cleanup");
        }
        let admin = canonical_git_dir(&record.canonical_path, "--git-dir").await?;
        let common = canonical_git_dir(&record.canonical_path, "--git-common-dir").await?;
        if Some(&admin) != record.git_admin_path.as_ref()
            || Some(&common) != record.git_common_dir.as_ref()
        {
            bail!("Git administrative metadata differs from the persisted owner record");
        }
        let removing = store
            .set_status(workspace_id, WorkspaceStatus::Removing, None, None)
            .await?;
        write_sidecar(&removing)
            .await
            .context("persist workspace removal intent")?;
        self.volatile.adopt_owned(
            workspace_id,
            repository.clone(),
            record.canonical_path.clone(),
        )?;
        if let Err(error) = self
            .volatile
            .remove(workspace_id, &record.canonical_path)
            .await
        {
            let still_registered = is_registered(&repository, &record.canonical_path)
                .await
                .unwrap_or(false);
            let restore = if still_registered {
                WorkspaceStatus::Registered
            } else {
                WorkspaceStatus::Uncertain
            };
            if let Ok(updated) = store.set_status(workspace_id, restore, None, None).await {
                let _ = write_sidecar(&updated).await;
            }
            return Err(error)
                .context("safe worktree cleanup refused or failed; workspace retained");
        }
        let removed = store
            .set_status(workspace_id, WorkspaceStatus::Removed, None, None)
            .await?;
        write_sidecar(&removed)
            .await
            .context("persist removed workspace status")?;
        Ok(())
    }

    pub async fn validate_record(record: &WorkspaceOwnershipRecord) -> Result<(), String> {
        if record.status == WorkspaceStatus::Removed {
            return verify_sidecar(record)
                .await
                .map_err(|error| error.to_string());
        }
        if record.status != WorkspaceStatus::Registered {
            return Err(format!("workspace status is {:?}", record.status));
        }
        verify_sidecar(record)
            .await
            .map_err(|error| error.to_string())?;
        let root = tokio::fs::canonicalize(&record.repository_root)
            .await
            .map_err(|e| e.to_string())?;
        if root != record.repository_root {
            return Err("repository root canonicalization mismatch".into());
        }
        let path = tokio::fs::canonicalize(&record.canonical_path)
            .await
            .map_err(|e| e.to_string())?;
        if path != record.canonical_path {
            return Err("workspace path canonicalization mismatch".into());
        }
        if !is_registered(&root, &path)
            .await
            .map_err(|e| e.to_string())?
        {
            return Err("Git registration missing".into());
        }
        let admin = canonical_git_dir(&path, "--git-dir")
            .await
            .map_err(|e| e.to_string())?;
        let common = canonical_git_dir(&path, "--git-common-dir")
            .await
            .map_err(|e| e.to_string())?;
        if Some(&admin) != record.git_admin_path.as_ref()
            || Some(&common) != record.git_common_dir.as_ref()
        {
            return Err("Git admin/common directory mismatch".into());
        }
        Ok(())
    }
}

async fn verify_sidecar(record: &WorkspaceOwnershipRecord) -> Result<()> {
    let metadata = tokio::fs::symlink_metadata(&record.sidecar_path)
        .await
        .context("ownership sidecar missing")?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        bail!("ownership sidecar is not a regular file");
    }
    if tokio::fs::canonicalize(&record.sidecar_path).await? != record.sidecar_path {
        bail!("ownership sidecar canonical path mismatch");
    }
    let bytes = tokio::fs::read(&record.sidecar_path).await?;
    let sidecar: OwnershipSidecar =
        serde_json::from_slice(&bytes).context("decode workspace ownership sidecar")?;
    let expected = sidecar_for(record);
    if sidecar.workspace_id != expected.workspace_id
        || sidecar.execution_id != expected.execution_id
        || sidecar.canonical_path != expected.canonical_path
        || sidecar.repository_identity != expected.repository_identity
        || sidecar.repository_root != expected.repository_root
        || sidecar.repository_revision != expected.repository_revision
        || sidecar.git_admin_path != expected.git_admin_path
        || sidecar.git_common_dir != expected.git_common_dir
        || sidecar.ownership_token != expected.ownership_token
        || sidecar.status != expected.status
    {
        bail!("ownership sidecar does not match the persisted owner record");
    }
    Ok(())
}

fn sidecar_for(record: &WorkspaceOwnershipRecord) -> OwnershipSidecar {
    OwnershipSidecar {
        workspace_id: record.workspace_id,
        execution_id: record.execution_id,
        canonical_path: record.canonical_path.clone(),
        repository_identity: record.repository_identity.clone(),
        repository_root: record.repository_root.clone(),
        repository_revision: record.repository_revision.clone(),
        git_admin_path: record.git_admin_path.clone(),
        git_common_dir: record.git_common_dir.clone(),
        ownership_token: record.ownership_token,
        status: record.status,
    }
}

async fn write_sidecar(record: &WorkspaceOwnershipRecord) -> Result<()> {
    let bytes = serde_json::to_vec(&sidecar_for(record))?;
    let temporary = record
        .sidecar_path
        .with_extension(format!("{}.tmp", Uuid::new_v4()));
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        options.mode(0o600);
    }
    let mut file = options.open(&temporary).await?;
    file.write_all(&bytes).await?;
    file.sync_all().await?;
    drop(file);
    tokio::fs::rename(&temporary, &record.sidecar_path).await?;
    Ok(())
}

async fn git_path(repository: &Path, args: &[&str]) -> Result<PathBuf> {
    let output = Command::new("git")
        .args(args)
        .current_dir(repository)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await?;
    if !output.status.success() {
        bail!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let path = PathBuf::from(String::from_utf8(output.stdout)?.trim());
    tokio::fs::canonicalize(if path.is_absolute() {
        path
    } else {
        repository.join(path)
    })
    .await
    .context("canonicalize Git repository root")
}

async fn canonical_git_dir(worktree: &Path, flag: &str) -> Result<PathBuf> {
    let output = Command::new("git")
        .args(["rev-parse", flag])
        .current_dir(worktree)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await?;
    if !output.status.success() {
        bail!(
            "git rev-parse {flag} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let path = PathBuf::from(String::from_utf8(output.stdout)?.trim());
    tokio::fs::canonicalize(if path.is_absolute() {
        path
    } else {
        worktree.join(path)
    })
    .await
    .with_context(|| format!("canonicalize Git metadata from {flag}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{ExecutionIntent, ExecutionRepository, PersistedExecutionConfig};
    use serde_json::json;
    use std::process::Command as StdCommand;
    use tempfile::TempDir;

    fn git_repo() -> TempDir {
        let dir = tempfile::tempdir().unwrap();
        for args in [
            vec!["init", "-q"],
            vec!["config", "user.email", "synroute@example.test"],
            vec!["config", "user.name", "Synroute Tests"],
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

    async fn execution(db: &SqliteStorage, repo: &Path) -> Uuid {
        let root = repo.canonicalize().unwrap();
        let revision = String::from_utf8(
            StdCommand::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(repo)
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap();
        let id = Uuid::new_v4();
        db.executions()
            .create_execution(ExecutionIntent {
                id,
                description: "workspace test".into(),
                metadata: json!({}),
                repository_identity: "fixture-repo".into(),
                repository_root: root.clone(),
                repository_revision: revision.trim().into(),
                working_directory: root,
                config: PersistedExecutionConfig {
                    timeout_seconds: 10,
                    max_recovery_attempts: 0,
                    memory_bytes: 1024,
                    cpu_millis: 100,
                    process_limit: 4,
                    output_limit_bytes: 1024,
                },
            })
            .await
            .unwrap();
        id
    }

    #[tokio::test]
    async fn durable_ownership_survives_manager_and_database_reopen() {
        let root = tempfile::tempdir().unwrap();
        let db_file = root.path().join("store.sqlite");
        let repository = git_repo();
        let base = tempfile::tempdir().unwrap();
        let workspace_id = Uuid::new_v4();
        let execution_id;
        let workspace_path;
        {
            let db = SqliteStorage::open(&db_file).await.unwrap();
            execution_id = execution(&db, repository.path()).await;
            let mut manager = PersistentWorkspaceManager::new(db);
            let revision = String::from_utf8(
                StdCommand::new("git")
                    .args(["rev-parse", "HEAD"])
                    .current_dir(repository.path())
                    .output()
                    .unwrap()
                    .stdout,
            )
            .unwrap();
            workspace_path = manager
                .create(
                    execution_id,
                    workspace_id,
                    "fixture-repo".into(),
                    repository.path(),
                    base.path(),
                    revision.trim(),
                )
                .await
                .unwrap();
        }
        let db = SqliteStorage::open(&db_file).await.unwrap();
        let record = db.workspaces().workspace(workspace_id).await.unwrap();
        assert_eq!(record.status, WorkspaceStatus::Registered);
        let mut restarted = PersistentWorkspaceManager::new(db.clone());
        restarted
            .remove(execution_id, workspace_id, &workspace_path)
            .await
            .unwrap();
        assert!(!workspace_path.exists());
        assert_eq!(
            db.workspaces()
                .workspace(workspace_id)
                .await
                .unwrap()
                .status,
            WorkspaceStatus::Removed
        );
    }

    #[tokio::test]
    async fn ownership_sidecar_mismatch_refuses_cleanup_without_removing_worktree() {
        let root = tempfile::tempdir().unwrap();
        let db = SqliteStorage::open(root.path().join("store.sqlite"))
            .await
            .unwrap();
        let repository = git_repo();
        let base = tempfile::tempdir().unwrap();
        let execution_id = execution(&db, repository.path()).await;
        let workspace_id = Uuid::new_v4();
        let revision = String::from_utf8(
            StdCommand::new("git")
                .args(["rev-parse", "HEAD"])
                .current_dir(repository.path())
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap();
        let mut manager = PersistentWorkspaceManager::new(db.clone());
        let path = manager
            .create(
                execution_id,
                workspace_id,
                "fixture-repo".into(),
                repository.path(),
                base.path(),
                revision.trim(),
            )
            .await
            .unwrap();
        let record = db.workspaces().workspace(workspace_id).await.unwrap();
        let mut sidecar: serde_json::Value =
            serde_json::from_slice(&tokio::fs::read(&record.sidecar_path).await.unwrap()).unwrap();
        sidecar["ownership_token"] = json!(Uuid::new_v4());
        tokio::fs::write(&record.sidecar_path, serde_json::to_vec(&sidecar).unwrap())
            .await
            .unwrap();
        let mut restarted = PersistentWorkspaceManager::new(db);
        let error = restarted
            .remove(execution_id, workspace_id, &path)
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("sidecar"));
        assert!(path.exists());
    }
}
