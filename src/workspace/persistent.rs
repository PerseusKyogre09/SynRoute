use super::{WorkspaceManager, is_registered};
use crate::git;
use crate::storage::{
    SqliteStorage, WorkspaceDiagnostic, WorkspaceIssueCode, WorkspaceOwnershipRecord,
    WorkspaceRepository, WorkspaceStatus,
};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;
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

    async fn mark_creation_uncertain(
        &self,
        original: &WorkspaceOwnershipRecord,
        code: WorkspaceIssueCode,
    ) -> Result<()> {
        let uncertain = self
            .storage
            .workspaces()
            .set_status(
                original.workspace_id,
                WorkspaceStatus::Uncertain,
                None,
                None,
                Some(WorkspaceDiagnostic::new(code)),
            )
            .await?;
        update_sidecar(original, &uncertain)
            .await
            .context("workspace uncertainty persisted in SQLite but sidecar update failed")
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
            uncertainty_code: None,
            uncertainty_detail: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
        };
        self.storage
            .workspaces()
            .reserve(&record)
            .await
            .context("reserve durable workspace ownership")?;
        if let Err(error) = create_sidecar(&record).await {
            let code = if error
                .chain()
                .find_map(|cause| cause.downcast_ref::<std::io::Error>())
                .is_some_and(|error| error.kind() == std::io::ErrorKind::AlreadyExists)
            {
                WorkspaceIssueCode::ExistingSidecar
            } else {
                WorkspaceIssueCode::SidecarUpdateFailed
            };
            self.storage
                .workspaces()
                .set_status(
                    workspace_id,
                    WorkspaceStatus::Uncertain,
                    None,
                    None,
                    Some(WorkspaceDiagnostic::new(code)),
                )
                .await
                .context(
                    "sidecar creation failed and workspace uncertainty could not be persisted",
                )?;
            return Err(error).context("write initial workspace ownership metadata");
        }

        let path = match self
            .volatile
            .create(&repository, &base, workspace_id, revision)
            .await
        {
            Ok(path) => path,
            Err(error) => {
                return Err(
                    match self
                        .mark_creation_uncertain(&record, WorkspaceIssueCode::GitOperationFailed)
                        .await
                    {
                        Ok(()) => error.context("create Git worktree; workspace marked uncertain"),
                        Err(mark_error) => error.context(format!(
                            "create Git worktree and persist uncertainty failed: {mark_error}"
                        )),
                    },
                );
            }
        };
        let git_admin = match canonical_git_dir(&path, "--git-dir").await {
            Ok(path) => path,
            Err(error) => {
                return Err(
                    match self
                        .mark_creation_uncertain(&record, WorkspaceIssueCode::GitInspectionFailed)
                        .await
                    {
                        Ok(()) => error.context("inspect new worktree; ownership is uncertain"),
                        Err(mark_error) => error.context(format!(
                            "inspect new worktree and persist uncertainty failed: {mark_error}"
                        )),
                    },
                );
            }
        };
        let git_common = match canonical_git_dir(&path, "--git-common-dir").await {
            Ok(path) => path,
            Err(error) => {
                return Err(
                    match self
                        .mark_creation_uncertain(&record, WorkspaceIssueCode::GitInspectionFailed)
                        .await
                    {
                        Ok(()) => error.context("inspect new worktree; ownership is uncertain"),
                        Err(mark_error) => error.context(format!(
                            "inspect new worktree and persist uncertainty failed: {mark_error}"
                        )),
                    },
                );
            }
        };
        let registered = match is_registered(&repository, &path).await {
            Ok(registered) => registered,
            Err(error) => {
                return Err(match self
                    .mark_creation_uncertain(&record, WorkspaceIssueCode::GitInspectionFailed)
                    .await
                {
                    Ok(()) => error.context("inspect new worktree registration; ownership is uncertain"),
                    Err(mark_error) => error.context(format!(
                        "inspect new worktree registration and persist uncertainty failed: {mark_error}"
                    )),
                });
            }
        };
        if !registered {
            let error = anyhow::anyhow!("created worktree is not registered with Git");
            return Err(match self
                .mark_creation_uncertain(&record, WorkspaceIssueCode::RegistrationMissing)
                .await
            {
                Ok(()) => error.context("new worktree registration is uncertain"),
                Err(mark_error) => error.context(format!(
                    "new worktree registration is uncertain and persistence failed: {mark_error}"
                )),
            });
        }
        let registered = self
            .storage
            .workspaces()
            .set_status(
                workspace_id,
                WorkspaceStatus::Registered,
                Some(&git_admin),
                Some(&git_common),
                None,
            )
            .await?;
        if let Err(error) = update_sidecar(&record, &registered).await {
            let uncertain = self
                .storage
                .workspaces()
                .set_status(
                    workspace_id,
                    WorkspaceStatus::Uncertain,
                    None,
                    None,
                    Some(WorkspaceDiagnostic::new(
                        WorkspaceIssueCode::SidecarUpdateFailed,
                    )),
                )
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
        let requested = match tokio::fs::canonicalize(requested_path).await {
            Ok(path) => path,
            Err(error) => {
                if requested_path == record.canonical_path {
                    store
                        .set_status(
                            workspace_id,
                            WorkspaceStatus::Uncertain,
                            None,
                            None,
                            Some(WorkspaceDiagnostic::new(
                                WorkspaceIssueCode::WorkspaceUnavailable,
                            )),
                        )
                        .await
                        .context(
                            "workspace is missing and its uncertainty could not be persisted",
                        )?;
                }
                return Err(error).context("workspace missing or inaccessible");
            }
        };
        if requested != record.canonical_path {
            bail!("requested path does not match persisted workspace path");
        }
        if let Err(issue) = Self::diagnose_record(&record).await {
            let detail = issue.code.safe_detail();
            store
                .set_status(
                    workspace_id,
                    WorkspaceStatus::Uncertain,
                    None,
                    None,
                    Some(issue),
                )
                .await
                .context("persist workspace ownership uncertainty")?;
            bail!("workspace ownership validation failed: {detail}");
        }
        let repository = record.repository_root.clone();
        let removing = store
            .set_status(workspace_id, WorkspaceStatus::Removing, None, None, None)
            .await?;
        update_sidecar(&record, &removing)
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
            let updated = match store
                .set_status(
                    workspace_id,
                    WorkspaceStatus::Uncertain,
                    None,
                    None,
                    Some(WorkspaceDiagnostic::new(
                        WorkspaceIssueCode::GitOperationFailed,
                    )),
                )
                .await
            {
                Ok(updated) => updated,
                Err(mark_error) => {
                    return Err(error.context(format!(
                        "cleanup failed and workspace uncertainty could not be persisted: {mark_error}"
                    )));
                }
            };
            if let Err(sidecar_error) = update_sidecar(&removing, &updated).await {
                return Err(error.context(format!(
                    "cleanup failed; uncertainty is persisted but sidecar update failed: {sidecar_error}"
                )));
            }
            return Err(error)
                .context("safe worktree cleanup failed; workspace is retained as uncertain");
        }
        let removed = store
            .set_status(workspace_id, WorkspaceStatus::Removed, None, None, None)
            .await?;
        update_sidecar(&removing, &removed)
            .await
            .context("persist removed workspace status")?;
        Ok(())
    }

    pub async fn diagnose_record(
        record: &WorkspaceOwnershipRecord,
    ) -> Result<(), WorkspaceDiagnostic> {
        if record.status == WorkspaceStatus::Preparing {
            return Err(WorkspaceDiagnostic::new(
                WorkspaceIssueCode::InterruptedCreation,
            ));
        }
        if record.status == WorkspaceStatus::Removing {
            return Err(WorkspaceDiagnostic::new(
                WorkspaceIssueCode::InterruptedRemoval,
            ));
        }
        if record.status == WorkspaceStatus::Uncertain {
            return Err(WorkspaceDiagnostic::new(issue_code_from_record(record)));
        }
        if verify_sidecar(record).await.is_err() {
            return Err(sidecar_diagnostic(record).await);
        }
        if record.status == WorkspaceStatus::Removed {
            return Ok(());
        }
        let root = tokio::fs::canonicalize(&record.repository_root)
            .await
            .map_err(|_| WorkspaceDiagnostic::new(WorkspaceIssueCode::RepositoryUnavailable))?;
        if root != record.repository_root {
            return Err(WorkspaceDiagnostic::new(WorkspaceIssueCode::PathMismatch));
        }
        let path = tokio::fs::canonicalize(&record.canonical_path)
            .await
            .map_err(|_| WorkspaceDiagnostic::new(WorkspaceIssueCode::WorkspaceUnavailable))?;
        if path != record.canonical_path {
            return Err(WorkspaceDiagnostic::new(WorkspaceIssueCode::PathMismatch));
        }
        if !is_registered(&root, &path)
            .await
            .map_err(|_| WorkspaceDiagnostic::new(WorkspaceIssueCode::GitInspectionFailed))?
        {
            return Err(WorkspaceDiagnostic::new(
                WorkspaceIssueCode::RegistrationMissing,
            ));
        }
        let admin = canonical_git_dir(&path, "--git-dir")
            .await
            .map_err(|_| WorkspaceDiagnostic::new(WorkspaceIssueCode::GitInspectionFailed))?;
        let common = canonical_git_dir(&path, "--git-common-dir")
            .await
            .map_err(|_| WorkspaceDiagnostic::new(WorkspaceIssueCode::GitInspectionFailed))?;
        if Some(&admin) != record.git_admin_path.as_ref()
            || Some(&common) != record.git_common_dir.as_ref()
        {
            return Err(WorkspaceDiagnostic::new(
                WorkspaceIssueCode::GitMetadataMismatch,
            ));
        }
        Ok(())
    }

    pub async fn validate_record(record: &WorkspaceOwnershipRecord) -> Result<(), String> {
        Self::diagnose_record(record)
            .await
            .map_err(|issue| format!("{}: {}", issue.code.as_str(), issue.code.safe_detail()))
    }
}

fn issue_code_from_record(record: &WorkspaceOwnershipRecord) -> WorkspaceIssueCode {
    match record.uncertainty_code.as_deref() {
        Some("existing_sidecar") => WorkspaceIssueCode::ExistingSidecar,
        Some("sidecar_missing") => WorkspaceIssueCode::SidecarMissing,
        Some("sidecar_invalid") => WorkspaceIssueCode::SidecarInvalid,
        Some("sidecar_mismatch") => WorkspaceIssueCode::SidecarMismatch,
        Some("repository_unavailable") => WorkspaceIssueCode::RepositoryUnavailable,
        Some("workspace_unavailable") => WorkspaceIssueCode::WorkspaceUnavailable,
        Some("workspace_path_mismatch") => WorkspaceIssueCode::PathMismatch,
        Some("git_registration_missing") => WorkspaceIssueCode::RegistrationMissing,
        Some("git_metadata_mismatch") => WorkspaceIssueCode::GitMetadataMismatch,
        Some("git_inspection_failed") => WorkspaceIssueCode::GitInspectionFailed,
        Some("removal_interrupted_or_incomplete") => WorkspaceIssueCode::InterruptedRemoval,
        Some("git_operation_failed") => WorkspaceIssueCode::GitOperationFailed,
        Some("sidecar_update_failed") => WorkspaceIssueCode::SidecarUpdateFailed,
        _ => WorkspaceIssueCode::InterruptedCreation,
    }
}

async fn sidecar_diagnostic(record: &WorkspaceOwnershipRecord) -> WorkspaceDiagnostic {
    match tokio::fs::symlink_metadata(&record.sidecar_path).await {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            WorkspaceDiagnostic::new(WorkspaceIssueCode::SidecarMissing)
        }
        Err(_) => WorkspaceDiagnostic::new(WorkspaceIssueCode::SidecarInvalid),
        Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
            WorkspaceDiagnostic::new(WorkspaceIssueCode::SidecarInvalid)
        }
        Ok(_) => {
            if !matches!(
                tokio::fs::canonicalize(&record.sidecar_path).await,
                Ok(path) if path == record.sidecar_path
            ) {
                return WorkspaceDiagnostic::new(WorkspaceIssueCode::SidecarInvalid);
            }
            match tokio::fs::read(&record.sidecar_path).await {
                Err(_) => WorkspaceDiagnostic::new(WorkspaceIssueCode::SidecarInvalid),
                Ok(bytes) => match serde_json::from_slice::<OwnershipSidecar>(&bytes) {
                    Err(_) => WorkspaceDiagnostic::new(WorkspaceIssueCode::SidecarInvalid),
                    Ok(_) => WorkspaceDiagnostic::new(WorkspaceIssueCode::SidecarMismatch),
                },
            }
        }
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

async fn create_sidecar(record: &WorkspaceOwnershipRecord) -> Result<()> {
    let bytes = serde_json::to_vec(&sidecar_for(record))?;
    let mut options = tokio::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        options.mode(0o600);
    }
    let mut file = options
        .open(&record.sidecar_path)
        .await
        .context("reserve ownership sidecar exclusively")?;
    file.write_all(&bytes).await?;
    file.sync_all().await?;
    Ok(())
}

async fn update_sidecar(
    previous: &WorkspaceOwnershipRecord,
    record: &WorkspaceOwnershipRecord,
) -> Result<()> {
    verify_sidecar(previous).await?;
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
    if let Err(error) = tokio::fs::rename(&temporary, &record.sidecar_path).await {
        let _ = tokio::fs::remove_file(&temporary).await;
        return Err(error.into());
    }
    Ok(())
}

async fn git_path(repository: &Path, args: &[&str]) -> Result<PathBuf> {
    let output = git::output(repository, args, false).await?;
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
    let output = git::output(worktree, &["rev-parse", flag], true).await?;
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
                required_verification: Vec::new(),
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

    #[tokio::test]
    async fn initial_sidecar_creation_preserves_existing_file_and_id_collision() {
        let root = tempfile::tempdir().unwrap();
        let db_file = root.path().join("store.sqlite");
        let db = SqliteStorage::open(&db_file).await.unwrap();
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
        let sidecar = base
            .path()
            .join(format!(".synroute-workspace-{workspace_id}.json"));
        let original = b"user-owned metadata; preserve exactly";
        tokio::fs::write(&sidecar, original).await.unwrap();

        let mut manager = PersistentWorkspaceManager::new(db.clone());
        assert!(
            manager
                .create(
                    execution_id,
                    workspace_id,
                    "fixture-repo".into(),
                    repository.path(),
                    base.path(),
                    revision.trim(),
                )
                .await
                .is_err()
        );
        assert_eq!(tokio::fs::read(&sidecar).await.unwrap(), original);
        let record = db.workspaces().workspace(workspace_id).await.unwrap();
        assert_eq!(record.status, WorkspaceStatus::Uncertain);
        assert_eq!(record.uncertainty_code.as_deref(), Some("existing_sidecar"));

        // A later attempt with the same workspace ID must not replace that sidecar either.
        assert!(
            manager
                .create(
                    execution_id,
                    workspace_id,
                    "fixture-repo".into(),
                    repository.path(),
                    base.path(),
                    revision.trim(),
                )
                .await
                .is_err()
        );
        assert_eq!(tokio::fs::read(&sidecar).await.unwrap(), original);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn workspace_uncertainty_reasons_survive_restart_for_validation_and_crash_states() {
        use std::os::unix::fs::symlink;

        for scenario in [
            "missing_sidecar",
            "path_mismatch",
            "registration_missing",
            "preparing",
            "removing",
        ] {
            let root = tempfile::tempdir().unwrap();
            let db_file = root.path().join("store.sqlite");
            let db = SqliteStorage::open(&db_file).await.unwrap();
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

            match scenario {
                "missing_sidecar" => tokio::fs::remove_file(&record.sidecar_path).await.unwrap(),
                "path_mismatch" => {
                    let moved = path.with_extension("moved");
                    tokio::fs::rename(&path, &moved).await.unwrap();
                    symlink(&moved, &path).unwrap();
                }
                "registration_missing" => {
                    let path_text = path.to_str().unwrap();
                    let output =
                        git::output(repository.path(), &["worktree", "remove", path_text], true)
                            .await
                            .unwrap();
                    assert!(output.status.success());
                    tokio::fs::create_dir(&path).await.unwrap();
                }
                "preparing" => {
                    // The status models a process crash after durable reservation.
                    let preparing = db.workspaces().workspace(workspace_id).await.unwrap();
                    let mut conn = db.pool().acquire().await.unwrap();
                    sqlx::query(
                        "UPDATE workspaces SET status = 'preparing' WHERE workspace_id = ?",
                    )
                    .bind(workspace_id.to_string())
                    .execute(&mut *conn)
                    .await
                    .unwrap();
                    drop(preparing);
                }
                "removing" => {
                    db.workspaces()
                        .set_status(workspace_id, WorkspaceStatus::Removing, None, None, None)
                        .await
                        .unwrap();
                }
                _ => unreachable!(),
            }

            let service = crate::recovery::RecoveryService::new(db.clone());
            let mut report = service
                .inspect_startup(None, chrono::Utc::now())
                .await
                .unwrap();
            service.apply_safe_recovery(&mut report).await.unwrap();

            let reopened = SqliteStorage::open(&db_file).await.unwrap();
            let persisted = reopened.workspaces().workspace(workspace_id).await.unwrap();
            assert_eq!(persisted.status, WorkspaceStatus::Uncertain, "{scenario}");
            assert!(persisted.uncertainty_code.is_some(), "{scenario}");
            assert!(persisted.uncertainty_detail.is_some(), "{scenario}");
        }
    }

    #[tokio::test]
    async fn uncertain_workspace_for_terminal_execution_remains_in_recovery_report() {
        use crate::{
            core::ExecutionState,
            storage::{ExecutionRepository, FailureClassification},
        };

        let root = tempfile::tempdir().unwrap();
        let db_file = root.path().join("store.sqlite");
        let db = SqliteStorage::open(&db_file).await.unwrap();
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
        manager
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
        db.executions()
            .transition(
                execution_id,
                ExecutionState::Failed,
                Some(FailureClassification::Unknown),
            )
            .await
            .unwrap();
        let record = db.workspaces().workspace(workspace_id).await.unwrap();
        tokio::fs::remove_file(&record.sidecar_path).await.unwrap();

        let service = crate::recovery::RecoveryService::new(db.clone());
        let mut report = service
            .inspect_startup(None, chrono::Utc::now())
            .await
            .unwrap();
        assert!(report.executions().is_empty());
        assert!(report.requires_attention());
        assert_eq!(report.workspace_issues().len(), 1);
        service.apply_safe_recovery(&mut report).await.unwrap();

        let reopened = SqliteStorage::open(&db_file).await.unwrap();
        let persisted = reopened.workspaces().workspace(workspace_id).await.unwrap();
        assert_eq!(persisted.status, WorkspaceStatus::Uncertain);
        assert_eq!(
            persisted.uncertainty_code.as_deref(),
            Some("sidecar_missing")
        );
        let next_report = crate::recovery::RecoveryService::new(reopened)
            .inspect_startup(None, chrono::Utc::now())
            .await
            .unwrap();
        assert_eq!(next_report.workspace_issues().len(), 1);
        assert!(next_report.requires_attention());
    }
}
