use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
};
use tokio::process::Command;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepositoryProfile {
    /// Canonical Git repository root.
    pub root: PathBuf,
    /// Canonical path supplied by the caller; may be a nested working directory.
    pub working_directory: PathBuf,
    pub revision: String,
    pub languages: Vec<String>,
    /// Informational command hints only. Discovery never executes these commands.
    pub verification_hints: Vec<Vec<String>>,
}

pub async fn discover(path: &Path) -> Result<RepositoryProfile> {
    let working_directory = tokio::fs::canonicalize(path).await.with_context(|| {
        format!(
            "repository path '{}' does not exist or is inaccessible",
            path.display()
        )
    })?;
    if !tokio::fs::metadata(&working_directory).await?.is_dir() {
        bail!("repository path '{}' is not a directory", path.display());
    }
    let root_text = git(&working_directory, &["rev-parse", "--show-toplevel"])
        .await
        .context("path is not inside a Git repository")?;
    let root = PathBuf::from(root_text);
    let revision = git(&root, &["rev-parse", "HEAD"])
        .await
        .context("could not determine repository revision (repository may have no commits)")?;

    let mut languages = Vec::new();
    let mut hints = Vec::new();
    if exists(&root, "Cargo.toml").await? {
        languages.push("Rust".into());
        hints.push(vec!["cargo".into(), "test".into()]);
    }
    if exists(&root, "package.json").await? {
        languages.push("JavaScript/TypeScript".into());
    }
    if exists(&root, "pyproject.toml").await? || exists(&root, "requirements.txt").await? {
        languages.push("Python".into());
    }
    if languages.is_empty() {
        languages.push("Unknown".into());
    }
    Ok(RepositoryProfile {
        root,
        working_directory,
        revision,
        languages,
        verification_hints: hints,
    })
}

async fn exists(root: &Path, file: &str) -> Result<bool> {
    tokio::fs::try_exists(root.join(file))
        .await
        .with_context(|| {
            format!(
                "inspect manifest '{}': filesystem access failed",
                root.join(file).display()
            )
        })
}

async fn git(root: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(root)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await
        .context("could not start Git")?;
    if !output.status.success() {
        bail!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
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

    #[tokio::test]
    async fn discovers_repository_root_and_revision() {
        let dir = repo();
        let profile = discover(dir.path()).await.unwrap();
        assert_eq!(profile.root, dir.path().canonicalize().unwrap());
        assert_eq!(profile.working_directory, profile.root);
        assert_eq!(profile.revision.len(), 40);
    }

    #[tokio::test]
    async fn nested_path_preserves_working_directory_and_scans_repository_root() {
        let dir = repo();
        std::fs::write(
            dir.path().join("Cargo.toml"),
            "[package]\nname='fixture'\nversion='0.1.0'\nedition='2021'\n",
        )
        .unwrap();
        let nested = dir.path().join("src");
        std::fs::create_dir(&nested).unwrap();
        let profile = discover(&nested).await.unwrap();
        assert_eq!(profile.root, dir.path().canonicalize().unwrap());
        assert_eq!(profile.working_directory, nested.canonicalize().unwrap());
        assert_eq!(profile.languages, ["Rust"]);
    }

    #[tokio::test]
    async fn rejects_non_git_directory() {
        let dir = tempfile::tempdir().unwrap();
        let error = discover(dir.path()).await.unwrap_err().to_string();
        assert!(error.contains("not inside a Git repository"), "{error}");
    }

    #[tokio::test]
    async fn rejects_file_where_directory_is_expected() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("not-a-directory");
        std::fs::write(&file, "fixture").unwrap();
        let error = discover(&file).await.unwrap_err().to_string();
        assert!(error.contains("is not a directory"), "{error}");
    }

    #[tokio::test]
    async fn reports_missing_path() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("absent");
        let error = discover(&missing).await.unwrap_err().to_string();
        assert!(
            error.contains("does not exist or is inaccessible"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn detects_multiple_project_manifests_without_running_hints() {
        let dir = repo();
        for name in [
            "Cargo.toml",
            "package.json",
            "pyproject.toml",
            "requirements.txt",
        ] {
            std::fs::write(dir.path().join(name), "").unwrap();
        }
        let profile = discover(dir.path()).await.unwrap();
        assert_eq!(
            profile.languages,
            ["Rust", "JavaScript/TypeScript", "Python"]
        );
        assert_eq!(
            profile.verification_hints,
            [vec!["cargo".to_string(), "test".to_string()]]
        );
    }
}
