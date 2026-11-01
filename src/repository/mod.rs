use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
};
use tokio::process::Command;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepositoryProfile {
    pub root: PathBuf,
    pub git_root: PathBuf,
    pub revision: String,
    pub languages: Vec<String>,
    pub verification_hints: Vec<Vec<String>>,
}

pub async fn discover(path: &Path) -> Result<RepositoryProfile> {
    let root = tokio::fs::canonicalize(path)
        .await
        .context("canonicalize repository")?;
    let git_root = git(&root, &["rev-parse", "--show-toplevel"]).await?;
    let revision = git(&root, &["rev-parse", "HEAD"]).await?;
    let mut languages = Vec::new();
    let mut hints = Vec::new();
    if tokio::fs::try_exists(root.join("Cargo.toml")).await? {
        languages.push("Rust".into());
        hints.push(vec!["cargo".into(), "test".into()]);
    }
    if tokio::fs::try_exists(root.join("package.json")).await? {
        languages.push("JavaScript/TypeScript".into());
    }
    if tokio::fs::try_exists(root.join("pyproject.toml")).await?
        || tokio::fs::try_exists(root.join("requirements.txt")).await?
    {
        languages.push("Python".into());
    }
    if languages.is_empty() {
        languages.push("Unknown".into());
    }
    Ok(RepositoryProfile {
        root,
        git_root: PathBuf::from(git_root),
        revision,
        languages,
        verification_hints: hints,
    })
}

async fn git(root: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(root)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await?;
    if !output.status.success() {
        bail!(
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_owned())
}
