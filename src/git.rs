//! Runs Git with an isolated configuration and no repository-configured helpers.
//! This limits host-side behavior; it does not sandbox Git or the filesystem.

use anyhow::{Context, Result, bail};
use std::{
    collections::BTreeSet,
    path::{Path, PathBuf},
    process::Output,
};
use tokio::process::Command;
use uuid::Uuid;

struct HookDirectory(PathBuf);

impl HookDirectory {
    fn create() -> Result<Self> {
        for _ in 0..8 {
            let path = std::env::temp_dir().join(format!("synroute-git-hooks-{}", Uuid::new_v4()));
            let mut builder = std::fs::DirBuilder::new();
            #[cfg(unix)]
            {
                use std::os::unix::fs::DirBuilderExt;
                builder.mode(0o700);
            }
            match builder.create(&path) {
                Ok(()) => return Ok(Self(path)),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => {
                    return Err(error).context("create private empty Git hooks directory");
                }
            }
        }
        bail!("could not reserve a private Git hooks directory")
    }
}

impl Drop for HookDirectory {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir(&self.0) {
            if error.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(path = %self.0.display(), %error, "could not remove temporary Git hooks directory");
            }
        }
    }
}

/// Execute Git with user/system configuration and Git-affecting inherited environment removed.
/// For repository operations that might checkout or inspect worktree files, local filter driver
/// commands are also replaced with empty commands.
pub(crate) async fn output(root: &Path, args: &[&str], isolate_filters: bool) -> Result<Output> {
    let hooks = HookDirectory::create()?;
    let filters = if isolate_filters {
        configured_filter_drivers(root, &hooks).await?
    } else {
        BTreeSet::new()
    };
    run_with_config(root, args, &hooks, &filters).await
}

async fn configured_filter_drivers(root: &Path, hooks: &HookDirectory) -> Result<BTreeSet<String>> {
    let args = [
        "config",
        "--local",
        "--includes",
        "--name-only",
        "--null",
        "--list",
    ];
    let output = run_with_config(root, &args, hooks, &BTreeSet::new()).await?;
    if !output.status.success() {
        bail!(
            "could not safely inspect repository Git filter configuration: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    let mut drivers = BTreeSet::new();
    for key in output.stdout.split(|byte| *byte == 0) {
        let Ok(key) = std::str::from_utf8(key) else {
            bail!(
                "repository Git configuration contains a non-UTF-8 key; refusing helper isolation"
            )
        };
        let Some(suffix) = key.strip_prefix("filter.") else {
            continue;
        };
        for field in [".clean", ".smudge", ".process"] {
            if let Some(driver) = suffix.strip_suffix(field) {
                if driver.is_empty() {
                    bail!("repository Git configuration contains an invalid filter name")
                }
                drivers.insert(driver.to_owned());
            }
        }
    }
    Ok(drivers)
}

async fn run_with_config(
    root: &Path,
    args: &[&str],
    hooks: &HookDirectory,
    filters: &BTreeSet<String>,
) -> Result<Output> {
    let path = std::env::var_os("PATH").context("PATH is required to locate Git")?;
    let mut command = Command::new("git");
    command
        .env_clear()
        .env("PATH", path)
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("GIT_CONFIG_SYSTEM", null_config())
        .env("GIT_CONFIG_GLOBAL", null_config())
        .env("GIT_ATTR_NOSYSTEM", "1")
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GIT_PAGER", "cat")
        .args([
            "--no-pager".to_owned(),
            "-c".into(),
            format!("core.hooksPath={}", path_argument(&hooks.0)?),
            "-c".into(),
            "core.fsmonitor=false".into(),
            "-c".into(),
            "core.untrackedCache=false".into(),
            "-c".into(),
            "core.attributesFile=".into(),
            "-c".into(),
            "diff.external=".into(),
            "-c".into(),
            "interactive.diffFilter=".into(),
            "-c".into(),
            "core.pager=cat".into(),
        ])
        .current_dir(root);
    #[cfg(windows)]
    if let Some(system_root) = std::env::var_os("SystemRoot") {
        command.env("SystemRoot", system_root);
    }
    for driver in filters {
        for field in ["clean", "smudge", "process"] {
            command.arg("-c").arg(format!("filter.{driver}.{field}="));
        }
    }
    let output = command
        .args(args)
        .output()
        .await
        .context("could not start isolated Git subprocess")?;
    Ok(output)
}

fn null_config() -> &'static str {
    if cfg!(windows) { "NUL" } else { "/dev/null" }
}

fn path_argument(path: &Path) -> Result<&str> {
    path.to_str()
        .context("temporary Git hooks path is not UTF-8")
}
