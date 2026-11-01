use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use synroute::{recovery::RecoveryService, sandbox::docker::DockerSandbox, storage::SqliteStorage};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "synroute",
    about = "Repository-aware coding agent orchestration"
)]
struct Cli {
    #[arg(
        long,
        env = "SYNROUTE_DATABASE",
        default_value = "sqlite://synroute.db"
    )]
    database: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Inspect repository identity and available verification hints.
    Inspect { repository: PathBuf },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("synroute=info".parse()?))
        .json()
        .init();

    let cli = Cli::parse();
    let storage = SqliteStorage::from_url(&cli.database).await?;
    let recovery_service = RecoveryService::new(storage);
    let mut recovery = recovery_service
        .inspect_startup(Some(&DockerSandbox::default()), chrono::Utc::now())
        .await?;
    recovery_service.apply_safe_recovery(&mut recovery).await?;
    if recovery.requires_attention() {
        tracing::warn!(recovery_report = ?recovery, "unfinished executions require manual recovery");
    }
    match cli.command {
        Command::Inspect { repository } => {
            let profile = synroute::repository::discover(&repository).await?;
            println!("{}", serde_json::to_string_pretty(&profile)?);
        }
    }
    Ok(())
}
