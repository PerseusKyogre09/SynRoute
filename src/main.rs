use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;
use synroute::{
    recovery::RecoveryService,
    sandbox::docker::DockerSandbox,
    storage::{ExecutionRepository, SqliteStorage},
};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(
    name = "synroute",
    about = "Repository-aware coding agent orchestration"
)]
struct Cli {
    #[arg(
        long,
        global = true,
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
    /// Run the fixed, explicitly authorized pytest contract in a Docker sandbox.
    Demo { repository: PathBuf },
    /// List persisted executions, newest first.
    History {
        #[arg(long, short, default_value_t = 20)]
        limit: u32,
        #[arg(long)]
        json: bool,
    },
    /// Show persisted lifecycle events, evidence, cleanup, and recovery details.
    Status {
        execution_id: uuid::Uuid,
        #[arg(long)]
        json: bool,
    },
    /// Summarize persisted execution and verification outcomes.
    Stats {
        #[arg(long)]
        json: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("synroute=info".parse()?))
        .json()
        .init();

    let cli = Cli::parse();
    let storage = SqliteStorage::from_url(&cli.database).await?;
    let recovery_service = RecoveryService::new(storage.clone());
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
        Command::Demo { repository } => {
            let result = synroute::demo::run(&repository, storage).await?;
            print_demo(result);
        }
        Command::History { limit, json } => {
            let rows = storage.executions().list_executions(Some(limit)).await?;
            let history = rows
                .iter()
                .map(synroute::observability::history_line)
                .collect::<Vec<_>>();
            if json {
                println!("{}", serde_json::to_string_pretty(&history)?);
            } else {
                print_history(&rows);
            }
        }
        Command::Status { execution_id, json } => {
            let snapshot = synroute::observability::status(&storage, execution_id).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&snapshot)?);
            } else {
                print_status(snapshot);
            }
        }
        Command::Stats { json } => {
            let stats = synroute::observability::stats(&storage).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&stats)?);
            } else {
                print_stats(stats);
            }
        }
    }
    Ok(())
}

fn print_history(records: &[synroute::storage::ExecutionRecord]) {
    if records.is_empty() {
        println!("No executions recorded yet.");
        return;
    }
    println!(
        "{:<36}  {:<20}  {:<22}  {:<25}  OUTCOME",
        "EXECUTION ID", "STATE", "CREATED", "REPOSITORY"
    );
    for record in records {
        println!(
            "{:<36}  {:<20?}  {:<22}  {:<25}  {}",
            record.id,
            record.state,
            record.created_at.format("%Y-%m-%d %H:%M:%S UTC"),
            record.repository_identity,
            record
                .terminal_outcome
                .map_or_else(|| "—".into(), |state| format!("{state:?}"))
        );
    }
}

fn print_status(snapshot: synroute::observability::StatusSnapshot) {
    let report = snapshot.report;
    println!("Execution: {}", report.execution.id);
    println!("Repository: {}", report.execution.repository_identity);
    println!(
        "State: {:?}   Outcome: {}",
        report.execution.state,
        report
            .execution
            .terminal_outcome
            .map_or_else(|| "not terminal".into(), |s| format!("{s:?}"))
    );
    println!("\nLifecycle timeline");
    if report.events.is_empty() {
        println!("  No lifecycle events recorded.");
    }
    for event in &report.events {
        println!(
            "  {}  {:?}  {}",
            event.occurred_at.format("%Y-%m-%d %H:%M:%S%.3f UTC"),
            event.event_type,
            event.payload
        );
    }
    println!("\nVerification evidence");
    if report.verifications.is_empty() {
        println!("  No verification evidence recorded (incomplete/unverified). ");
    }
    for (index, verification) in report.verifications.iter().enumerate() {
        println!(
            "  Report {}: {:?} (contract {})",
            index + 1,
            verification.report.status,
            verification.contract_version
        );
        for check in &verification.report.checks {
            println!(
                "    {}: {:?}; exit={:?}; duration={:?}; argv={:?}",
                check.name, check.status, check.exit_code, check.duration, check.argv
            );
            if let Some(detail) = &check.detail {
                println!("      detail: {detail}");
            }
            print_evidence("stdout", &check.stdout);
            print_evidence("stderr", &check.stderr);
            println!(
                "      container cleanup: {:?}{}",
                check.cleanup.status,
                if check.cleanup.container_may_remain {
                    " (container may remain)"
                } else {
                    ""
                }
            );
        }
        println!(
            "    sandbox session cleanup: {:?}",
            verification.report.session_cleanup.status
        );
    }
    println!("\nWorkspace cleanup");
    if report.workspaces.is_empty() {
        println!("  No managed workspace recorded.");
    }
    for workspace in &report.workspaces {
        println!(
            "  {}: {:?} at {}{}",
            workspace.workspace_id,
            workspace.status,
            workspace.canonical_path.display(),
            workspace
                .uncertainty_detail
                .as_ref()
                .map_or_else(String::new, |detail| format!(" — {detail}"))
        );
    }
    println!("\nDocker diagnostics");
    if report.container_diagnostics.is_empty() && report.container_intents.is_empty() {
        println!("  No container intents or cleanup diagnostics recorded.");
    }
    for intent in &report.container_intents {
        println!(
            "  Intent {}: {:?}; container id={:?}",
            intent.container_name, intent.status, intent.container_id
        );
    }
    for diagnostic in &report.container_diagnostics {
        println!(
            "  {:?}: {} — {} ({:?})",
            diagnostic.reason, diagnostic.container_name, diagnostic.detail, diagnostic.status
        );
    }
    println!("\nUnresolved recovery diagnostics");
    if snapshot.unresolved_recovery.is_empty() {
        println!("  None.");
    }
    for case in &snapshot.unresolved_recovery {
        println!("  {}: {} — {}", case.case_id, case.reason, case.details);
    }
}

fn print_evidence(label: &str, bytes: &[u8]) {
    if bytes.is_empty() {
        return;
    }
    let text = String::from_utf8_lossy(bytes);
    let mut excerpt = text.chars().take(800).collect::<String>();
    if text.chars().count() > 800 {
        excerpt.push_str("… [truncated for display]");
    }
    println!("      {label}: {}", excerpt.replace('\n', "\\n"));
}

fn print_stats(stats: synroute::observability::Stats) {
    println!("Execution history");
    println!("  Total: {}", stats.total_executions);
    println!("  Completed: {}", stats.completed);
    println!("  Failed: {}", stats.failed);
    println!("  Interrupted: {}", stats.interrupted);
    println!("  Cancelled: {}", stats.cancelled);
    println!("  Timed out: {}", stats.timed_out);
    println!("  Recovery required: {}", stats.recovery_required);
    println!(
        "  Verification pass rate: {}",
        stats.verification_pass_rate.map_or_else(
            || "n/a (no verification evidence)".into(),
            |rate| format!("{:.1}%", rate * 100.0)
        )
    );
    println!(
        "  Average verification duration: {}",
        stats
            .average_verification_duration_ms
            .map_or_else(|| stats.timing_note.to_owned(), |ms| format!("{ms:.1} ms"))
    );
    println!(
        "  Workspace cleanup succeeded: {}",
        stats.workspace_cleanup_succeeded
    );
    println!(
        "  Workspace cleanup failed / uncertain: {}",
        stats.workspace_cleanup_failed_or_uncertain
    );
    println!("\nBy repository");
    if stats.repositories.is_empty() {
        println!("  No repository history recorded.");
    }
    for (repository, group) in stats.repositories {
        let rate = if group.verifications_recorded == 0 {
            "n/a".into()
        } else {
            format!(
                "{:.1}%",
                100.0 * group.verifications_passed as f64 / group.verifications_recorded as f64
            )
        };
        println!(
            "  {repository}: {} executions; completed {}; failed {}; interrupted {}; cancelled {}; timed out {}; recovery required {}; verification {rate}",
            group.executions,
            group.completed,
            group.failed,
            group.interrupted,
            group.cancelled,
            group.timed_out,
            group.recovery_required
        );
    }
}

fn print_demo(result: synroute::demo::DemoResult) {
    use synroute::verification::CheckStatus;
    println!("Synroute local demo");
    println!("Repository: {}", result.profile.root.display());
    println!(
        "Detected: {} (framework detection unavailable)",
        result.profile.languages.join(", ")
    );
    println!(
        "Verification hints: {}",
        if result.profile.verification_hints.is_empty() {
            "none".into()
        } else {
            result
                .profile
                .verification_hints
                .iter()
                .map(|v| v.join(" "))
                .collect::<Vec<_>>()
                .join("; ")
        }
    );
    println!("Execution ID: {}", result.execution_id);
    println!(
        "Workspace: {}",
        result
            .workspace
            .as_ref()
            .map_or_else(|| "not created".into(), |p| p.display().to_string())
    );
    println!("Sandbox: {}", result.sandbox_status);
    if let Some(report) = result.verification {
        println!("Verification: {:?}", report.status);
        for check in report.checks {
            let status = match check.status {
                CheckStatus::Passed => "PASSED",
                CheckStatus::Failed => "FAILED",
                CheckStatus::TimedOut => "TIMED OUT",
                CheckStatus::Cancelled => "CANCELLED",
                CheckStatus::Skipped => "SKIPPED",
            };
            println!(
                "  {} — {} (exit: {})",
                check.name,
                status,
                check
                    .exit_code
                    .map_or_else(|| "none".into(), |c| c.to_string())
            );
            if let Some(detail) = check.detail {
                println!("    {detail}");
            }
        }
        println!("Docker cleanup: {:?}", report.session_cleanup.status);
    } else {
        println!("Verification: INCOMPLETE (not run)");
    }
    println!("Workspace cleanup: {}", result.cleanup);
    println!("Final state: {:?}", result.state);
}
