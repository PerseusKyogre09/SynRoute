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
    /// Profile a repository into a compact reusable JSON document.
    Profile {
        repository: PathBuf,
        #[arg(long)]
        output: Option<PathBuf>,
        /// OpenAI-compatible chat-completions URL (local by default; remote requires --allow-remote).
        #[arg(long)]
        provider: Option<String>,
        #[arg(long)]
        model: Option<String>,
        /// Explicitly consent to sending repository structure and paths to a remote provider.
        #[arg(long)]
        allow_remote: bool,
        #[arg(long)]
        json: bool,
    },
    /// Submit a coding task (currently fails closed when no safe agent adapter is available).
    Run {
        repository: PathBuf,
        #[arg(long)]
        task: String,
        #[arg(long, default_value_t = 1800)]
        timeout: u64,
        #[arg(long, default_value = "codex-app-server")]
        agent: String,
        #[arg(long)]
        json: bool,
    },
    /// Inspect repository identity and available verification hints.
    Inspect { repository: PathBuf },
    /// Show repository analysis, model selection/inference, and isolated execution status.
    Demo {
        repository: PathBuf,
        /// Add a task to demonstrate selection and inference before coding-agent orchestration.
        #[arg(long)]
        task: Option<String>,
        #[arg(long, default_value_t = 1800)]
        timeout: u64,
        #[arg(long)]
        json: bool,
    },
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
    /// Discover and list configured OpenAI-compatible model providers.
    Models {
        #[arg(long)]
        json: bool,
        /// Query providers now (otherwise configured IDs are listed without a request).
        #[arg(long)]
        refresh: bool,
    },
    /// Benchmark available configured models against a compact repository profile.
    Benchmark {
        repository: PathBuf,
        #[arg(long, default_value_t = 3)]
        rounds: u8,
        #[arg(long)]
        model: Option<String>,
        #[arg(long)]
        json: bool,
        #[arg(long, default_value_t = 60)]
        timeout: u64,
    },
    /// Select a currently available model using repository-specific benchmark evidence.
    Select {
        repository: PathBuf,
        #[arg(long)]
        task: String,
        #[arg(long)]
        json: bool,
    },
    /// Ask an available model using repository profile metadata, without running a coding agent.
    Ask {
        repository: PathBuf,
        #[arg(long)]
        task: String,
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
    if let Command::Models { json, refresh } = &cli.command {
        let config = synroute::models::RegistryConfig::load().await?;
        let snapshot = synroute::models::discover(&config, *refresh).await;
        if *json {
            println!("{}", serde_json::to_string_pretty(&snapshot)?);
        } else {
            for provider in snapshot.providers {
                println!(
                    "{}: {}",
                    provider.provider_id,
                    if provider.available {
                        "available"
                    } else {
                        "unavailable"
                    }
                );
                if let Some(error) = provider.error {
                    println!("  Error: {error}");
                }
                for model in provider.models {
                    println!(
                        "  {} — {} [{}] context={} capabilities={}",
                        model.model_id,
                        model.display_name,
                        if model.local { "local" } else { "remote" },
                        model
                            .context_window
                            .map_or_else(|| "unknown".into(), |n| n.to_string()),
                        model
                            .capabilities
                            .as_ref()
                            .map_or_else(|| "unknown".into(), |c| c.join(","))
                    );
                }
            }
        }
        return Ok(());
    }
    let command = match cli.command {
        Command::Profile {
            repository,
            output,
            provider,
            model,
            allow_remote,
            json,
        } => {
            let profile = synroute::repository::profiler::profile(
                &repository,
                &synroute::repository::profiler::ProfileOptions {
                    provider,
                    model,
                    allow_remote,
                },
            )
            .await?;
            let output =
                output.unwrap_or_else(|| profile.repository.root.join("synroute-profile.json"));
            let serialized = serde_json::to_string_pretty(&profile)?;
            tokio::fs::write(&output, format!("{serialized}\n")).await?;
            if json {
                println!("{serialized}");
            } else {
                println!(
                    "Profile written: {}\nProject type: {}\nLanguages: {}\nFiles profiled: {}",
                    output.display(),
                    profile.project.project_type,
                    profile
                        .languages
                        .iter()
                        .map(|l| l.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    profile.files.len()
                );
            }
            return Ok(());
        }
        Command::Inspect { repository } => {
            let profile = synroute::repository::discover(&repository).await?;
            println!("{}", serde_json::to_string_pretty(&profile)?);
            return Ok(());
        }
        command => command,
    };
    let storage = SqliteStorage::from_url(&cli.database).await?;
    let recovery_service = RecoveryService::new(storage.clone());
    let mut recovery = recovery_service
        .inspect_startup(Some(&DockerSandbox::default()), chrono::Utc::now())
        .await?;
    recovery_service.apply_safe_recovery(&mut recovery).await?;
    if recovery.requires_attention() {
        tracing::warn!(recovery_report = ?recovery, "unfinished executions require manual recovery");
    }
    match command {
        Command::Profile {
            repository,
            output,
            provider,
            model,
            allow_remote,
            json,
        } => {
            let profile = synroute::repository::profiler::profile(
                &repository,
                &synroute::repository::profiler::ProfileOptions {
                    provider,
                    model,
                    allow_remote,
                },
            )
            .await?;
            let root = profile.repository.root.clone();
            let output = output.unwrap_or_else(|| root.join("synroute-profile.json"));
            let serialized = serde_json::to_string_pretty(&profile)?;
            tokio::fs::write(&output, format!("{serialized}\n")).await?;
            if json {
                println!("{serialized}");
            } else {
                println!(
                    "Profile written: {}\nProject type: {}\nLanguages: {}\nFiles profiled: {}",
                    output.display(),
                    profile.project.project_type,
                    profile
                        .languages
                        .iter()
                        .map(|l| l.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    profile.files.len()
                );
            }
        }
        Command::Run {
            repository,
            task,
            timeout,
            agent,
            json,
        } => {
            anyhow::ensure!(
                agent == "codex-app-server",
                "unsupported agent selection '{agent}'"
            );
            let result = synroute::execution::coordinator::run(
                storage,
                &repository,
                &task,
                std::time::Duration::from_secs(timeout),
                &synroute::agent::CodexAppServer,
            )
            .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&result)?);
            } else {
                println!(
                    "Execution: {}\nState: {:?}\nProvider: {}\nWorkspace: {}\nFailure: {}",
                    result.execution_id,
                    result.state,
                    result.provider,
                    result
                        .workspace
                        .as_ref()
                        .map_or_else(|| "none".into(), |p| p.display().to_string()),
                    result.primary_error.as_deref().unwrap_or("none")
                );
                if let Some(error) = result.cleanup_error {
                    println!("Cleanup diagnostics: {error}");
                }
            }
            anyhow::ensure!(
                result.state == synroute::core::ExecutionState::Completed,
                "task did not complete; agent execution and independent verification are unavailable"
            )
        }
        Command::Inspect { repository } => {
            let profile = synroute::repository::discover(&repository).await?;
            println!("{}", serde_json::to_string_pretty(&profile)?);
        }
        Command::Demo {
            repository,
            task,
            timeout,
            json,
        } => {
            if let Some(task) = task {
                let config = synroute::models::RegistryConfig::load().await?;
                let report = synroute::demo::run_task(
                    &repository,
                    &task,
                    std::time::Duration::from_secs(timeout),
                    &config,
                    storage,
                )
                .await?;
                if json {
                    println!("{}", serde_json::to_string_pretty(&report)?);
                } else {
                    print_task_demo(report);
                }
            } else {
                let result = synroute::demo::run(&repository, storage).await?;
                print_demo(result);
            }
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
        Command::Benchmark {
            repository,
            rounds,
            model,
            json,
            timeout,
        } => {
            let config = synroute::models::RegistryConfig::load().await?;
            let report = synroute::benchmark::run(
                &repository,
                &config,
                &storage,
                model.as_deref(),
                rounds,
                std::time::Duration::from_secs(timeout),
            )
            .await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!(
                    "Benchmark run {} for {} ({})",
                    report.run.run_id, report.run.repository_root, report.run.repository_revision
                );
                println!("Scoring: {}", report.scoring_notice);
                for round in &report.rounds {
                    println!(
                        "{} / {} round {}: {} latency={}ms total_tokens={} relevance={:?} context={:?} technical={:?} concise={:?}",
                        round.provider_id,
                        round.model_id.as_deref().unwrap_or("unavailable"),
                        round.round_number,
                        round.status,
                        round
                            .latency_ms
                            .map_or_else(|| "n/a".into(), |v| v.to_string()),
                        round
                            .total_tokens
                            .map_or_else(|| "n/a".into(), |v| v.to_string()),
                        round.task_relevance,
                        round.repository_context,
                        round.technical_correctness,
                        round.conciseness
                    );
                    if let Some(detail) = &round.failure_detail {
                        println!("  Failure: {detail}");
                    }
                    if let Some(response) = &round.response_text {
                        println!("  Response: {response}");
                    }
                }
                println!("Aggregate scores");
                for aggregate in &report.aggregates {
                    println!(
                        "{} / {}: rounds={}/{} relevance={:?} context={:?} technical={:?} concise={:?} latency={:?}ms tokens={:?}",
                        aggregate.provider_id,
                        aggregate.model_id,
                        aggregate.successful_rounds,
                        aggregate.successful_rounds + aggregate.failed_rounds,
                        aggregate.task_relevance,
                        aggregate.repository_context,
                        aggregate.technical_correctness,
                        aggregate.conciseness,
                        aggregate.average_latency_ms,
                        aggregate.average_total_tokens
                    );
                }
                if report.rounds.is_empty() {
                    println!(
                        "No available models were benchmarked; the run was persisted with no invented results."
                    );
                }
            }
        }
        Command::Select {
            repository,
            task,
            json,
        } => {
            let config = synroute::models::RegistryConfig::load().await?;
            let report = synroute::selector::select(&repository, &task, &config, &storage).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!(
                    "Task category: {:?}\nSelection mode: {:?}",
                    report.task_category, report.mode
                );
                if let Some(selected) = &report.selected {
                    println!(
                        "Selected: {} / {} ({})",
                        selected.provider_id, selected.model_id, selected.display_name
                    );
                    if let Some(evidence) = &selected.benchmark_evidence {
                        println!(
                            "Evidence: {} successful stored round(s); derived rank score={:.3}; relevance={:?}, context={:?}, technical={:?}, conciseness={:?}, average latency={:?} ms",
                            evidence.successful_rounds,
                            selected.evidence_rank_score.unwrap(),
                            evidence.task_relevance,
                            evidence.repository_context,
                            evidence.technical_correctness,
                            evidence.conciseness,
                            evidence.average_latency_ms
                        );
                    } else {
                        println!(
                            "Fallback metadata: capability match={:?}, context window={:?}; no benchmark score is available.",
                            selected.capability_match, selected.context_window
                        );
                    }
                } else {
                    println!("No model selected.");
                }
                println!("{}", report.explanation);
                if !report.unavailable_providers.is_empty() {
                    println!("Unavailable providers:");
                    for provider in &report.unavailable_providers {
                        println!(
                            "  {}: {}",
                            provider.provider_id,
                            provider.error.as_deref().unwrap_or("unavailable")
                        );
                    }
                }
                if !report.available_models_without_evidence.is_empty() {
                    println!("Available models without benchmark evidence:");
                    for model in &report.available_models_without_evidence {
                        println!("  {} / {}", model.provider_id, model.model_id);
                    }
                }
                println!("Ranked models:");
                for model in &report.ranked_models {
                    println!(
                        "  {}. {} / {} — basis: {}",
                        model.rank, model.provider_id, model.model_id, model.ranking_basis
                    );
                }
            }
        }
        Command::Ask {
            repository,
            task,
            json,
        } => {
            let config = synroute::models::RegistryConfig::load().await?;
            let report = synroute::inference::ask(&repository, &task, &config, &storage).await?;
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                println!(
                    "Selection mode: {}",
                    synroute::inference::selection_mode_name(report.selection.mode)
                );
                println!("Task category: {:?}", report.selection.task_category);
                if let Some(selected) = &report.selection.selected {
                    println!(
                        "Provider/model: {} / {}",
                        selected.provider_id, selected.model_id
                    );
                    println!("{}", report.selection.explanation);
                } else {
                    println!("{}", report.selection.explanation);
                }
                if let Some(response) = report.inference {
                    println!("\nResponse:\n{}", response.response_text);
                    println!(
                        "\nLatency: {} ms; token usage: prompt={:?}, completion={:?}, total={:?}",
                        response.latency_ms,
                        response.token_usage.prompt,
                        response.token_usage.completion,
                        response.token_usage.total
                    );
                } else if let Some(error) = report.inference_error {
                    println!("Inference failed: {error}");
                }
            }
        }
        Command::Models { .. } => {
            unreachable!("models command returned before database initialization")
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

fn print_task_demo(report: synroute::demo::TaskDemoReport) {
    println!("SynRoute task demo");
    println!("\n1. Repository analysis");
    println!(
        "{} — {} at {} (fingerprint {})",
        report.repository.name,
        report.repository.project_type,
        report.repository.revision,
        report.repository.fingerprint
    );
    println!("Languages: {}", report.repository.languages.join(", "));

    println!("\n2. Model discovery and task classification");
    println!("Task category: {:?}", report.selection.task_category);
    println!("Selection mode: {:?}", report.selection.mode);
    for model in &report.selection.ranked_models {
        println!("Available: {} / {}", model.provider_id, model.model_id);
    }
    for provider in &report.selection.unavailable_providers {
        println!(
            "Unavailable provider: {} ({})",
            provider.provider_id,
            provider.error.as_deref().unwrap_or("unavailable")
        );
    }
    if let Some(selected) = &report.selection.selected {
        println!(
            "Recommendation: {} / {} ({})",
            selected.provider_id, selected.model_id, selected.display_name
        );
        match &selected.benchmark_evidence {
            Some(evidence) => println!(
                "Stored benchmark evidence: {} successful round(s); relevance={:?}, context={:?}, technical={:?}, conciseness={:?}, latency={:?} ms",
                evidence.successful_rounds,
                evidence.task_relevance,
                evidence.repository_context,
                evidence.technical_correctness,
                evidence.conciseness,
                evidence.average_latency_ms
            ),
            None => println!(
                "Benchmark evidence: none; recommendation uses labelled metadata fallback."
            ),
        }
        println!("Selection explanation: {}", report.selection.explanation);
    } else {
        println!("No available model was selected.");
        println!("{}", report.selection.explanation);
    }

    println!("\n3. Inference");
    if let Some(response) = &report.inference_response {
        println!(
            "Actual response from {} / {}:",
            response.provider_id, response.model_id
        );
        println!("{}", response.response_text);
        println!(
            "Latency: {} ms; token usage: {:?}",
            response.latency_ms, response.token_usage
        );
    } else if let Some(error) = &report.inference_error {
        println!("No response received: {error}");
    } else {
        println!("No inference was run because no model was available.");
    }
    println!("{}", report.inference_notice);

    println!("\n4. Coding-agent execution");
    println!("Status: {}", report.execution_status);
    if let Some(summary) = &report.execution_summary {
        println!(
            "Execution ID: {}; lifecycle state: {:?}",
            summary.execution_id, summary.state
        );
    }
    if let Some(error) = &report.execution_error {
        println!("Execution detail: {error}");
    }

    println!("\n5. Independent verification");
    println!("Status: {}", report.verification_status);
    if report.verification_evidence.is_empty() {
        println!("No persisted verification evidence is available.");
    } else {
        for record in &report.verification_evidence {
            println!(
                "{:?}: {} check(s)",
                record.report.status,
                record.report.checks.len()
            );
        }
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
