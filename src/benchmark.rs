//! Repository-aware model benchmark prompts, execution, and transparent heuristic scoring.
use crate::{
    models::{self, ModelEntry, ProviderConfig, ProviderResult, RegistryConfig},
    openai,
    repository::profiler::RepositoryProfileDocument,
    storage::{BenchmarkRoundRecord, BenchmarkRunRecord, SqliteStorage},
};
use chrono::Utc;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};
use uuid::Uuid;

pub const BENCHMARK_VERSION: &str = "synroute-benchmark-v1";
pub const SCORING_VERSION: &str = "heuristic-v1";
const MAX_RESPONSE_BYTES: usize = 16_384;
const RESPONSE_TOKEN_LIMIT: u32 = 50;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scores {
    pub task_relevance: f64,
    pub repository_context: f64,
    pub technical_correctness: f64,
    pub conciseness: f64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelAggregate {
    pub provider_id: String,
    pub model_id: String,
    pub successful_rounds: usize,
    pub failed_rounds: usize,
    pub task_relevance: Option<f64>,
    pub repository_context: Option<f64>,
    pub technical_correctness: Option<f64>,
    pub conciseness: Option<f64>,
    pub average_latency_ms: Option<f64>,
    pub average_total_tokens: Option<f64>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkReport {
    pub run: BenchmarkRunRecord,
    pub rounds: Vec<BenchmarkRoundRecord>,
    pub aggregates: Vec<ModelAggregate>,
    pub unavailable_providers: Vec<ProviderResult>,
    pub scoring_notice: String,
}

pub fn generate_prompt(profile: &RepositoryProfileDocument) -> String {
    let languages = profile
        .languages
        .iter()
        .take(8)
        .map(|l| format!("{} ({})", l.name, l.files))
        .collect::<Vec<_>>()
        .join(", ");
    let manifests = profile
        .manifests
        .iter()
        .take(8)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    let tests = profile
        .tests
        .iter()
        .take(8)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    let directories = profile
        .directories
        .iter()
        .take(8)
        .map(|d| format!("{}: {}", d.path, d.responsibility))
        .collect::<Vec<_>>()
        .join("; ");
    format!(
        "You are given repository profile metadata, not source files. In at most 50 tokens, identify the project's type, its strongest language/framework clues, and one useful engineering focus suggested by the profile. Do not guess details absent from the profile.\nRepository: {}\nProject type: {}\nSummary: {}\nLanguages: {}\nManifests: {}\nTest paths: {}\nDirectory roles: {}",
        profile.repository.name,
        profile.project.project_type,
        profile.project.summary,
        languages,
        manifests,
        tests,
        directories
    )
}

pub fn prompt_hash(prompt: &str) -> String {
    hash(prompt.as_bytes())
}

pub fn repository_fingerprint(
    profile: &RepositoryProfileDocument,
) -> Result<String, serde_json::Error> {
    let structural = serde_json::to_vec(&(
        &profile.repository.root,
        &profile.repository.revision,
        &profile.project,
        &profile.languages,
        &profile.manifests,
        &profile.tests,
        &profile.directories,
    ))?;
    Ok(hash(&structural))
}
fn validate_rounds(rounds: u8) -> anyhow::Result<()> {
    anyhow::ensure!(
        (1..=3).contains(&rounds),
        "--rounds must be between 1 and 3"
    );
    Ok(())
}
fn validate_provider_consent(
    provider: &ProviderConfig,
    remote_allowed: bool,
) -> Result<(), String> {
    if !provider.local && !remote_allowed {
        Err("remote provider request requires SYNROUTE_MODELS_ALLOW_REMOTE=true".into())
    } else {
        Ok(())
    }
}
fn hash(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub fn parse_chat_response(value: &Value) -> Result<(String, TokenUsage), String> {
    let bytes = serde_json::to_vec(value).map_err(|e| e.to_string())?;
    let parsed = openai::parse_chat_body(&bytes, None, MAX_RESPONSE_BYTES)?;
    Ok((parsed.text, parsed.usage))
}
fn classify_provider_status(status: StatusCode) -> Result<(), String> {
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        Err("provider authentication failed".into())
    } else if !status.is_success() {
        Err(format!("provider returned HTTP status {}", status.as_u16()))
    } else {
        Ok(())
    }
}
pub use crate::openai::TokenUsage;

pub fn score_response(profile: &RepositoryProfileDocument, response: &str) -> Scores {
    let response_words = words(response);
    let relevance_terms = [
        "project",
        "repository",
        "language",
        "structure",
        "test",
        "engineering",
        "code",
        "manifest",
        "framework",
    ];
    let relevant = relevance_terms
        .iter()
        .filter(|term| response_words.contains(**term))
        .count();
    let task_relevance = (relevant as f64 / 3.0).min(1.0);

    let expected = words(&format!(
        "{} {} {}",
        profile.project.project_type,
        profile
            .languages
            .iter()
            .take(8)
            .map(|l| l.name.as_str())
            .collect::<Vec<_>>()
            .join(" "),
        profile
            .manifests
            .iter()
            .take(8)
            .map(String::as_str)
            .collect::<Vec<_>>()
            .join(" ")
    ));
    let expected_hits = expected
        .iter()
        .filter(|w| response_words.contains(*w))
        .count();
    let repository_context = if expected.is_empty() {
        0.0
    } else {
        (expected_hits as f64 / expected.len().min(5) as f64).min(1.0)
    };
    // This deterministic proxy flags known language contradictions; it cannot verify code-level claims.
    let supported_languages: std::collections::HashSet<String> = profile
        .languages
        .iter()
        .flat_map(|language| words(&language.name))
        .collect();
    let known_languages = [
        "rust",
        "python",
        "javascript",
        "typescript",
        "java",
        "go",
        "ruby",
        "php",
        "swift",
        "kotlin",
        "scala",
        "elixir",
        "haskell",
        "lua",
        "c",
        "cpp",
        "csharp",
    ];
    let claimed_languages: Vec<_> = known_languages
        .iter()
        .filter(|language| response_words.contains(**language))
        .collect();
    let contradictory_language = claimed_languages
        .iter()
        .any(|language| !supported_languages.contains(**language));
    let technical_correctness = if contradictory_language {
        0.0
    } else if claimed_languages
        .iter()
        .any(|language| supported_languages.contains(**language))
    {
        1.0
    } else {
        0.0
    };
    let word_count = response.split_whitespace().count();
    let conciseness = if word_count == 0 {
        0.0
    } else {
        (50.0 / word_count as f64).min(1.0)
    };
    Scores {
        task_relevance,
        repository_context,
        technical_correctness,
        conciseness,
    }
}
fn words(value: &str) -> std::collections::HashSet<String> {
    value
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|w| w.len() > 2)
        .map(str::to_ascii_lowercase)
        .collect()
}

pub fn aggregate(rounds: &[BenchmarkRoundRecord]) -> Vec<ModelAggregate> {
    let mut grouped: BTreeMap<(String, String), Vec<&BenchmarkRoundRecord>> = BTreeMap::new();
    for row in rounds {
        if let Some(model) = &row.model_id {
            grouped
                .entry((row.provider_id.clone(), model.clone()))
                .or_default()
                .push(row);
        }
    }
    grouped
        .into_iter()
        .map(|((provider_id, model_id), rows)| {
            let successes: Vec<_> = rows
                .iter()
                .copied()
                .filter(|r| {
                    r.status == "succeeded"
                        && r.response_text.as_deref().is_some_and(has_final_response)
                })
                .collect();
            let mean = |f: fn(&BenchmarkRoundRecord) -> Option<f64>| -> Option<f64> {
                let v: Vec<f64> = successes.iter().filter_map(|r| f(r)).collect();
                (!v.is_empty()).then(|| v.iter().sum::<f64>() / v.len() as f64)
            };
            ModelAggregate {
                provider_id,
                model_id,
                successful_rounds: successes.len(),
                failed_rounds: rows.len() - successes.len(),
                task_relevance: mean(|r| r.task_relevance),
                repository_context: mean(|r| r.repository_context),
                technical_correctness: mean(|r| r.technical_correctness),
                conciseness: mean(|r| r.conciseness),
                average_latency_ms: mean(|r| r.latency_ms.map(|v| v as f64)),
                average_total_tokens: mean(|r| r.total_tokens.map(|v| v as f64)),
            }
        })
        .collect()
}

/// Old benchmark rows may have been marked succeeded when a provider returned an
/// empty content string. Reject those and explicit reasoning-only tagged output.
pub fn has_final_response(response: &str) -> bool {
    if response.trim().is_empty() {
        return false;
    }
    let lower = response.to_ascii_lowercase();
    let mut visible = String::with_capacity(response.len());
    let mut cursor = 0;
    while let Some(relative_start) = lower[cursor..].find("<think>") {
        let start = cursor + relative_start;
        visible.push_str(&response[cursor..start]);
        let content_start = start + "<think>".len();
        if let Some(relative_end) = lower[content_start..].find("</think>") {
            cursor = content_start + relative_end + "</think>".len();
        } else {
            cursor = response.len();
            break;
        }
    }
    visible.push_str(&response[cursor..]);
    !visible.trim().is_empty()
}

pub async fn run(
    repository: &std::path::Path,
    config: &RegistryConfig,
    storage: &SqliteStorage,
    model_filter: Option<&str>,
    rounds_requested: u8,
    timeout: Duration,
) -> anyhow::Result<BenchmarkReport> {
    validate_rounds(rounds_requested)?;
    anyhow::ensure!(
        !timeout.is_zero() && timeout <= Duration::from_secs(300),
        "--timeout must be between 1 and 300 seconds"
    );
    let profile = crate::repository::profiler::profile(
        repository,
        &crate::repository::profiler::ProfileOptions {
            provider: None,
            model: None,
            allow_remote: false,
        },
    )
    .await?;
    let fingerprint = repository_fingerprint(&profile)?;
    let run = BenchmarkRunRecord {
        run_id: Uuid::new_v4(),
        repository_root: profile.repository.root.to_string_lossy().into(),
        repository_revision: profile.repository.revision.clone(),
        repository_fingerprint: fingerprint,
        benchmark_version: BENCHMARK_VERSION.into(),
        scoring_version: SCORING_VERSION.into(),
        requested_rounds: rounds_requested,
        created_at: Utc::now(),
    };
    let repo_storage = storage.benchmarks();
    repo_storage.create_run(&run).await?;
    let prompt = generate_prompt(&profile);
    let p_hash = prompt_hash(&prompt);
    let discovery = models::discover(config, true).await;
    let unavailable: Vec<_> = discovery
        .providers
        .iter()
        .filter(|p| !p.available)
        .cloned()
        .collect();
    let mut round_records = Vec::new();
    for provider_result in &unavailable {
        let mut row = BenchmarkRoundRecord::new(
            run.run_id,
            provider_result.provider_id.clone(),
            None,
            0,
            p_hash.clone(),
        );
        row.status = "unavailable".into();
        row.failure_detail = provider_result.error.as_ref().map(|s| bounded(s, 1000));
        repo_storage.insert_round(&row).await?;
        round_records.push(row);
    }
    for provider_result in discovery.providers.iter().filter(|p| p.available) {
        let provider = config
            .providers
            .iter()
            .find(|p| p.id == provider_result.provider_id)
            .expect("registry result corresponds to configured provider");
        for model in provider_result
            .models
            .iter()
            .filter(|m| m.provider_reported_metadata != Value::Null)
        {
            if model_filter.is_some_and(|filter| filter != model.model_id) {
                continue;
            }
            for round_number in 1..=rounds_requested {
                let started = Instant::now();
                let result = request_model(provider, model, &prompt, timeout).await;
                let mut row = BenchmarkRoundRecord::new(
                    run.run_id,
                    model.provider_id.clone(),
                    Some(model.model_id.clone()),
                    round_number,
                    p_hash.clone(),
                );
                row.latency_ms = Some(started.elapsed().as_millis().min(u64::MAX as u128) as u64);
                match result {
                    Ok((response, usage)) => {
                        let scores = score_response(&profile, &response);
                        row.response_hash = Some(hash(response.as_bytes()));
                        row.response_text = Some(response);
                        row.prompt_tokens = usage.prompt;
                        row.completion_tokens = usage.completion;
                        row.total_tokens = usage.total;
                        row.task_relevance = Some(scores.task_relevance);
                        row.repository_context = Some(scores.repository_context);
                        row.technical_correctness = Some(scores.technical_correctness);
                        row.conciseness = Some(scores.conciseness);
                        row.status = "succeeded".into();
                    }
                    Err(error) => {
                        row.status = "failed".into();
                        row.failure_detail = Some(bounded(&error.detail, 1000));
                        row.prompt_tokens = error.usage.prompt;
                        row.completion_tokens = error.usage.completion;
                        row.total_tokens = error.usage.total;
                    }
                }
                repo_storage.insert_round(&row).await?;
                round_records.push(row);
            }
        }
    }
    Ok(BenchmarkReport { aggregates: aggregate(&round_records), run, rounds: round_records, unavailable_providers: unavailable, scoring_notice: "Scores are deterministic heuristic signals. Heuristic scoring cannot prove technical correctness.".into() })
}

async fn request_model(
    provider: &ProviderConfig,
    model: &ModelEntry,
    prompt: &str,
    timeout: Duration,
) -> Result<(String, TokenUsage), RequestFailure> {
    validate_provider_consent(
        provider,
        std::env::var("SYNROUTE_MODELS_ALLOW_REMOTE")
            .ok()
            .as_deref()
            == Some("true"),
    )?;
    let duration = timeout.min(Duration::from_secs(provider.request_timeout_seconds));
    let url = format!(
        "{}/chat/completions",
        provider.base_url.trim_end_matches('/')
    );
    let api_key = provider
        .api_key_env
        .as_deref()
        .map(|variable| {
            std::env::var(variable)
                .map_err(|_| format!("API key environment variable '{variable}' is not set"))
        })
        .transpose()?;
    let body = openai::chat_request_body(
        &model.model_id,
        vec![serde_json::json!({"role":"user","content":prompt})],
        Some(RESPONSE_TOKEN_LIMIT),
    );
    let response = openai::post_chat(&url, api_key.as_deref(), body, duration, MAX_RESPONSE_BYTES)
        .await
        .map_err(|error| RequestFailure::new(error.to_string()))?;
    let status = StatusCode::from_u16(response.status)
        .map_err(|error| RequestFailure::new(error.to_string()))?;
    if let Err(status_error) = classify_provider_status(status) {
        let detail = match response.error_detail.filter(|detail| !detail.is_empty()) {
            Some(detail) => format!(
                "{status_error}; provider detail: {}",
                bounded(&detail, 1000)
            ),
            None => status_error,
        };
        return Err(RequestFailure::new(detail));
    }
    let parsed = openai::parse_chat_body(
        &response.body,
        response.content_type.as_deref(),
        MAX_RESPONSE_BYTES,
    )
    .map_err(|detail| RequestFailure {
        detail,
        usage: openai::usage_from_body(&response.body, response.content_type.as_deref()),
    })?;
    Ok((parsed.text, parsed.usage))
}

#[derive(Debug)]
struct RequestFailure {
    detail: String,
    usage: TokenUsage,
}

impl RequestFailure {
    fn new(detail: String) -> Self {
        Self {
            detail,
            usage: TokenUsage::default(),
        }
    }
}

impl From<String> for RequestFailure {
    fn from(detail: String) -> Self {
        Self::new(detail)
    }
}

impl From<&str> for RequestFailure {
    fn from(detail: &str) -> Self {
        Self::new(detail.to_owned())
    }
}

fn bounded(value: &str, max: usize) -> String {
    value.chars().take(max).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture_profile() -> RepositoryProfileDocument {
        serde_json::from_value(serde_json::json!({"schema_version":"1.0","repository":{"name":"sample","root":"/tmp/sample","revision":"abc123"},"project":{"project_type":"Rust application","summary":"Rust project with tests","confidence":0.8,"evidence":[]},"languages":[{"name":"Rust","files":4}],"manifests":["Cargo.toml"],"tests":["tests/basic.rs"],"directories":[],"files":[{"path":"src/main.rs","language":"Rust","task_types":["coding"],"confidence":0.8,"evidence":["PRIVATE_FILE_TEXT_SENTINEL"]}],"model_categories":[],"profiling":{"method":"deterministic","llm_provider":null,"files_considered":0,"files_excluded":0,"truncated":false}})).unwrap()
    }
    #[test]
    fn prompt_is_profile_specific_without_file_contents() {
        let p = generate_prompt(&fixture_profile());
        assert!(p.contains("Rust application"));
        assert!(p.contains("Cargo.toml"));
        assert!(!p.contains("PRIVATE_FILE_TEXT_SENTINEL"));
    }
    #[test]
    fn response_parsing_reads_actual_usage_and_bounds_shape() {
        let (text,usage)=parse_chat_response(&serde_json::json!({"choices":[{"message":{"content":"A Rust project"}}],"usage":{"prompt_tokens":20,"completion_tokens":4,"total_tokens":24}})).unwrap();
        assert_eq!(text, "A Rust project");
        assert_eq!(usage.total, Some(24));
        assert!(parse_chat_response(&serde_json::json!({})).is_err());
        let empty = serde_json::json!({
            "choices":[{"message":{"content":""},"finish_reason":"length"}],
            "usage":{"completion_tokens":50}
        });
        let error = parse_chat_response(&empty).unwrap_err();
        assert!(error.contains("empty"));
        assert!(error.contains("finish_reason=length"));
        let encoded = serde_json::to_vec(&empty).unwrap();
        assert_eq!(
            openai::usage_from_body(&encoded, Some("application/json")).completion,
            Some(50)
        );
    }
    #[test]
    fn benchmark_generation_budget_remains_fifty_tokens() {
        let body = openai::chat_request_body(
            "benchmark-model",
            vec![serde_json::json!({"role":"user", "content":"benchmark prompt"})],
            Some(RESPONSE_TOKEN_LIMIT),
        );
        assert_eq!(RESPONSE_TOKEN_LIMIT, 50);
        assert_eq!(body["max_tokens"], 50);
    }
    #[test]
    fn provider_error_redaction_keeps_diagnostic_but_not_credential() {
        let detail = openai::sanitize_error_detail(
            r#"{"error":{"message":"bad token secret-123"}}"#,
            Some("secret-123"),
        );
        assert!(detail.contains("bad token"));
        assert!(detail.contains("[REDACTED]"));
        assert!(!detail.contains("secret-123"));
    }
    #[test]
    fn provider_http_failures_are_classified_independently() {
        assert!(
            classify_provider_status(StatusCode::UNAUTHORIZED)
                .unwrap_err()
                .contains("authentication")
        );
        assert!(
            classify_provider_status(StatusCode::SERVICE_UNAVAILABLE)
                .unwrap_err()
                .contains("503")
        );
        assert!(classify_provider_status(StatusCode::OK).is_ok());
    }
    #[test]
    fn scores_are_deterministic_and_latency_is_not_a_score() {
        let p = fixture_profile();
        let a = score_response(&p, "Rust application repository project with tests");
        let b = score_response(&p, "Rust application repository project with tests");
        assert_eq!(a.task_relevance, b.task_relevance);
        assert_eq!(a.repository_context, b.repository_context);
        assert_eq!(a.technical_correctness, b.technical_correctness);
        assert_eq!(a.conciseness, 1.0);
        assert_eq!(a.technical_correctness, 1.0);
        let contradiction = score_response(&p, "Python application repository project with tests");
        assert_eq!(contradiction.technical_correctness, 0.0);
    }
    #[test]
    fn maximum_round_limit_is_three() {
        assert!(validate_rounds(3).is_ok());
        assert!(validate_rounds(4).is_err());
    }
    #[test]
    fn aggregates_only_actual_successes_and_separates_latency_tokens() {
        let mut a =
            BenchmarkRoundRecord::new(Uuid::new_v4(), "p".into(), Some("m".into()), 1, "p".into());
        a.status = "succeeded".into();
        a.response_text = Some("A usable response".into());
        a.task_relevance = Some(0.5);
        a.latency_ms = Some(100);
        a.total_tokens = Some(30);
        let mut b = a.clone();
        b.round_id = Uuid::new_v4();
        b.round_number = 2;
        b.task_relevance = Some(1.0);
        b.latency_ms = Some(200);
        b.status = "failed".into();
        let agg = aggregate(&[a, b]).remove(0);
        assert_eq!(agg.task_relevance, Some(0.5));
        assert_eq!(agg.average_latency_ms, Some(100.0));
        assert_eq!(agg.average_total_tokens, Some(30.0));
        assert_eq!(agg.failed_rounds, 1);
    }
    #[test]
    fn historical_empty_and_reasoning_only_success_rows_are_excluded() {
        let mut empty = BenchmarkRoundRecord::new(
            Uuid::new_v4(),
            "provider".into(),
            Some("model".into()),
            1,
            "prompt".into(),
        );
        empty.status = "succeeded".into();
        empty.response_text = Some("  \n".into());
        empty.task_relevance = Some(0.0);
        let mut reasoning = empty.clone();
        reasoning.round_id = Uuid::new_v4();
        reasoning.round_number = 2;
        reasoning.response_text = Some("<think>analysis without a final answer</think>".into());
        reasoning.task_relevance = Some(0.0);
        let mut valid = empty.clone();
        valid.round_id = Uuid::new_v4();
        valid.round_number = 3;
        valid.response_text = Some("A real answer".into());
        valid.task_relevance = Some(0.8);

        let aggregate = aggregate(&[empty, reasoning, valid]).remove(0);
        assert_eq!(aggregate.successful_rounds, 1);
        assert_eq!(aggregate.failed_rounds, 2);
        assert_eq!(aggregate.task_relevance, Some(0.8));
    }
    #[test]
    fn hashes_are_stable_and_provider_consent_is_checked_before_post() {
        assert_eq!(prompt_hash("same"), prompt_hash("same"));
        let p = ProviderConfig {
            local: false,
            ..ProviderConfig::default()
        };
        assert!(validate_provider_consent(&p, false).is_err());
        assert!(validate_provider_consent(&p, true).is_ok());
    }
}
