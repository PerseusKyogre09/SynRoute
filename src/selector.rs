//! Minimal evidence-based task model selector; it ranks only currently available models.
use crate::{
    benchmark::{self, BENCHMARK_VERSION, ModelAggregate, SCORING_VERSION},
    models::{self, ModelEntry, ModelMetadata, ProviderResult, RegistryConfig},
    storage::{BenchmarkRoundRecord, SqliteStorage},
};
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, collections::HashMap, path::Path};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskCategory {
    Coding,
    Debugging,
    Testing,
    Documentation,
    Reasoning,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SelectionMode {
    BenchmarkEvidence,
    MetadataFallback,
    NoAvailableModels,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvailableModelRef {
    pub provider_id: String,
    pub model_id: String,
    pub display_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankedModel {
    pub rank: usize,
    pub provider_id: String,
    pub model_id: String,
    pub display_name: String,
    pub local: bool,
    pub evidence_rank_score: Option<f64>,
    pub benchmark_evidence: Option<ModelAggregate>,
    pub capability_match: Option<bool>,
    pub context_window: Option<u64>,
    pub capabilities: Option<Vec<String>>,
    pub provider_reported_metadata: serde_json::Value,
    pub user_configured_metadata: ModelMetadata,
    pub ranking_basis: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectionReport {
    pub task: String,
    pub task_category: TaskCategory,
    pub repository_fingerprint: String,
    pub mode: SelectionMode,
    pub selected: Option<RankedModel>,
    pub ranked_models: Vec<RankedModel>,
    pub available_models_without_evidence: Vec<AvailableModelRef>,
    pub unavailable_providers: Vec<ProviderResult>,
    pub explanation: String,
}

pub fn classify_task(task: &str) -> TaskCategory {
    let text = task.to_ascii_lowercase();
    if contains_any(
        &text,
        &[
            "documentation",
            "document",
            "docs",
            "readme",
            "guide",
            "comment",
        ],
    ) {
        TaskCategory::Documentation
    } else if contains_any(
        &text,
        &[
            "test",
            "tests",
            "testing",
            "spec",
            "assertion",
            "coverage",
            "fixture",
        ],
    ) {
        TaskCategory::Testing
    } else if contains_any(
        &text,
        &[
            "debug",
            "bug",
            "fix",
            "repair",
            "crash",
            "error",
            "broken",
            "regression",
            "root cause",
        ],
    ) {
        TaskCategory::Debugging
    } else if contains_any(
        &text,
        &[
            "implement",
            "create",
            "add",
            "refactor",
            "code",
            "feature",
            "modify",
            "update",
        ],
    ) {
        TaskCategory::Coding
    } else {
        TaskCategory::Reasoning
    }
}
fn contains_any(text: &str, needles: &[&str]) -> bool {
    let words: Vec<_> = text.split(|c: char| !c.is_ascii_alphanumeric()).collect();
    needles.iter().any(|needle| {
        if needle.contains(' ') {
            text.contains(needle)
        } else {
            words.contains(needle)
        }
    })
}

pub async fn select(
    repository: &Path,
    task: &str,
    config: &RegistryConfig,
    storage: &SqliteStorage,
) -> anyhow::Result<SelectionReport> {
    let profile = crate::repository::profiler::profile(
        repository,
        &crate::repository::profiler::ProfileOptions {
            provider: None,
            model: None,
            allow_remote: false,
        },
    )
    .await?;
    select_profile(&profile, task, config, storage).await
}

pub async fn select_profile(
    profile: &crate::repository::profiler::RepositoryProfileDocument,
    task: &str,
    config: &RegistryConfig,
    storage: &SqliteStorage,
) -> anyhow::Result<SelectionReport> {
    anyhow::ensure!(!task.trim().is_empty(), "--task must not be empty");
    let fingerprint = benchmark::repository_fingerprint(profile)?;
    let discovery = models::discover(config, true).await;
    let available: Vec<ModelEntry> = discovery
        .models
        .into_iter()
        .filter(|model| {
            model.available && model.provider_reported_metadata != serde_json::Value::Null
        })
        .collect();
    let unavailable: Vec<ProviderResult> = discovery
        .providers
        .into_iter()
        .filter(|provider| !provider.available)
        .collect();
    let rounds = storage
        .benchmarks()
        .rounds_for_repository(&fingerprint, BENCHMARK_VERSION, SCORING_VERSION)
        .await?;
    Ok(rank(task, &fingerprint, &available, &rounds, unavailable))
}

pub fn rank(
    task: &str,
    fingerprint: &str,
    available: &[ModelEntry],
    rounds: &[BenchmarkRoundRecord],
    unavailable_providers: Vec<ProviderResult>,
) -> SelectionReport {
    let category = classify_task(task);
    let eligible: Vec<&ModelEntry> = available
        .iter()
        .filter(|model| {
            model.available && model.provider_reported_metadata != serde_json::Value::Null
        })
        .collect();
    let aggregates: HashMap<(String, String), ModelAggregate> = benchmark::aggregate(rounds)
        .into_iter()
        .map(|aggregate| {
            (
                (aggregate.provider_id.clone(), aggregate.model_id.clone()),
                aggregate,
            )
        })
        .collect();
    let mut evidenced = Vec::new();
    let mut without_evidence = Vec::new();
    for model in &eligible {
        let aggregate = aggregates
            .get(&(model.provider_id.clone(), model.model_id.clone()))
            .cloned();
        let score = aggregate.as_ref().and_then(|a| evidence_score(category, a));
        if let (Some(aggregate), Some(score)) = (aggregate, score) {
            evidenced.push(RankedModel {
                rank: 0,
                provider_id: model.provider_id.clone(),
                model_id: model.model_id.clone(),
                display_name: model.display_name.clone(),
                local: model.local,
                evidence_rank_score: Some(score),
                benchmark_evidence: Some(aggregate),
                capability_match: None,
                context_window: model.context_window,
                capabilities: model.capabilities.clone(),
                provider_reported_metadata: model.provider_reported_metadata.clone(),
                user_configured_metadata: model.user_configured_metadata.clone(),
                ranking_basis: "persisted benchmark evidence".into(),
            });
        } else {
            without_evidence.push(AvailableModelRef {
                provider_id: model.provider_id.clone(),
                model_id: model.model_id.clone(),
                display_name: model.display_name.clone(),
            });
        }
    }
    let mode;
    let mut ranked;
    let explanation;
    if !evidenced.is_empty() {
        mode = SelectionMode::BenchmarkEvidence;
        evidenced.sort_by(compare_evidence);
        for (index, row) in evidenced.iter_mut().enumerate() {
            row.rank = index + 1;
        }
        let best = &evidenced[0];
        let evidence = best.benchmark_evidence.as_ref().unwrap();
        explanation = format!(
            "Selected {} from {} successful stored benchmark round(s) for this repository. The task-weighted score {:.3} uses actual benchmark dimensions; average latency is {:?} ms and is used only to break score ties. Other currently available models without evidence are not compared against benchmark scores.",
            best.model_id,
            evidence.successful_rounds,
            best.evidence_rank_score.unwrap(),
            evidence.average_latency_ms
        );
        ranked = evidenced;
    } else if eligible.is_empty() {
        mode = SelectionMode::NoAvailableModels;
        ranked = Vec::new();
        explanation =
            "No currently available model was discovered, so no model was selected.".into();
    } else {
        mode = SelectionMode::MetadataFallback;
        ranked = eligible
            .iter()
            .map(|model| {
                let capability_match = model
                    .capabilities
                    .as_ref()
                    .map(|capabilities| capability_matches(category, capabilities));
                RankedModel {
                    rank: 0,
                    provider_id: model.provider_id.clone(),
                    model_id: model.model_id.clone(),
                    display_name: model.display_name.clone(),
                    local: model.local,
                    evidence_rank_score: None,
                    benchmark_evidence: None,
                    capability_match,
                    context_window: model.context_window,
                    capabilities: model.capabilities.clone(),
                    provider_reported_metadata: model.provider_reported_metadata.clone(),
                    user_configured_metadata: model.user_configured_metadata.clone(),
                    ranking_basis:
                        "available model metadata fallback; insufficient benchmark evidence".into(),
                }
            })
            .collect();
        ranked.sort_by(compare_metadata);
        for (index, row) in ranked.iter_mut().enumerate() {
            row.rank = index + 1;
        }
        explanation = "Insufficient benchmark evidence exists for this repository and scoring version. This fallback uses only reported or user-configured capability metadata and known context windows; it is not a benchmark score or performance claim.".into();
        without_evidence.clear();
    }
    let selected = ranked.first().cloned();
    SelectionReport {
        task: task.to_owned(),
        task_category: category,
        repository_fingerprint: fingerprint.to_owned(),
        mode,
        selected,
        ranked_models: ranked,
        available_models_without_evidence: without_evidence,
        unavailable_providers,
        explanation,
    }
}

fn weights(category: TaskCategory) -> [(f64, fn(&ModelAggregate) -> Option<f64>); 4] {
    match category {
        TaskCategory::Coding => [
            (0.25, |a| a.task_relevance),
            (0.25, |a| a.repository_context),
            (0.40, |a| a.technical_correctness),
            (0.10, |a| a.conciseness),
        ],
        TaskCategory::Debugging => [
            (0.20, |a| a.task_relevance),
            (0.30, |a| a.repository_context),
            (0.40, |a| a.technical_correctness),
            (0.10, |a| a.conciseness),
        ],
        TaskCategory::Testing => [
            (0.30, |a| a.task_relevance),
            (0.25, |a| a.repository_context),
            (0.35, |a| a.technical_correctness),
            (0.10, |a| a.conciseness),
        ],
        TaskCategory::Documentation => [
            (0.35, |a| a.task_relevance),
            (0.25, |a| a.repository_context),
            (0.15, |a| a.technical_correctness),
            (0.25, |a| a.conciseness),
        ],
        TaskCategory::Reasoning => [
            (0.30, |a| a.task_relevance),
            (0.30, |a| a.repository_context),
            (0.25, |a| a.technical_correctness),
            (0.15, |a| a.conciseness),
        ],
    }
}
fn evidence_score(category: TaskCategory, aggregate: &ModelAggregate) -> Option<f64> {
    if aggregate.successful_rounds == 0 {
        return None;
    }
    let (weighted, available_weight) =
        weights(category)
            .into_iter()
            .fold((0.0, 0.0), |(sum, total), (weight, get)| {
                match get(aggregate) {
                    Some(value) => (sum + value * weight, total + weight),
                    None => (sum, total),
                }
            });
    (available_weight > 0.0).then_some(weighted / available_weight)
}
fn compare_evidence(a: &RankedModel, b: &RankedModel) -> Ordering {
    b.evidence_rank_score
        .unwrap()
        .total_cmp(&a.evidence_rank_score.unwrap())
        .then_with(|| {
            compare_latency(
                a.benchmark_evidence
                    .as_ref()
                    .and_then(|e| e.average_latency_ms),
                b.benchmark_evidence
                    .as_ref()
                    .and_then(|e| e.average_latency_ms),
            )
        })
        .then_with(|| a.provider_id.cmp(&b.provider_id))
        .then_with(|| a.model_id.cmp(&b.model_id))
}
fn compare_latency(a: Option<f64>, b: Option<f64>) -> Ordering {
    match (a, b) {
        (Some(a), Some(b)) => a.total_cmp(&b),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}
fn compare_metadata(a: &RankedModel, b: &RankedModel) -> Ordering {
    metadata_match_rank(b.capability_match)
        .cmp(&metadata_match_rank(a.capability_match))
        .then_with(|| match (a.context_window, b.context_window) {
            (Some(a), Some(b)) => b.cmp(&a),
            (Some(_), None) => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (None, None) => Ordering::Equal,
        })
        .then_with(|| a.provider_id.cmp(&b.provider_id))
        .then_with(|| a.model_id.cmp(&b.model_id))
}
fn metadata_match_rank(value: Option<bool>) -> u8 {
    u8::from(value == Some(true))
}
fn capability_matches(category: TaskCategory, capabilities: &[String]) -> bool {
    let targets: &[&str] = match category {
        TaskCategory::Coding => &["coding", "code"],
        TaskCategory::Debugging => &["debugging", "coding", "code"],
        TaskCategory::Testing => &["testing", "test"],
        TaskCategory::Documentation => &["documentation", "docs"],
        TaskCategory::Reasoning => &["reasoning"],
    };
    capabilities
        .iter()
        .any(|capability| targets.contains(&capability.to_ascii_lowercase().as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::ModelMetadata;

    fn model(
        provider: &str,
        id: &str,
        capabilities: Option<Vec<&str>>,
        context: Option<u64>,
    ) -> ModelEntry {
        ModelEntry {
            provider_id: provider.into(),
            provider_name: provider.into(),
            model_id: id.into(),
            display_name: id.into(),
            provider_reported_metadata: serde_json::json!({"id":id}),
            user_configured_metadata: ModelMetadata::default(),
            local: true,
            context_window: context,
            capabilities: capabilities.map(|caps| caps.into_iter().map(str::to_owned).collect()),
            available: true,
            last_checked: chrono::Utc::now(),
        }
    }
    fn record(provider: &str, id: &str, score: f64, latency: u64) -> BenchmarkRoundRecord {
        let mut r = BenchmarkRoundRecord::new(
            uuid::Uuid::new_v4(),
            provider.into(),
            Some(id.into()),
            1,
            "hash".into(),
        );
        r.status = "succeeded".into();
        r.response_text = Some("repository project response".into());
        r.task_relevance = Some(score);
        r.repository_context = Some(0.7);
        r.technical_correctness = Some(0.8);
        r.conciseness = Some(0.6);
        r.latency_ms = Some(latency);
        r
    }

    #[test]
    fn classifies_task_categories() {
        assert_eq!(
            classify_task("Fix CPU arithmetic flags"),
            TaskCategory::Debugging
        );
        assert_eq!(classify_task("Add a parser feature"), TaskCategory::Coding);
        assert_eq!(classify_task("Write unit tests"), TaskCategory::Testing);
        assert_eq!(
            classify_task("Update the README guide"),
            TaskCategory::Documentation
        );
        assert_eq!(
            classify_task("Compare two approaches"),
            TaskCategory::Reasoning
        );
    }
    #[test]
    fn ranks_only_persisted_evidence_and_considers_latency() {
        let rows = [
            record("p", "slower", 0.9, 500),
            record("p", "faster", 0.9, 100),
        ];
        let report = rank(
            "Fix CPU flags",
            "fp",
            &[
                model("p", "slower", None, None),
                model("p", "faster", None, None),
            ],
            &rows,
            vec![],
        );
        assert_eq!(report.mode, SelectionMode::BenchmarkEvidence);
        assert_eq!(report.selected.unwrap().model_id, "faster");
        assert_eq!(
            report.ranked_models[0]
                .benchmark_evidence
                .as_ref()
                .unwrap()
                .successful_rounds,
            1
        );
    }
    #[test]
    fn missing_evidence_uses_metadata_fallback_without_scores() {
        let report = rank(
            "Add a feature",
            "fp",
            &[
                model("p", "b", Some(vec!["coding"]), Some(4096)),
                model("p", "a", None, Some(8192)),
            ],
            &[],
            vec![],
        );
        assert_eq!(report.mode, SelectionMode::MetadataFallback);
        assert!(
            report
                .explanation
                .contains("Insufficient benchmark evidence")
        );
        assert_eq!(report.selected.unwrap().model_id, "b");
        assert!(
            report
                .ranked_models
                .iter()
                .all(|m| m.evidence_rank_score.is_none() && m.benchmark_evidence.is_none())
        );
    }
    #[test]
    fn historical_empty_or_reasoning_only_rows_do_not_enable_evidence_selection() {
        let mut empty = record("p", "model", 1.0, 100);
        empty.response_text = Some(String::new());
        let mut reasoning = record("p", "model", 1.0, 100);
        reasoning.response_text = Some("<think>internal reasoning only</think>".into());
        let report = rank(
            "Fix CPU flags",
            "fp",
            &[model("p", "model", Some(vec!["coding"]), Some(4096))],
            &[empty, reasoning],
            vec![],
        );
        assert_eq!(report.mode, SelectionMode::MetadataFallback);
        assert!(report.selected.is_some());
        assert!(report.selected.unwrap().benchmark_evidence.is_none());
    }
    #[test]
    fn no_available_models_never_selects() {
        let unavailable = ProviderResult {
            provider_id: "offline".into(),
            available: false,
            models: vec![],
            error: Some("unavailable".into()),
        };
        let mut unavailable_model = model("offline", "model", Some(vec!["debugging"]), Some(8192));
        unavailable_model.available = false;
        let report = rank(
            "Fix bug",
            "fp",
            &[unavailable_model],
            &[],
            vec![unavailable],
        );
        assert_eq!(report.mode, SelectionMode::NoAvailableModels);
        assert!(report.selected.is_none());
        assert!(report.ranked_models.is_empty());
        assert_eq!(report.unavailable_providers.len(), 1);
    }
    #[test]
    fn deterministic_ties_use_provider_and_model_ids() {
        let items = [
            model("z", "same", None, None),
            model("a", "same", None, None),
        ];
        let rows = [record("z", "same", 0.8, 100), record("a", "same", 0.8, 100)];
        let first = rank("reason about code", "fp", &items, &rows, vec![]);
        let second = rank("reason about code", "fp", &items, &rows, vec![]);
        let ids = |r: &SelectionReport| {
            r.ranked_models
                .iter()
                .map(|m| format!("{}/{}", m.provider_id, m.model_id))
                .collect::<Vec<_>>()
        };
        assert_eq!(ids(&first), vec!["a/same", "z/same"]);
        assert_eq!(ids(&first), ids(&second));
    }
}
