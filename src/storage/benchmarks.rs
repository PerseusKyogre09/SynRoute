use super::*;
use chrono::Utc;
use sqlx::Row;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkRunRecord {
    pub run_id: Uuid,
    pub repository_root: String,
    pub repository_revision: String,
    pub repository_fingerprint: String,
    pub benchmark_version: String,
    pub scoring_version: String,
    pub requested_rounds: u8,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkRoundRecord {
    pub round_id: Uuid,
    pub run_id: Uuid,
    pub provider_id: String,
    pub model_id: Option<String>,
    pub round_number: u8,
    pub prompt_hash: String,
    pub response_hash: Option<String>,
    pub response_text: Option<String>,
    pub prompt_tokens: Option<u64>,
    pub completion_tokens: Option<u64>,
    pub total_tokens: Option<u64>,
    pub latency_ms: Option<u64>,
    pub task_relevance: Option<f64>,
    pub repository_context: Option<f64>,
    pub technical_correctness: Option<f64>,
    pub conciseness: Option<f64>,
    pub status: String,
    pub failure_detail: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct SqliteBenchmarkRepository {
    pool: SqlitePool,
}
impl SqliteBenchmarkRepository {
    pub(crate) fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn create_run(&self, run: &BenchmarkRunRecord) -> Result<(), StorageError> {
        if !(1..=3).contains(&run.requested_rounds) || run.repository_fingerprint.is_empty() {
            return Err(StorageError::Invalid(
                "benchmark run fields are invalid".into(),
            ));
        }
        sqlx::query("INSERT INTO benchmark_runs (run_id, repository_root, repository_revision, repository_fingerprint, benchmark_version, scoring_version, requested_rounds, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(run.run_id.to_string()).bind(&run.repository_root).bind(&run.repository_revision).bind(&run.repository_fingerprint).bind(&run.benchmark_version).bind(&run.scoring_version).bind(i64::from(run.requested_rounds)).bind(timestamp(run.created_at)).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn insert_round(&self, round: &BenchmarkRoundRecord) -> Result<(), StorageError> {
        if round.round_number > 3
            || round
                .response_text
                .as_ref()
                .is_some_and(|s| s.len() > 16_384)
            || round
                .failure_detail
                .as_ref()
                .is_some_and(|s| s.len() > 1000)
            || [
                round.prompt_tokens,
                round.completion_tokens,
                round.total_tokens,
                round.latency_ms,
            ]
            .into_iter()
            .flatten()
            .any(|value| value > i64::MAX as u64)
            || [
                round.task_relevance,
                round.repository_context,
                round.technical_correctness,
                round.conciseness,
            ]
            .into_iter()
            .flatten()
            .any(|score| !score.is_finite() || !(0.0..=1.0).contains(&score))
            || !matches!(
                round.status.as_str(),
                "succeeded" | "failed" | "unavailable"
            )
        {
            return Err(StorageError::Invalid(
                "benchmark round exceeds supported bounds".into(),
            ));
        }
        sqlx::query("INSERT INTO benchmark_rounds (round_id, run_id, provider_id, model_id, round_number, prompt_hash, response_hash, response_text, prompt_tokens, completion_tokens, total_tokens, latency_ms, task_relevance, repository_context, technical_correctness, conciseness, status, failure_detail, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(round.round_id.to_string()).bind(round.run_id.to_string()).bind(&round.provider_id).bind(&round.model_id).bind(i64::from(round.round_number)).bind(&round.prompt_hash).bind(&round.response_hash).bind(&round.response_text).bind(round.prompt_tokens.map(|v| v as i64)).bind(round.completion_tokens.map(|v| v as i64)).bind(round.total_tokens.map(|v| v as i64)).bind(round.latency_ms.map(|v| v as i64)).bind(round.task_relevance).bind(round.repository_context).bind(round.technical_correctness).bind(round.conciseness).bind(&round.status).bind(&round.failure_detail).bind(timestamp(round.created_at)).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn rounds(&self, run_id: Uuid) -> Result<Vec<BenchmarkRoundRecord>, StorageError> {
        let rows = sqlx::query("SELECT * FROM benchmark_rounds WHERE run_id = ? ORDER BY provider_id, model_id, round_number")
            .bind(run_id.to_string()).fetch_all(&self.pool).await?;
        rows.iter().map(|r| decode_round(r, run_id)).collect()
    }

    pub async fn rounds_for_repository(
        &self,
        fingerprint: &str,
        benchmark_version: &str,
        scoring_version: &str,
    ) -> Result<Vec<BenchmarkRoundRecord>, StorageError> {
        let rows = sqlx::query("SELECT rounds.* FROM benchmark_rounds rounds JOIN benchmark_runs runs ON runs.run_id = rounds.run_id WHERE runs.repository_fingerprint = ? AND runs.benchmark_version = ? AND runs.scoring_version = ? ORDER BY runs.created_at, rounds.provider_id, rounds.model_id, rounds.round_number")
            .bind(fingerprint).bind(benchmark_version).bind(scoring_version).fetch_all(&self.pool).await?;
        rows.iter()
            .map(|r| {
                let run_id = Uuid::parse_str(&r.try_get::<String, _>("run_id")?)
                    .map_err(|e| StorageError::Invalid(e.to_string()))?;
                decode_round(r, run_id)
            })
            .collect()
    }
}

fn decode_round(
    row: &sqlx::sqlite::SqliteRow,
    run_id: Uuid,
) -> Result<BenchmarkRoundRecord, StorageError> {
    Ok(BenchmarkRoundRecord {
        round_id: Uuid::parse_str(&row.try_get::<String, _>("round_id")?)
            .map_err(|e| StorageError::Invalid(e.to_string()))?,
        run_id,
        provider_id: row.try_get("provider_id")?,
        model_id: row.try_get("model_id")?,
        round_number: row.try_get::<i64, _>("round_number")? as u8,
        prompt_hash: row.try_get("prompt_hash")?,
        response_hash: row.try_get("response_hash")?,
        response_text: row.try_get("response_text")?,
        prompt_tokens: row
            .try_get::<Option<i64>, _>("prompt_tokens")?
            .map(|v| v as u64),
        completion_tokens: row
            .try_get::<Option<i64>, _>("completion_tokens")?
            .map(|v| v as u64),
        total_tokens: row
            .try_get::<Option<i64>, _>("total_tokens")?
            .map(|v| v as u64),
        latency_ms: row
            .try_get::<Option<i64>, _>("latency_ms")?
            .map(|v| v as u64),
        task_relevance: row.try_get("task_relevance")?,
        repository_context: row.try_get("repository_context")?,
        technical_correctness: row.try_get("technical_correctness")?,
        conciseness: row.try_get("conciseness")?,
        status: row.try_get("status")?,
        failure_detail: row.try_get("failure_detail")?,
        created_at: parse_timestamp(&row.try_get::<String, _>("created_at")?)?,
    })
}

impl BenchmarkRoundRecord {
    pub fn new(
        run_id: Uuid,
        provider_id: String,
        model_id: Option<String>,
        round_number: u8,
        prompt_hash: String,
    ) -> Self {
        Self {
            round_id: Uuid::new_v4(),
            run_id,
            provider_id,
            model_id,
            round_number,
            prompt_hash,
            response_hash: None,
            response_text: None,
            prompt_tokens: None,
            completion_tokens: None,
            total_tokens: None,
            latency_ms: None,
            task_relevance: None,
            repository_context: None,
            technical_correctness: None,
            conciseness: None,
            status: "failed".into(),
            failure_detail: None,
            created_at: Utc::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn persists_benchmark_run_and_round_actual_response() {
        let dir = TempDir::new().unwrap();
        let db = SqliteStorage::open(dir.path().join("benchmarks.sqlite"))
            .await
            .unwrap();
        let run = BenchmarkRunRecord {
            run_id: Uuid::new_v4(),
            repository_root: "/repo".into(),
            repository_revision: "rev".into(),
            repository_fingerprint: "fingerprint".into(),
            benchmark_version: "v1".into(),
            scoring_version: "s1".into(),
            requested_rounds: 3,
            created_at: Utc::now(),
        };
        let repository = db.benchmarks();
        repository.create_run(&run).await.unwrap();
        let mut round = BenchmarkRoundRecord::new(
            run.run_id,
            "local".into(),
            Some("model-a".into()),
            1,
            "prompt-hash".into(),
        );
        round.response_text = Some("actual provider output".into());
        round.response_hash = Some("response-hash".into());
        round.status = "succeeded".into();
        round.total_tokens = Some(15);
        round.task_relevance = Some(0.75);
        repository.insert_round(&round).await.unwrap();
        let loaded = repository.rounds(run.run_id).await.unwrap();
        assert_eq!(loaded.len(), 1);
        assert_eq!(
            loaded[0].response_text.as_deref(),
            Some("actual provider output")
        );
        assert_eq!(loaded[0].total_tokens, Some(15));
        assert_eq!(loaded[0].task_relevance, Some(0.75));
        let repository_history = repository
            .rounds_for_repository("fingerprint", "v1", "s1")
            .await
            .unwrap();
        assert_eq!(repository_history.len(), 1);
        assert!(
            repository
                .rounds_for_repository("different-repository", "v1", "s1")
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn rejects_rounds_beyond_storage_bounds() {
        let dir = TempDir::new().unwrap();
        let db = SqliteStorage::open(dir.path().join("bounds.sqlite"))
            .await
            .unwrap();
        let repo = db.benchmarks();
        let mut row = BenchmarkRoundRecord::new(
            Uuid::new_v4(),
            "p".into(),
            Some("m".into()),
            4,
            "hash".into(),
        );
        assert!(repo.insert_round(&row).await.is_err());
        row.round_number = 1;
        row.response_text = Some("x".repeat(16_385));
        assert!(repo.insert_round(&row).await.is_err());
    }
}
