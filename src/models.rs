//! Provider-independent discovery and listing of OpenAI-compatible model registries.
use chrono::{DateTime, Utc};
use reqwest::{Client, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, env, path::PathBuf, time::Duration};
use thiserror::Error;

const REMOTE_ENV: &str = "SYNROUTE_MODELS_ALLOW_REMOTE";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProviderKind {
    LmStudio,
    LlamaCpp,
    OpenAiCompatible,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RegistryConfig {
    pub providers: Vec<ProviderConfig>,
}
impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            providers: vec![
                ProviderConfig::builtin(
                    "lm-studio",
                    ProviderKind::LmStudio,
                    "http://127.0.0.1:1234/v1",
                    true,
                ),
                ProviderConfig::builtin(
                    "llama-cpp",
                    ProviderKind::LlamaCpp,
                    "http://127.0.0.1:8080/v1",
                    true,
                ),
            ],
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ProviderConfig {
    pub id: String,
    pub name: String,
    pub kind: ProviderKind,
    pub base_url: String,
    pub local: bool,
    pub api_key_env: Option<String>,
    pub model_ids: Vec<String>,
    /// Explicitly configured metadata keyed by model ID.
    pub metadata: BTreeMap<String, ModelMetadata>,
    pub request_timeout_seconds: u64,
    pub enabled: bool,
}
impl Default for ProviderConfig {
    fn default() -> Self {
        Self {
            id: String::new(),
            name: String::new(),
            kind: ProviderKind::OpenAiCompatible,
            base_url: String::new(),
            local: false,
            api_key_env: None,
            model_ids: vec![],
            metadata: BTreeMap::new(),
            request_timeout_seconds: 15,
            enabled: true,
        }
    }
}
impl ProviderConfig {
    fn builtin(id: &str, kind: ProviderKind, url: &str, local: bool) -> Self {
        Self {
            id: id.into(),
            name: id.into(),
            kind,
            base_url: url.into(),
            local,
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct ModelMetadata {
    pub display_name: Option<String>,
    pub context_window: Option<u64>,
    pub capabilities: Option<Vec<String>>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelEntry {
    pub provider_id: String,
    pub provider_name: String,
    pub model_id: String,
    pub display_name: String,
    pub provider_reported_metadata: Value,
    pub user_configured_metadata: ModelMetadata,
    pub local: bool,
    pub context_window: Option<u64>,
    pub capabilities: Option<Vec<String>>,
    pub available: bool,
    pub last_checked: DateTime<Utc>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProviderResult {
    pub provider_id: String,
    pub available: bool,
    pub models: Vec<ModelEntry>,
    pub error: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistrySnapshot {
    pub providers: Vec<ProviderResult>,
    pub models: Vec<ModelEntry>,
}

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("invalid model registry configuration: {0}")]
    Config(String),
    #[error("provider is unreachable or timed out")]
    Unreachable,
    #[error("provider request timed out")]
    Timeout,
    #[error("provider authentication failed")]
    Authentication,
    #[error("provider returned HTTP status {0}")]
    Http(u16),
    #[error("provider returned an invalid model response")]
    InvalidResponse,
    #[error("provider has no discoverable or configured model IDs")]
    MissingModels,
    #[error("remote provider discovery requires SYNROUTE_MODELS_ALLOW_REMOTE=true")]
    RemoteConsent,
    #[error("API key environment variable '{0}' is not set")]
    MissingCredential(String),
}

impl RegistryConfig {
    pub async fn load() -> Result<Self, RegistryError> {
        let path = env::var_os("SYNROUTE_MODELS_CONFIG")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("synroute-models.json"));
        let config_exists = tokio::fs::try_exists(&path)
            .await
            .map_err(|e| RegistryError::Config(format!("could not inspect config file: {e}")))?;
        let mut config = if config_exists {
            let bytes = tokio::fs::read(&path)
                .await
                .map_err(|e| RegistryError::Config(e.to_string()))?;
            serde_json::from_slice(&bytes).map_err(|e| RegistryError::Config(e.to_string()))?
        } else {
            Self::default()
        };
        config.apply_env_overrides()?;
        config.validate()?;
        Ok(config)
    }
    fn apply_env_overrides(&mut self) -> Result<(), RegistryError> {
        if let Ok(value) = env::var("SYNROUTE_MODELS_LM_STUDIO_URL") {
            self.provider_mut("lm-studio").base_url = value;
        }
        if let Ok(value) = env::var("SYNROUTE_MODELS_LLAMA_CPP_URL") {
            self.provider_mut("llama-cpp").base_url = value;
        }
        if let Ok(value) = env::var("SYNROUTE_MODELS_REQUEST_TIMEOUT_SECONDS") {
            let timeout = value.parse().map_err(|_| {
                RegistryError::Config("invalid SYNROUTE_MODELS_REQUEST_TIMEOUT_SECONDS".into())
            })?;
            for p in &mut self.providers {
                p.request_timeout_seconds = timeout;
            }
        }
        Ok(())
    }
    fn provider_mut(&mut self, id: &str) -> &mut ProviderConfig {
        if let Some(i) = self.providers.iter().position(|p| p.id == id) {
            return &mut self.providers[i];
        }
        let (kind, url) = if id == "lm-studio" {
            (ProviderKind::LmStudio, "http://127.0.0.1:1234/v1")
        } else {
            (ProviderKind::LlamaCpp, "http://127.0.0.1:8080/v1")
        };
        self.providers
            .push(ProviderConfig::builtin(id, kind, url, true));
        self.providers.last_mut().unwrap()
    }
    fn validate(&self) -> Result<(), RegistryError> {
        let mut ids = std::collections::HashSet::new();
        for p in &self.providers {
            if p.id.trim().is_empty()
                || !ids.insert(&p.id)
                || p.name.trim().is_empty()
                || p.request_timeout_seconds == 0
                || p.request_timeout_seconds > 300
                || p.base_url.trim().is_empty()
                || p.base_url.contains('@')
            {
                return Err(RegistryError::Config(
                    "provider IDs must be unique and names, URLs, and timeout values must be valid"
                        .into(),
                ));
            }
            let url = reqwest::Url::parse(&p.base_url).map_err(|_| {
                RegistryError::Config(format!("invalid base URL for provider {}", p.id))
            })?;
            if !matches!(url.scheme(), "http" | "https") {
                return Err(RegistryError::Config(format!(
                    "provider {} URL must use HTTP or HTTPS",
                    p.id
                )));
            }
            if p.local
                && !url
                    .host_str()
                    .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "::1"))
            {
                return Err(RegistryError::Config(format!(
                    "provider {} is marked local but its base URL is not loopback",
                    p.id
                )));
            }
        }
        Ok(())
    }
}

pub async fn discover(config: &RegistryConfig, refresh: bool) -> RegistrySnapshot {
    let mut providers = Vec::new();
    let mut models = Vec::new();
    for p in config.providers.iter().filter(|p| p.enabled) {
        match discover_provider(p, refresh).await {
            Ok(items) => {
                models.extend(items.clone());
                providers.push(ProviderResult {
                    provider_id: p.id.clone(),
                    available: true,
                    models: items,
                    error: None,
                });
            }
            Err(e) => providers.push(ProviderResult {
                provider_id: p.id.clone(),
                available: false,
                models: Vec::new(),
                error: Some(e.to_string()),
            }),
        }
    }
    RegistrySnapshot { providers, models }
}

async fn discover_provider(
    p: &ProviderConfig,
    _refresh: bool,
) -> Result<Vec<ModelEntry>, RegistryError> {
    if !p.local && env::var(REMOTE_ENV).ok().as_deref() != Some("true") {
        return Err(RegistryError::RemoteConsent);
    }
    let mut ids = p.model_ids.clone();
    let mut raw: BTreeMap<String, Value> = BTreeMap::new();
    {
        let endpoint = format!("{}/models", p.base_url.trim_end_matches('/'));
        let client = Client::builder()
            .timeout(Duration::from_secs(p.request_timeout_seconds))
            .build()
            .map_err(|_| RegistryError::Config("could not initialize HTTP client".into()))?;
        let mut request = client.get(endpoint);
        if let Some(variable) = &p.api_key_env {
            let key = env::var(variable)
                .map_err(|_| RegistryError::MissingCredential(variable.clone()))?;
            request = request.bearer_auth(key);
        }
        let response = request.send().await.map_err(|error| {
            if error.is_timeout() {
                RegistryError::Timeout
            } else {
                RegistryError::Unreachable
            }
        })?;
        classify_status(response.status())?;
        let body: Value = response
            .json()
            .await
            .map_err(|_| RegistryError::InvalidResponse)?;
        append_model_rows(&body, &mut ids, &mut raw)?;
    }
    ids.sort();
    ids.dedup();
    build_models(p, ids, raw)
}

fn build_models(
    p: &ProviderConfig,
    mut ids: Vec<String>,
    mut raw: BTreeMap<String, Value>,
) -> Result<Vec<ModelEntry>, RegistryError> {
    ids.sort();
    ids.dedup();
    if ids.is_empty() {
        return Err(RegistryError::MissingModels);
    }
    let checked = Utc::now();
    Ok(ids
        .into_iter()
        .map(|id| {
            let reported = raw.remove(&id).unwrap_or(Value::Null);
            let user = p.metadata.get(&id).cloned().unwrap_or_default();
            let display = user
                .display_name
                .clone()
                .or_else(|| {
                    reported
                        .get("name")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
                .unwrap_or_else(|| id.clone());
            let context = user
                .context_window
                .or_else(|| reported.get("context_window").and_then(Value::as_u64));
            let capabilities = user.capabilities.clone().or_else(|| {
                reported
                    .get("capabilities")
                    .and_then(Value::as_array)
                    .map(|a| {
                        a.iter()
                            .filter_map(Value::as_str)
                            .map(str::to_owned)
                            .collect()
                    })
            });
            ModelEntry {
                provider_id: p.id.clone(),
                provider_name: p.name.clone(),
                model_id: id,
                display_name: display,
                provider_reported_metadata: reported,
                user_configured_metadata: user,
                local: p.local,
                context_window: context,
                capabilities,
                available: true,
                last_checked: checked,
            }
        })
        .collect())
}

fn classify_status(status: StatusCode) -> Result<(), RegistryError> {
    if status == StatusCode::UNAUTHORIZED || status == StatusCode::FORBIDDEN {
        Err(RegistryError::Authentication)
    } else if !status.is_success() {
        Err(RegistryError::Http(status.as_u16()))
    } else {
        Ok(())
    }
}

fn append_model_rows(
    body: &Value,
    ids: &mut Vec<String>,
    raw: &mut BTreeMap<String, Value>,
) -> Result<(), RegistryError> {
    let rows = body
        .get("data")
        .and_then(Value::as_array)
        .ok_or(RegistryError::InvalidResponse)?;
    for row in rows {
        let id = row
            .get("id")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or(RegistryError::InvalidResponse)?;
        ids.push(id.to_owned());
        raw.insert(id.to_owned(), row.clone());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn test_provider(base_url: String) -> ProviderConfig {
        ProviderConfig {
            id: "test".into(),
            name: "Test".into(),
            base_url,
            local: true,
            ..ProviderConfig::default()
        }
    }

    #[test]
    fn parses_and_validates_configuration() {
        let config: RegistryConfig = serde_json::from_str(
            r#"{"providers":[{"id":"x","name":"X","kind":"open_ai_compatible","base_url":"http://127.0.0.1:8000/v1","local":true}]}"#,
        )
        .unwrap();
        assert_eq!(config.providers[0].id, "x");
        assert!(config.validate().is_ok());
    }

    #[test]
    fn discovery_preserves_provenance_and_unknown_capabilities() {
        let mut provider = test_provider("http://127.0.0.1:1/v1".into());
        provider.metadata.insert(
            "m1".into(),
            ModelMetadata {
                display_name: Some("My model".into()),
                context_window: Some(4096),
                capabilities: None,
            },
        );
        let body = serde_json::json!({"data":[{"id":"m1","owned_by":"vendor"}]});
        let mut ids = vec![];
        let mut raw = BTreeMap::new();
        append_model_rows(&body, &mut ids, &mut raw).unwrap();
        let model = build_models(&provider, ids, raw).unwrap().remove(0);
        assert_eq!(model.display_name, "My model");
        assert_eq!(model.context_window, Some(4096));
        assert_eq!(model.capabilities, None);
        assert_eq!(model.provider_reported_metadata["owned_by"], "vendor");
        assert_eq!(model.user_configured_metadata.context_window, Some(4096));
    }

    #[test]
    fn handles_authentication_invalid_responses_and_unavailable_providers() {
        assert!(matches!(
            classify_status(StatusCode::UNAUTHORIZED),
            Err(RegistryError::Authentication)
        ));
        assert!(matches!(
            classify_status(StatusCode::FORBIDDEN),
            Err(RegistryError::Authentication)
        ));
        assert!(matches!(
            classify_status(StatusCode::SERVICE_UNAVAILABLE),
            Err(RegistryError::Http(503))
        ));
        assert!(matches!(classify_status(StatusCode::OK), Ok(())));
        let mut ids = vec![];
        let mut raw = BTreeMap::new();
        assert!(matches!(
            append_model_rows(&serde_json::json!({}), &mut ids, &mut raw),
            Err(RegistryError::InvalidResponse)
        ));
        assert!(matches!(
            build_models(&test_provider("http://127.0.0.1:1/v1".into()), vec![], raw),
            Err(RegistryError::MissingModels)
        ));
    }

    #[tokio::test]
    async fn missing_credentials_and_remote_consent_fail_closed() {
        let mut provider = test_provider("http://127.0.0.1:1/v1".into());
        provider.api_key_env = Some("SYNROUTE_TEST_ABSENT_KEY".into());
        assert!(matches!(
            discover_provider(&provider, true).await,
            Err(RegistryError::MissingCredential(_))
        ));
        provider.api_key_env = None;
        provider.local = false;
        assert!(matches!(
            discover_provider(&provider, true).await,
            Err(RegistryError::RemoteConsent)
        ));
    }

    #[tokio::test]
    async fn configured_models_and_json_output_are_available_offline() {
        let mut provider = test_provider("http://127.0.0.1:1/v1".into());
        provider.model_ids = vec!["offline-model".into()];
        let mut remote = test_provider("https://remote.example.invalid/v1".into());
        remote.id = "remote".into();
        remote.local = false;
        let models = build_models(&provider, provider.model_ids.clone(), BTreeMap::new()).unwrap();
        let snapshot = RegistrySnapshot {
            providers: vec![
                ProviderResult {
                    provider_id: provider.id,
                    available: true,
                    models: models.clone(),
                    error: None,
                },
                ProviderResult {
                    provider_id: remote.id,
                    available: false,
                    models: vec![],
                    error: Some(RegistryError::RemoteConsent.to_string()),
                },
            ],
            models,
        };
        assert_eq!(snapshot.models[0].model_id, "offline-model");
        assert!(
            serde_json::to_string(&snapshot)
                .unwrap()
                .contains("offline-model")
        );
    }

    #[test]
    fn empty_model_ids_report_missing_models() {
        let provider = ProviderConfig::default();
        assert!(
            RegistryConfig {
                providers: vec![provider]
            }
            .validate()
            .is_err()
        );
    }
}
