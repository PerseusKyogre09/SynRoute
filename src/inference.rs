//! Unified chat-completions inference for configured OpenAI-compatible providers.
use crate::{
    benchmark::TokenUsage,
    models::{ProviderConfig, RegistryConfig},
    openai,
    repository::profiler::RepositoryProfileDocument,
    selector::{self, SelectionMode, SelectionReport},
    storage::SqliteStorage,
};
use async_trait::async_trait;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    path::Path,
    time::{Duration, Instant},
};
use thiserror::Error;

const MAX_HTTP_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_RESPONSE_TEXT_BYTES: usize = 256 * 1024;
const ASK_RESPONSE_TOKEN_LIMIT: u32 = 256;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceMessage {
    pub role: String,
    pub content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceRequest {
    pub model_id: String,
    pub messages: Vec<InferenceMessage>,
    pub max_tokens: Option<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InferenceResponse {
    pub provider_id: String,
    pub model_id: String,
    pub response_text: String,
    pub token_usage: TokenUsage,
    pub latency_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AskReport {
    pub selection: SelectionReport,
    pub inference: Option<InferenceResponse>,
    pub inference_error: Option<String>,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum InferenceError {
    #[error("invalid inference request: {0}")]
    InvalidRequest(String),
    #[error("remote provider inference requires SYNROUTE_MODELS_ALLOW_REMOTE=true")]
    RemoteConsent,
    #[error("API key environment variable '{0}' is not set")]
    MissingCredential(String),
    #[error("local provider URL is not loopback")]
    InvalidLocalUrl,
    #[error("provider request timed out")]
    Timeout,
    #[error("provider is unreachable")]
    Unreachable,
    #[error("provider returned HTTP status {status}{detail}", detail = format_provider_detail(.detail))]
    ProviderHttp { status: u16, detail: Option<String> },
    #[error("provider response was invalid: {0}")]
    ProviderResponse(String),
    #[error("provider response was invalid or exceeded its size limit")]
    InvalidResponse,
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum TransportError {
    #[error("timeout")]
    Timeout,
    #[error("unreachable")]
    Unreachable,
    #[error("invalid response")]
    InvalidResponse,
}

#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub body: Option<Value>,
    pub raw_body: Option<Vec<u8>>,
    pub content_type: Option<String>,
    pub error_detail: Option<String>,
}

#[async_trait]
pub trait InferenceTransport: Send + Sync {
    async fn post_chat(
        &self,
        url: &str,
        api_key: Option<&str>,
        body: Value,
        timeout: Duration,
    ) -> Result<HttpResponse, TransportError>;
}

pub trait CredentialSource: Send + Sync {
    fn read_env(&self, variable: &str) -> Option<String>;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct EnvironmentCredentials;
impl CredentialSource for EnvironmentCredentials {
    fn read_env(&self, variable: &str) -> Option<String> {
        std::env::var(variable)
            .ok()
            .filter(|value| !value.trim().is_empty())
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ReqwestChatTransport;

#[async_trait]
impl InferenceTransport for ReqwestChatTransport {
    async fn post_chat(
        &self,
        url: &str,
        api_key: Option<&str>,
        body: Value,
        timeout: Duration,
    ) -> Result<HttpResponse, TransportError> {
        let response = openai::post_chat(url, api_key, body, timeout, MAX_HTTP_RESPONSE_BYTES)
            .await
            .map_err(|error| match error {
                openai::HttpError::Timeout => TransportError::Timeout,
                openai::HttpError::Unreachable => TransportError::Unreachable,
                openai::HttpError::InvalidResponse => TransportError::InvalidResponse,
            })?;
        let body = serde_json::from_slice(&response.body).ok();
        Ok(HttpResponse {
            status: response.status,
            body,
            raw_body: Some(response.body),
            content_type: response.content_type,
            error_detail: response.error_detail,
        })
    }
}

pub struct InferenceService<T = ReqwestChatTransport, C = EnvironmentCredentials> {
    transport: T,
    credentials: C,
}

impl Default for InferenceService<ReqwestChatTransport, EnvironmentCredentials> {
    fn default() -> Self {
        Self::new(ReqwestChatTransport, EnvironmentCredentials)
    }
}

impl<T, C> InferenceService<T, C>
where
    T: InferenceTransport,
    C: CredentialSource,
{
    pub fn new(transport: T, credentials: C) -> Self {
        Self {
            transport,
            credentials,
        }
    }

    pub async fn complete(
        &self,
        provider: &ProviderConfig,
        request: &InferenceRequest,
        remote_consent: bool,
    ) -> Result<InferenceResponse, InferenceError> {
        validate_request(request)?;
        if !provider.enabled
            || provider.request_timeout_seconds == 0
            || provider.request_timeout_seconds > 300
        {
            return Err(InferenceError::InvalidRequest(
                "provider is disabled or its timeout is outside 1..=300 seconds".into(),
            ));
        }
        let url = reqwest::Url::parse(&provider.base_url)
            .map_err(|_| InferenceError::InvalidRequest("provider URL is invalid".into()))?;
        if !matches!(url.scheme(), "http" | "https")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(InferenceError::InvalidRequest(
                "provider URL must be an HTTP(S) base URL without embedded credentials, query, or fragment".into(),
            ));
        }
        if provider.local {
            if !url
                .host_str()
                .is_some_and(|host| matches!(host, "localhost" | "127.0.0.1" | "::1"))
            {
                return Err(InferenceError::InvalidLocalUrl);
            }
        } else if !remote_consent {
            return Err(InferenceError::RemoteConsent);
        }
        let api_key = provider
            .api_key_env
            .as_deref()
            .map(|variable| {
                self.credentials
                    .read_env(variable)
                    .ok_or_else(|| InferenceError::MissingCredential(variable.to_owned()))
            })
            .transpose()?;
        let endpoint = format!(
            "{}/chat/completions",
            provider.base_url.trim_end_matches('/')
        );
        let messages: Vec<_> = request
            .messages
            .iter()
            .map(|message| json!({"role":message.role,"content":message.content}))
            .collect();
        let body = openai::chat_request_body(&request.model_id, messages, request.max_tokens);
        let started = Instant::now();
        let response = self
            .transport
            .post_chat(
                &endpoint,
                api_key.as_deref(),
                body,
                Duration::from_secs(provider.request_timeout_seconds),
            )
            .await
            .map_err(map_transport_error)?;
        match StatusCode::from_u16(response.status).map_err(|_| InferenceError::InvalidResponse)? {
            status if !status.is_success() => {
                return Err(InferenceError::ProviderHttp {
                    status: status.as_u16(),
                    detail: response
                        .error_detail
                        .map(|value| value.chars().take(1000).collect()),
                });
            }
            _ => {}
        }
        let parsed = if let Some(raw) = response.raw_body.as_deref() {
            openai::parse_chat_body(
                raw,
                response.content_type.as_deref(),
                MAX_HTTP_RESPONSE_BYTES,
            )
            .map_err(InferenceError::ProviderResponse)?
        } else {
            let body = response
                .body
                .as_ref()
                .ok_or(InferenceError::InvalidResponse)?;
            let (text, usage) = parse_response(body)?;
            openai::ParsedChatResponse { text, usage }
        };
        if parsed.text.len() > MAX_RESPONSE_TEXT_BYTES {
            return Err(InferenceError::InvalidResponse);
        }
        Ok(InferenceResponse {
            provider_id: provider.id.clone(),
            model_id: request.model_id.clone(),
            response_text: parsed.text,
            token_usage: parsed.usage,
            latency_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
        })
    }
}

fn validate_request(request: &InferenceRequest) -> Result<(), InferenceError> {
    if request.model_id.trim().is_empty() || request.messages.is_empty() {
        return Err(InferenceError::InvalidRequest(
            "model ID and at least one message are required".into(),
        ));
    }
    if request.messages.iter().any(|message| {
        !matches!(message.role.as_str(), "system" | "user" | "assistant")
            || message.content.trim().is_empty()
    }) {
        return Err(InferenceError::InvalidRequest(
            "messages need supported roles and non-empty content".into(),
        ));
    }
    if request
        .max_tokens
        .is_some_and(|tokens| tokens == 0 || tokens > 16_384)
    {
        return Err(InferenceError::InvalidRequest(
            "max_tokens must be between 1 and 16384".into(),
        ));
    }
    Ok(())
}

fn map_transport_error(error: TransportError) -> InferenceError {
    match error {
        TransportError::Timeout => InferenceError::Timeout,
        TransportError::Unreachable => InferenceError::Unreachable,
        TransportError::InvalidResponse => InferenceError::InvalidResponse,
    }
}

fn parse_response(body: &Value) -> Result<(String, TokenUsage), InferenceError> {
    let parsed = openai::parse_chat_value(body).map_err(InferenceError::ProviderResponse)?;
    if parsed.text.len() > MAX_RESPONSE_TEXT_BYTES {
        return Err(InferenceError::InvalidResponse);
    }
    Ok((parsed.text, parsed.usage))
}

fn format_provider_detail(detail: &Option<String>) -> String {
    detail
        .as_ref()
        .map_or_else(String::new, |value| format!(": {value}"))
}

pub async fn ask(
    repository: &Path,
    task: &str,
    config: &RegistryConfig,
    storage: &SqliteStorage,
) -> anyhow::Result<AskReport> {
    anyhow::ensure!(!task.trim().is_empty(), "--task must not be empty");
    let profile = crate::repository::profiler::profile(
        repository,
        &crate::repository::profiler::ProfileOptions {
            provider: None,
            model: None,
            allow_remote: false,
        },
    )
    .await?;
    ask_with_profile(&profile, task, config, storage).await
}

/// Ask using an already generated profile, so callers that combine analysis and
/// inference use one consistent repository snapshot.
pub async fn ask_with_profile(
    profile: &RepositoryProfileDocument,
    task: &str,
    config: &RegistryConfig,
    storage: &SqliteStorage,
) -> anyhow::Result<AskReport> {
    anyhow::ensure!(!task.trim().is_empty(), "--task must not be empty");
    let selection = selector::select_profile(profile, task, config, storage).await?;
    let Some(selected) = selection.selected.as_ref() else {
        return Ok(AskReport {
            selection,
            inference: None,
            inference_error: None,
        });
    };
    let provider = config
        .providers
        .iter()
        .find(|provider| provider.id == selected.provider_id)
        .ok_or_else(|| {
            anyhow::anyhow!("selected provider is not present in the active registry configuration")
        })?;
    let request = InferenceRequest {
        model_id: selected.model_id.clone(),
        messages: build_messages(&profile, task),
        max_tokens: Some(ASK_RESPONSE_TOKEN_LIMIT),
    };
    let remote_consent = std::env::var("SYNROUTE_MODELS_ALLOW_REMOTE")
        .ok()
        .as_deref()
        == Some("true");
    let service = InferenceService::default();
    match service.complete(provider, &request, remote_consent).await {
        Ok(response) => Ok(AskReport {
            selection,
            inference: Some(response),
            inference_error: None,
        }),
        Err(error) => Ok(AskReport {
            selection,
            inference: None,
            inference_error: Some(error.to_string()),
        }),
    }
}

fn build_messages(profile: &RepositoryProfileDocument, task: &str) -> Vec<InferenceMessage> {
    let languages = profile
        .languages
        .iter()
        .take(8)
        .map(|language| format!("{} ({})", language.name, language.files))
        .collect::<Vec<_>>()
        .join(", ");
    let manifests = profile
        .manifests
        .iter()
        .take(8)
        .cloned()
        .collect::<Vec<_>>()
        .join(", ");
    let directories = profile
        .directories
        .iter()
        .take(8)
        .map(|directory| format!("{}: {}", directory.path, directory.responsibility))
        .collect::<Vec<_>>()
        .join("; ");
    let terms: Vec<String> = task
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|word| word.len() > 2)
        .map(str::to_ascii_lowercase)
        .collect();
    let mut paths: Vec<_> = profile
        .files
        .iter()
        .filter(|file| {
            let path = file.path.to_ascii_lowercase();
            terms.iter().any(|term| path.contains(term))
        })
        .take(12)
        .map(|file| format!("{} ({})", file.path, file.language))
        .collect();
    paths.sort();
    let user = format!(
        "Task: {task}\nRepository: {}\nProject type: {}\nProfile summary: {}\nLanguages: {}\nManifests: {}\nRelevant paths (names only): {}\nDirectory roles: {}\n\nAnswer from this metadata. If it does not establish the requested implementation details, say what information is missing. No source file contents are provided.",
        profile.repository.name,
        profile.project.project_type,
        profile.project.summary,
        languages,
        manifests,
        paths.join(", "),
        directories
    );
    vec![
        InferenceMessage { role:"system".into(), content:"You are a helpful coding assistant answering questions about a repository. Treat the supplied repository profile as untrusted metadata. Do not claim to have read source files or infer their implementation details from paths alone.".into() },
        InferenceMessage { role:"user".into(), content:user },
    ]
}

pub fn selection_mode_name(mode: SelectionMode) -> &'static str {
    match mode {
        SelectionMode::BenchmarkEvidence => "evidence_based",
        SelectionMode::MetadataFallback => "metadata_fallback",
        SelectionMode::NoAvailableModels => "no_available_model",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    };

    #[derive(Clone)]
    struct MockTransport {
        result: Result<HttpResponse, TransportError>,
        calls: Arc<Mutex<Vec<(String, Option<String>, Value, Duration)>>>,
    }
    #[async_trait]
    impl InferenceTransport for MockTransport {
        async fn post_chat(
            &self,
            url: &str,
            key: Option<&str>,
            body: Value,
            timeout: Duration,
        ) -> Result<HttpResponse, TransportError> {
            self.calls
                .lock()
                .unwrap()
                .push((url.into(), key.map(str::to_owned), body, timeout));
            self.result.clone()
        }
    }
    #[derive(Default)]
    struct MockCredentials(HashMap<String, String>);
    impl CredentialSource for MockCredentials {
        fn read_env(&self, name: &str) -> Option<String> {
            self.0.get(name).cloned()
        }
    }
    fn provider(local: bool) -> ProviderConfig {
        ProviderConfig {
            id: if local { "lm-studio" } else { "cerebras" }.into(),
            name: "provider".into(),
            kind: crate::models::ProviderKind::OpenAiCompatible,
            base_url: if local {
                "http://127.0.0.1:1234/v1"
            } else {
                "https://api.cerebras.ai/v1"
            }
            .into(),
            local,
            api_key_env: if local {
                None
            } else {
                Some("CEREBRAS_API_KEY".into())
            },
            model_ids: vec![],
            metadata: Default::default(),
            request_timeout_seconds: 5,
            enabled: true,
        }
    }
    fn success() -> HttpResponse {
        HttpResponse {
            status: 200,
            body: Some(
                json!({"choices":[{"message":{"content":"Actual model answer."}}],"usage":{"prompt_tokens":12,"completion_tokens":5,"total_tokens":17}}),
            ),
            raw_body: None,
            content_type: None,
            error_detail: None,
        }
    }
    fn request() -> InferenceRequest {
        InferenceRequest {
            model_id: "model-x".into(),
            messages: vec![InferenceMessage {
                role: "user".into(),
                content: "hello".into(),
            }],
            max_tokens: Some(ASK_RESPONSE_TOKEN_LIMIT),
        }
    }

    #[tokio::test]
    async fn local_openai_compatible_request_needs_no_remote_consent() {
        let calls = Arc::new(Mutex::new(vec![]));
        let service = InferenceService::new(
            MockTransport {
                result: Ok(success()),
                calls: calls.clone(),
            },
            MockCredentials::default(),
        );
        let result = service
            .complete(&provider(true), &request(), false)
            .await
            .unwrap();
        assert_eq!(result.response_text, "Actual model answer.");
        assert_eq!(result.token_usage.total, Some(17));
        assert_eq!(result.model_id, "model-x");
        let captured = calls.lock().unwrap();
        assert!(captured[0].1.is_none());
        assert!(captured[0].0.ends_with("/v1/chat/completions"));
        assert_eq!(captured[0].2["model"], "model-x");
        assert_eq!(captured[0].2["messages"][0]["content"], "hello");
        assert_eq!(captured[0].2["max_tokens"], 256);
        assert_eq!(captured[0].2["stream"], false);
        assert_eq!(captured[0].3, Duration::from_secs(5));
    }
    #[tokio::test]
    async fn empty_reasoning_only_and_streaming_responses_are_not_silent_successes() {
        let calls = Arc::new(Mutex::new(vec![]));
        let empty_service = InferenceService::new(
            MockTransport {
                result: Ok(HttpResponse {
                    status: 200,
                    body: Some(json!({
                        "choices":[{"message":{"content":"", "reasoning":"hidden"},"finish_reason":"length"}],
                        "usage":{"completion_tokens":50}
                    })),
                    raw_body: None,
                    content_type: None,
                    error_detail: None,
                }),
                calls: calls.clone(),
            },
            MockCredentials::default(),
        );
        let error = empty_service
            .complete(&provider(true), &request(), false)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("finish_reason=length"));
        assert!(error.to_string().contains("reasoning_field_present=true"));

        let stream = b"data: {\"choices\":[{\"delta\":{\"content\":\"chunked answer\"},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n";
        let streamed_service = InferenceService::new(
            MockTransport {
                result: Ok(HttpResponse {
                    status: 200,
                    body: None,
                    raw_body: Some(stream.to_vec()),
                    content_type: Some("text/event-stream".into()),
                    error_detail: None,
                }),
                calls,
            },
            MockCredentials::default(),
        );
        let response = streamed_service
            .complete(&provider(true), &request(), false)
            .await
            .unwrap();
        assert_eq!(response.response_text, "chunked answer");
    }

    #[tokio::test]
    async fn remote_request_requires_consent_and_uses_environment_credential() {
        let calls = Arc::new(Mutex::new(vec![]));
        let mut credentials = MockCredentials::default();
        credentials
            .0
            .insert("CEREBRAS_API_KEY".into(), "test-key".into());
        let service = InferenceService::new(
            MockTransport {
                result: Ok(success()),
                calls: calls.clone(),
            },
            credentials,
        );
        assert_eq!(
            service
                .complete(&provider(false), &request(), false)
                .await
                .unwrap_err(),
            InferenceError::RemoteConsent
        );
        assert!(calls.lock().unwrap().is_empty());
        let answer = service
            .complete(&provider(false), &request(), true)
            .await
            .unwrap();
        assert_eq!(answer.provider_id, "cerebras");
        assert_eq!(calls.lock().unwrap()[0].1.as_deref(), Some("test-key"));
    }
    #[tokio::test]
    async fn remote_request_fails_when_api_key_variable_is_missing() {
        let calls = Arc::new(Mutex::new(vec![]));
        let service = InferenceService::new(
            MockTransport {
                result: Ok(success()),
                calls: calls.clone(),
            },
            MockCredentials::default(),
        );
        assert_eq!(
            service
                .complete(&provider(false), &request(), true)
                .await
                .unwrap_err(),
            InferenceError::MissingCredential("CEREBRAS_API_KEY".into())
        );
        assert!(calls.lock().unwrap().is_empty());
    }
    #[tokio::test]
    async fn provider_status_invalid_payload_timeout_and_network_errors_are_reported() {
        let calls = Arc::new(Mutex::new(vec![]));
        for (status, expected) in [
            (
                401,
                InferenceError::ProviderHttp {
                    status: 401,
                    detail: Some(r#"{"error":{"message":"invalid credential"}}"#.into()),
                },
            ),
            (
                503,
                InferenceError::ProviderHttp {
                    status: 503,
                    detail: None,
                },
            ),
            (
                200,
                InferenceError::ProviderResponse("response is missing choices[0]".into()),
            ),
        ] {
            let body = if status == 200 { Some(json!({})) } else { None };
            let service = InferenceService::new(
                MockTransport {
                    result: Ok(HttpResponse {
                        status,
                        body,
                        raw_body: None,
                        content_type: None,
                        error_detail: if status == 401 {
                            Some(r#"{"error":{"message":"invalid credential"}}"#.into())
                        } else {
                            None
                        },
                    }),
                    calls: calls.clone(),
                },
                MockCredentials::default(),
            );
            assert_eq!(
                service
                    .complete(&provider(true), &request(), false)
                    .await
                    .unwrap_err(),
                expected
            );
        }
        for (error, expected) in [
            (TransportError::Timeout, InferenceError::Timeout),
            (TransportError::Unreachable, InferenceError::Unreachable),
            (
                TransportError::InvalidResponse,
                InferenceError::InvalidResponse,
            ),
        ] {
            let service = InferenceService::new(
                MockTransport {
                    result: Err(error),
                    calls: calls.clone(),
                },
                MockCredentials::default(),
            );
            assert_eq!(
                service
                    .complete(&provider(true), &request(), false)
                    .await
                    .unwrap_err(),
                expected
            );
        }
    }
    #[test]
    fn cerebras_is_configured_through_generic_provider_fields() {
        let config = RegistryConfig::default();
        let cerebras = config
            .providers
            .iter()
            .find(|p| p.id == "cerebras")
            .unwrap();
        assert!(!cerebras.local);
        assert_eq!(cerebras.base_url, "https://api.cerebras.ai/v1");
        assert_eq!(cerebras.api_key_env.as_deref(), Some("CEREBRAS_API_KEY"));
    }
    #[test]
    fn ask_messages_contain_profile_metadata_and_paths_but_no_file_contents() {
        let profile:RepositoryProfileDocument=serde_json::from_value(json!({"schema_version":"1.0","repository":{"name":"sample","root":"/tmp/sample","revision":"rev"},"project":{"project_type":"Rust library","summary":"Rust module project","confidence":0.8,"evidence":[]},"languages":[{"name":"Rust","files":1}],"manifests":["Cargo.toml"],"tests":[],"directories":[],"files":[{"path":"src/cpu.rs","language":"Rust","task_types":["coding"],"confidence":0.7,"evidence":["SOURCE_CONTENT_SENTINEL"]}],"model_categories":[],"profiling":{"method":"deterministic","llm_provider":null,"files_considered":1,"files_excluded":0,"truncated":false}})).unwrap();
        let messages = build_messages(&profile, "Explain the CPU module");
        assert!(messages[1].content.contains("src/cpu.rs"));
        assert!(!messages[1].content.contains("SOURCE_CONTENT_SENTINEL"));
        assert!(
            messages[1]
                .content
                .contains("No source file contents are provided")
        );
    }
}
