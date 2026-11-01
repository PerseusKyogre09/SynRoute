//! Response parsing shared by OpenAI-compatible inference and benchmark requests.
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;
use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum HttpError {
    #[error("provider request timed out")]
    Timeout,
    #[error("provider is unreachable")]
    Unreachable,
    #[error("provider response could not be read or exceeded its size limit")]
    InvalidResponse,
}

#[derive(Debug, Clone)]
pub struct HttpReply {
    pub status: u16,
    pub body: Vec<u8>,
    pub content_type: Option<String>,
    pub error_detail: Option<String>,
}

/// Shared bounded HTTP transport for inference and benchmarks.
pub async fn post_chat(
    url: &str,
    api_key: Option<&str>,
    body: Value,
    timeout: Duration,
    success_body_limit: usize,
) -> Result<HttpReply, HttpError> {
    let client = Client::builder()
        .timeout(timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|_| HttpError::Unreachable)?;
    let mut request = client.post(url).json(&body);
    if let Some(key) = api_key {
        request = request.bearer_auth(key);
    }
    let response = request.send().await.map_err(|error| {
        if error.is_timeout() {
            HttpError::Timeout
        } else {
            HttpError::Unreachable
        }
    })?;
    let status = response.status().as_u16();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    if (200..300).contains(&status)
        && response
            .content_length()
            .is_some_and(|length| length > success_body_limit as u64)
    {
        return Err(HttpError::InvalidResponse);
    }
    let limit = if (200..300).contains(&status) {
        success_body_limit
    } else {
        4096
    };
    let mut bytes = Vec::new();
    let mut response = response;
    while bytes.len() < limit {
        let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| HttpError::InvalidResponse)?
        else {
            break;
        };
        let remaining = limit - bytes.len();
        bytes.extend_from_slice(&chunk[..chunk.len().min(remaining)]);
    }
    let error_detail = if (200..300).contains(&status) || bytes.is_empty() {
        None
    } else {
        let detail = String::from_utf8_lossy(&bytes).into_owned();
        Some(sanitize_error_detail(&detail, api_key))
    };
    Ok(HttpReply {
        status,
        body: bytes,
        content_type,
        error_detail,
    })
}

pub fn sanitize_error_detail(detail: &str, api_key: Option<&str>) -> String {
    api_key.filter(|key| !key.is_empty()).map_or_else(
        || detail.to_owned(),
        |key| detail.replace(key, "[REDACTED]"),
    )
}

pub fn chat_request_body(model_id: &str, messages: Vec<Value>, max_tokens: Option<u32>) -> Value {
    let mut body = json!({
        "model": model_id,
        "messages": messages,
        "temperature": 0,
        "stream": false
    });
    if let Some(max_tokens) = max_tokens {
        body["max_tokens"] = json!(max_tokens);
    }
    body
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenUsage {
    pub prompt: Option<u64>,
    pub completion: Option<u64>,
    pub total: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedChatResponse {
    pub text: String,
    pub usage: TokenUsage,
}

/// Parse either a non-streaming chat completion JSON document or OpenAI SSE chunks.
/// Reasoning-only fields are deliberately not surfaced as answer text.
pub fn parse_chat_body(
    bytes: &[u8],
    content_type: Option<&str>,
    max_bytes: usize,
) -> Result<ParsedChatResponse, String> {
    if bytes.len() > max_bytes {
        return Err(format!(
            "provider response exceeded the {max_bytes} byte limit"
        ));
    }
    let is_sse = content_type
        .is_some_and(|kind| kind.to_ascii_lowercase().contains("text/event-stream"))
        || bytes
            .iter()
            .copied()
            .skip_while(u8::is_ascii_whitespace)
            .take(5)
            .eq(b"data:".iter().copied());
    if is_sse {
        parse_sse(bytes)
    } else {
        let value: Value = serde_json::from_slice(bytes).map_err(|_| {
            "provider returned invalid JSON (and was not a recognized event stream)".to_owned()
        })?;
        parse_chat_value(&value)
    }
}

pub fn parse_chat_value(value: &Value) -> Result<ParsedChatResponse, String> {
    let choice = value
        .pointer("/choices/0")
        .ok_or_else(|| "response is missing choices[0]".to_owned())?;
    let message = choice.get("message");
    let content = message
        .and_then(|message| message.get("content"))
        .and_then(content_text)
        .or_else(|| {
            choice
                .get("text")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .unwrap_or_default();
    let finish_reason = choice
        .get("finish_reason")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let reasoning_present = message.is_some_and(|message| {
        ["reasoning", "reasoning_content", "analysis"]
            .iter()
            .any(|field| message.get(field).is_some_and(nonempty_value))
    });
    if content.trim().is_empty() {
        return Err(format!(
            "assistant response content was empty (finish_reason={finish_reason}; reasoning_field_present={reasoning_present})"
        ));
    }
    let usage = parse_usage(value.get("usage"));
    Ok(ParsedChatResponse {
        text: content,
        usage,
    })
}

fn parse_sse(bytes: &[u8]) -> Result<ParsedChatResponse, String> {
    let body = std::str::from_utf8(bytes)
        .map_err(|_| "provider event stream was not valid UTF-8".to_owned())?;
    let mut text = String::new();
    let mut usage = TokenUsage::default();
    let mut finish_reason: Option<String> = None;
    let mut reasoning_present = false;
    let mut found_event = false;
    for line in body.lines() {
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let data = data.trim();
        if data.is_empty() || data == "[DONE]" {
            continue;
        }
        let event: Value = serde_json::from_str(data)
            .map_err(|_| "provider returned an invalid OpenAI event-stream chunk".to_owned())?;
        found_event = true;
        if let Some(chunk_usage) = event.get("usage") {
            usage = parse_usage(Some(chunk_usage));
        }
        if let Some(choice) = event.pointer("/choices/0") {
            if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
                finish_reason = Some(reason.to_owned());
            }
            if let Some(delta) = choice.get("delta") {
                if let Some(part) = delta.get("content").and_then(content_text) {
                    text.push_str(&part);
                }
                reasoning_present |= ["reasoning", "reasoning_content", "analysis"]
                    .iter()
                    .any(|field| delta.get(field).is_some_and(nonempty_value));
            }
        }
    }
    if !found_event {
        return Err("provider event stream contained no data events".to_owned());
    }
    if text.trim().is_empty() {
        return Err(format!(
            "assistant event stream contained no response content (finish_reason={}; reasoning_field_present={reasoning_present})",
            finish_reason.as_deref().unwrap_or("unknown")
        ));
    }
    Ok(ParsedChatResponse { text, usage })
}

fn content_text(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Array(parts) => {
            let mut result = String::new();
            for part in parts {
                if part
                    .get("type")
                    .and_then(Value::as_str)
                    .is_some_and(|kind| kind != "text")
                {
                    continue;
                }
                if let Some(text) = part.get("text").and_then(Value::as_str) {
                    result.push_str(text);
                }
            }
            Some(result)
        }
        _ => None,
    }
}

fn parse_usage(usage: Option<&Value>) -> TokenUsage {
    let token = |name: &str| {
        usage
            .and_then(|usage| usage.get(name))
            .and_then(Value::as_u64)
    };
    TokenUsage {
        prompt: token("prompt_tokens"),
        completion: token("completion_tokens"),
        total: token("total_tokens"),
    }
}

pub fn usage_from_body(bytes: &[u8], content_type: Option<&str>) -> TokenUsage {
    let is_sse = content_type
        .is_some_and(|kind| kind.to_ascii_lowercase().contains("text/event-stream"))
        || bytes
            .iter()
            .copied()
            .skip_while(u8::is_ascii_whitespace)
            .take(5)
            .eq(b"data:".iter().copied());
    if is_sse {
        let mut usage = TokenUsage::default();
        if let Ok(body) = std::str::from_utf8(bytes) {
            for line in body.lines() {
                if let Some(data) = line.strip_prefix("data:") {
                    if let Ok(event) = serde_json::from_str::<Value>(data.trim()) {
                        if let Some(value) = event.get("usage") {
                            usage = parse_usage(Some(value));
                        }
                    }
                }
            }
        }
        usage
    } else {
        serde_json::from_slice::<Value>(bytes)
            .ok()
            .map(|value| parse_usage(value.get("usage")))
            .unwrap_or_default()
    }
}

fn nonempty_value(value: &Value) -> bool {
    value.as_str().is_some_and(|text| !text.trim().is_empty())
        || value.as_array().is_some_and(|parts| !parts.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_text_parts_and_usage_from_chat_completion() {
        let parsed = parse_chat_value(&json!({
            "choices": [{"message": {"content": [
                {"type":"text", "text":"Answer "},
                {"type":"image", "url":"ignored"},
                {"type":"text", "text":"here."}
            ]}, "finish_reason":"stop"}],
            "usage":{"prompt_tokens":8,"completion_tokens":2,"total_tokens":10}
        }))
        .unwrap();
        assert_eq!(parsed.text, "Answer here.");
        assert_eq!(parsed.usage.total, Some(10));
    }

    #[test]
    fn empty_reasoning_only_response_is_an_error_with_safe_diagnostics() {
        let error = parse_chat_value(&json!({
            "choices":[{"message":{"content":"", "reasoning":"private reasoning text"},"finish_reason":"length"}],
            "usage":{"completion_tokens":50}
        })).unwrap_err();
        assert!(error.contains("finish_reason=length"));
        assert!(error.contains("reasoning_field_present=true"));
        assert!(!error.contains("private reasoning text"));
    }

    #[test]
    fn parses_openai_sse_content_and_usage() {
        let stream = b"data: {\"choices\":[{\"delta\":{\"content\":\"hello \"},\"finish_reason\":null}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"world\"},\"finish_reason\":\"stop\"}],\"usage\":{\"total_tokens\":3}}\n\ndata: [DONE]\n";
        let parsed = parse_chat_body(stream, Some("text/event-stream"), 1024).unwrap();
        assert_eq!(parsed.text, "hello world");
        assert_eq!(parsed.usage.total, Some(3));
    }

    #[test]
    fn raw_provider_error_detail_redacts_the_api_key() {
        let detail = sanitize_error_detail(
            r#"{"error":{"message":"invalid key secret-value"}}"#,
            Some("secret-value"),
        );
        assert!(detail.contains("invalid key"));
        assert!(detail.contains("[REDACTED]"));
        assert!(!detail.contains("secret-value"));
    }

    #[test]
    fn common_chat_request_builder_sets_non_streaming_and_limits_tokens() {
        let body = chat_request_body(
            "qwen2.5-coder-1.5b-instruct",
            vec![json!({"role":"user", "content":"probe"})],
            Some(256),
        );
        assert_eq!(body["model"], "qwen2.5-coder-1.5b-instruct");
        assert_eq!(body["max_tokens"], 256);
        assert_eq!(body["temperature"], 0);
        assert_eq!(body["stream"], false);
        assert_eq!(body["messages"][0]["content"], "probe");
    }
}
