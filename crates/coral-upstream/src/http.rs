use std::collections::BTreeMap;

use coral_capabilities::{HttpMethod, ResponseTrust, is_json_media_type};
use reqwest::header::{CONTENT_TYPE, USER_AGENT};
use serde_json::{Map, Value};

use crate::model::structured_provider_error_detail;
use crate::{
    HttpRequestPlan, HttpUpstreamResponse, ProviderErrorKind, RedactableString, Result,
    UpstreamError, UpstreamRequestBody, UpstreamResponseBody,
};

pub(crate) const MAX_PROVIDER_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
pub(crate) const MAX_PROVIDER_ERROR_BODY_BYTES: usize = 64 * 1024;
pub(crate) const DEFAULT_USER_AGENT: &str = concat!("coral/", env!("CARGO_PKG_VERSION"));

pub(crate) async fn execute_http_plan(plan: &HttpRequestPlan) -> Result<HttpUpstreamResponse> {
    let client = upstream_http_client()?;
    let mut request = client.request(reqwest_method(plan.method), plan.url.clone());
    request = apply_plan_headers(request, &plan.headers);
    if let Some(timeout) = plan.timeout {
        request = request.timeout(timeout);
    }
    request = match &plan.body {
        Some(UpstreamRequestBody::Json(value)) => request.json(value),
        Some(UpstreamRequestBody::Empty) | None => request,
    };

    let response = request
        .send()
        .await
        .map_err(|error| UpstreamError::Transport(error.to_string()))?;
    let status = response.status().as_u16();
    let headers = response_headers(response.headers());
    let media_type = response_media_type(&headers);
    if !(200..300).contains(&status) {
        let body = limited_error_response_body(response).await?;
        return Err(UpstreamError::Provider {
            kind: ProviderErrorKind::HttpError,
            detail: http_provider_error_detail_from_preview(
                "HTTP provider",
                status,
                media_type.as_deref(),
                &body.bytes,
                body.body_truncated,
                body.body_bytes,
                body.body_bytes_exact,
            ),
        });
    }
    let bytes = limited_response_bytes(response, "HTTP provider").await?;
    Ok(HttpUpstreamResponse {
        status,
        headers,
        media_type: media_type.clone(),
        body: decode_response_body(media_type.as_deref(), &bytes)?,
        response_trust: ResponseTrust::UntrustedProviderData,
    })
}

pub(crate) fn apply_plan_headers(
    mut request: reqwest::RequestBuilder,
    headers: &[(String, RedactableString)],
) -> reqwest::RequestBuilder {
    if !headers
        .iter()
        .any(|(name, _value)| name.eq_ignore_ascii_case(USER_AGENT.as_str()))
    {
        request = request.header(USER_AGENT, DEFAULT_USER_AGENT);
    }
    for (name, value) in headers {
        request = request.header(name, value.expose_secret());
    }
    request
}

fn reqwest_method(method: HttpMethod) -> reqwest::Method {
    match method {
        HttpMethod::Get => reqwest::Method::GET,
        HttpMethod::Head => reqwest::Method::HEAD,
        HttpMethod::Options => reqwest::Method::OPTIONS,
        HttpMethod::Post => reqwest::Method::POST,
        HttpMethod::Put => reqwest::Method::PUT,
        HttpMethod::Patch => reqwest::Method::PATCH,
        HttpMethod::Delete => reqwest::Method::DELETE,
    }
}

pub(crate) fn upstream_http_client() -> Result<reqwest::Client> {
    // One process-wide client so reqwest's connection pool and TLS config are
    // reused across provider invocations instead of rebuilt per request. The
    // configuration is constant, so cache the first successful build.
    static CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
    if let Some(client) = CLIENT.get() {
        return Ok(client.clone());
    }
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| UpstreamError::Transport(error.to_string()))?;
    Ok(CLIENT.get_or_init(|| client).clone())
}

pub(crate) async fn limited_response_bytes(
    mut response: reqwest::Response,
    context: &str,
) -> Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|length| length > MAX_PROVIDER_RESPONSE_BYTES as u64)
    {
        return Err(UpstreamError::Provider {
            kind: ProviderErrorKind::InvalidResponse,
            detail: format!("{context} response exceeds {MAX_PROVIDER_RESPONSE_BYTES} bytes"),
        });
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| UpstreamError::Transport(error.to_string()))?
    {
        if bytes.len().saturating_add(chunk.len()) > MAX_PROVIDER_RESPONSE_BYTES {
            return Err(UpstreamError::Provider {
                kind: ProviderErrorKind::InvalidResponse,
                detail: format!("{context} response exceeds {MAX_PROVIDER_RESPONSE_BYTES} bytes"),
            });
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

pub(crate) struct ProviderErrorBodyPreview {
    pub(crate) bytes: Vec<u8>,
    pub(crate) body_bytes: usize,
    pub(crate) body_bytes_exact: bool,
    pub(crate) body_truncated: bool,
}

pub(crate) async fn limited_error_response_body(
    mut response: reqwest::Response,
) -> Result<ProviderErrorBodyPreview> {
    let content_length = response
        .content_length()
        .and_then(|length| usize::try_from(length).ok());
    let mut bytes = Vec::new();
    let mut observed_bytes = 0usize;
    let mut body_truncated = false;
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| UpstreamError::Transport(error.to_string()))?
    {
        observed_bytes = observed_bytes.saturating_add(chunk.len());
        if bytes.len() < MAX_PROVIDER_ERROR_BODY_BYTES {
            let remaining = MAX_PROVIDER_ERROR_BODY_BYTES - bytes.len();
            let copy_len = chunk.len().min(remaining);
            let Some(preview_chunk) = chunk.get(..copy_len) else {
                return Err(UpstreamError::InvalidResponse(
                    "failed to read provider error body preview".to_string(),
                ));
            };
            bytes.extend_from_slice(preview_chunk);
        }
        if bytes.len() >= MAX_PROVIDER_ERROR_BODY_BYTES {
            if let Some(content_length) = content_length {
                body_truncated = content_length > bytes.len();
                break;
            }
            if observed_bytes > bytes.len() {
                body_truncated = true;
                break;
            }
        }
    }
    let (body_bytes, body_bytes_exact) = match content_length {
        Some(content_length) => (content_length, true),
        None => (observed_bytes, !body_truncated),
    };
    Ok(ProviderErrorBodyPreview {
        bytes,
        body_bytes,
        body_bytes_exact,
        body_truncated,
    })
}

pub(crate) fn response_headers(headers: &reqwest::header::HeaderMap) -> BTreeMap<String, String> {
    headers
        .iter()
        .filter_map(|(name, value)| {
            value
                .to_str()
                .ok()
                .map(|value| (name.as_str().to_ascii_lowercase(), value.to_string()))
        })
        .collect()
}

pub(crate) fn response_media_type(headers: &BTreeMap<String, String>) -> Option<String> {
    headers
        .get(CONTENT_TYPE.as_str())
        .and_then(|value| value.split(';').next())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn decode_response_body(media_type: Option<&str>, bytes: &[u8]) -> Result<UpstreamResponseBody> {
    if bytes.is_empty() {
        return Ok(UpstreamResponseBody::Empty);
    }
    if media_type.is_some_and(is_json_media_type) {
        let value = serde_json::from_slice(bytes)
            .map_err(|error| UpstreamError::InvalidResponse(error.to_string()))?;
        return Ok(UpstreamResponseBody::Json(value));
    }
    match std::str::from_utf8(bytes) {
        Ok(text) => Ok(UpstreamResponseBody::Text(text.to_string())),
        Err(_) => Ok(UpstreamResponseBody::Bytes(bytes.to_vec())),
    }
}

pub(crate) fn http_provider_error_detail(
    context: &str,
    status: u16,
    media_type: Option<&str>,
    bytes: &[u8],
) -> String {
    http_provider_error_detail_from_preview(
        context,
        status,
        media_type,
        bytes,
        bytes.len() > MAX_PROVIDER_ERROR_BODY_BYTES,
        bytes.len(),
        true,
    )
}

pub(crate) fn http_provider_error_detail_from_preview(
    context: &str,
    status: u16,
    media_type: Option<&str>,
    bytes: &[u8],
    body_truncated: bool,
    body_bytes: usize,
    body_bytes_exact: bool,
) -> String {
    let (body, body_encoding, preview_bytes) = provider_error_body_value(media_type, bytes);
    let mut detail = Map::new();
    detail.insert("http_status".to_string(), Value::from(status));
    if let Some(media_type) = media_type {
        detail.insert(
            "media_type".to_string(),
            Value::String(media_type.to_string()),
        );
    }
    detail.insert("body".to_string(), body);
    detail.insert("body_truncated".to_string(), Value::Bool(body_truncated));
    detail.insert("body_bytes".to_string(), Value::from(body_bytes));
    detail.insert(
        "body_bytes_exact".to_string(),
        Value::Bool(body_bytes_exact),
    );
    detail.insert("body_preview_bytes".to_string(), Value::from(preview_bytes));
    detail.insert(
        "body_encoding".to_string(),
        Value::String(body_encoding.to_string()),
    );
    structured_provider_error_detail(format!("{context} returned HTTP {status}"), detail)
}

fn provider_error_body_value(
    media_type: Option<&str>,
    bytes: &[u8],
) -> (Value, &'static str, usize) {
    if bytes.is_empty() {
        return (Value::Null, "empty", 0);
    }
    let preview_len = bytes.len().min(MAX_PROVIDER_ERROR_BODY_BYTES);
    let preview = bytes
        .get(..preview_len)
        .expect("preview length is capped to the source byte length");
    if preview_len == bytes.len()
        && media_type.is_some_and(is_json_media_type)
        && let Ok(value) = serde_json::from_slice(preview)
    {
        return (value, "json", preview_len);
    }
    match std::str::from_utf8(preview) {
        Ok(text) => (Value::String(text.to_string()), "text", preview_len),
        Err(_) => (
            Value::String(String::from_utf8_lossy(preview).to_string()),
            "utf8_lossy",
            preview_len,
        ),
    }
}
