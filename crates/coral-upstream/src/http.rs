use std::collections::BTreeMap;

use coral_capabilities::{HttpMethod, ResponseTrust};
use reqwest::header::{CONTENT_TYPE, USER_AGENT};

use crate::{
    HttpRequestPlan, HttpUpstreamResponse, ProviderErrorKind, RedactableString, Result,
    UpstreamError, UpstreamRequestBody, UpstreamResponseBody,
};

pub(crate) const MAX_PROVIDER_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
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
        Some(UpstreamRequestBody::Form(fields)) => request.form(fields),
        Some(UpstreamRequestBody::Bytes(bytes)) => request.body(bytes.clone()),
        Some(UpstreamRequestBody::Empty) | None => request,
    };

    let response = request
        .send()
        .await
        .map_err(|error| UpstreamError::Transport(error.to_string()))?;
    let status = response.status().as_u16();
    let headers = response_headers(response.headers());
    let media_type = response_media_type(&headers);
    let bytes = limited_response_bytes(response, "HTTP provider").await?;
    if !(200..300).contains(&status) {
        return Err(UpstreamError::Provider {
            kind: ProviderErrorKind::HttpError,
            detail: format!("HTTP provider returned status {status}"),
        });
    }
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
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|error| UpstreamError::Transport(error.to_string()))
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
    if media_type.is_some_and(|value| {
        value == "application/json"
            || value.ends_with("+json")
            || value == "application/problem+json"
    }) {
        let value = serde_json::from_slice(bytes)
            .map_err(|error| UpstreamError::InvalidResponse(error.to_string()))?;
        return Ok(UpstreamResponseBody::Json(value));
    }
    match std::str::from_utf8(bytes) {
        Ok(text) => Ok(UpstreamResponseBody::Text(text.to_string())),
        Err(_) => Ok(UpstreamResponseBody::Bytes(bytes.to_vec())),
    }
}
