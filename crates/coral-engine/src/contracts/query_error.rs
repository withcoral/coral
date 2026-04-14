//! Engine-internal structured query error with user- and agent-facing hints.
//!
//! This type is **not** re-exported from the `coral-engine` crate root.
//! The app boundary converts it to AIP-193 standard error details;
//! consumers outside the engine see only `coral-client::CoralQueryError`.

use std::collections::HashMap;

use super::error::StatusCode;

/// Engine-internal structured query error.
#[derive(Debug, Clone)]
pub(crate) struct QueryError {
    pub(crate) reason: &'static str,
    pub(crate) summary: String,
    pub(crate) detail: String,
    pub(crate) hint: Option<String>,
    pub(crate) retryable: bool,
    pub(crate) metadata: HashMap<String, String>,
    pub(crate) status: StatusCode,
}

// ---------------------------------------------------------------------------
// String helpers
// ---------------------------------------------------------------------------

fn shell_arg(value: &str) -> String {
    let is_safe = !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'));
    if is_safe {
        value.to_string()
    } else {
        let escaped = value.replace('\'', "'\\''");
        format!("'{escaped}'")
    }
}

fn sanitize_request_url(raw: &str) -> Option<String> {
    let without_fragment = raw.split_once('#').map_or(raw, |(before, _)| before);
    let without_query = without_fragment
        .split_once('?')
        .map_or(without_fragment, |(before, _)| before);
    let (scheme, rest) = without_query.split_once("://")?;
    if scheme.is_empty() || rest.is_empty() {
        return None;
    }
    let (authority, path) = rest.split_once('/').map_or((rest, ""), |(a, p)| (a, p));
    let host_and_port = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host)| host);
    if host_and_port.is_empty() {
        return None;
    }
    if path.is_empty() {
        Some(format!("{scheme}://{host_and_port}"))
    } else {
        Some(format!("{scheme}://{host_and_port}/{path}"))
    }
}

fn enrich_provider_detail(detail: &str, method: Option<&str>, url: Option<&str>) -> String {
    match (method, url) {
        (Some(method), Some(url)) => format!("{detail} [{method}] {url}"),
        (Some(method), None) => format!("{detail} [{method}]"),
        (None, Some(url)) => format!("{detail} {url}"),
        (None, None) => detail.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Constructors
// ---------------------------------------------------------------------------

impl QueryError {
    pub(crate) fn missing_required_filter(
        schema: impl Into<String>,
        table: impl Into<String>,
        field: impl Into<String>,
        detail: impl Into<String>,
    ) -> Self {
        let schema = schema.into();
        let table = table.into();
        let field = field.into();
        let mut metadata = HashMap::new();
        metadata.insert("schema".to_string(), schema.clone());
        metadata.insert("table".to_string(), table.clone());
        metadata.insert("field".to_string(), field.clone());
        Self {
            reason: "MISSING_REQUIRED_FILTER",
            summary: format!("{schema}.{table} requires `WHERE {field} = <constant>`"),
            detail: detail.into(),
            hint: Some(format!(
                "Add a constant equality filter on `{field}` or inspect `coral.columns` / `coral.tables` first."
            )),
            retryable: false,
            metadata,
            status: StatusCode::FailedPrecondition,
        }
    }

    #[allow(
        clippy::needless_pass_by_value,
        reason = "method and url are conditionally moved into the metadata HashMap"
    )]
    pub(crate) fn provider_request(
        source: impl Into<String>,
        table: impl Into<String>,
        http_status: Option<u16>,
        method: Option<String>,
        url: Option<String>,
        detail: impl Into<String>,
    ) -> Self {
        let source = source.into();
        let table = table.into();
        let raw_detail = detail.into();
        let source_shell = shell_arg(&source);
        let sanitized_url = url.and_then(|raw| sanitize_request_url(&raw));

        let (reason, summary, hint, status) = match http_status {
            Some(400) => (
                "INVALID_QUERY_SHAPE",
                "Source rejected the request".to_string(),
                Some("Adjust the query filters or shape to match the target table's supported inputs.".to_string()),
                StatusCode::FailedPrecondition,
            ),
            Some(401) => (
                "PROVIDER_REQUEST_FAILED",
                "Source authentication failed".to_string(),
                Some(format!(
                    "Credentials for this source are invalid or expired. Re-install it to refresh: \
                     `coral source add {source_shell}` for bundled sources, or \
                     `coral source import <manifest-path>` for imported sources."
                )),
                StatusCode::FailedPrecondition,
            ),
            Some(403) => (
                "PROVIDER_REQUEST_FAILED",
                "Source request was rejected".to_string(),
                Some("Check the configured credentials and whether they have access to this resource.".to_string()),
                StatusCode::FailedPrecondition,
            ),
            Some(404) => (
                "PROVIDER_REQUEST_FAILED",
                "Source resource was not found".to_string(),
                Some("Verify the identifier or filter values you passed; the upstream resource was not found.".to_string()),
                StatusCode::NotFound,
            ),
            Some(429) => (
                "PROVIDER_REQUEST_FAILED",
                "Source rate limit exceeded".to_string(),
                Some("The upstream API is rate-limiting requests. Wait briefly and retry.".to_string()),
                StatusCode::Unavailable,
            ),
            Some(s) if (500..600).contains(&s) => (
                "PROVIDER_REQUEST_FAILED",
                "Source server error".to_string(),
                Some("The upstream API returned a server error. This may be transient — retry after a brief wait.".to_string()),
                StatusCode::Unavailable,
            ),
            _ => (
                "PROVIDER_REQUEST_FAILED",
                "Source request failed".to_string(),
                None,
                StatusCode::FailedPrecondition,
            ),
        };

        let summary = match http_status {
            Some(s) => format!("{summary} ({s})"),
            None => summary,
        };
        let detail =
            enrich_provider_detail(&raw_detail, method.as_deref(), sanitized_url.as_deref());
        let is_retryable = matches!(http_status, Some(429 | 500..=599));

        let mut metadata = HashMap::new();
        metadata.insert("source".to_string(), source);
        metadata.insert("table".to_string(), table);
        if let Some(s) = http_status {
            metadata.insert("http_status".to_string(), s.to_string());
        }
        if let Some(m) = &method {
            metadata.insert("http_method".to_string(), m.clone());
        }
        if let Some(u) = &sanitized_url {
            metadata.insert("url".to_string(), u.clone());
        }

        let mut error = Self {
            reason,
            summary,
            detail,
            hint: None,
            retryable: is_retryable,
            metadata,
            status,
        };
        if let Some(h) = hint {
            error.hint = Some(h);
        }
        error
    }

    /// Renders a plain-text message preserving the summary, detail, and hint.
    pub(crate) fn to_plain_message(&self) -> String {
        let mut message = self.summary.clone();
        if !self.detail.is_empty() {
            message.push('\n');
            message.push_str(&self.detail);
        }
        if let Some(hint) = &self.hint {
            message.push_str("\nHint: ");
            message.push_str(hint);
        }
        message
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_required_filter_sets_reason_and_metadata() {
        let error = QueryError::missing_required_filter(
            "github",
            "issues",
            "repo",
            "missing required filter",
        );
        assert_eq!(error.reason, "MISSING_REQUIRED_FILTER");
        assert_eq!(error.metadata.get("schema").unwrap(), "github");
        assert_eq!(error.metadata.get("table").unwrap(), "issues");
        assert_eq!(error.metadata.get("field").unwrap(), "repo");
        assert!(error.summary.contains("repo"));
        assert!(error.hint.is_some());
        assert_eq!(error.status, StatusCode::FailedPrecondition);
    }

    #[test]
    fn provider_request_401_includes_both_install_paths() {
        let error = QueryError::provider_request(
            "github",
            "issues",
            Some(401),
            Some("GET".to_string()),
            Some("https://api.github.com/repos/coral/coral/issues".to_string()),
            "Bad credentials",
        );
        assert_eq!(error.reason, "PROVIDER_REQUEST_FAILED");
        assert_eq!(error.metadata.get("http_status").unwrap(), "401");
        assert!(!error.retryable);
        let hint = error.hint.as_ref().expect("401 should have a hint");
        assert!(hint.contains("coral source add github"));
        assert!(hint.contains("coral source import"));
    }

    #[test]
    fn provider_request_500_is_retryable() {
        let error = QueryError::provider_request("github", "issues", Some(500), None, None, "boom");
        assert!(error.retryable);
        assert_eq!(error.status, StatusCode::Unavailable);
    }

    #[test]
    fn provider_request_redacts_secret_query_params_from_url() {
        let error = QueryError::provider_request(
            "datadog",
            "events",
            Some(500),
            Some("GET".to_string()),
            Some("https://api.datadoghq.eu/api/v1/events?api_key=SECRET".to_string()),
            "boom",
        );
        let url = error.metadata.get("url").expect("url should be sanitized");
        assert_eq!(url, "https://api.datadoghq.eu/api/v1/events");
        assert!(!error.detail.contains("SECRET"));
    }

    #[test]
    fn provider_request_detail_preserves_method_and_sanitized_url() {
        let error = QueryError::provider_request(
            "github",
            "issues",
            Some(500),
            Some("GET".to_string()),
            Some("https://api.github.com/issues?page=3".to_string()),
            "upstream boom",
        );
        assert!(error.detail.contains("[GET] https://api.github.com/issues"));
        assert!(!error.detail.contains("page=3"));
    }

    #[test]
    fn to_plain_message_includes_summary_detail_and_hint() {
        let error =
            QueryError::missing_required_filter("github", "issues", "repo", "missing filter");
        let text = error.to_plain_message();
        assert!(text.contains(&error.summary));
        assert!(text.contains("missing filter"));
        assert!(text.contains("Hint: "));
    }
}
