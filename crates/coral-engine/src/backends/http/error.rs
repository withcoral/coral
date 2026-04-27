//! Error types and structured error mapping for HTTP-backed source queries.

use std::collections::HashMap;

use reqwest::StatusCode as HttpStatus;

use crate::contracts::{StatusCode, StructuredQueryError};

// Named bindings for the HTTP status codes the dispatch table matches
// against. Declared as `const` so they can appear directly in match arm
// patterns — the raw numeric literals read as magic; these do not.
const BAD_REQUEST: u16 = HttpStatus::BAD_REQUEST.as_u16();
const UNAUTHORIZED: u16 = HttpStatus::UNAUTHORIZED.as_u16();
const FORBIDDEN: u16 = HttpStatus::FORBIDDEN.as_u16();
const NOT_FOUND: u16 = HttpStatus::NOT_FOUND.as_u16();
const TOO_MANY_REQUESTS: u16 = HttpStatus::TOO_MANY_REQUESTS.as_u16();

/// Structured query-time failures for HTTP-backed tables.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ProviderQueryError {
    #[error(
        "{schema}.{table} table requires a constant equality filter: WHERE {column} = <constant>"
    )]
    MissingRequiredFilter {
        schema: String,
        table: String,
        column: String,
    },

    #[error("{source_schema}.{table} API error: {detail}")]
    ApiRequest {
        source_schema: String,
        table: String,
        status: Option<u16>,
        method: Option<String>,
        url: Option<String>,
        detail: String,
    },

    #[error("{source_schema}.{table}: {detail}")]
    RateLimited {
        source_schema: String,
        table: String,
        method: Option<String>,
        url: Option<String>,
        detail: String,
    },
}

// ---------------------------------------------------------------------------
// Mapping: ProviderQueryError → StructuredQueryError
// ---------------------------------------------------------------------------

impl ProviderQueryError {
    /// Converts this HTTP-specific error into the canonical structured error.
    pub(crate) fn to_structured(&self) -> StructuredQueryError {
        match self {
            Self::MissingRequiredFilter {
                schema,
                table,
                column,
            } => {
                let mut metadata = HashMap::new();
                metadata.insert("schema".to_string(), schema.clone());
                metadata.insert("table".to_string(), table.clone());
                metadata.insert("column".to_string(), column.clone());
                StructuredQueryError::new(
                    "MISSING_REQUIRED_FILTER",
                    format!("{schema}.{table} requires `WHERE {column} = <constant>`"),
                    format!("{schema}.{table} requires a constant equality filter on {column}"),
                    Some(format!(
                        "Add a constant equality filter on `{column}` or inspect \
                         `coral.columns` / `coral.tables` first."
                    )),
                    false,
                    StatusCode::FailedPrecondition,
                    metadata,
                )
            }
            Self::ApiRequest {
                source_schema,
                table,
                status,
                method,
                url,
                detail,
            } => http_request_to_structured(
                source_schema,
                table,
                *status,
                method.as_deref(),
                url.as_deref(),
                detail,
            ),
            Self::RateLimited {
                source_schema,
                table,
                method,
                url,
                detail,
            } => http_request_to_structured(
                source_schema,
                table,
                Some(TOO_MANY_REQUESTS),
                method.as_deref(),
                url.as_deref(),
                detail,
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// HTTP status dispatch
// ---------------------------------------------------------------------------

fn http_request_to_structured(
    source: &str,
    table: &str,
    http_status: Option<u16>,
    method: Option<&str>,
    url: Option<&str>,
    raw_detail: &str,
) -> StructuredQueryError {
    let source_shell = shell_arg(source);
    let sanitized_url = url.and_then(sanitize_request_url);

    let (reason, summary, hint, status) = match http_status {
        Some(BAD_REQUEST) => (
            "INVALID_QUERY_SHAPE",
            "Source rejected the request".to_string(),
            Some(
                "Adjust the query filters or shape to match the target table's supported inputs."
                    .to_string(),
            ),
            StatusCode::InvalidArgument,
        ),
        Some(UNAUTHORIZED) => (
            "PROVIDER_REQUEST_FAILED",
            "Source authentication failed".to_string(),
            Some(format!(
                "Credentials for this source are invalid or expired. Re-install it to refresh: \
                 `coral source add {source_shell}` for bundled sources, or \
                 `coral source add --file <manifest-path>` for imported sources."
            )),
            StatusCode::FailedPrecondition,
        ),
        Some(FORBIDDEN) => (
            "PROVIDER_REQUEST_FAILED",
            "Source request was rejected".to_string(),
            Some(
                "Check the configured credentials and whether they have access to this resource."
                    .to_string(),
            ),
            StatusCode::FailedPrecondition,
        ),
        Some(NOT_FOUND) => (
            "PROVIDER_REQUEST_FAILED",
            "Source resource was not found".to_string(),
            Some(
                "Verify the identifier or filter values you passed; the upstream resource was not found."
                    .to_string(),
            ),
            StatusCode::NotFound,
        ),
        Some(TOO_MANY_REQUESTS) => (
            "PROVIDER_REQUEST_FAILED",
            "Source rate limit exceeded".to_string(),
            Some(
                "The upstream API is rate-limiting requests. Wait briefly and retry.".to_string(),
            ),
            StatusCode::Unavailable,
        ),
        Some(s) if is_server_error(s) => (
            "PROVIDER_REQUEST_FAILED",
            "Source server error".to_string(),
            Some(
                "The upstream API returned a server error. This may be transient — retry after a brief wait."
                    .to_string(),
            ),
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
    let detail = enrich_provider_detail(raw_detail, method, sanitized_url.as_deref());
    let is_retryable =
        matches!(http_status, Some(s) if s == TOO_MANY_REQUESTS || is_server_error(s));

    let mut metadata = HashMap::new();
    metadata.insert("source".to_string(), source.to_string());
    metadata.insert("table".to_string(), table.to_string());
    if let Some(s) = http_status {
        metadata.insert("http_status".to_string(), s.to_string());
    }
    if let Some(m) = method {
        metadata.insert("http_method".to_string(), m.to_string());
    }
    if let Some(u) = &sanitized_url {
        metadata.insert("url".to_string(), u.clone());
    }

    StructuredQueryError::new(
        reason,
        summary,
        detail,
        hint,
        is_retryable,
        status,
        metadata,
    )
}

/// Returns `true` when the given status belongs to the 5xx server-error class.
fn is_server_error(status: u16) -> bool {
    HttpStatus::from_u16(status).is_ok_and(|code| code.is_server_error())
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

#[cfg(test)]
mod tests {
    use crate::contracts::StatusCode;

    use super::ProviderQueryError;

    #[test]
    fn missing_required_filter_sets_reason_and_metadata() {
        let error = ProviderQueryError::MissingRequiredFilter {
            schema: "github".to_string(),
            table: "issues".to_string(),
            column: "repo".to_string(),
        }
        .to_structured();
        assert_eq!(error.reason(), "MISSING_REQUIRED_FILTER");
        assert_eq!(error.metadata().get("schema").unwrap(), "github");
        assert_eq!(error.metadata().get("table").unwrap(), "issues");
        assert_eq!(error.metadata().get("column").unwrap(), "repo");
        assert!(error.summary().contains("repo"));
        assert!(error.hint().is_some());
        assert_eq!(error.status(), StatusCode::FailedPrecondition);
    }

    #[test]
    fn http_401_includes_both_install_paths() {
        let error = ProviderQueryError::ApiRequest {
            source_schema: "github".to_string(),
            table: "issues".to_string(),
            status: Some(401),
            method: Some("GET".to_string()),
            url: Some("https://api.github.com/repos/coral/coral/issues".to_string()),
            detail: "Bad credentials".to_string(),
        }
        .to_structured();
        assert_eq!(error.reason(), "PROVIDER_REQUEST_FAILED");
        assert_eq!(error.metadata().get("http_status").unwrap(), "401");
        assert!(!error.retryable());
        let hint = error.hint().expect("401 should have a hint");
        assert!(hint.contains("coral source add github"));
        assert!(hint.contains("coral source add --file"));
    }

    #[test]
    fn http_400_maps_to_invalid_argument() {
        let error = ProviderQueryError::ApiRequest {
            source_schema: "github".to_string(),
            table: "issues".to_string(),
            status: Some(400),
            method: None,
            url: None,
            detail: "bad request".to_string(),
        }
        .to_structured();
        assert_eq!(error.reason(), "INVALID_QUERY_SHAPE");
        assert_eq!(error.status(), StatusCode::InvalidArgument);
    }

    #[test]
    fn http_500_is_retryable() {
        let error = ProviderQueryError::ApiRequest {
            source_schema: "github".to_string(),
            table: "issues".to_string(),
            status: Some(500),
            method: None,
            url: None,
            detail: "boom".to_string(),
        }
        .to_structured();
        assert!(error.retryable());
        assert_eq!(error.status(), StatusCode::Unavailable);
    }

    #[test]
    fn redacts_secret_query_params_from_url() {
        let error = ProviderQueryError::ApiRequest {
            source_schema: "datadog".to_string(),
            table: "events".to_string(),
            status: Some(500),
            method: Some("GET".to_string()),
            url: Some("https://api.datadoghq.eu/api/v1/events?api_key=SECRET".to_string()),
            detail: "boom".to_string(),
        }
        .to_structured();
        let url = error
            .metadata()
            .get("url")
            .expect("url should be sanitized");
        assert_eq!(url, "https://api.datadoghq.eu/api/v1/events");
        assert!(!error.detail().contains("SECRET"));
    }

    #[test]
    fn detail_preserves_method_and_sanitized_url() {
        let error = ProviderQueryError::ApiRequest {
            source_schema: "github".to_string(),
            table: "issues".to_string(),
            status: Some(500),
            method: Some("GET".to_string()),
            url: Some("https://api.github.com/issues?page=3".to_string()),
            detail: "upstream boom".to_string(),
        }
        .to_structured();
        assert!(
            error
                .detail()
                .contains("[GET] https://api.github.com/issues")
        );
        assert!(!error.detail().contains("page=3"));
    }

    #[test]
    fn display_renders_full_message() {
        let error = ProviderQueryError::MissingRequiredFilter {
            schema: "github".to_string(),
            table: "issues".to_string(),
            column: "repo".to_string(),
        }
        .to_structured();
        let text = error.to_string();
        assert!(text.contains(error.summary()));
        assert!(text.contains("Hint: "));
    }
}
