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
        filters: HashMap<String, String>,
        detail: String,
    },

    #[error("{source_schema}.{table} request failed: {detail}")]
    Request {
        source_schema: String,
        table: String,
        method: Option<String>,
        url: Option<String>,
        detail: String,
        timed_out: bool,
    },

    #[error("{source_schema}.{table} response decode failed: {detail}")]
    Decode {
        source_schema: String,
        table: String,
        method: Option<String>,
        url: Option<String>,
        detail: String,
    },

    #[error("{source_schema}.{table} pagination failed: {detail}")]
    Pagination {
        source_schema: String,
        table: String,
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
                filters,
                detail,
            } => http_request_to_structured(
                source_schema,
                table,
                *status,
                method.as_deref(),
                url.as_deref(),
                filters,
                detail,
            ),
            Self::Request {
                source_schema,
                table,
                method,
                url,
                detail,
                timed_out,
            } => provider_stage_failure_to_structured(provider_request_failure(
                source_schema,
                table,
                method.as_deref(),
                url.as_deref(),
                detail,
                *timed_out,
            )),
            Self::Decode {
                source_schema,
                table,
                method,
                url,
                detail,
            } => provider_stage_failure_to_structured(provider_decode_failure(
                source_schema,
                table,
                method.as_deref(),
                url.as_deref(),
                detail,
            )),
            Self::Pagination {
                source_schema,
                table,
                method,
                url,
                detail,
            } => provider_stage_failure_to_structured(provider_pagination_failure(
                source_schema,
                table,
                method.as_deref(),
                url.as_deref(),
                detail,
            )),
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
                &HashMap::new(),
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
    filters: &HashMap<String, String>,
    raw_detail: &str,
) -> StructuredQueryError {
    let source_shell = shell_arg(source);
    let sanitized_url = url.and_then(sanitize_request_url);

    let (reason, summary, hint, status) = match http_status {
        Some(BAD_REQUEST) => (
            "INVALID_QUERY_SHAPE",
            "Source rejected the request".to_string(),
            Some(bad_request_hint(source, table, filters)),
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
    if !filters.is_empty() {
        metadata.insert("filters".to_string(), render_filter_values(filters));
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

fn bad_request_hint(source: &str, table: &str, filters: &HashMap<String, String>) -> String {
    let generic = "Adjust the query filters or shape to match the target table's supported inputs.";
    match (source, table) {
        ("datadog", "events") => format!(
            "{generic} Sent filters: {}. `datadog.events` requires `start` and `end` as Unix epoch seconds, for example 1777593600.",
            render_filter_values(filters)
        ),
        ("datadog", "logs") => format!(
            "{generic} Sent filters: {}. `datadog.logs` accepts Datadog log search times such as RFC3339 timestamps (`2026-05-01T12:00:00Z`) or relative strings (`now-1h`, `now`).",
            render_filter_values(filters)
        ),
        _ => generic.to_string(),
    }
}

fn render_filter_values(filters: &HashMap<String, String>) -> String {
    if filters.is_empty() {
        return "<none>".to_string();
    }
    let mut pairs = filters
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>();
    pairs.sort();
    pairs.join(", ")
}

struct ProviderStageFailure<'a> {
    source: &'a str,
    table: &'a str,
    stage: &'a str,
    summary: &'a str,
    detail: &'a str,
    method: Option<&'a str>,
    url: Option<&'a str>,
    hint: Option<String>,
    retryable: bool,
    status: StatusCode,
    timed_out: bool,
}

fn provider_request_failure<'a>(
    source: &'a str,
    table: &'a str,
    method: Option<&'a str>,
    url: Option<&'a str>,
    detail: &'a str,
    timed_out: bool,
) -> ProviderStageFailure<'a> {
    ProviderStageFailure {
        source,
        table,
        stage: "request",
        summary: if timed_out {
            "Source request timed out"
        } else {
            "Source request failed"
        },
        detail,
        method,
        url,
        hint: Some(
            "The upstream API could not be reached. Check connectivity and retry.".to_string(),
        ),
        retryable: true,
        status: StatusCode::Unavailable,
        timed_out,
    }
}

fn provider_decode_failure<'a>(
    source: &'a str,
    table: &'a str,
    method: Option<&'a str>,
    url: Option<&'a str>,
    detail: &'a str,
) -> ProviderStageFailure<'a> {
    ProviderStageFailure {
        source,
        table,
        stage: "decode",
        summary: "Source response decode failed",
        detail,
        method,
        url,
        hint: Some(
            "The upstream API returned a response that does not match the source manifest."
                .to_string(),
        ),
        retryable: false,
        status: StatusCode::FailedPrecondition,
        timed_out: false,
    }
}

fn provider_pagination_failure<'a>(
    source: &'a str,
    table: &'a str,
    method: Option<&'a str>,
    url: Option<&'a str>,
    detail: &'a str,
) -> ProviderStageFailure<'a> {
    ProviderStageFailure {
        source,
        table,
        stage: "pagination",
        summary: "Source pagination failed",
        detail,
        method,
        url,
        hint: Some(
            "The source pagination configuration or upstream pagination link is invalid."
                .to_string(),
        ),
        retryable: false,
        status: StatusCode::FailedPrecondition,
        timed_out: false,
    }
}

fn provider_stage_failure_to_structured(failure: ProviderStageFailure<'_>) -> StructuredQueryError {
    let sanitized_url = failure.url.and_then(sanitize_request_url);
    let detail = enrich_provider_detail(failure.detail, failure.method, sanitized_url.as_deref());

    let mut metadata = HashMap::new();
    metadata.insert("source".to_string(), failure.source.to_string());
    metadata.insert("table".to_string(), failure.table.to_string());
    metadata.insert(
        "provider_failure_stage".to_string(),
        failure.stage.to_string(),
    );
    if failure.timed_out {
        metadata.insert("timeout".to_string(), "true".to_string());
    }
    if let Some(method) = failure.method {
        metadata.insert("http_method".to_string(), method.to_string());
    }
    if let Some(url) = &sanitized_url {
        metadata.insert("url".to_string(), url.clone());
    }

    StructuredQueryError::new(
        "PROVIDER_REQUEST_FAILED",
        failure.summary,
        detail,
        failure.hint,
        failure.retryable,
        failure.status,
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
    use std::collections::HashMap;

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
            filters: HashMap::new(),
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
            filters: HashMap::new(),
            detail: "bad request".to_string(),
        }
        .to_structured();
        assert_eq!(error.reason(), "INVALID_QUERY_SHAPE");
        assert_eq!(error.status(), StatusCode::InvalidArgument);
    }

    #[test]
    fn datadog_events_400_hint_includes_filter_values_and_time_format() {
        let error = ProviderQueryError::ApiRequest {
            source_schema: "datadog".to_string(),
            table: "events".to_string(),
            status: Some(400),
            method: Some("GET".to_string()),
            url: Some("https://api.datadoghq.eu/api/v1/events?start=bad".to_string()),
            filters: HashMap::from([
                ("start".to_string(), "2026-04-30T00:00:00Z".to_string()),
                ("end".to_string(), "now-7d".to_string()),
            ]),
            detail: "bad request".to_string(),
        }
        .to_structured();

        let hint = error.hint().expect("400 should include a hint");
        assert!(hint.contains("start=2026-04-30T00:00:00Z"));
        assert!(hint.contains("end=now-7d"));
        assert!(hint.contains("Unix epoch seconds"));
        assert_eq!(
            error.metadata().get("filters").map(String::as_str),
            Some("end=now-7d, start=2026-04-30T00:00:00Z")
        );
    }

    #[test]
    fn request_timeout_maps_to_unavailable_provider_failure() {
        let error = ProviderQueryError::Request {
            source_schema: "github".to_string(),
            table: "issues".to_string(),
            method: Some("GET".to_string()),
            url: Some("https://api.github.com/issues?token=secret".to_string()),
            detail: "source API request timed out after 30s".to_string(),
            timed_out: true,
        }
        .to_structured();
        assert_eq!(error.reason(), "PROVIDER_REQUEST_FAILED");
        assert_eq!(error.summary(), "Source request timed out");
        assert_eq!(error.status(), StatusCode::Unavailable);
        assert!(error.retryable());
        assert_eq!(
            error.metadata().get("provider_failure_stage").unwrap(),
            "request"
        );
        assert_eq!(error.metadata().get("timeout").unwrap(), "true");
        assert!(!error.detail().contains("token=secret"));
    }

    #[test]
    fn decode_failure_maps_to_failed_precondition_provider_failure() {
        let error = ProviderQueryError::Decode {
            source_schema: "github".to_string(),
            table: "issues".to_string(),
            method: Some("GET".to_string()),
            url: Some("https://api.github.com/issues".to_string()),
            detail: "expected value at line 1 column 1".to_string(),
        }
        .to_structured();
        assert_eq!(error.reason(), "PROVIDER_REQUEST_FAILED");
        assert_eq!(error.summary(), "Source response decode failed");
        assert_eq!(error.status(), StatusCode::FailedPrecondition);
        assert!(!error.retryable());
        assert_eq!(
            error.metadata().get("provider_failure_stage").unwrap(),
            "decode"
        );
    }

    #[test]
    fn pagination_failure_maps_to_failed_precondition_provider_failure() {
        let error = ProviderQueryError::Pagination {
            source_schema: "github".to_string(),
            table: "issues".to_string(),
            method: Some("GET".to_string()),
            url: Some("https://api.github.com/issues".to_string()),
            detail: "invalid pagination Link header item".to_string(),
        }
        .to_structured();
        assert_eq!(error.reason(), "PROVIDER_REQUEST_FAILED");
        assert_eq!(error.summary(), "Source pagination failed");
        assert_eq!(error.status(), StatusCode::FailedPrecondition);
        assert_eq!(
            error.metadata().get("provider_failure_stage").unwrap(),
            "pagination"
        );
    }

    #[test]
    fn http_500_is_retryable() {
        let error = ProviderQueryError::ApiRequest {
            source_schema: "github".to_string(),
            table: "issues".to_string(),
            status: Some(500),
            method: None,
            url: None,
            filters: HashMap::new(),
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
            filters: HashMap::new(),
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
            filters: HashMap::new(),
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
