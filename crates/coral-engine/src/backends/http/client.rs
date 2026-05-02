//! HTTP client used by HTTP-backed source tables.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use datafusion::error::{DataFusionError, Result};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::{Map, Value, json};

use crate::RequestAuthenticator;
use crate::backends::http::ProviderQueryError;
use crate::backends::http::auth::{resolve_auth_headers, validate_auth_inputs};
use crate::backends::http::rate_limit::{RateLimitDecision, check_rate_limit};
use crate::backends::shared::json_path::get_path_value;
use crate::backends::shared::template::{
    render_template, resolve_value_source, validate_input_dependencies,
    validate_value_source_inputs, value_to_string,
};
use coral_spec::backends::http::{HttpSourceManifest, HttpTableSpec, RateLimitSpec};
use coral_spec::{
    AuthSpec, BodySpec, HeaderSpec, HttpMethod, PageSizeSpec, ParsedTemplate, RequestRouteSpec,
    RequestSpec as ManifestRequestSpec, ResponseBodyFormat, RowStrategy, ValidatedPagination,
    ValidatedPaginationMode,
};

const DEFAULT_MAX_PAGES: usize = 10_000;
const DEFAULT_HTTP_REQUEST_TIMEOUT_SECS: u64 = 30;
const DEFAULT_HTTP_USER_AGENT: &str = concat!("coral/", env!("CARGO_PKG_VERSION"));

/// Executes manifest-driven HTTP requests for one registered source.
#[derive(Clone)]
pub(crate) struct HttpSourceClient {
    http: reqwest::Client,
    request_timeout: Duration,
    source_schema: String,
    base_url: ParsedTemplate,
    auth: AuthSpec,
    request_headers: Vec<HeaderSpec>,
    request_authenticators: HashMap<String, Arc<dyn RequestAuthenticator>>,
    rate_limit: RateLimitSpec,
    resolved_inputs: Arc<BTreeMap<String, String>>,
}

impl std::fmt::Debug for HttpSourceClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpSourceClient")
            .field("source_schema", &self.source_schema)
            .field("base_url", &self.base_url)
            .field("auth", &self.auth)
            .field("request_headers", &self.request_headers)
            .field("rate_limit", &self.rate_limit)
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Default)]
struct PageState {
    cursor: Option<String>,
    page: i64,
    offset: i64,
    next_url: Option<String>,
}

/// Concrete request body shape passed to the HTTP layer.
#[derive(Debug, Clone)]
enum RequestBody {
    Json(Value),
    Text(String),
}

struct RequestSpec<'a> {
    auth: &'a AuthSpec,
    request_headers: &'a [HeaderSpec],
    request_authenticators: &'a HashMap<String, Arc<dyn RequestAuthenticator>>,
    table_headers: &'a [HeaderSpec],
    table_name: &'a str,
    method: HttpMethod,
    base_url: &'a str,
    url: &'a str,
    query_pairs: &'a [(String, String)],
    body: Option<&'a RequestBody>,
    response_format: ResponseBodyFormat,
    source_schema: &'a str,
    rate_limit: &'a RateLimitSpec,
    filters: &'a HashMap<String, String>,
    state: &'a HashMap<String, String>,
    resolved_inputs: &'a BTreeMap<String, String>,
    allow_404_empty: bool,
    link_header_require_results: bool,
}

impl HttpSourceClient {
    /// Build a backend client from a validated source spec.
    ///
    /// # Errors
    ///
    /// Returns a `DataFusionError` if required credentials are missing or if an
    /// authentication header template cannot be resolved.
    pub(crate) fn from_manifest(
        manifest: &HttpSourceManifest,
        source_secrets: &BTreeMap<String, String>,
        source_variables: &BTreeMap<String, String>,
        request_authenticators: &HashMap<String, Arc<dyn RequestAuthenticator>>,
    ) -> Result<Self> {
        let resolved_inputs =
            coral_spec::resolve_inputs(&manifest.declared_inputs, source_secrets, source_variables);
        validate_source_scoped_http_config(manifest, request_authenticators, &resolved_inputs)?;

        let request_timeout = Duration::from_secs(DEFAULT_HTTP_REQUEST_TIMEOUT_SECS);
        let http = reqwest::Client::builder()
            .timeout(request_timeout)
            .user_agent(DEFAULT_HTTP_USER_AGENT)
            .build()
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "failed to build HTTP client for source '{}': {error}",
                    manifest.common.name
                ))
            })?;

        Ok(Self {
            http,
            request_timeout,
            source_schema: manifest.common.name.clone(),
            base_url: manifest.base_url.clone(),
            auth: manifest.auth.clone(),
            request_headers: manifest.request_headers.clone(),
            request_authenticators: request_authenticators.clone(),
            rate_limit: manifest.rate_limit.clone(),
            resolved_inputs: Arc::new(resolved_inputs),
        })
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Paginated fetch logic is stateful and easier to audit in one sequential function"
    )]
    /// Fetch rows for a single table from the backend API.
    ///
    /// # Errors
    ///
    /// Returns a `DataFusionError` if request templates cannot be resolved, the
    /// `HTTP` request fails, the response payload cannot be interpreted, or the
    /// fetched rows cannot be extracted for the table strategy.
    pub(crate) async fn fetch(
        &self,
        table: &HttpTableSpec,
        filters: &HashMap<String, String>,
        sql_limit: Option<usize>,
    ) -> Result<Vec<Value>> {
        let mut all_rows = Vec::new();
        let effective_limit = sql_limit.or(table.fetch_limit_default());
        let pagination = table
            .pagination
            .validated(&self.source_schema, table.name())
            .map_err(|error| {
                provider_error(ProviderQueryError::Pagination {
                    source_schema: self.source_schema.clone(),
                    table: table.name().to_string(),
                    method: None,
                    url: None,
                    detail: error.to_string(),
                })
            })?;
        let page_size = resolve_page_size(pagination.page_size.as_ref(), sql_limit);

        let filter_keys: HashSet<String> = filters.keys().cloned().collect();
        let active_request = table.resolve_request(&filter_keys);

        let mut state = PageState {
            page: table.pagination.page_start,
            offset: match &pagination.mode {
                ValidatedPaginationMode::Offset(offset) => offset.start,
                _ => table.pagination.offset_start,
            },
            ..PageState::default()
        };

        let mut page_count = 0usize;
        let max_pages = table.pagination.max_pages.unwrap_or(DEFAULT_MAX_PAGES);

        loop {
            page_count += 1;
            if page_count > max_pages {
                return Err(provider_error(ProviderQueryError::Pagination {
                    source_schema: self.source_schema.clone(),
                    table: table.name().to_string(),
                    method: None,
                    url: None,
                    detail: format!("exceeded pagination max_pages={max_pages}"),
                }));
            }

            let base_url = render_template(
                &self.base_url,
                filters,
                &pagination_state_values(&state),
                self.resolved_inputs.as_ref(),
            )?;
            let base_url = normalize_base_url(&base_url);
            let following_link_header = matches!(
                pagination.mode,
                ValidatedPaginationMode::LinkHeader | ValidatedPaginationMode::Auto
            ) && state.next_url.is_some();

            let url = if matches!(
                pagination.mode,
                ValidatedPaginationMode::LinkHeader | ValidatedPaginationMode::Auto
            ) && let Some(next) = state.next_url.clone()
            {
                next
            } else {
                let rendered_path = render_template(
                    &active_request.path,
                    filters,
                    &pagination_state_values(&state),
                    self.resolved_inputs.as_ref(),
                )?;
                join_url(&base_url, &rendered_path)?
            };

            let (query_pairs, body) = if following_link_header {
                (Vec::new(), None)
            } else {
                let mut query_pairs = build_query_pairs(
                    active_request,
                    filters,
                    &state,
                    self.resolved_inputs.as_ref(),
                )?;
                apply_pagination_query_pairs(
                    &mut query_pairs,
                    table,
                    &pagination,
                    &state,
                    page_size,
                )
                .map_err(|error| {
                    pagination_error(&self.source_schema, table.name(), None, Some(&url), &error)
                })?;

                let mut body = build_request_body(
                    active_request,
                    filters,
                    &state,
                    self.resolved_inputs.as_ref(),
                )?;
                apply_pagination_body_fields(
                    &mut body,
                    &active_request.body,
                    table,
                    &pagination,
                    &state,
                    page_size,
                )
                .map_err(|error| {
                    pagination_error(&self.source_schema, table.name(), None, Some(&url), &error)
                })?;
                (query_pairs, body)
            };

            let pagination_values = pagination_state_values(&state);
            let request = execute_request(
                &self.http,
                self.request_timeout,
                RequestSpec {
                    auth: &self.auth,
                    request_headers: &self.request_headers,
                    request_authenticators: &self.request_authenticators,
                    table_headers: &active_request.headers,
                    table_name: table.name(),
                    method: active_request.method,
                    base_url: &base_url,
                    url: &url,
                    query_pairs: &query_pairs,
                    body: body.as_ref(),
                    response_format: table.response.format,
                    source_schema: &self.source_schema,
                    rate_limit: &self.rate_limit,
                    filters,
                    state: &pagination_values,
                    resolved_inputs: self.resolved_inputs.as_ref(),
                    allow_404_empty: table.response.allow_404_empty,
                    link_header_require_results: pagination.link_header_require_results,
                },
            )
            .await?;

            let Some((payload, next_url)) = request else {
                break;
            };

            if !table.response.ok_path.is_empty() {
                let ok = get_path_value(&payload, &table.response.ok_path)
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                if !ok {
                    let err = if table.response.error_path.is_empty() {
                        "unknown source API error".to_string()
                    } else {
                        get_path_value(&payload, &table.response.error_path)
                            .and_then(Value::as_str)
                            .unwrap_or("unknown source API error")
                            .to_string()
                    };
                    return Err(DataFusionError::External(Box::new(
                        ProviderQueryError::ApiRequest {
                            source_schema: self.source_schema.clone(),
                            table: table.name().to_string(),
                            status: None,
                            method: None,
                            url: None,
                            filters: filters.clone(),
                            detail: err,
                        },
                    )));
                }
            }

            let mut rows = extract_rows(table, &payload)?;
            let rows_on_page = rows.len();
            all_rows.append(&mut rows);

            if let Some(limit) = effective_limit
                && all_rows.len() >= limit
            {
                all_rows.truncate(limit);
                break;
            }

            match &pagination.mode {
                ValidatedPaginationMode::None => break,
                ValidatedPaginationMode::CursorQuery | ValidatedPaginationMode::CursorBody => {
                    let next_cursor =
                        get_path_value(&payload, &table.pagination.response_cursor_path)
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .map(ToOwned::to_owned);
                    match next_cursor {
                        Some(cursor) => state.cursor = Some(cursor),
                        None => break,
                    }
                }
                ValidatedPaginationMode::Page => {
                    if page_is_exhausted(rows_on_page, page_size) {
                        break;
                    }
                    state.page = state.page.saturating_add(table.pagination.page_step);
                }
                ValidatedPaginationMode::Offset(offset) => {
                    if page_is_exhausted(rows_on_page, page_size) {
                        break;
                    }
                    let step = offset
                        .resolve_step(page_size, &self.source_schema, table.name())
                        .map_err(|error| {
                            provider_error(ProviderQueryError::Pagination {
                                source_schema: self.source_schema.clone(),
                                table: table.name().to_string(),
                                method: None,
                                url: None,
                                detail: error.to_string(),
                            })
                        })?;
                    state.offset = state.offset.saturating_add(step);
                }
                ValidatedPaginationMode::LinkHeader | ValidatedPaginationMode::Auto => {
                    match next_url {
                        Some(next) => state.next_url = Some(next),
                        None => break,
                    }
                }
            }
        }

        Ok(all_rows)
    }
}

fn validate_source_scoped_http_config(
    manifest: &HttpSourceManifest,
    request_authenticators: &HashMap<String, Arc<dyn RequestAuthenticator>>,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<()> {
    check_base_url_inputs(manifest, resolved_inputs)?;
    check_request_header_inputs(manifest, resolved_inputs)?;
    check_table_request_inputs(manifest, resolved_inputs)?;
    check_auth_inputs(manifest, request_authenticators, resolved_inputs)?;
    Ok(())
}

/// `base_url` may reference `{{filter.*}}` / `{{state.*}}` that only resolve
/// per-request. Check input-token deps only; runtime renders the rest.
fn check_base_url_inputs(
    manifest: &HttpSourceManifest,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<()> {
    validate_input_dependencies(&manifest.base_url, resolved_inputs)
        .map_err(|error| registration_error(&manifest.common.name, "base_url", &error))
}

/// Same tolerance for filter/state tokens as `base_url`.
fn check_request_header_inputs(
    manifest: &HttpSourceManifest,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<()> {
    validate_header_inputs(
        &manifest.common.name,
        "request_headers",
        &manifest.request_headers,
        resolved_inputs,
    )?;
    Ok(())
}

fn check_table_request_inputs(
    manifest: &HttpSourceManifest,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<()> {
    for table in &manifest.tables {
        validate_request_template_inputs(
            &manifest.common.name,
            table.name(),
            "request",
            &table.request,
            resolved_inputs,
        )?;
        for route in &table.requests {
            validate_request_route_inputs(
                &manifest.common.name,
                table.name(),
                route,
                resolved_inputs,
            )?;
        }
    }
    Ok(())
}

/// Auth is source-scoped: all template dependencies must resolve from inputs
/// before any request is issued.
fn check_auth_inputs(
    manifest: &HttpSourceManifest,
    request_authenticators: &HashMap<String, Arc<dyn RequestAuthenticator>>,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<()> {
    validate_auth_inputs(&manifest.auth, request_authenticators, resolved_inputs)
        .map_err(|error| registration_error(&manifest.common.name, "auth", &error))
}

fn registration_error(source: &str, field: &str, error: &DataFusionError) -> DataFusionError {
    DataFusionError::Execution(format!(
        "source '{source}' {field} could not be resolved: {error}"
    ))
}

fn validate_request_route_inputs(
    source_name: &str,
    table_name: &str,
    route: &RequestRouteSpec,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<()> {
    let route_label = if route.when_filters.is_empty() {
        "request route".to_string()
    } else {
        format!(
            "request route for filters [{}]",
            route.when_filters.join(", ")
        )
    };
    validate_request_template_inputs(
        source_name,
        table_name,
        &route_label,
        &route.request,
        resolved_inputs,
    )
}

fn validate_request_template_inputs(
    source_name: &str,
    table_name: &str,
    request_label: &str,
    request: &ManifestRequestSpec,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<()> {
    validate_input_dependencies(&request.path, resolved_inputs).map_err(|error| {
        registration_error(
            source_name,
            &format!("table '{table_name}' {request_label} path"),
            &error,
        )
    })?;
    validate_header_inputs(
        source_name,
        &format!("table '{table_name}' {request_label} header"),
        &request.headers,
        resolved_inputs,
    )?;
    for param in &request.query {
        validate_value_source_inputs(&param.value, resolved_inputs).map_err(|error| {
            registration_error(
                source_name,
                &format!(
                    "table '{table_name}' {request_label} query param '{}'",
                    param.name
                ),
                &error,
            )
        })?;
    }
    match &request.body {
        BodySpec::Json { fields } => {
            for field in fields {
                let field_path = if field.path.is_empty() {
                    "<root>".to_string()
                } else {
                    field.path.join(".")
                };
                validate_value_source_inputs(&field.value, resolved_inputs).map_err(|error| {
                    registration_error(
                        source_name,
                        &format!("table '{table_name}' {request_label} body field '{field_path}'"),
                        &error,
                    )
                })?;
            }
        }
        BodySpec::Text { content } => {
            validate_value_source_inputs(content, resolved_inputs).map_err(|error| {
                registration_error(
                    source_name,
                    &format!("table '{table_name}' {request_label} body text"),
                    &error,
                )
            })?;
        }
    }
    Ok(())
}

fn validate_header_inputs(
    source_name: &str,
    context: &str,
    headers: &[HeaderSpec],
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<()> {
    for header in headers {
        validate_value_source_inputs(&header.value, resolved_inputs).map_err(|error| {
            registration_error(source_name, &format!("{context} '{}'", header.name), &error)
        })?;
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "HTTP request execution keeps retry, auth, logging, and response handling in one audited flow"
)]
async fn execute_request(
    http: &reqwest::Client,
    request_timeout: Duration,
    request: RequestSpec<'_>,
) -> Result<Option<(Value, Option<String>)>> {
    let RequestSpec {
        auth,
        request_headers,
        request_authenticators,
        table_headers,
        table_name,
        method,
        base_url,
        url,
        query_pairs,
        body,
        response_format,
        source_schema,
        rate_limit,
        filters,
        state,
        resolved_inputs,
        allow_404_empty,
        link_header_require_results,
    } = request;
    let mut server_error_retries = 0usize;
    let mut throttle_retries = 0usize;
    loop {
        let method_label = http_method_label(method);
        let mut request = build_http_request(http, method, url);

        let mut header_map = HeaderMap::new();
        for header in request_headers.iter().chain(table_headers.iter()) {
            if let Some(value) =
                resolve_value_source(&header.value, filters, state, resolved_inputs)?
            {
                let name = HeaderName::try_from(header.name.as_str()).map_err(|error| {
                    DataFusionError::Execution(format!(
                        "invalid request header name '{}': {error}",
                        header.name
                    ))
                })?;
                let value =
                    HeaderValue::try_from(value_to_string(&value).as_str()).map_err(|error| {
                        DataFusionError::Execution(format!(
                            "invalid request header value for '{}': {error}",
                            header.name
                        ))
                    })?;
                header_map.insert(name, value);
            }
        }
        if matches!(body, Some(RequestBody::Text(_)))
            && !header_map.contains_key(reqwest::header::CONTENT_TYPE)
        {
            header_map.insert(
                reqwest::header::CONTENT_TYPE,
                HeaderValue::from_static("text/plain"),
            );
        }
        if !header_map.is_empty() {
            request = request.headers(header_map);
        }

        if !query_pairs.is_empty() {
            request = request.query(query_pairs);
        }

        match body {
            Some(RequestBody::Json(value)) => {
                request = request.json(value);
            }
            Some(RequestBody::Text(text)) => {
                request = request.body(text.clone());
            }
            None => {}
        }

        let logged_url = build_logged_url(url, query_pairs);
        let _logged_body = body.and_then(|b| match b {
            RequestBody::Json(value) => serde_json::to_string_pretty(value).ok(),
            RequestBody::Text(text) => Some(text.clone()),
        });

        let built = resolve_auth_headers(auth, request, request_authenticators, resolved_inputs)?;

        let response = http.execute(built).await.map_err(|error| {
            request_error(
                source_schema,
                table_name,
                method_label,
                &logged_url,
                request_timeout,
                &error,
            )
        })?;

        match check_rate_limit(
            response.status(),
            response.headers(),
            rate_limit,
            throttle_retries,
        ) {
            RateLimitDecision::Continue => {}
            RateLimitDecision::Retry(wait) => {
                throttle_retries += 1;
                tokio::time::sleep(wait).await;
                continue;
            }
            RateLimitDecision::Fail(error) => {
                return Err(DataFusionError::External(Box::new(
                    ProviderQueryError::RateLimited {
                        source_schema: source_schema.to_string(),
                        table: table_name.to_string(),
                        method: Some(method_label.to_string()),
                        url: Some(logged_url),
                        detail: error.to_string(),
                    },
                )));
            }
        }

        if response.status().is_server_error() && server_error_retries < 2 {
            server_error_retries += 1;
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        }

        if response.status() == reqwest::StatusCode::NOT_FOUND && allow_404_empty {
            return Ok(None);
        }

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(DataFusionError::External(Box::new(
                ProviderQueryError::ApiRequest {
                    source_schema: source_schema.to_string(),
                    table: table_name.to_string(),
                    status: Some(status.as_u16()),
                    method: Some(method_label.to_string()),
                    url: Some(logged_url),
                    filters: filters.clone(),
                    detail: body,
                },
            )));
        }

        let next_url =
            extract_next_link_url(response.headers(), base_url, link_header_require_results)
                .map_err(|error| {
                    pagination_error(
                        source_schema,
                        table_name,
                        Some(method_label),
                        Some(&logged_url),
                        &error,
                    )
                })?;

        let payload = decode_response_body(
            response,
            response_format,
            source_schema,
            table_name,
            method_label,
            &logged_url,
        )
        .await?;
        return Ok(Some((payload, next_url)));
    }
}

async fn decode_response_body(
    response: reqwest::Response,
    format: ResponseBodyFormat,
    source_schema: &str,
    table_name: &str,
    method_label: &str,
    logged_url: &str,
) -> Result<Value> {
    match format {
        ResponseBodyFormat::Json => response.json().await.map_err(|error| {
            decode_error(source_schema, table_name, method_label, logged_url, &error)
        }),
        ResponseBodyFormat::JsonEachRow => {
            let text = response.text().await.map_err(|error| {
                decode_error(source_schema, table_name, method_label, logged_url, &error)
            })?;
            let mut rows = Vec::new();
            for (index, line) in text.lines().enumerate() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let row: Value = serde_json::from_str(trimmed).map_err(|error| {
                    provider_error(ProviderQueryError::Decode {
                        source_schema: source_schema.to_string(),
                        table: table_name.to_string(),
                        method: Some(method_label.to_string()),
                        url: Some(logged_url.to_string()),
                        detail: format!(
                            "source API response decoding failed: json_each_row line {} is not valid JSON: {error}",
                            index + 1
                        ),
                    })
                })?;
                rows.push(row);
            }
            Ok(Value::Array(rows))
        }
    }
}

fn request_error(
    source_schema: &str,
    table_name: &str,
    method_label: &str,
    logged_url: &str,
    request_timeout: Duration,
    error: &reqwest::Error,
) -> DataFusionError {
    let detail = if error.is_timeout() {
        format!(
            "source API request timed out after {}s",
            request_timeout.as_secs_f64()
        )
    } else {
        "source API request failed before a response was received".to_string()
    };

    provider_error(ProviderQueryError::Request {
        source_schema: source_schema.to_string(),
        table: table_name.to_string(),
        method: Some(method_label.to_string()),
        url: Some(logged_url.to_string()),
        detail,
        timed_out: error.is_timeout(),
    })
}

fn decode_error(
    source_schema: &str,
    table_name: &str,
    method_label: &str,
    logged_url: &str,
    error: &reqwest::Error,
) -> DataFusionError {
    provider_error(ProviderQueryError::Decode {
        source_schema: source_schema.to_string(),
        table: table_name.to_string(),
        method: Some(method_label.to_string()),
        url: Some(logged_url.to_string()),
        detail: format!("source API response decoding failed: {error}"),
    })
}

fn pagination_error(
    source_schema: &str,
    table_name: &str,
    method_label: Option<&str>,
    logged_url: Option<&str>,
    error: &DataFusionError,
) -> DataFusionError {
    provider_error(ProviderQueryError::Pagination {
        source_schema: source_schema.to_string(),
        table: table_name.to_string(),
        method: method_label.map(ToOwned::to_owned),
        url: logged_url.map(ToOwned::to_owned),
        detail: datafusion_detail(error),
    })
}

fn provider_error(error: ProviderQueryError) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}

fn datafusion_detail(error: &DataFusionError) -> String {
    match error {
        DataFusionError::Execution(detail) => detail.clone(),
        other => other.to_string(),
    }
}

fn http_method_label(method: HttpMethod) -> &'static str {
    match method {
        HttpMethod::GET => "GET",
        HttpMethod::POST => "POST",
    }
}

fn build_http_request(
    http: &reqwest::Client,
    method: HttpMethod,
    url: &str,
) -> reqwest::RequestBuilder {
    match method {
        HttpMethod::GET => http.get(url),
        HttpMethod::POST => http.post(url),
    }
}

fn build_query_pairs(
    request: &coral_spec::RequestSpec,
    filters: &HashMap<String, String>,
    state: &PageState,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<Vec<(String, String)>> {
    let state_values = pagination_state_values(state);
    let mut params = Vec::new();

    for param in &request.query {
        let value = resolve_value_source(&param.value, filters, &state_values, resolved_inputs)?;
        if let Some(value) = value {
            params.push((param.name.clone(), value_to_string(&value)));
        }
    }

    Ok(params)
}

fn apply_pagination_query_pairs(
    params: &mut Vec<(String, String)>,
    table: &HttpTableSpec,
    pagination: &ValidatedPagination,
    state: &PageState,
    page_size: Option<usize>,
) -> Result<()> {
    if let (Some(page_size), Some(spec)) = (page_size, pagination.page_size.as_ref())
        && let Some(name) = &spec.query_param
    {
        params.push((name.clone(), page_size.to_string()));
    }

    match &pagination.mode {
        ValidatedPaginationMode::None
        | ValidatedPaginationMode::Auto
        | ValidatedPaginationMode::CursorBody
        | ValidatedPaginationMode::LinkHeader => {}
        ValidatedPaginationMode::CursorQuery => {
            if let Some(cursor) = &state.cursor {
                let name = table.pagination.cursor_param.clone().ok_or_else(|| {
                    DataFusionError::Execution(
                        "cursor_query pagination requires cursor_param".to_string(),
                    )
                })?;
                params.push((name, cursor.clone()));
            }
        }
        ValidatedPaginationMode::Page => {
            let name = table.pagination.page_param.clone().ok_or_else(|| {
                DataFusionError::Execution("page pagination requires page_param".to_string())
            })?;
            params.push((name, state.page.to_string()));
        }
        ValidatedPaginationMode::Offset(offset) => {
            params.push((offset.param.clone(), state.offset.to_string()));
        }
    }

    Ok(())
}

fn build_request_body(
    request: &coral_spec::RequestSpec,
    filters: &HashMap<String, String>,
    state: &PageState,
    resolved_inputs: &BTreeMap<String, String>,
) -> Result<Option<RequestBody>> {
    let state_values = pagination_state_values(state);
    match &request.body {
        BodySpec::Json { fields } => {
            if fields.is_empty() {
                return Ok(None);
            }
            let mut root = Value::Object(Map::new());
            for field in fields {
                if let Some(value) =
                    resolve_value_source(&field.value, filters, &state_values, resolved_inputs)?
                {
                    set_path_value(&mut root, &field.path, value)?;
                }
            }
            Ok(Some(RequestBody::Json(root)))
        }
        BodySpec::Text { content } => {
            let Some(value) =
                resolve_value_source(content, filters, &state_values, resolved_inputs)?
            else {
                return Ok(None);
            };
            Ok(Some(RequestBody::Text(value_to_string(&value))))
        }
    }
}

fn apply_pagination_body_fields(
    body: &mut Option<RequestBody>,
    body_spec: &BodySpec,
    table: &HttpTableSpec,
    pagination: &ValidatedPagination,
    state: &PageState,
    page_size: Option<usize>,
) -> Result<()> {
    let needs_page_size_body = page_size
        .zip(pagination.page_size.as_ref())
        .is_some_and(|(_, spec)| !spec.body_path.is_empty());
    let needs_cursor_body = matches!(pagination.mode, ValidatedPaginationMode::CursorBody)
        && !table.pagination.cursor_body_path.is_empty()
        && state.cursor.is_some();

    if !needs_page_size_body && !needs_cursor_body {
        return Ok(());
    }

    if matches!(body_spec, BodySpec::Text { .. }) || matches!(body, Some(RequestBody::Text(_))) {
        return Err(DataFusionError::Execution(
            "pagination body fields are not supported with text request bodies".to_string(),
        ));
    }

    if body.is_none() {
        *body = Some(RequestBody::Json(Value::Object(Map::new())));
    }
    let root = match body.as_mut().expect("body is present") {
        RequestBody::Json(root) => root,
        RequestBody::Text(_) => unreachable!("text body rejected above"),
    };

    if let (Some(page_size), Some(spec)) = (page_size, pagination.page_size.as_ref())
        && !spec.body_path.is_empty()
    {
        set_path_value(root, &spec.body_path, json!(page_size))?;
    }

    if matches!(pagination.mode, ValidatedPaginationMode::CursorBody)
        && let Some(cursor) = &state.cursor
    {
        if table.pagination.cursor_body_path.is_empty() {
            return Err(DataFusionError::Execution(
                "cursor_body pagination requires cursor_body_path".to_string(),
            ));
        }
        set_path_value(root, &table.pagination.cursor_body_path, json!(cursor))?;
    }

    Ok(())
}

fn resolve_page_size(spec: Option<&PageSizeSpec>, sql_limit: Option<usize>) -> Option<usize> {
    let spec = spec?;
    let base = sql_limit.unwrap_or(spec.default);
    Some(base.min(spec.max).max(1))
}

fn page_is_exhausted(rows_on_page: usize, page_size: Option<usize>) -> bool {
    rows_on_page == 0 || page_size.is_some_and(|requested| rows_on_page < requested)
}

fn pagination_state_values(state: &PageState) -> HashMap<String, String> {
    let mut values = HashMap::new();
    values.insert("page".to_string(), state.page.to_string());
    values.insert("offset".to_string(), state.offset.to_string());
    if let Some(cursor) = &state.cursor {
        values.insert("cursor".to_string(), cursor.clone());
    }
    values
}

fn build_logged_url(url: &str, query_pairs: &[(String, String)]) -> String {
    if query_pairs.is_empty() {
        return url.to_string();
    }
    let suffix = query_pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    if url.contains('?') {
        format!("{url}&{suffix}")
    } else {
        format!("{url}?{suffix}")
    }
}

fn join_url(base: &str, path: &str) -> Result<String> {
    let trimmed = path.trim();
    if reqwest::Url::parse(trimmed).is_ok() || trimmed.starts_with("//") {
        return Err(DataFusionError::Execution(
            "request path must be relative; absolute URLs are not allowed".to_string(),
        ));
    }
    let base = base.trim_end_matches('/');
    if trimmed.starts_with('/') {
        Ok(format!("{base}{trimmed}"))
    } else {
        Ok(format!("{base}/{trimmed}"))
    }
}

fn normalize_base_url(base: &str) -> String {
    let trimmed = base.trim();
    if trimmed.is_empty() {
        return String::new();
    }
    if trimmed.starts_with("https://") || trimmed.starts_with("http://") {
        return trimmed.to_string();
    }
    if trimmed.starts_with("//") {
        return format!("https:{trimmed}");
    }
    format!("https://{trimmed}")
}

fn set_path_value(root: &mut Value, path: &[String], value: Value) -> Result<()> {
    if path.is_empty() {
        *root = value;
        return Ok(());
    }

    set_path_value_at(root, path, value)
}

fn set_path_value_at(cursor: &mut Value, path: &[String], value: Value) -> Result<()> {
    let Some((head, tail)) = path.split_first() else {
        *cursor = value;
        return Ok(());
    };

    if let Ok(index) = head.parse::<usize>() {
        if !cursor.is_array() {
            *cursor = Value::Array(Vec::new());
        }
        let array = cursor.as_array_mut().ok_or_else(|| {
            DataFusionError::Execution("failed to create JSON array path".to_string())
        })?;
        if array.len() <= index {
            const MAX_JSON_ARRAY_INDEX: usize = 10_000;
            if index > MAX_JSON_ARRAY_INDEX {
                return Err(DataFusionError::Execution(format!(
                    "JSON array index {index} exceeds supported maximum {MAX_JSON_ARRAY_INDEX}"
                )));
            }
            array.resize_with(index + 1, || Value::Null);
        }
        return set_path_value_at(&mut array[index], tail, value);
    }

    if !cursor.is_object() {
        *cursor = Value::Object(Map::new());
    }
    let obj = cursor.as_object_mut().ok_or_else(|| {
        DataFusionError::Execution("failed to create JSON object path".to_string())
    })?;
    let next = obj.entry(head.clone()).or_insert(Value::Null);
    set_path_value_at(next, tail, value)
}

#[allow(
    clippy::unnecessary_wraps,
    reason = "Keeping a Result return type preserves a uniform extraction interface for callers"
)]
fn extract_rows(table: &HttpTableSpec, payload: &Value) -> Result<Vec<Value>> {
    match table.response.row_strategy {
        RowStrategy::Direct => {
            let root = if table.response.rows_path.is_empty() {
                payload
            } else {
                get_path_value(payload, &table.response.rows_path).unwrap_or(&Value::Null)
            };
            match root {
                Value::Array(items) => Ok(items.clone()),
                Value::Null => Ok(Vec::new()),
                other => Ok(vec![other.clone()]),
            }
        }
        RowStrategy::SeriesPointList => {
            let mut rows = Vec::new();
            let series = get_path_value(payload, &["series".to_string()])
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();

            for item in series {
                let metric = item
                    .get("metric")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                let scope = item
                    .get("scope")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_string();
                if let Some(pointlist) = item.get("pointlist").and_then(Value::as_array) {
                    for point in pointlist {
                        if let Some(pair) = point.as_array() {
                            let Some(raw_timestamp) = pair.first().and_then(Value::as_f64) else {
                                continue;
                            };
                            let Some(value) = pair.get(1).and_then(Value::as_f64) else {
                                continue;
                            };
                            #[allow(
                                clippy::cast_possible_truncation,
                                reason = "Series timestamps are integral epoch values that fit in i64"
                            )]
                            let timestamp = raw_timestamp as i64;
                            rows.push(json!({
                                "metric": metric,
                                "scope": scope,
                                "timestamp": timestamp,
                                "value": value
                            }));
                        }
                    }
                }
            }

            Ok(rows)
        }
        RowStrategy::DictEntries => {
            let root = if table.response.rows_path.is_empty() {
                payload
            } else {
                get_path_value(payload, &table.response.rows_path).unwrap_or(&Value::Null)
            };
            match root {
                Value::Object(map) => {
                    let mut rows = Vec::with_capacity(map.len());
                    for (key, value) in map {
                        let mut row = if let Value::Object(obj) = value {
                            obj.clone()
                        } else {
                            let mut row = serde_json::Map::new();
                            row.insert("_value".to_string(), value.clone());
                            row
                        };
                        row.insert("_key".to_string(), Value::String(key.clone()));
                        rows.push(Value::Object(row));
                    }
                    Ok(rows)
                }
                _ => Ok(Vec::new()),
            }
        }
    }
}

fn extract_next_link_url(
    headers: &HeaderMap,
    base_url: &str,
    require_results_true: bool,
) -> Result<Option<String>> {
    let Some(header) = headers.get("link") else {
        return Ok(None);
    };
    let Ok(header) = header.to_str() else {
        return Ok(None);
    };
    let base = reqwest::Url::parse(base_url).map_err(|e| {
        DataFusionError::Execution(format!(
            "invalid base URL for pagination links '{base_url}': {e}"
        ))
    })?;
    for part in header.split(',') {
        let item = part.trim();
        if !item.contains("rel=\"next\"") {
            continue;
        }
        if require_results_true && !item.contains("results=\"true\"") {
            continue;
        }
        let start = item.find('<').ok_or_else(|| {
            DataFusionError::Execution(format!("invalid pagination Link header item '{item}'"))
        })?;
        let end = item.find('>').ok_or_else(|| {
            DataFusionError::Execution(format!("invalid pagination Link header item '{item}'"))
        })?;
        let next_raw = &item[start + 1..end];
        let next_url = base.join(next_raw).map_err(|e| {
            DataFusionError::Execution(format!("invalid pagination next link '{next_raw}': {e}"))
        })?;
        if next_url.origin() != base.origin() {
            return Err(DataFusionError::Execution(format!(
                "pagination next link must stay on origin {}: {next_raw}",
                base.origin().ascii_serialization()
            )));
        }
        return Ok(Some(next_url.to_string()));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::time::Duration;

    use datafusion::error::DataFusionError;
    use reqwest::header::{HeaderMap, HeaderValue};
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    use super::{
        HttpSourceClient, PageState, RequestSpec as HttpRequestSpec, apply_pagination_body_fields,
        apply_pagination_query_pairs, execute_request, extract_next_link_url, extract_rows,
        join_url, normalize_base_url, page_is_exhausted, resolve_value_source, set_path_value,
    };
    use crate::backends::http::ProviderQueryError;
    use coral_spec::PaginationMode;
    use coral_spec::backends::http::{HttpSourceManifest, HttpTableSpec, RateLimitSpec};
    use coral_spec::{
        AuthSpec, BodySpec, HttpMethod, PaginationSpec, ParsedTemplate, RequestSpec,
        ResponseBodyFormat, RowStrategy, ValidatedPaginationMode, ValueSourceSpec,
        parse_source_manifest_value,
    };

    fn parse_http_manifest(value: serde_json::Value) -> HttpSourceManifest {
        parse_source_manifest_value(value)
            .expect("manifest should deserialize")
            .as_http()
            .expect("http manifest")
            .clone()
    }

    async fn spawn_hanging_http_server() -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind hanging http server");
        let addr = listener.local_addr().expect("local addr");
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept hanging request");
            let _socket = socket;
            std::future::pending::<()>().await;
        });

        (format!("http://{addr}"), task)
    }

    fn test_http_table_spec(columns: &serde_json::Value, request: &RequestSpec) -> HttpTableSpec {
        parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "http",
            "base_url": "https://api.example.com",
            "tables": [{
                "name": "items",
                "description": "items",
                "request": request_json(request),
                "columns": columns
            }]
        }))
        .tables
        .into_iter()
        .next()
        .expect("table should exist")
    }

    #[test]
    fn set_path_value_builds_arrays_from_numeric_segments() {
        let mut root = json!({});

        set_path_value(
            &mut root,
            &[
                "Dimensions".to_string(),
                "0".to_string(),
                "Name".to_string(),
            ],
            json!("ClusterName"),
        )
        .expect("path assignment should succeed");
        set_path_value(
            &mut root,
            &[
                "Dimensions".to_string(),
                "0".to_string(),
                "Value".to_string(),
            ],
            json!("titaness"),
        )
        .expect("path assignment should succeed");
        set_path_value(
            &mut root,
            &["Statistics".to_string(), "0".to_string()],
            json!("Average"),
        )
        .expect("path assignment should succeed");

        assert_eq!(
            root,
            json!({
                "Dimensions": [{
                    "Name": "ClusterName",
                    "Value": "titaness"
                }],
                "Statistics": ["Average"]
            })
        );
    }

    fn request_json(request: &RequestSpec) -> serde_json::Value {
        let body = match &request.body {
            BodySpec::Json { fields } => fields
                .iter()
                .map(|field| {
                    json!({
                        "path": field.path,
                        "value": value_source_json(&field.value),
                    })
                })
                .collect::<Vec<_>>(),
            BodySpec::Text { .. } => Vec::new(),
        };
        json!({
            "method": format!("{:?}", request.method),
            "path": request.path,
            "query": request.query.iter().map(|query| json!({
                "name": query.name,
                "value": value_source_json(&query.value),
            })).collect::<Vec<_>>(),
            "body": body,
            "headers": request.headers.iter().map(|header| json!({
                "name": header.name,
                "value": value_source_json(&header.value),
            })).collect::<Vec<_>>(),
        })
    }

    fn value_source_json(value: &ValueSourceSpec) -> serde_json::Value {
        match value {
            ValueSourceSpec::Literal { value } => json!({
                "from": "literal",
                "value": value,
            }),
            ValueSourceSpec::Filter { key, default } => json!({
                "from": "filter",
                "key": key,
                "default": default,
            }),
            ValueSourceSpec::FilterInt { key, default } => json!({
                "from": "filter_int",
                "key": key,
                "default": default,
            }),
            ValueSourceSpec::FilterBool { key, default } => json!({
                "from": "filter_bool",
                "key": key,
                "default": default,
            }),
            ValueSourceSpec::Input { key } => json!({
                "from": "input",
                "key": key,
            }),
            ValueSourceSpec::Template { template } => json!({
                "from": "template",
                "template": template,
            }),
            ValueSourceSpec::State { key } => json!({
                "from": "state",
                "key": key,
            }),
            ValueSourceSpec::NowEpochMinusSeconds { seconds } => json!({
                "from": "now_epoch_minus_seconds",
                "seconds": seconds,
            }),
        }
    }

    #[test]
    fn normalize_base_url_adds_https_scheme_for_host_only_values() {
        assert_eq!(
            normalize_base_url("eu.posthog.com"),
            "https://eu.posthog.com"
        );
        assert_eq!(
            normalize_base_url("//api.example.com"),
            "https://api.example.com"
        );
    }

    #[test]
    fn normalize_base_url_preserves_existing_schemes() {
        assert_eq!(
            normalize_base_url("https://api.github.com"),
            "https://api.github.com"
        );
        assert_eq!(
            normalize_base_url("http://localhost:8080"),
            "http://localhost:8080"
        );
    }

    #[test]
    fn join_url_handles_relative_paths() {
        assert_eq!(
            join_url("https://api.example.com", "/v1/resources").unwrap(),
            "https://api.example.com/v1/resources"
        );
        assert_eq!(
            join_url("https://api.example.com/", "v1/resources").unwrap(),
            "https://api.example.com/v1/resources"
        );
    }

    #[test]
    fn join_url_rejects_absolute_paths() {
        let err = join_url("https://api.example.com", "https://next.example.com/page").unwrap_err();
        assert!(
            err.to_string()
                .contains("request path must be relative; absolute URLs are not allowed")
        );
    }

    #[test]
    fn extract_next_link_url_resolves_relative_links_on_same_origin() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "link",
            HeaderValue::from_static("</v1/resources?page=2>; rel=\"next\""),
        );

        let next = extract_next_link_url(&headers, "https://api.example.com", false).unwrap();

        assert_eq!(
            next,
            Some("https://api.example.com/v1/resources?page=2".to_string())
        );
    }

    #[test]
    fn extract_next_link_url_rejects_cross_origin_absolute_links() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "link",
            HeaderValue::from_static("<https://attacker.example/steal>; rel=\"next\""),
        );

        let err = extract_next_link_url(&headers, "https://api.example.com", false).unwrap_err();

        assert!(
            err.to_string()
                .contains("pagination next link must stay on origin https://api.example.com")
        );
    }

    #[test]
    fn resolve_value_source_uses_provider_scoped_credentials() {
        let resolved_inputs = BTreeMap::from([("API_KEY".to_string(), "alpha-secret".to_string())]);

        let value = resolve_value_source(
            &ValueSourceSpec::Input {
                key: "API_KEY".to_string(),
            },
            &HashMap::new(),
            &HashMap::new(),
            &resolved_inputs,
        )
        .expect("input lookup should succeed");

        assert_eq!(value, Some(json!("alpha-secret")));
    }

    #[test]
    fn resolve_value_source_uses_declared_store_without_fallback() {
        let resolved_inputs = BTreeMap::new();

        let value = resolve_value_source(
            &ValueSourceSpec::Input {
                key: "API_KEY".to_string(),
            },
            &HashMap::new(),
            &HashMap::new(),
            &resolved_inputs,
        )
        .expect("input lookup should succeed");

        assert_eq!(value, None);
    }

    #[test]
    fn resolve_value_source_parses_filter_ints_as_numbers() {
        let filters = HashMap::from([("start_time".to_string(), "1700000000000000".to_string())]);

        let value = resolve_value_source(
            &ValueSourceSpec::FilterInt {
                key: "start_time".to_string(),
                default: None,
            },
            &filters,
            &HashMap::new(),
            &BTreeMap::new(),
        )
        .expect("integer filter should resolve");

        assert_eq!(value, Some(json!(1_700_000_000_000_000_i64)));
    }

    #[test]
    fn resolve_value_source_rejects_invalid_filter_ints() {
        let filters = HashMap::from([("start_time".to_string(), "not-a-number".to_string())]);

        let error = resolve_value_source(
            &ValueSourceSpec::FilterInt {
                key: "start_time".to_string(),
                default: None,
            },
            &filters,
            &HashMap::new(),
            &BTreeMap::new(),
        )
        .expect_err("invalid integer filter should fail");

        assert!(
            error
                .to_string()
                .contains("filter 'start_time' value 'not-a-number' is not a valid i64")
        );
    }

    #[test]
    fn resolve_value_source_parses_filter_bools_as_bools() {
        let filters = HashMap::from([("descending".to_string(), "false".to_string())]);

        let value = resolve_value_source(
            &ValueSourceSpec::FilterBool {
                key: "descending".to_string(),
                default: None,
            },
            &filters,
            &HashMap::new(),
            &BTreeMap::new(),
        )
        .expect("bool filter should resolve");

        assert_eq!(value, Some(json!(false)));
    }

    #[test]
    fn backend_client_requires_source_scoped_credentials() {
        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "alpha",
            "version": "0.1.0",
            "backend": "http",
            "base_url": "https://api.example.com",
            "auth": {
                "type": "HeaderAuth",
                "headers": [{
                    "name": "Authorization",
                    "from": "template",
                    "template": "Bearer {{input.API_KEY}}"
                }]
            },
            "inputs": {
                "API_KEY": { "kind": "secret" }
            },
            "tables": [{
                "name": "items",
                "description": "items",
                "request": { "path": "/items" },
                "columns": [{
                    "name": "id",
                    "type": "Utf8"
                }]
            }]
        }));
        let source_secrets = BTreeMap::new();

        let error = HttpSourceClient::from_manifest(
            &manifest,
            &source_secrets,
            &BTreeMap::new(),
            &HashMap::new(),
        )
        .expect_err("missing source-scoped credentials must fail");

        assert!(
            error
                .to_string()
                .contains("missing source input 'API_KEY' for template token")
        );
    }

    #[test]
    fn backend_client_rejects_unresolved_table_request_path_inputs() {
        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "alpha",
            "version": "0.1.0",
            "backend": "http",
            "base_url": "https://api.example.com",
            "inputs": {
                "API_KEY": { "kind": "secret" },
                "ACCOUNT_ID": { "kind": "variable" }
            },
            "tables": [{
                "name": "items",
                "description": "items",
                "request": {
                    "path": "/{{input.ACCOUNT_ID}}/items"
                },
                "columns": [{
                    "name": "id",
                    "type": "Utf8"
                }]
            }]
        }));

        let error = HttpSourceClient::from_manifest(
            &manifest,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &HashMap::new(),
        )
        .expect_err("missing table request path inputs must fail");

        assert!(
            error
                .to_string()
                .contains("table 'items' request path could not be resolved")
        );
    }

    #[test]
    fn backend_client_rejects_unresolved_table_request_header_inputs() {
        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "alpha",
            "version": "0.1.0",
            "backend": "http",
            "base_url": "https://api.example.com",
            "inputs": {
                "ACCOUNT_ID": { "kind": "variable" }
            },
            "tables": [{
                "name": "items",
                "description": "items",
                "request": {
                    "path": "/items",
                    "headers": [{
                        "name": "X-Account",
                        "from": "input",
                        "key": "ACCOUNT_ID"
                    }]
                },
                "columns": [{
                    "name": "id",
                    "type": "Utf8"
                }]
            }]
        }));

        let error = HttpSourceClient::from_manifest(
            &manifest,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &HashMap::new(),
        )
        .expect_err("missing table request header inputs must fail");

        assert!(
            error
                .to_string()
                .contains("table 'items' request header 'X-Account' could not be resolved")
        );
    }

    #[test]
    fn backend_client_rejects_unresolved_table_request_query_inputs() {
        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "alpha",
            "version": "0.1.0",
            "backend": "http",
            "base_url": "https://api.example.com",
            "inputs": {
                "ACCOUNT_ID": { "kind": "variable" }
            },
            "tables": [{
                "name": "items",
                "description": "items",
                "request": {
                    "path": "/items",
                    "query": [{
                        "name": "account_id",
                        "from": "input",
                        "key": "ACCOUNT_ID"
                    }]
                },
                "columns": [{
                    "name": "id",
                    "type": "Utf8"
                }]
            }]
        }));

        let error = HttpSourceClient::from_manifest(
            &manifest,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &HashMap::new(),
        )
        .expect_err("missing table request query inputs must fail");

        assert!(
            error
                .to_string()
                .contains("table 'items' request query param 'account_id' could not be resolved")
        );
    }

    #[test]
    fn backend_client_rejects_unresolved_table_request_body_inputs() {
        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "alpha",
            "version": "0.1.0",
            "backend": "http",
            "base_url": "https://api.example.com",
            "inputs": {
                "ACCOUNT_ID": { "kind": "variable" }
            },
            "tables": [{
                "name": "items",
                "description": "items",
                "request": {
                    "method": "POST",
                    "path": "/items",
                    "body": [{
                        "path": ["account", "id"],
                        "from": "input",
                        "key": "ACCOUNT_ID"
                    }]
                },
                "columns": [{
                    "name": "id",
                    "type": "Utf8"
                }]
            }]
        }));

        let error = HttpSourceClient::from_manifest(
            &manifest,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &HashMap::new(),
        )
        .expect_err("missing table request body inputs must fail");

        assert!(
            error
                .to_string()
                .contains("table 'items' request body field 'account.id' could not be resolved")
        );
    }

    #[test]
    fn backend_client_rejects_unresolved_request_route_inputs() {
        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "alpha",
            "version": "0.1.0",
            "backend": "http",
            "base_url": "https://api.example.com",
            "inputs": {
                "ACCOUNT_ID": { "kind": "variable" }
            },
            "tables": [{
                "name": "items",
                "description": "items",
                "request": { "path": "/items" },
                "requests": [{
                    "when_filters": ["account_id"],
                    "method": "GET",
                    "path": "/{{input.ACCOUNT_ID}}/items"
                }],
                "filters": [{
                    "name": "account_id"
                }],
                "columns": [{
                    "name": "id",
                    "type": "Utf8"
                }]
            }]
        }));

        let error = HttpSourceClient::from_manifest(
            &manifest,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &HashMap::new(),
        )
        .expect_err("missing request route inputs must fail");

        assert!(error.to_string().contains(
            "table 'items' request route for filters [account_id] path could not be resolved"
        ));
    }

    #[test]
    fn apply_pagination_query_pairs_uses_typed_offset_param() {
        let table = test_http_table_spec(
            &json!([]),
            &RequestSpec {
                method: HttpMethod::GET,
                path: ParsedTemplate::parse("/items").expect("template"),
                query: vec![],
                body: BodySpec::default(),
                headers: vec![],
            },
        );
        let pagination = PaginationSpec {
            mode: PaginationMode::Offset,
            page_size: Some(coral_spec::PageSizeSpec {
                default: 25,
                max: 100,
                query_param: Some("limit".to_string()),
                body_path: vec![],
            }),
            offset_param: Some("start".to_string()),
            offset_start: 10,
            offset_step: Some(25),
            ..PaginationSpec::default()
        }
        .validated("demo", "items")
        .unwrap();
        let mut params = Vec::new();
        let state = PageState {
            offset: 35,
            ..PageState::default()
        };

        apply_pagination_query_pairs(&mut params, &table, &pagination, &state, Some(25)).unwrap();

        assert_eq!(
            params,
            vec![
                ("limit".to_string(), "25".to_string()),
                ("start".to_string(), "35".to_string()),
            ]
        );
        assert!(matches!(
            pagination.mode,
            ValidatedPaginationMode::Offset(_)
        ));
    }

    #[test]
    fn apply_pagination_body_fields_rejects_declared_text_body_even_when_absent() {
        let table = test_http_table_spec(
            &json!([]),
            &RequestSpec {
                method: HttpMethod::GET,
                path: ParsedTemplate::parse("/items").expect("template"),
                query: vec![],
                body: BodySpec::default(),
                headers: vec![],
            },
        );
        let body_spec = BodySpec::Text {
            content: ValueSourceSpec::Filter {
                key: "sql".to_string(),
                default: None,
            },
        };
        let pagination = PaginationSpec {
            page_size: Some(coral_spec::PageSizeSpec {
                default: 25,
                max: 100,
                query_param: None,
                body_path: vec!["limit".to_string()],
            }),
            ..PaginationSpec::default()
        }
        .validated("demo", "items")
        .unwrap();
        let mut body = None;

        let error = apply_pagination_body_fields(
            &mut body,
            &body_spec,
            &table,
            &pagination,
            &PageState::default(),
            Some(25),
        )
        .expect_err("text request bodies must not receive pagination body fields");

        assert!(
            error
                .to_string()
                .contains("pagination body fields are not supported with text request bodies")
        );
        assert!(body.is_none());
    }

    #[test]
    fn page_is_exhausted_handles_empty_short_and_full_pages() {
        for (rows_on_page, page_size, expected) in
            [(0, Some(50), true), (24, Some(25), true), (24, None, false)]
        {
            assert_eq!(page_is_exhausted(rows_on_page, page_size), expected);
        }
    }

    fn make_table_with_row_strategy(
        strategy: RowStrategy,
        rows_path: Vec<String>,
    ) -> coral_spec::backends::http::HttpTableSpec {
        let mut table = test_http_table_spec(
            &json!([]),
            &RequestSpec {
                method: HttpMethod::GET,
                path: ParsedTemplate::parse("/items").expect("template"),
                query: vec![],
                body: BodySpec::default(),
                headers: vec![],
            },
        );
        table.response.rows_path = rows_path;
        table.response.row_strategy = strategy;
        table
    }

    #[test]
    fn dict_entries_flattens_object_values() {
        let table =
            make_table_with_row_strategy(RowStrategy::DictEntries, vec!["result".to_string()]);
        let payload = json!({
            "result": {
                "2024-02-27 EST": {"Open": 8.29, "Close": 8.15},
                "2024-02-28 EST": {"Open": 7.85, "Close": 7.90}
            }
        });

        let rows = extract_rows(&table, &payload).unwrap();
        assert_eq!(rows.len(), 2);
        for row in &rows {
            assert!(row.get("_key").is_some());
            assert!(row.get("Open").is_some());
            assert!(row.get("Close").is_some());
        }

        let keys: Vec<&str> = rows
            .iter()
            .filter_map(|row| row.get("_key").and_then(|value| value.as_str()))
            .collect();
        assert!(keys.contains(&"2024-02-27 EST"));
        assert!(keys.contains(&"2024-02-28 EST"));
    }

    #[test]
    fn dict_entries_uses_value_field_for_scalars() {
        let table =
            make_table_with_row_strategy(RowStrategy::DictEntries, vec!["result".to_string()]);
        let payload = json!({
            "result": {
                "2020-01-15 EST": 0.058,
                "2020-06-12 EST": 0.2
            }
        });

        let rows = extract_rows(&table, &payload).unwrap();
        assert_eq!(rows.len(), 2);
        for row in &rows {
            assert!(row.get("_key").is_some());
            assert!(row.get("_value").is_some());
        }
    }

    #[test]
    fn dict_entries_returns_empty_for_null() {
        let table =
            make_table_with_row_strategy(RowStrategy::DictEntries, vec!["result".to_string()]);
        let payload = json!({ "result": null });

        let rows = extract_rows(&table, &payload).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn dict_entries_returns_empty_for_missing_path() {
        let table =
            make_table_with_row_strategy(RowStrategy::DictEntries, vec!["missing".to_string()]);
        let payload = json!({ "result": { "a": 1 } });

        let rows = extract_rows(&table, &payload).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn dict_entries_returns_empty_for_non_object() {
        let table =
            make_table_with_row_strategy(RowStrategy::DictEntries, vec!["result".to_string()]);
        let payload = json!({ "result": [1, 2, 3] });

        let rows = extract_rows(&table, &payload).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn dict_entries_empty_dict_returns_empty() {
        let table =
            make_table_with_row_strategy(RowStrategy::DictEntries, vec!["result".to_string()]);
        let payload = json!({ "result": {} });

        let rows = extract_rows(&table, &payload).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn series_point_list_skips_malformed_points() {
        let table = make_table_with_row_strategy(RowStrategy::SeriesPointList, vec![]);
        let payload = json!({
            "series": [{
                "metric": "system.cpu.user",
                "scope": "host:demo",
                "pointlist": [
                    [1_710_000_000, 42.5],
                    [1_710_000_060],
                    [null, 1.0],
                    ["1710000120", 2.0],
                    [1_710_000_180, "3.0"],
                    {"timestamp": 1_710_000_240, "value": 4.0}
                ]
            }]
        });

        let rows = extract_rows(&table, &payload).unwrap();

        assert_eq!(
            rows,
            vec![json!({
                "metric": "system.cpu.user",
                "scope": "host:demo",
                "timestamp": 1_710_000_000_i64,
                "value": 42.5
            })]
        );
    }

    #[test]
    fn parse_manifest_accepts_dict_entries_row_strategy() {
        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "alpha",
            "version": "0.1.0",
            "backend": "http",
            "base_url": "https://api.example.com",
            "tables": [{
                "name": "items",
                "description": "items",
                "request": { "path": "/items" },
                "response": {
                    "rows_path": ["result"],
                    "row_strategy": "dict_entries"
                },
                "columns": [{
                    "name": "_key",
                    "type": "Utf8"
                }]
            }]
        }));
        assert!(matches!(
            manifest.tables[0].response.row_strategy,
            RowStrategy::DictEntries
        ));
    }

    #[tokio::test]
    async fn execute_request_times_out_when_upstream_stalls() {
        let (base_url, task) = spawn_hanging_http_server().await;
        let request_timeout = Duration::from_millis(100);
        let http = reqwest::Client::builder()
            .timeout(request_timeout)
            .build()
            .expect("build test client");
        let url = format!("{base_url}/items");
        let query_pairs = vec![("api_key".to_string(), "secret-token".to_string())];
        let filters = HashMap::new();
        let state = HashMap::new();
        let resolved_inputs = BTreeMap::new();

        let error = execute_request(
            &http,
            request_timeout,
            HttpRequestSpec {
                auth: &AuthSpec::default(),
                request_headers: &[],
                request_authenticators: &HashMap::new(),
                table_headers: &[],
                table_name: "items",
                method: HttpMethod::GET,
                base_url: &base_url,
                url: &url,
                query_pairs: &query_pairs,
                body: None,
                response_format: ResponseBodyFormat::default(),
                source_schema: "demo",
                rate_limit: &RateLimitSpec::default(),
                filters: &filters,
                state: &state,
                resolved_inputs: &resolved_inputs,
                allow_404_empty: false,
                link_header_require_results: false,
            },
        )
        .await
        .expect_err("hung upstream should time out");

        match error {
            DataFusionError::External(inner) => {
                let provider_error = inner
                    .downcast_ref::<ProviderQueryError>()
                    .expect("timeout should be a provider query error");
                match provider_error {
                    ProviderQueryError::Request {
                        source_schema,
                        table,
                        detail,
                        timed_out,
                        ..
                    } => {
                        assert_eq!(source_schema, "demo");
                        assert_eq!(table, "items");
                        assert!(*timed_out);
                        assert!(detail.contains("timed out"));
                        assert!(!detail.contains("secret-token"));
                    }
                    other => panic!("expected request provider error, got {other:?}"),
                }
                let structured = provider_error.to_structured();
                assert_eq!(
                    structured.metadata().get("url").map(String::as_str),
                    Some(format!("{base_url}/items").as_str())
                );
                assert!(!structured.detail().contains("secret-token"));
            }
            other => panic!("expected external provider error, got {other:?}"),
        }
        task.abort();
    }

    #[test]
    fn parse_manifest_accepts_source_rate_limit_policy() {
        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "alpha",
            "version": "0.1.0",
            "backend": "http",
            "base_url": "https://api.example.com",
            "rate_limit": {
                "extra_statuses": [403],
                "remaining_header": "X-RateLimit-Remaining",
                "reset_header": "X-RateLimit-Reset"
            },
            "tables": [{
                "name": "items",
                "description": "items",
                "request": { "path": "/items" },
                "columns": [{
                    "name": "id",
                    "type": "Utf8"
                }]
            }]
        }));

        assert_eq!(manifest.rate_limit.extra_statuses, vec![403]);
        assert_eq!(
            manifest.rate_limit.remaining_header.as_deref(),
            Some("X-RateLimit-Remaining")
        );
    }
}
