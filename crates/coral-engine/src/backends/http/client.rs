//! HTTP client used by HTTP-backed source tables.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use datafusion::error::{DataFusionError, Result};
use reqwest::header::HeaderMap;
use serde_json::{Map, Value, json};

use crate::backends::http::ProviderQueryError;
use crate::backends::http::rate_limit::{RateLimitDecision, check_rate_limit};
use crate::backends::shared::json_path::get_path_value;
use coral_spec::backends::http::{HttpSourceManifest, HttpTableSpec, RateLimitSpec};
use coral_spec::{
    HeaderSpec, HttpMethod, PageSizeSpec, ParsedTemplate, RowStrategy, TemplateNamespace,
    TemplatePart, ValidatedPagination, ValidatedPaginationMode, ValueSourceSpec,
};

const DEFAULT_MAX_PAGES: usize = 10_000;
const DEFAULT_HTTP_REQUEST_TIMEOUT_SECS: u64 = 30;

/// Executes manifest-driven HTTP requests for one registered source.
#[derive(Clone)]
pub(crate) struct HttpSourceClient {
    http: reqwest::Client,
    request_timeout: Duration,
    source_schema: String,
    base_url: ParsedTemplate,
    auth_headers: Vec<HeaderSpec>,
    rate_limit: RateLimitSpec,
    source_secrets: Arc<BTreeMap<String, String>>,
    source_variables: Arc<BTreeMap<String, String>>,
}

impl std::fmt::Debug for HttpSourceClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HttpSourceClient")
            .field("source_schema", &self.source_schema)
            .field("base_url", &self.base_url)
            .field("auth_headers", &self.auth_headers)
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

struct RequestSpec<'a> {
    auth_headers: &'a [HeaderSpec],
    table_headers: &'a [HeaderSpec],
    table_name: &'a str,
    method: HttpMethod,
    base_url: &'a str,
    url: &'a str,
    query_pairs: &'a [(String, String)],
    body: Option<&'a Value>,
    source_schema: &'a str,
    rate_limit: &'a RateLimitSpec,
    filters: &'a HashMap<String, String>,
    state: &'a HashMap<String, String>,
    source_secrets: &'a BTreeMap<String, String>,
    source_variables: &'a BTreeMap<String, String>,
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
        source_secrets: BTreeMap<String, String>,
        source_variables: BTreeMap<String, String>,
    ) -> Result<Self> {
        let auth = &manifest.auth;

        for key in &auth.required_secrets {
            if !source_secrets.contains_key(key) {
                return Err(DataFusionError::Execution(format!(
                    "{} source requires credential {}",
                    manifest.common.name, key
                )));
            }
        }

        for header in &auth.headers {
            let resolved = resolve_value_source(
                &header.value,
                &HashMap::new(),
                &HashMap::new(),
                &source_secrets,
                &source_variables,
            )?;
            if resolved.is_none() {
                return Err(DataFusionError::Execution(format!(
                    "{} source auth header '{}' could not be resolved",
                    manifest.common.name, header.name
                )));
            }
        }

        let request_timeout = Duration::from_secs(DEFAULT_HTTP_REQUEST_TIMEOUT_SECS);
        let http = reqwest::Client::builder()
            .timeout(request_timeout)
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
            auth_headers: manifest.auth.headers.clone(),
            rate_limit: manifest.rate_limit.clone(),
            source_secrets: Arc::new(source_secrets),
            source_variables: Arc::new(source_variables),
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
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
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
                return Err(DataFusionError::Execution(format!(
                    "source '{}' table '{}' exceeded pagination max_pages={max_pages}",
                    self.source_schema,
                    table.name()
                )));
            }

            let base_url = render_template(
                &self.base_url,
                filters,
                &pagination_state_values(&state),
                self.source_secrets.as_ref(),
                self.source_variables.as_ref(),
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
                    self.source_secrets.as_ref(),
                    self.source_variables.as_ref(),
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
                    self.source_secrets.as_ref(),
                    self.source_variables.as_ref(),
                )?;
                apply_pagination_query_pairs(
                    &mut query_pairs,
                    table,
                    &pagination,
                    &state,
                    page_size,
                )?;

                let mut body = build_request_body(
                    active_request,
                    filters,
                    &state,
                    self.source_secrets.as_ref(),
                    self.source_variables.as_ref(),
                )?;
                apply_pagination_body_fields(&mut body, table, &pagination, &state, page_size)?;
                (query_pairs, body)
            };

            let pagination_values = pagination_state_values(&state);
            let request = execute_request(
                &self.http,
                self.request_timeout,
                RequestSpec {
                    auth_headers: &self.auth_headers,
                    table_headers: &active_request.headers,
                    table_name: table.name(),
                    method: active_request.method,
                    base_url: &base_url,
                    url: &url,
                    query_pairs: &query_pairs,
                    body: body.as_ref(),
                    source_schema: &self.source_schema,
                    rate_limit: &self.rate_limit,
                    filters,
                    state: &pagination_values,
                    source_secrets: self.source_secrets.as_ref(),
                    source_variables: self.source_variables.as_ref(),
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
                        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
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
        auth_headers,
        table_headers,
        table_name,
        method,
        base_url,
        url,
        query_pairs,
        body,
        source_schema,
        rate_limit,
        filters,
        state,
        source_secrets,
        source_variables,
        allow_404_empty,
        link_header_require_results,
    } = request;
    let mut server_error_retries = 0usize;
    let mut throttle_retries = 0usize;
    loop {
        let method_label = http_method_label(method);
        let mut request = build_http_request(http, method, url);

        for header in auth_headers {
            let value = resolve_value_source(
                &header.value,
                filters,
                state,
                source_secrets,
                source_variables,
            )?
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "missing value for auth header '{}'",
                    header.name
                ))
            })?;
            request = request.header(&header.name, value_to_string(&value));
        }

        for header in table_headers {
            if let Some(value) = resolve_value_source(
                &header.value,
                filters,
                state,
                source_secrets,
                source_variables,
            )? {
                request = request.header(&header.name, value_to_string(&value));
            }
        }

        if !query_pairs.is_empty() {
            request = request.query(query_pairs);
        }

        if let Some(body) = body {
            request = request.json(body);
        }

        let logged_url = build_logged_url(url, query_pairs);
        let _logged_body = body
            .and_then(|b| serde_json::to_string_pretty(b).ok())
            .filter(|s| !s.is_empty());

        let response = request.send().await.map_err(|error| {
            request_error(
                "request",
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

        let next_url =
            extract_next_link_url(response.headers(), base_url, link_header_require_results)?;

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
                    detail: body,
                },
            )));
        }

        let payload: Value = response.json().await.map_err(|error| {
            request_error(
                "response decoding",
                method_label,
                &logged_url,
                request_timeout,
                &error,
            )
        })?;
        return Ok(Some((payload, next_url)));
    }
}

fn request_error(
    stage: &str,
    method_label: &str,
    logged_url: &str,
    request_timeout: Duration,
    error: &reqwest::Error,
) -> DataFusionError {
    if error.is_timeout() {
        return DataFusionError::Execution(format!(
            "source API {stage} timed out for {method_label} {logged_url} after {}s",
            request_timeout.as_secs_f64()
        ));
    }

    DataFusionError::Execution(format!(
        "source API {stage} failed for {method_label} {logged_url}: {error}"
    ))
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
    source_secrets: &BTreeMap<String, String>,
    source_variables: &BTreeMap<String, String>,
) -> Result<Vec<(String, String)>> {
    let state_values = pagination_state_values(state);
    let mut params = Vec::new();

    for param in &request.query {
        let value = resolve_value_source(
            &param.value,
            filters,
            &state_values,
            source_secrets,
            source_variables,
        )?;
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
    source_secrets: &BTreeMap<String, String>,
    source_variables: &BTreeMap<String, String>,
) -> Result<Option<Value>> {
    if request.body.is_empty() {
        return Ok(None);
    }

    let state_values = pagination_state_values(state);
    let mut root = Value::Object(Map::new());

    for field in &request.body {
        if let Some(value) = resolve_value_source(
            &field.value,
            filters,
            &state_values,
            source_secrets,
            source_variables,
        )? {
            set_path_value(&mut root, &field.path, value)?;
        }
    }

    Ok(Some(root))
}

fn apply_pagination_body_fields(
    body: &mut Option<Value>,
    table: &HttpTableSpec,
    pagination: &ValidatedPagination,
    state: &PageState,
    page_size: Option<usize>,
) -> Result<()> {
    if body.is_none()
        && pagination
            .page_size
            .as_ref()
            .is_none_or(|s| s.body_path.is_empty())
        && !(matches!(pagination.mode, ValidatedPaginationMode::CursorBody)
            && !table.pagination.cursor_body_path.is_empty()
            && state.cursor.is_some())
    {
        return Ok(());
    }

    if body.is_none() {
        *body = Some(Value::Object(Map::new()));
    }
    let root = body.as_mut().expect("body is present");

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

fn resolve_value_source(
    value: &ValueSourceSpec,
    filters: &HashMap<String, String>,
    state: &HashMap<String, String>,
    source_secrets: &BTreeMap<String, String>,
    source_variables: &BTreeMap<String, String>,
) -> Result<Option<Value>> {
    match value {
        ValueSourceSpec::Template { template } => {
            let rendered =
                render_template(template, filters, state, source_secrets, source_variables)?;
            Ok(Some(Value::String(rendered)))
        }
        ValueSourceSpec::Literal { value } => Ok(Some(value.clone())),
        ValueSourceSpec::Filter { key, default } => Ok(filters
            .get(key)
            .map(|v| Value::String(v.clone()))
            .or_else(|| default.clone())),
        ValueSourceSpec::FilterInt { key, default } => {
            let value = if let Some(filter) = filters.get(key) {
                let parsed = filter.parse::<i64>().map_err(|error| {
                    DataFusionError::Execution(format!(
                        "filter '{key}' value '{filter}' is not a valid i64: {error}"
                    ))
                })?;
                Some(json!(parsed))
            } else {
                default.map(|value| json!(value))
            };
            Ok(value)
        }
        ValueSourceSpec::Secret { key, default } => Ok(source_secrets
            .get(key)
            .cloned()
            .map(Value::String)
            .or_else(|| default.clone().map(Value::String))),
        ValueSourceSpec::Variable { key, default } => Ok(source_variables
            .get(key)
            .cloned()
            .map(Value::String)
            .or_else(|| default.clone().map(Value::String))),
        ValueSourceSpec::State { key } => Ok(state.get(key).map(|v| Value::String(v.clone()))),
        ValueSourceSpec::NowEpochMinusSeconds { seconds } => {
            #[allow(
                clippy::cast_possible_wrap,
                reason = "Current Unix epoch seconds fit within i64 for centuries"
            )]
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64;
            let value = now.saturating_sub(*seconds);
            Ok(Some(json!(value)))
        }
    }
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

fn render_template(
    template: &ParsedTemplate,
    filters: &HashMap<String, String>,
    state: &HashMap<String, String>,
    source_secrets: &BTreeMap<String, String>,
    source_variables: &BTreeMap<String, String>,
) -> Result<String> {
    let mut out = String::with_capacity(template.raw().len());
    for part in template.parts() {
        match part {
            TemplatePart::Literal(part) => out.push_str(part),
            TemplatePart::Token(token) => out.push_str(&resolve_template_token(
                token,
                filters,
                state,
                source_secrets,
                source_variables,
            )?),
        }
    }
    Ok(out)
}

fn resolve_template_token(
    token: &coral_spec::TemplateToken,
    filters: &HashMap<String, String>,
    state: &HashMap<String, String>,
    source_secrets: &BTreeMap<String, String>,
    source_variables: &BTreeMap<String, String>,
) -> Result<String> {
    let default = token.default_value().map(ToString::to_string);

    if token.namespace() == &TemplateNamespace::Secret {
        return source_secrets
            .get(token.key())
            .cloned()
            .or(default)
            .ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "missing source secret '{}' for template token",
                    token.key()
                ))
            });
    }

    if token.namespace() == &TemplateNamespace::Filter {
        return filters
            .get(token.key())
            .cloned()
            .or(default)
            .ok_or_else(|| {
                DataFusionError::Execution(format!("missing filter '{}'", token.key()))
            });
    }

    if token.namespace() == &TemplateNamespace::Variable {
        return source_variables
            .get(token.key())
            .cloned()
            .or(default)
            .ok_or_else(|| {
                DataFusionError::Execution(format!("missing source variable '{}'", token.key()))
            });
    }

    if token.namespace() == &TemplateNamespace::State {
        return state.get(token.key()).cloned().or(default).ok_or_else(|| {
            DataFusionError::Execution(format!("missing state value '{}'", token.key()))
        });
    }

    Err(DataFusionError::Execution(format!(
        "unsupported template token '{}'",
        token.raw()
    )))
}

fn value_to_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(v) => v.to_string(),
        Value::Number(v) => v.to_string(),
        Value::String(v) => v.clone(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value).unwrap_or_default(),
    }
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

    let mut cursor = root;
    for key in &path[..path.len() - 1] {
        if !cursor.is_object() {
            *cursor = Value::Object(Map::new());
        }
        let obj = cursor.as_object_mut().ok_or_else(|| {
            DataFusionError::Execution("failed to create JSON object path".to_string())
        })?;
        cursor = obj
            .entry(key.clone())
            .or_insert_with(|| Value::Object(Map::new()));
    }

    let last = path
        .last()
        .cloned()
        .ok_or_else(|| DataFusionError::Execution("invalid empty JSON path segment".to_string()))?;

    let obj = cursor.as_object_mut().ok_or_else(|| {
        DataFusionError::Execution("failed to assign JSON path value".to_string())
    })?;
    obj.insert(last, value);
    Ok(())
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
                            #[allow(
                                clippy::cast_possible_truncation,
                                reason = "Series timestamps are integral epoch values that fit in i64"
                            )]
                            let timestamp =
                                pair.first().and_then(Value::as_f64).unwrap_or(0.0) as i64;
                            let value = pair.get(1).and_then(Value::as_f64).unwrap_or(0.0);
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

    use reqwest::header::{HeaderMap, HeaderValue};
    use serde_json::json;
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    use super::{
        HttpSourceClient, PageState, RequestSpec as HttpRequestSpec, apply_pagination_query_pairs,
        execute_request, extract_next_link_url, extract_rows, join_url, normalize_base_url,
        page_is_exhausted, resolve_value_source,
    };
    use coral_spec::PaginationMode;
    use coral_spec::backends::http::{HttpSourceManifest, HttpTableSpec, RateLimitSpec};
    use coral_spec::{
        HttpMethod, PaginationSpec, ParsedTemplate, RequestSpec, RowStrategy,
        ValidatedPaginationMode, ValueSourceSpec, parse_source_manifest_value,
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

    fn request_json(request: &RequestSpec) -> serde_json::Value {
        json!({
            "method": format!("{:?}", request.method),
            "path": request.path,
            "query": request.query.iter().map(|query| json!({
                "name": query.name,
                "value": value_source_json(&query.value),
            })).collect::<Vec<_>>(),
            "body": request.body.iter().map(|field| json!({
                "path": field.path,
                "value": value_source_json(&field.value),
            })).collect::<Vec<_>>(),
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
            ValueSourceSpec::Variable { key, default } => json!({
                "from": "variable",
                "key": key,
                "default": default,
            }),
            ValueSourceSpec::Secret { key, default } => json!({
                "from": "secret",
                "key": key,
                "default": default,
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
        let source_secrets = BTreeMap::from([("API_KEY".to_string(), "alpha-secret".to_string())]);

        let value = resolve_value_source(
            &ValueSourceSpec::Secret {
                key: "API_KEY".to_string(),
                default: None,
            },
            &HashMap::new(),
            &HashMap::new(),
            &source_secrets,
            &BTreeMap::new(),
        )
        .expect("secret lookup should succeed");

        assert_eq!(value, Some(json!("alpha-secret")));
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
    fn backend_client_requires_source_scoped_credentials() {
        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "alpha",
            "version": "0.1.0",
            "backend": "http",
            "base_url": "https://api.example.com",
            "auth": {
                "required_secrets": ["API_KEY"]
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

        let error = HttpSourceClient::from_manifest(&manifest, source_secrets, BTreeMap::new())
            .expect_err("missing source-scoped credentials must fail");

        assert!(
            error
                .to_string()
                .contains("alpha source requires credential API_KEY")
        );
    }

    #[test]
    fn apply_pagination_query_pairs_uses_typed_offset_param() {
        let table = test_http_table_spec(
            &json!([]),
            &RequestSpec {
                method: HttpMethod::GET,
                path: ParsedTemplate::parse("/items").expect("template"),
                query: vec![],
                body: vec![],
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
                body: vec![],
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
        let query_pairs = Vec::new();
        let filters = HashMap::new();
        let state = HashMap::new();
        let source_secrets = BTreeMap::new();
        let source_variables = BTreeMap::new();

        let error = execute_request(
            &http,
            request_timeout,
            HttpRequestSpec {
                auth_headers: &[],
                table_headers: &[],
                table_name: "items",
                method: HttpMethod::GET,
                base_url: &base_url,
                url: &url,
                query_pairs: &query_pairs,
                body: None,
                source_schema: "demo",
                rate_limit: &RateLimitSpec::default(),
                filters: &filters,
                state: &state,
                source_secrets: &source_secrets,
                source_variables: &source_variables,
                allow_404_empty: false,
                link_header_require_results: false,
            },
        )
        .await
        .expect_err("hung upstream should time out");

        assert!(error.to_string().contains("timed out"));
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
