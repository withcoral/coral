//! Paginated HTTP fetch orchestration.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use datafusion::error::{DataFusionError, Result};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::Value;

use crate::backends::http::ProviderQueryError;
use crate::backends::http::cache::{
    HttpCacheEntry, build_cache_key, estimate_json_bytes, resolved_inputs_cache_fingerprint,
};
use crate::backends::http::client::HttpSourceClient;
use crate::backends::http::error::{pagination_error, provider_error};
use crate::backends::http::pagination::{
    PageState, apply_pagination_body_fields, apply_pagination_query_pairs, page_is_exhausted,
    pagination_state_values, resolve_page_size,
};
use crate::backends::http::request::{RequestBody, build_query_pairs, build_request_body};
use crate::backends::http::target::HttpFetchTarget;
use crate::backends::http::transport::{OutgoingHttpRequest, execute_request};
use crate::backends::http::url::{join_url, normalize_base_url};
use crate::backends::shared::cache::hash_cache_bytes;
use crate::backends::shared::json_path::get_path_value;
use crate::backends::shared::response_rows::extract_rows;
use crate::backends::shared::template::{
    RenderContext, render_template, resolve_value_source, value_to_string,
};
use coral_spec::backends::http::HttpCacheMode;
use coral_spec::{HeaderSpec, HttpMethod, ValidatedPaginationMode};

const DEFAULT_MAX_PAGES: usize = 10_000;

/// Single-flight outcome carried as `Err` for non-cacheable fetches.
/// `Clone` is required by moka; `Arc` keeps the inner error intact.
#[derive(Clone)]
enum FetchSkipped {
    NoData,
    NotCacheable {
        payload: Value,
        next_url: Option<String>,
    },
    NetworkError(Arc<DataFusionError>),
}

impl FetchSkipped {
    fn network_error(err: DataFusionError) -> Self {
        Self::NetworkError(Arc::new(err))
    }

    fn no_data() -> Self {
        Self::NoData
    }

    fn not_cacheable(payload: Value, next_url: Option<String>) -> Self {
        Self::NotCacheable { payload, next_url }
    }
}

/// Fallback wrapper exposing the inner error via `Error::source()`.
#[derive(Debug)]
struct SharedDataFusionError(Arc<DataFusionError>);

impl std::fmt::Display for SharedDataFusionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&*self.0, f)
    }
}

impl std::error::Error for SharedDataFusionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&*self.0)
    }
}

/// moka keeps an internal `Arc` clone, so `try_unwrap` never succeeds;
/// downcast-clone preserves the original variant for the common case.
fn unwrap_network_error(err: Arc<DataFusionError>) -> DataFusionError {
    if let DataFusionError::External(boxed) = &*err
        && let Some(provider_err) = boxed.downcast_ref::<ProviderQueryError>()
    {
        return DataFusionError::External(Box::new(provider_err.clone()));
    }
    DataFusionError::External(Box::new(SharedDataFusionError(err)))
}

fn http_method_label(method: HttpMethod) -> &'static str {
    match method {
        HttpMethod::GET => "GET",
        HttpMethod::POST => "POST",
    }
}

fn hash_request_body(body: &RequestBody) -> u64 {
    match body {
        RequestBody::Json(value) => {
            hash_cache_bytes(serde_json::to_string(value).unwrap_or_default().as_bytes())
        }
        RequestBody::Text(text) => hash_cache_bytes(text.as_bytes()),
    }
}

fn cache_vary_header_hashes(
    request_headers: &[HeaderSpec],
    table_headers: &[HeaderSpec],
    body: Option<&RequestBody>,
    render_context: &RenderContext<'_>,
    vary_headers: &[String],
) -> Result<Vec<(String, Option<u64>)>> {
    if vary_headers.is_empty() {
        return Ok(Vec::new());
    }

    let mut header_map = HeaderMap::new();
    for header in request_headers.iter().chain(table_headers.iter()) {
        if let Some(value) = resolve_value_source(&header.value, render_context)? {
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

    vary_headers
        .iter()
        .map(|header| {
            let name = HeaderName::try_from(header.as_str()).map_err(|error| {
                DataFusionError::Execution(format!(
                    "invalid cache vary header name '{header}': {error}"
                ))
            })?;
            let value_hash = header_map
                .get(&name)
                .map(|value| hash_cache_bytes(value.as_bytes()));
            Ok((name.as_str().to_string(), value_hash))
        })
        .collect()
}

#[derive(Debug, Clone, Copy)]
struct FetchLimits {
    effective_limit: Option<usize>,
    page_size_limit: Option<usize>,
    max_search_calls: Option<usize>,
}

#[expect(
    clippy::too_many_lines,
    reason = "Paginated fetch logic is stateful and easier to audit in one sequential function"
)]
pub(super) async fn fetch_rows(
    client: &HttpSourceClient,
    target: &HttpFetchTarget,
    filter_values: &HashMap<String, String>,
    arg_values: &HashMap<String, String>,
    sql_limit: Option<usize>,
) -> Result<Vec<Value>> {
    let mut all_rows = Vec::new();
    let limits = resolve_fetch_limits(target, sql_limit);
    let pagination = target
        .pagination()
        .validated(&client.source_schema, target.name())
        .map_err(|error| {
            provider_error(ProviderQueryError::Pagination {
                source_schema: client.source_schema.clone(),
                table: target.name().to_string(),
                method: None,
                url: None,
                detail: error.to_string(),
            })
        })?;
    let page_size = resolve_page_size(pagination.page_size.as_ref(), limits.page_size_limit);

    let active_request = target.resolved_request();

    let mut state = PageState {
        page: target.pagination().page_start,
        offset: match &pagination.mode {
            ValidatedPaginationMode::Offset(offset) => offset.start,
            _ => target.pagination().offset_start,
        },
        ..PageState::default()
    };

    let mut page_count = 0usize;
    let max_pages = target.pagination().max_pages.unwrap_or(DEFAULT_MAX_PAGES);

    loop {
        page_count += 1;
        if page_count > max_pages {
            return Err(provider_error(ProviderQueryError::Pagination {
                source_schema: client.source_schema.clone(),
                table: target.name().to_string(),
                method: None,
                url: None,
                detail: format!("exceeded pagination max_pages={max_pages}"),
            }));
        }

        let resolved_inputs = client.resolved_inputs_for_request().await?;
        let resolved_input_fingerprint = resolved_inputs_cache_fingerprint(&resolved_inputs);
        let state_values = pagination_state_values(&state);
        let render_context = RenderContext::new(
            filter_values,
            arg_values,
            &state_values,
            resolved_inputs.as_ref(),
        );
        let base_url = render_template(&client.base_url, &render_context)?;
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
            let rendered_path = render_template(&active_request.path, &render_context)?;
            join_url(&base_url, &rendered_path)?
        };

        let (query_pairs, body) = if following_link_header {
            (Vec::new(), None)
        } else {
            let mut query_pairs = build_query_pairs(active_request, &render_context)?;
            apply_pagination_query_pairs(&mut query_pairs, target, &pagination, &state, page_size)
                .map_err(|error| {
                    pagination_error(
                        &client.source_schema,
                        target.name(),
                        None,
                        Some(&url),
                        &error,
                    )
                })?;

            let mut body = build_request_body(active_request, &render_context)?;
            apply_pagination_body_fields(
                &mut body,
                &active_request.body,
                target,
                &pagination,
                &state,
                page_size,
            )
            .map_err(|error| {
                pagination_error(
                    &client.source_schema,
                    target.name(),
                    None,
                    Some(&url),
                    &error,
                )
            })?;
            (query_pairs, body)
        };

        let cache_key: Option<(String, usize, Duration)> = if client.cache.is_some() {
            target
                .cache()
                .filter(|p| p.mode == HttpCacheMode::Ttl)
                .filter(|policy| {
                    policy
                        .max_pages
                        .is_none_or(|max_cache_pages| page_count <= max_cache_pages)
                })
                .map(|policy| {
                    let body_hash = body.as_ref().map(hash_request_body);
                    let vary_headers = cache_vary_header_hashes(
                        &client.request_headers,
                        &active_request.headers,
                        body.as_ref(),
                        &render_context,
                        &policy.vary_headers,
                    )?;
                    let key = build_cache_key(
                        &client.source_schema,
                        &client.source_version,
                        resolved_input_fingerprint,
                        target.name(),
                        http_method_label(active_request.method),
                        &url,
                        &query_pairs,
                        body_hash,
                        &vary_headers,
                        policy.ttl.as_secs(),
                    );
                    let max_entry = policy.max_entry_bytes.unwrap_or(usize::MAX);
                    Ok::<_, DataFusionError>((key, max_entry, policy.ttl))
                })
                .transpose()?
        } else {
            None
        };

        let page = if let (Some(cache), Some((ref key, max_entry_bytes, ttl))) =
            (client.cache.as_ref(), cache_key)
        {
            let source_schema = client.source_schema.clone();
            let table_name = target.name().to_string();
            let ok_path = target.response().ok_path.clone();
            let result = cache
                .try_get_or_insert_with::<_, FetchSkipped>(key, async {
                    tracing::trace!(
                        source = %source_schema,
                        table = %table_name,
                        "http cache miss"
                    );
                    let result = execute_request(
                        &client.http,
                        client.request_timeout,
                        OutgoingHttpRequest {
                            auth: &client.auth,
                            request_headers: &client.request_headers,
                            request_authenticators: &client.request_authenticators,
                            table_headers: &active_request.headers,
                            table_name: target.name(),
                            method: active_request.method,
                            base_url: &base_url,
                            url: &url,
                            query_pairs: &query_pairs,
                            body: body.as_ref(),
                            response_format: target.response().format,
                            source_schema: &client.source_schema,
                            rate_limit: &client.rate_limit,
                            body_capture: client.body_capture,
                            render_context,
                            allow_404_empty: target.response().allow_404_empty,
                            link_header_require_results: pagination.link_header_require_results,
                        },
                    )
                    .await
                    .map_err(FetchSkipped::network_error)?;
                    let Some((payload, next_url)) = result else {
                        return Err(FetchSkipped::no_data());
                    };
                    let estimated_bytes = estimate_json_bytes(&payload);
                    let ok_for_cache = ok_path.is_empty()
                        || get_path_value(&payload, &ok_path)
                            .and_then(Value::as_bool)
                            .unwrap_or(false);
                    if !ok_for_cache {
                        tracing::trace!(
                            source = %source_schema,
                            table = %table_name,
                            "http cache entry skipped: ok_path=false"
                        );
                        return Err(FetchSkipped::not_cacheable(payload, next_url));
                    }
                    if estimated_bytes > max_entry_bytes {
                        tracing::trace!(
                            source = %source_schema,
                            table = %table_name,
                            estimated_bytes,
                            "http cache entry skipped: exceeds max_entry_bytes"
                        );
                        return Err(FetchSkipped::not_cacheable(payload, next_url));
                    }
                    if !cache.try_admit(estimated_bytes as u64) {
                        tracing::trace!(
                            source = %source_schema,
                            table = %table_name,
                            estimated_bytes,
                            "http cache entry skipped: exceeds total_max_bytes"
                        );
                        return Err(FetchSkipped::not_cacheable(payload, next_url));
                    }
                    Ok(HttpCacheEntry {
                        payload,
                        next_url,
                        ttl,
                        estimated_bytes,
                    })
                })
                .await;

            match result {
                Ok((entry, is_fresh)) => {
                    if !is_fresh {
                        tracing::trace!(
                            source = %client.source_schema,
                            table = %target.name(),
                            "http cache hit"
                        );
                    }
                    Some((entry.payload, entry.next_url))
                }
                Err(arc) => {
                    let skipped = Arc::try_unwrap(arc).unwrap_or_else(|a| (*a).clone());
                    match skipped {
                        FetchSkipped::NetworkError(err) => {
                            return Err(unwrap_network_error(err));
                        }
                        FetchSkipped::NoData => None,
                        FetchSkipped::NotCacheable { payload, next_url } => {
                            Some((payload, next_url))
                        }
                    }
                }
            }
        } else {
            execute_request(
                &client.http,
                client.request_timeout,
                OutgoingHttpRequest {
                    auth: &client.auth,
                    request_headers: &client.request_headers,
                    request_authenticators: &client.request_authenticators,
                    table_headers: &active_request.headers,
                    table_name: target.name(),
                    method: active_request.method,
                    base_url: &base_url,
                    url: &url,
                    query_pairs: &query_pairs,
                    body: body.as_ref(),
                    response_format: target.response().format,
                    source_schema: &client.source_schema,
                    rate_limit: &client.rate_limit,
                    body_capture: client.body_capture,
                    render_context,
                    allow_404_empty: target.response().allow_404_empty,
                    link_header_require_results: pagination.link_header_require_results,
                },
            )
            .await?
        };

        let Some((payload, next_url)) = page else {
            break;
        };

        if !target.response().ok_path.is_empty() {
            let ok = get_path_value(&payload, &target.response().ok_path)
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if !ok {
                let err = if target.response().error_path.is_empty() {
                    "unknown source API error".to_string()
                } else {
                    get_path_value(&payload, &target.response().error_path)
                        .and_then(Value::as_str)
                        .unwrap_or("unknown source API error")
                        .to_string()
                };
                return Err(DataFusionError::External(Box::new(
                    ProviderQueryError::ApiRequest {
                        source_schema: client.source_schema.clone(),
                        table: target.name().to_string(),
                        status: None,
                        method: None,
                        url: None,
                        filters: filter_values.clone(),
                        detail: err,
                    },
                )));
            }
        }

        let mut rows = extract_rows(target.response(), &payload);
        let rows_on_page = rows.len();
        all_rows.append(&mut rows);

        if let Some(limit) = limits.effective_limit
            && all_rows.len() >= limit
        {
            all_rows.truncate(limit);
            break;
        }

        if limits
            .max_search_calls
            .is_some_and(|max_calls| page_count >= max_calls)
        {
            break;
        }

        match &pagination.mode {
            ValidatedPaginationMode::None => break,
            ValidatedPaginationMode::CursorQuery | ValidatedPaginationMode::CursorBody => {
                let next_cursor =
                    get_path_value(&payload, &target.pagination().response_cursor_path)
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
                state.page = state.page.saturating_add(target.pagination().page_step);
            }
            ValidatedPaginationMode::Offset(offset) => {
                if page_is_exhausted(rows_on_page, page_size) {
                    break;
                }
                let step = offset
                    .resolve_step(page_size, &client.source_schema, target.name())
                    .map_err(|error| {
                        provider_error(ProviderQueryError::Pagination {
                            source_schema: client.source_schema.clone(),
                            table: target.name().to_string(),
                            method: None,
                            url: None,
                            detail: error.to_string(),
                        })
                    })?;
                state.offset = state.offset.saturating_add(step);
            }
            ValidatedPaginationMode::LinkHeader | ValidatedPaginationMode::Auto => match next_url {
                Some(next) => state.next_url = Some(next),
                None => break,
            },
        }
    }

    Ok(all_rows)
}

fn resolve_fetch_limits(target: &HttpFetchTarget, sql_limit: Option<usize>) -> FetchLimits {
    let Some(search_limits) = target.search_limits() else {
        return FetchLimits {
            effective_limit: sql_limit.or(target.fetch_limit_default()),
            page_size_limit: sql_limit,
            max_search_calls: None,
        };
    };

    let requested_top_k = sql_limit.unwrap_or(search_limits.default_top_k);
    let max_candidates = search_limits
        .max_top_k
        .saturating_mul(search_limits.max_calls_per_query);

    FetchLimits {
        effective_limit: Some(requested_top_k.min(max_candidates)),
        page_size_limit: Some(requested_top_k.min(search_limits.max_top_k)),
        max_search_calls: Some(search_limits.max_calls_per_query),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use serde_json::json;

    use super::*;
    use crate::backends::http::test_support::{parse_http_manifest, test_http_request_target};
    use coral_spec::backends::http::{HttpSourceManifest, HttpTableSpec};
    use coral_spec::parse_source_manifest_value;

    // ── Cache tests ───────────────────────────────────────────────────────────

    fn cached_users_manifest(base_url: &str) -> HttpSourceManifest {
        parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "http",
            "base_url": base_url,
            "tables": [{
                "name": "users",
                "description": "Users",
                "request": { "path": "/api/users" },
                "response": { "rows_path": ["data"] },
                "cache": { "mode": "ttl", "ttl": "1h" },
                "columns": [
                    { "name": "id", "type": "Int64" },
                    { "name": "name", "type": "Utf8" }
                ]
            }]
        }))
    }

    fn build_test_client(manifest: &HttpSourceManifest) -> HttpSourceClient {
        HttpSourceClient::from_manifest(
            manifest,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &HashMap::new(),
            None,
            reqwest::Client::new(),
        )
        .expect("test client should build")
    }

    fn build_test_client_without_cache(manifest: &HttpSourceManifest) -> HttpSourceClient {
        HttpSourceClient::from_manifest_without_cache(
            manifest,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &HashMap::new(),
            None,
            reqwest::Client::new(),
        )
        .expect("test client should build")
    }

    fn first_table(manifest: &HttpSourceManifest) -> &HttpTableSpec {
        manifest
            .tables
            .first()
            .expect("manifest should have a table")
    }

    async fn fetch_table(
        client: &HttpSourceClient,
        table: &HttpTableSpec,
        filters: &HashMap<String, String>,
        sql_limit: Option<usize>,
    ) -> datafusion::error::Result<Vec<serde_json::Value>> {
        let target = test_http_request_target(table);
        client
            .fetch(&target, filters, &HashMap::new(), sql_limit)
            .await
    }

    #[tokio::test]
    async fn cache_hit_avoids_second_outbound_request() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/users"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [
                    { "id": 1, "name": "Ada" },
                    { "id": 2, "name": "Grace" }
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let manifest = cached_users_manifest(&server.uri());
        let client = build_test_client(&manifest);
        let table = first_table(&manifest);
        let filters = HashMap::new();

        let rows1 = fetch_table(&client, table, &filters, None)
            .await
            .expect("first fetch");
        let rows2 = fetch_table(&client, table, &filters, None)
            .await
            .expect("second fetch from cache");

        assert_eq!(rows1, rows2);
        assert_eq!(rows1.len(), 2);
        // MockServer verifies .expect(1) on drop — panics if != 1 request made
    }

    #[tokio::test]
    async fn cache_miss_on_different_filter_values() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "http",
            "base_url": server.uri(),
            "tables": [{
                "name": "items",
                "description": "Items",
                "filters": [{ "name": "status" }],
                "request": {
                    "path": "/api/items",
                    "query": [{ "name": "status", "from": "filter", "key": "status" }]
                },
                "cache": { "mode": "ttl", "ttl": "1h" },
                "columns": [{ "name": "id", "type": "Int64" }]
            }]
        }));

        Mock::given(method("GET"))
            .and(path("/api/items"))
            .and(query_param("status", "open"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([{ "id": 1 }])))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/items"))
            .and(query_param("status", "closed"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([{ "id": 2 }])))
            .expect(1)
            .mount(&server)
            .await;

        let client = build_test_client(&manifest);
        let table = first_table(&manifest);

        let rows_open = fetch_table(
            &client,
            table,
            &HashMap::from([("status".into(), "open".into())]),
            None,
        )
        .await
        .expect("open fetch");
        let rows_closed = fetch_table(
            &client,
            table,
            &HashMap::from([("status".into(), "closed".into())]),
            None,
        )
        .await
        .expect("closed fetch");

        assert_ne!(rows_open, rows_closed);
        // Both mocks have .expect(1) — verified on drop
    }

    #[tokio::test]
    async fn cache_miss_on_different_vary_header_values() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "http",
            "base_url": server.uri(),
            "tables": [{
                "name": "items",
                "description": "Items",
                "filters": [{ "name": "mode" }],
                "request": {
                    "path": "/api/items",
                    "headers": [{ "name": "X-Mode", "from": "filter", "key": "mode" }]
                },
                "cache": {
                    "mode": "ttl",
                    "ttl": "1h",
                    "vary": { "headers": ["X-Mode"] }
                },
                "columns": [{ "name": "id", "type": "Int64" }]
            }]
        }));

        Mock::given(method("GET"))
            .and(path("/api/items"))
            .and(header("X-Mode", "alpha"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([{ "id": 1 }])))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/items"))
            .and(header("X-Mode", "beta"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([{ "id": 2 }])))
            .expect(1)
            .mount(&server)
            .await;

        let client = build_test_client(&manifest);
        let table = first_table(&manifest);

        let rows_alpha = fetch_table(
            &client,
            table,
            &HashMap::from([("mode".into(), "alpha".into())]),
            None,
        )
        .await
        .expect("alpha fetch");
        let rows_beta = fetch_table(
            &client,
            table,
            &HashMap::from([("mode".into(), "beta".into())]),
            None,
        )
        .await
        .expect("beta fetch");

        assert_ne!(rows_alpha, rows_beta);
    }

    #[tokio::test]
    async fn cache_second_identical_query_uses_first_result() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "http",
            "base_url": server.uri(),
            "tables": [{
                "name": "items",
                "description": "Items",
                "filters": [{ "name": "status" }],
                "request": {
                    "path": "/api/items",
                    "query": [{ "name": "status", "from": "filter", "key": "status" }]
                },
                "cache": { "mode": "ttl", "ttl": "1h" },
                "columns": [{ "name": "id", "type": "Int64" }]
            }]
        }));

        Mock::given(method("GET"))
            .and(path("/api/items"))
            .and(query_param("status", "open"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([{ "id": 1 }])))
            .expect(1)
            .mount(&server)
            .await;

        let client = build_test_client(&manifest);
        let table = first_table(&manifest);
        let filters = HashMap::from([("status".to_string(), "open".to_string())]);

        let r1 = fetch_table(&client, table, &filters, None)
            .await
            .expect("first");
        let r2 = fetch_table(&client, table, &filters, None)
            .await
            .expect("second, from cache");
        assert_eq!(r1, r2);
        // .expect(1) verified on drop
    }

    #[tokio::test]
    async fn cache_does_not_cache_failed_responses() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;

        // Use 400 (not retried, unlike 5xx) to get exactly one request per fetch() call.
        // The second call must still hit the server, proving failed responses are not cached.
        Mock::given(method("GET"))
            .and(path("/api/users"))
            .respond_with(ResponseTemplate::new(400).set_body_string("bad request"))
            .expect(2)
            .mount(&server)
            .await;

        let manifest = cached_users_manifest(&server.uri());
        let client = build_test_client(&manifest);
        let table = first_table(&manifest);
        let filters = HashMap::new();

        assert!(
            fetch_table(&client, table, &filters, None).await.is_err(),
            "first call should fail"
        );
        assert!(
            fetch_table(&client, table, &filters, None).await.is_err(),
            "second call should also fail"
        );
        // .expect(2) verifies 2 separate outbound requests (no caching of errors)
    }

    #[tokio::test]
    async fn cache_expires_after_ttl() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/users"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "data": [{ "id": 1 }] })),
            )
            .expect(2)
            .mount(&server)
            .await;

        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "http",
            "base_url": server.uri(),
            "tables": [{
                "name": "users",
                "description": "Users",
                "request": { "path": "/api/users" },
                "response": { "rows_path": ["data"] },
                "cache": { "mode": "ttl", "ttl": "1s" },
                "columns": [{ "name": "id", "type": "Int64" }]
            }]
        }));

        let client = build_test_client(&manifest);
        let table = first_table(&manifest);
        let filters = HashMap::new();

        fetch_table(&client, table, &filters, None)
            .await
            .expect("first fetch");
        // Wait for the 1s TTL to expire
        tokio::time::sleep(Duration::from_millis(1100)).await;
        fetch_table(&client, table, &filters, None)
            .await
            .expect("second fetch after expiry");
        // .expect(2) verifies 2 outbound requests were made
    }

    #[tokio::test]
    async fn cache_disabled_by_default() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/users"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "data": [{ "id": 1 }] })),
            )
            .expect(2)
            .mount(&server)
            .await;

        // Manifest with no cache field — caching must stay disabled.
        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "http",
            "base_url": server.uri(),
            "tables": [{
                "name": "users",
                "description": "Users",
                "request": { "path": "/api/users" },
                "response": { "rows_path": ["data"] },
                "columns": [{ "name": "id", "type": "Int64" }]
            }]
        }));

        let client = build_test_client(&manifest);
        let table = first_table(&manifest);
        let filters = HashMap::new();

        fetch_table(&client, table, &filters, None)
            .await
            .expect("first");
        fetch_table(&client, table, &filters, None)
            .await
            .expect("second");
        // .expect(2) verifies both calls made it to the server (no caching)
    }

    #[tokio::test]
    async fn cache_runtime_absence_disables_table_cache_policy() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/users"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!({ "data": [{ "id": 1 }] })),
            )
            .expect(2)
            .mount(&server)
            .await;

        let manifest = cached_users_manifest(&server.uri());
        let client = build_test_client_without_cache(&manifest);
        let table = first_table(&manifest);
        let filters = HashMap::new();

        fetch_table(&client, table, &filters, None)
            .await
            .expect("first");
        fetch_table(&client, table, &filters, None)
            .await
            .expect("second");
        // The table declares `cache: { mode: ttl }`, but no runtime cache is
        // installed, so both calls must reach the server.
    }

    #[test]
    fn parse_manifest_accepts_cache_ttl_policy() {
        use coral_spec::backends::http::HttpCacheMode;

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
                "cache": {
                    "mode": "ttl",
                    "ttl": "5m",
                    "vary": { "headers": ["Accept"] },
                    "max_pages": 50,
                    "max_entry_bytes": 1_048_576
                },
                "columns": [{ "name": "id", "type": "Utf8" }]
            }]
        }));

        let cache = first_table(&manifest)
            .cache
            .as_ref()
            .expect("cache policy should be set");
        assert_eq!(cache.mode, HttpCacheMode::Ttl);
        assert_eq!(cache.ttl.as_secs(), 300);
        assert_eq!(cache.vary_headers, vec!["Accept"]);
        assert_eq!(cache.max_pages, Some(50));
        assert_eq!(cache.max_entry_bytes, Some(1_048_576));
    }

    #[test]
    fn parse_manifest_rejects_overflowing_cache_ttl() {
        let error = parse_source_manifest_value(json!({
            "dsl_version": 3,
            "name": "alpha",
            "version": "0.1.0",
            "backend": "http",
            "base_url": "https://api.example.com",
            "tables": [{
                "name": "items",
                "description": "items",
                "request": { "path": "/items" },
                "cache": { "mode": "ttl", "ttl": "18446744073709551615h" },
                "columns": [{ "name": "id", "type": "Utf8" }]
            }]
        }))
        .expect_err("overflowing ttl should fail");

        assert!(
            error
                .to_string()
                .contains("cache ttl '18446744073709551615h' overflows u64 seconds"),
            "unexpected ttl overflow error: {error}"
        );
    }

    #[test]
    fn parse_manifest_no_cache_field_gives_none() {
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
                "columns": [{ "name": "id", "type": "Utf8" }]
            }]
        }));

        assert!(first_table(&manifest).cache.is_none());
    }

    #[tokio::test]
    async fn cache_different_post_body_causes_miss() {
        use wiremock::matchers::{body_json, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "http",
            "base_url": server.uri(),
            "tables": [{
                "name": "items",
                "description": "Items",
                "filters": [{ "name": "status" }],
                "request": {
                    "method": "POST",
                    "path": "/api/items",
                    "body": [{ "path": ["filter"], "from": "filter", "key": "status" }]
                },
                "cache": { "mode": "ttl", "ttl": "1h" },
                "columns": [{ "name": "id", "type": "Int64" }]
            }]
        }));

        Mock::given(method("POST"))
            .and(path("/api/items"))
            .and(body_json(json!({ "filter": "open" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([{ "id": 1 }])))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/api/items"))
            .and(body_json(json!({ "filter": "closed" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([{ "id": 2 }])))
            .expect(1)
            .mount(&server)
            .await;

        let client = build_test_client(&manifest);
        let table = first_table(&manifest);

        let rows_open = fetch_table(
            &client,
            table,
            &HashMap::from([("status".into(), "open".into())]),
            None,
        )
        .await
        .expect("open fetch");
        let rows_closed = fetch_table(
            &client,
            table,
            &HashMap::from([("status".into(), "closed".into())]),
            None,
        )
        .await
        .expect("closed fetch");

        assert_ne!(rows_open, rows_closed);
        // Both mocks have .expect(1) — verified on drop
    }

    #[tokio::test]
    async fn cache_different_pagination_state_causes_miss() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        // No page_size query_param so the page number is the only URL-level pagination
        // state. This ensures the cache key for page 1 is the same regardless of the
        // SQL limit used by the caller.
        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "http",
            "base_url": server.uri(),
            "tables": [{
                "name": "items",
                "description": "Items",
                "pagination": {
                    "mode": "page",
                    "page_param": "page",
                    "page_start": 1
                },
                "request": { "path": "/api/items" },
                "cache": { "mode": "ttl", "ttl": "1h" },
                "columns": [{ "name": "id", "type": "Int64" }]
            }]
        }));

        // Page 1 is fetched once from the server (first call), then served from cache
        // (second call) — same URL so same cache key.
        Mock::given(method("GET"))
            .and(path("/api/items"))
            .and(query_param("page", "1"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!([{ "id": 1 }, { "id": 2 }])),
            )
            .expect(1)
            .mount(&server)
            .await;
        // Page 2 is only fetched by the second call (different cache key: page=2).
        Mock::given(method("GET"))
            .and(path("/api/items"))
            .and(query_param("page", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([{ "id": 3 }])))
            .expect(1)
            .mount(&server)
            .await;

        let client = build_test_client(&manifest);
        let table = first_table(&manifest);

        // First call: limit=2 → fetches page 1 (2 rows = limit), stops.
        let rows1 = fetch_table(&client, table, &HashMap::new(), Some(2))
            .await
            .expect("first fetch");
        assert_eq!(rows1.len(), 2);

        // Second call: limit=3 → page 1 served from cache (2 rows), page 2 fresh from server.
        let rows2 = fetch_table(&client, table, &HashMap::new(), Some(3))
            .await
            .expect("second fetch");
        assert_eq!(rows2.len(), 3);
        // Page 1 expect(1) and page 2 expect(1) are verified on drop
    }

    #[tokio::test]
    async fn cache_pagination_stores_pages_independently() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "http",
            "base_url": server.uri(),
            "tables": [{
                "name": "items",
                "description": "Items",
                "pagination": {
                    "mode": "page",
                    "page_param": "page",
                    "page_start": 1,
                    "page_size": { "default": 2, "max": 100, "query_param": "per_page" }
                },
                "request": { "path": "/api/items" },
                "cache": { "mode": "ttl", "ttl": "1h" },
                "columns": [{ "name": "id", "type": "Int64" }]
            }]
        }));

        // Both pages are fetched once each on the first run, then served from
        // cache on the second run — total expect(1) per page.
        Mock::given(method("GET"))
            .and(path("/api/items"))
            .and(query_param("page", "1"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!([{ "id": 1 }, { "id": 2 }])),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/items"))
            .and(query_param("page", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([{ "id": 3 }])))
            .expect(1)
            .mount(&server)
            .await;

        let client = build_test_client(&manifest);
        let table = first_table(&manifest);

        // First run: fetches both pages from server
        let rows1 = fetch_table(&client, table, &HashMap::new(), None)
            .await
            .expect("first fetch");
        assert_eq!(rows1.len(), 3);

        // Second run: both pages served from cache — no additional server hits
        let rows2 = fetch_table(&client, table, &HashMap::new(), None)
            .await
            .expect("second fetch");
        assert_eq!(rows2, rows1);
        // expect(1) per page verified on mock drop
    }

    #[tokio::test]
    async fn cache_max_pages_limits_cached_pages_per_fetch() {
        use wiremock::matchers::{method, path, query_param};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "http",
            "base_url": server.uri(),
            "tables": [{
                "name": "items",
                "description": "Items",
                "pagination": {
                    "mode": "page",
                    "page_param": "page",
                    "page_start": 1,
                    "page_size": { "default": 2, "max": 100, "query_param": "per_page" }
                },
                "request": { "path": "/api/items" },
                "cache": { "mode": "ttl", "ttl": "1h", "max_pages": 1 },
                "columns": [{ "name": "id", "type": "Int64" }]
            }]
        }));

        Mock::given(method("GET"))
            .and(path("/api/items"))
            .and(query_param("page", "1"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!([{ "id": 1 }, { "id": 2 }])),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/items"))
            .and(query_param("page", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([{ "id": 3 }])))
            .expect(2)
            .mount(&server)
            .await;

        let client = build_test_client(&manifest);
        let table = first_table(&manifest);

        let rows1 = fetch_table(&client, table, &HashMap::new(), None)
            .await
            .expect("first fetch");
        let rows2 = fetch_table(&client, table, &HashMap::new(), None)
            .await
            .expect("second fetch");

        assert_eq!(rows2, rows1);
    }

    #[tokio::test]
    async fn cache_does_not_cache_5xx_responses() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        // 500 triggers 2 retries → 3 requests for first fetch(); served up_to 3 times.
        Mock::given(method("GET"))
            .and(path("/api/users"))
            .respond_with(ResponseTemplate::new(500).set_body_string("server error"))
            .up_to_n_times(3)
            .expect(3)
            .mount(&server)
            .await;
        // After the 500 mock is exhausted, the second fetch hits the server and gets 200.
        // If 5xx responses were incorrectly cached, this mock would never be reached.
        Mock::given(method("GET"))
            .and(path("/api/users"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({ "data": [{ "id": 1, "name": "Ada" }] })),
            )
            .expect(1)
            .mount(&server)
            .await;

        let manifest = cached_users_manifest(&server.uri());
        let client = build_test_client(&manifest);
        let table = first_table(&manifest);
        let filters = HashMap::new();

        assert!(
            fetch_table(&client, table, &filters, None).await.is_err(),
            "first call should fail with server error"
        );
        let rows = fetch_table(&client, table, &filters, None)
            .await
            .expect("second call should succeed from server — 5xx response was not cached");
        assert_eq!(rows.len(), 1);
        // expect(3) on 500 mock and expect(1) on 200 mock verified on drop
    }

    #[tokio::test]
    async fn cache_does_not_cache_429_responses() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        // Retry-After: 60 exceeds MAX_SHORT_RETRY_AFTER (15s) → rate limit fails
        // immediately without retrying, so each fetch() makes exactly one request.
        Mock::given(method("GET"))
            .and(path("/api/users"))
            .respond_with(ResponseTemplate::new(429).append_header("Retry-After", "60"))
            .expect(2)
            .mount(&server)
            .await;

        let manifest = cached_users_manifest(&server.uri());
        let client = build_test_client(&manifest);
        let table = first_table(&manifest);
        let filters = HashMap::new();

        assert!(
            fetch_table(&client, table, &filters, None).await.is_err(),
            "first call should fail with rate-limit error"
        );
        assert!(
            fetch_table(&client, table, &filters, None).await.is_err(),
            "second call should also fail — 429 responses are not cached"
        );
        // expect(2) verifies 2 outbound requests (no caching of rate-limit errors)
    }

    #[tokio::test]
    async fn cache_does_not_cache_malformed_json() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/users"))
            .respond_with(ResponseTemplate::new(200).set_body_string("this is not json"))
            .expect(2)
            .mount(&server)
            .await;

        let manifest = cached_users_manifest(&server.uri());
        let client = build_test_client(&manifest);
        let table = first_table(&manifest);
        let filters = HashMap::new();

        assert!(
            fetch_table(&client, table, &filters, None).await.is_err(),
            "first call should fail to decode"
        );
        assert!(
            fetch_table(&client, table, &filters, None).await.is_err(),
            "second call should also fail — malformed JSON is not cached"
        );
        // expect(2) verifies 2 outbound requests (decode error, no cache write)
    }

    #[tokio::test]
    async fn cache_does_not_cache_allow_404_empty() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/items"))
            .respond_with(ResponseTemplate::new(404))
            .expect(2)
            .mount(&server)
            .await;

        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "http",
            "base_url": server.uri(),
            "tables": [{
                "name": "items",
                "description": "Items",
                "request": { "path": "/api/items" },
                "response": { "allow_404_empty": true },
                "cache": { "mode": "ttl", "ttl": "1h" },
                "columns": [{ "name": "id", "type": "Int64" }]
            }]
        }));

        let client = build_test_client(&manifest);
        let table = first_table(&manifest);
        let filters = HashMap::new();

        let rows1 = fetch_table(&client, table, &filters, None)
            .await
            .expect("first");
        assert!(rows1.is_empty(), "allow_404_empty should return empty rows");

        let rows2 = fetch_table(&client, table, &filters, None)
            .await
            .expect("second");
        assert!(rows2.is_empty());
        // expect(2) verifies both calls hit the server (empty result is not cached)
    }

    #[tokio::test]
    async fn cache_skips_oversized_entry_without_failing() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/users"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [
                    { "id": 1, "name": "Ada" },
                    { "id": 2, "name": "Grace" }
                ]
            })))
            .expect(2) // entry is never cached, so both calls hit server
            .mount(&server)
            .await;

        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "http",
            "base_url": server.uri(),
            "tables": [{
                "name": "users",
                "description": "Users",
                "request": { "path": "/api/users" },
                "response": { "rows_path": ["data"] },
                "cache": { "mode": "ttl", "ttl": "1h", "max_entry_bytes": 10 },
                "columns": [
                    { "name": "id", "type": "Int64" },
                    { "name": "name", "type": "Utf8" }
                ]
            }]
        }));

        let client = build_test_client(&manifest);
        let table = first_table(&manifest);
        let filters = HashMap::new();

        let rows1 = fetch_table(&client, table, &filters, None)
            .await
            .expect("first");
        assert_eq!(
            rows1.len(),
            2,
            "rows should be returned even when entry is skipped"
        );

        let rows2 = fetch_table(&client, table, &filters, None)
            .await
            .expect("second");
        assert_eq!(rows2, rows1);
        // expect(2) verifies both calls hit server (oversized entry was not stored)
    }

    #[tokio::test]
    async fn cache_does_not_cache_ok_path_false() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/users"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({ "ok": false, "error": "rate limited" })),
            )
            .expect(2)
            .mount(&server)
            .await;

        let manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "http",
            "base_url": server.uri(),
            "tables": [{
                "name": "users",
                "description": "Users",
                "request": { "path": "/api/users" },
                "response": {
                    "ok_path": ["ok"],
                    "error_path": ["error"]
                },
                "cache": { "mode": "ttl", "ttl": "1h" },
                "columns": [{ "name": "id", "type": "Int64" }]
            }]
        }));

        let client = build_test_client(&manifest);
        let table = first_table(&manifest);
        let filters = HashMap::new();

        assert!(
            fetch_table(&client, table, &filters, None).await.is_err(),
            "first call should fail: ok_path=false"
        );
        assert!(
            fetch_table(&client, table, &filters, None).await.is_err(),
            "second call should also fail — ok_path=false response was not cached"
        );
        // expect(2) verifies both calls hit the server (bad response not cached)
    }

    #[tokio::test]
    async fn cache_preserves_original_datafusion_error_variant_when_uncontended() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/users"))
            .respond_with(ResponseTemplate::new(400).set_body_string("bad request"))
            .mount(&server)
            .await;

        let manifest = cached_users_manifest(&server.uri());
        let client = build_test_client(&manifest);
        let table = first_table(&manifest);
        let filters = HashMap::new();

        let err = fetch_table(&client, table, &filters, None)
            .await
            .expect_err("400 should fail");

        let mut current: &dyn std::error::Error = &err;
        let mut found_provider_error = false;
        loop {
            if current.downcast_ref::<ProviderQueryError>().is_some() {
                found_provider_error = true;
                break;
            }
            match current.source() {
                Some(next) => current = next,
                None => break,
            }
        }
        assert!(
            found_provider_error,
            "structured ProviderQueryError should be reachable via Error::source() chain; got: {err:?}"
        );
        assert_eq!(
            err.to_string(),
            "External error: demo.users API error: bad request"
        );
    }

    #[tokio::test]
    async fn cache_preserves_original_datafusion_error_under_concurrent_single_flight() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/users"))
            .respond_with(ResponseTemplate::new(400).set_body_string("bad request"))
            .mount(&server)
            .await;

        let manifest = cached_users_manifest(&server.uri());
        let client = Arc::new(build_test_client(&manifest));
        let table = first_table(&manifest);
        let filters = HashMap::new();

        let target = test_http_request_target(table);
        let target = Arc::new(target);
        let filters_a = filters.clone();
        let filters_b = filters.clone();
        let client_a = Arc::clone(&client);
        let client_b = Arc::clone(&client);
        let target_a = Arc::clone(&target);
        let target_b = Arc::clone(&target);

        let (res_a, res_b) = tokio::join!(
            async move {
                client_a
                    .fetch(&target_a, &filters_a, &HashMap::new(), None)
                    .await
            },
            async move {
                client_b
                    .fetch(&target_b, &filters_b, &HashMap::new(), None)
                    .await
            },
        );

        for err in [
            res_a.expect_err("first concurrent call should fail"),
            res_b.expect_err("second concurrent call should fail"),
        ] {
            let mut current: &dyn std::error::Error = &err;
            let mut found_provider_error = false;
            loop {
                if current.downcast_ref::<ProviderQueryError>().is_some() {
                    found_provider_error = true;
                    break;
                }
                match current.source() {
                    Some(next) => current = next,
                    None => break,
                }
            }
            assert!(
                found_provider_error,
                "structured ProviderQueryError should still be reachable via Error::source() chain under single-flight contention; got: {err:?}"
            );
            assert_eq!(
                err.to_string(),
                "External error: demo.users API error: bad request"
            );
        }
    }
}
