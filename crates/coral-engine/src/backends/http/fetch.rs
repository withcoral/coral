//! Paginated HTTP fetch orchestration.

use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result};
use serde_json::Value;

use crate::backends::http::ProviderQueryError;
use crate::backends::http::client::HttpSourceClient;
use crate::backends::http::error::{execution_stopped_error, pagination_error, provider_error};
use crate::backends::http::pagination::{
    PageAdvance, PageAdvanceContext, advance_pagination_state, apply_pagination_body_fields,
    apply_pagination_query_pairs, has_explicit_continuation, initial_page_state,
    pagination_state_values, resolve_page_size,
};
use crate::backends::http::request::{build_query_pairs, build_request_body};
use crate::backends::http::target::HttpFetchTarget;
use crate::backends::http::transport::{OutgoingHttpRequest, execute_request};
use crate::backends::http::url::{join_url, normalize_base_url};
use crate::backends::shared::function_args::FunctionArgumentValues;
use crate::backends::shared::json_path::get_path_value;
use crate::backends::shared::response_rows::extract_rows;
use crate::backends::shared::template::{RenderContext, render_template};
use crate::{QueryExecutionControls, QueryPaginationPolicy};
use coral_spec::HttpMethod;

const DEFAULT_MAX_PAGES: usize = 10_000;

#[derive(Debug, Clone, Copy)]
struct FetchLimits {
    effective_limit: Option<usize>,
    page_size_limit: Option<usize>,
    max_search_calls: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FetchCompleteness {
    Default,
    Complete,
}

#[expect(
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "Paginated fetch inputs and state are easier to audit in one sequential function"
)]
pub(super) async fn fetch_rows(
    client: &HttpSourceClient,
    target: &HttpFetchTarget,
    filter_values: &HashMap<String, String>,
    arguments: &FunctionArgumentValues,
    row_limit: Option<usize>,
    page_hint: Option<usize>,
    completeness: FetchCompleteness,
    controls: &QueryExecutionControls,
) -> Result<Vec<Value>> {
    controls
        .check_active()
        .map_err(|kind| execution_stopped_error(&client.source_schema, target.name(), kind))?;
    let mut all_rows = Vec::new();
    let limits = resolve_fetch_limits(target, row_limit, page_hint, completeness);
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

    let mut state = initial_page_state(&pagination);

    let mut page_count = 0usize;
    let max_pages = pagination.max_pages.unwrap_or(DEFAULT_MAX_PAGES);

    loop {
        controls
            .check_active()
            .map_err(|kind| execution_stopped_error(&client.source_schema, target.name(), kind))?;
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

        let resolved_inputs = controls
            .run_until_stopped(client.resolved_inputs_for_request())
            .await
            .map_err(|kind| {
                execution_stopped_error(&client.source_schema, target.name(), kind)
            })??;
        let state_values = pagination_state_values(&state);
        let render_context = RenderContext::with_argument_texts(
            filter_values,
            arguments.values(),
            arguments.text_values(),
            &state_values,
            resolved_inputs.as_ref(),
        );
        let base_url = render_template(&client.base_url, &render_context)?;
        let base_url = normalize_base_url(&base_url);
        // Resolved once, then both decisions read that one binding: a mode
        // that advances by URL must both request that URL and skip rebuilding
        // the request Coral would otherwise have made. Deriving "are we
        // following?" from the URL itself rather than re-testing the mode
        // means the two can never disagree — and a new mode that forgets to
        // answer `follows_response_next_url` stops after page one at both
        // sites together, not at one of them.
        let next_url = state
            .next_url
            .clone()
            .filter(|_| pagination.mode.follows_response_next_url());
        let following_next_url = next_url.is_some();

        let url = if let Some(next) = next_url {
            next
        } else {
            let rendered_path = render_template(&active_request.path, &render_context)?;
            join_url(&base_url, &rendered_path)?
        };

        let (query_pairs, body) = if following_next_url {
            (Vec::new(), None)
        } else {
            let mut query_pairs = build_query_pairs(active_request, &render_context)?;
            apply_pagination_query_pairs(&mut query_pairs, &pagination, &state, page_size)
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

        let request = execute_request(
            client.http_for(controls),
            client.request_timeout,
            OutgoingHttpRequest {
                auth: &client.auth,
                request_headers: &client.request_headers,
                request_authenticators: &client.request_authenticators,
                request_identity_http_authenticator: client
                    .request_identity_http_authenticator
                    .as_ref(),
                trace_context: client.trace_context.as_ref(),
                table_headers: &active_request.headers,
                table_name: target.name(),
                method: active_request.method,
                url: &url,
                query_pairs: &query_pairs,
                body: body.as_ref(),
                response_format: target.response().format,
                source_schema: &client.source_schema,
                rate_limit: &client.rate_limit,
                body_capture: client.body_capture,
                render_context,
                allow_404_empty: target.response().allow_404_empty,
            },
            controls,
        )
        .await?;

        let Some(response) = request else {
            break;
        };
        let payload = response.payload;

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

        if controls.pagination_policy() == QueryPaginationPolicy::FirstPageOnly
            && has_explicit_continuation(
                &pagination,
                PageAdvanceContext {
                    payload: &payload,
                    response_headers: &response.headers,
                    request_url: &url,
                    rows_on_page,
                    page_size,
                    source_schema: &client.source_schema,
                    table_name: target.name(),
                },
            )
            .map_err(|error| {
                pagination_error(
                    &client.source_schema,
                    target.name(),
                    Some(http_method_label(active_request.method)),
                    Some(&url),
                    &error,
                )
            })?
        {
            controls.mark_explicit_continuation();
        }

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

        if controls.pagination_policy() == QueryPaginationPolicy::FirstPageOnly {
            break;
        }

        controls
            .check_active()
            .map_err(|kind| execution_stopped_error(&client.source_schema, target.name(), kind))?;

        let page_advance = advance_pagination_state(
            &mut state,
            &pagination,
            PageAdvanceContext {
                payload: &payload,
                response_headers: &response.headers,
                request_url: &url,
                rows_on_page,
                page_size,
                source_schema: &client.source_schema,
                table_name: target.name(),
            },
        )
        .map_err(|error| {
            pagination_error(
                &client.source_schema,
                target.name(),
                Some(http_method_label(active_request.method)),
                Some(&url),
                &error,
            )
        })?;
        if page_advance == PageAdvance::Stop {
            break;
        }
    }

    Ok(all_rows)
}

fn http_method_label(method: HttpMethod) -> &'static str {
    match method {
        HttpMethod::GET => "GET",
        HttpMethod::POST => "POST",
    }
}

fn resolve_fetch_limits(
    target: &HttpFetchTarget,
    row_limit: Option<usize>,
    page_hint: Option<usize>,
    completeness: FetchCompleteness,
) -> FetchLimits {
    let Some(search_limits) = target.search_limits() else {
        return FetchLimits {
            effective_limit: row_limit,
            page_size_limit: page_hint,
            max_search_calls: None,
        };
    };

    let default_top_k = match completeness {
        FetchCompleteness::Default => search_limits.default_top_k,
        FetchCompleteness::Complete => search_limits.max_top_k,
    };
    let requested_top_k = page_hint.unwrap_or(default_top_k);
    let requested_top_k = row_limit.map_or(requested_top_k, |limit| requested_top_k.min(limit));
    let max_candidates = search_limits
        .max_top_k
        .saturating_mul(search_limits.max_calls_per_query);
    let effective_limit = match (row_limit, completeness) {
        (Some(limit), _) => Some(limit),
        (None, FetchCompleteness::Default) => Some(requested_top_k),
        (None, FetchCompleteness::Complete) => Some(max_candidates),
    };

    FetchLimits {
        effective_limit: effective_limit.map(|limit| limit.min(max_candidates)),
        page_size_limit: Some(requested_top_k.min(search_limits.max_top_k)),
        max_search_calls: Some(search_limits.max_calls_per_query),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use serde_json::json;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;
    use crate::{
        QueryCancellationToken, QueryPaginationPolicy, QueryRetryPolicy, RequestAuthenticator,
    };
    use coral_spec::parse_source_manifest_value;

    #[tokio::test]
    async fn first_page_only_stops_before_pagination_advance() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/items"))
            .and(query_param("page", "1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"id": 1}]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/items"))
            .and(query_param("page", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"id": 2}]
            })))
            .expect(0)
            .mount(&server)
            .await;
        let manifest = parse_source_manifest_value(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "http",
            "base_url": server.uri(),
            "tables": [{
                "name": "items",
                "description": "items",
                "request": {"path": "/items"},
                "response": {"rows_path": ["data"]},
                "pagination": {
                    "mode": "page",
                    "page_param": "page",
                    "page_start": 1
                },
                "columns": [{"name": "id", "type": "Int64"}]
            }]
        }))
        .expect("manifest")
        .as_http()
        .expect("HTTP manifest")
        .clone();
        let request_authenticators: HashMap<String, std::sync::Arc<dyn RequestAuthenticator>> =
            HashMap::new();
        let client = HttpSourceClient::from_manifest(
            &manifest,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &request_authenticators,
            None,
            reqwest::Client::new(),
        )
        .expect("client");
        let table = manifest.tables.first().expect("table");
        let target = HttpFetchTarget::from_resolved_table_request(table, table.request.clone());
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );
        let arguments = FunctionArgumentValues::default();

        let rows = client
            .fetch(&target, &HashMap::new(), &arguments, None, &controls)
            .await
            .expect("first page fetch");

        assert_eq!(rows, [json!({"id": 1})]);
        assert!(controls.upstream_started());
        assert!(
            !controls.has_more(),
            "a full numbered page is not explicit continuation metadata"
        );
    }

    #[tokio::test]
    async fn first_page_only_records_an_explicit_response_cursor() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/items"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"id": 1}],
                "meta": {"next_cursor": "page-2"}
            })))
            .expect(1)
            .mount(&server)
            .await;
        let manifest = parse_source_manifest_value(json!({
            "dsl_version": 3,
            "name": "demo",
            "version": "0.1.0",
            "backend": "http",
            "base_url": server.uri(),
            "tables": [{
                "name": "items",
                "description": "items",
                "request": {"path": "/items"},
                "response": {"rows_path": ["data"]},
                "pagination": {
                    "mode": "cursor_query",
                    "cursor_param": "cursor",
                    "response_cursor_path": ["meta", "next_cursor"]
                },
                "columns": [{"name": "id", "type": "Int64"}]
            }]
        }))
        .expect("manifest")
        .as_http()
        .expect("HTTP manifest")
        .clone();
        let request_authenticators: HashMap<String, std::sync::Arc<dyn RequestAuthenticator>> =
            HashMap::new();
        let client = HttpSourceClient::from_manifest(
            &manifest,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &request_authenticators,
            None,
            reqwest::Client::new(),
        )
        .expect("client");
        let table = manifest.tables.first().expect("table");
        let target = HttpFetchTarget::from_resolved_table_request(table, table.request.clone());
        let controls = QueryExecutionControls::for_fanout(
            tokio::time::Instant::now() + std::time::Duration::from_secs(1),
            QueryCancellationToken::new(),
        );
        let arguments = FunctionArgumentValues::default();

        let rows = client
            .fetch(&target, &HashMap::new(), &arguments, None, &controls)
            .await
            .expect("first page fetch");

        assert_eq!(rows, [json!({"id": 1})]);
        assert!(controls.upstream_started());
        assert!(controls.has_more());
    }
}
