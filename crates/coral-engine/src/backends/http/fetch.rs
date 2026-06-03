//! Paginated HTTP fetch orchestration.

use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result};
use serde_json::Value;

use crate::backends::http::ProviderQueryError;
use crate::backends::http::client::HttpSourceClient;
use crate::backends::http::error::{pagination_error, provider_error};
use crate::backends::http::pagination::{
    PageState, apply_pagination_body_fields, apply_pagination_query_pairs, page_is_exhausted,
    pagination_state_values, resolve_page_size,
};
use crate::backends::http::request::{build_query_pairs, build_request_body};
use crate::backends::http::target::HttpFetchTarget;
use crate::backends::http::transport::{OutgoingHttpRequest, execute_request};
use crate::backends::http::url::{join_url, normalize_base_url};
use crate::backends::shared::json_path::get_path_value;
use crate::backends::shared::response_rows::extract_rows;
use crate::backends::shared::template::{RenderContext, render_template};
use coral_spec::ValidatedPaginationMode;

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
    clippy::too_many_lines,
    reason = "Paginated fetch logic is stateful and easier to audit in one sequential function"
)]
pub(super) async fn fetch_rows(
    client: &HttpSourceClient,
    target: &HttpFetchTarget,
    filter_values: &HashMap<String, String>,
    arg_values: &HashMap<String, String>,
    row_limit: Option<usize>,
    page_hint: Option<usize>,
    completeness: FetchCompleteness,
) -> Result<Vec<Value>> {
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

        let request = execute_request(
            &client.http,
            client.request_timeout,
            OutgoingHttpRequest {
                auth: &client.auth,
                request_headers: &client.request_headers,
                request_authenticators: &client.request_authenticators,
                trace_context: client.trace_context.as_ref(),
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
        .await?;

        let Some((payload, next_url)) = request else {
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
