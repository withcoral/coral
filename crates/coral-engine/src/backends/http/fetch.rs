//! HTTP fetch planning and execution.

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
use coral_spec::{RequestSpec, ValidatedPagination, ValidatedPaginationMode};

const DEFAULT_MAX_PAGES: usize = 10_000;

#[derive(Debug, Clone, Copy)]
struct FetchLimits {
    effective_limit: Option<usize>,
    page_size_limit: Option<usize>,
    max_search_calls: Option<usize>,
}

#[derive(Debug, Clone, Copy)]
struct FetchContext<'a> {
    client: &'a HttpSourceClient,
    target: &'a HttpFetchTarget,
    filter_values: &'a HashMap<String, String>,
    arg_values: &'a HashMap<String, String>,
    active_request: &'a RequestSpec,
}

#[derive(Debug, Clone)]
enum HttpFetchPlan {
    Sequential(SequentialFetchPlan),
}

/// One provider request chain: fetch a page, inspect the response, then decide
/// whether there is another page to request.
#[derive(Debug, Clone)]
struct SequentialFetchPlan {
    initial_state: PageState,
    max_pages: usize,
}

#[derive(Debug)]
struct PageFetchRequest {
    state: PageState,
}

#[derive(Debug)]
struct FetchedPage {
    payload: Value,
    next_url: Option<String>,
    rows: Vec<Value>,
    rows_on_page: usize,
}

pub(super) async fn fetch_rows(
    client: &HttpSourceClient,
    target: &HttpFetchTarget,
    filter_values: &HashMap<String, String>,
    arg_values: &HashMap<String, String>,
    sql_limit: Option<usize>,
) -> Result<Vec<Value>> {
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

    let context = FetchContext {
        client,
        target,
        filter_values,
        arg_values,
        active_request: target.resolved_request(),
    };

    match plan_fetch(target, &pagination) {
        HttpFetchPlan::Sequential(plan) => {
            fetch_rows_sequential(context, limits, &pagination, page_size, plan).await
        }
    }
}

fn plan_fetch(target: &HttpFetchTarget, pagination: &ValidatedPagination) -> HttpFetchPlan {
    HttpFetchPlan::Sequential(SequentialFetchPlan {
        initial_state: PageState {
            page: target.pagination().page_start,
            offset: match &pagination.mode {
                ValidatedPaginationMode::Offset(offset) => offset.start,
                _ => target.pagination().offset_start,
            },
            ..PageState::default()
        },
        max_pages: target.pagination().max_pages.unwrap_or(DEFAULT_MAX_PAGES),
    })
}

async fn fetch_rows_sequential(
    context: FetchContext<'_>,
    limits: FetchLimits,
    pagination: &ValidatedPagination,
    page_size: Option<usize>,
    plan: SequentialFetchPlan,
) -> Result<Vec<Value>> {
    let mut all_rows = Vec::new();
    let mut state = plan.initial_state;
    let mut page_count = 0usize;

    loop {
        page_count += 1;
        if page_count > plan.max_pages {
            return Err(provider_error(ProviderQueryError::Pagination {
                source_schema: context.client.source_schema.clone(),
                table: context.target.name().to_string(),
                method: None,
                url: None,
                detail: format!("exceeded pagination max_pages={}", plan.max_pages),
            }));
        }

        let Some(mut fetched_page) = fetch_page(
            context,
            pagination,
            page_size,
            &PageFetchRequest {
                state: state.clone(),
            },
        )
        .await?
        else {
            break;
        };

        let rows_on_page = fetched_page.rows_on_page;
        all_rows.append(&mut fetched_page.rows);

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
                let next_cursor = get_path_value(
                    &fetched_page.payload,
                    &context.target.pagination().response_cursor_path,
                )
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
                state.page = state
                    .page
                    .saturating_add(context.target.pagination().page_step);
            }
            ValidatedPaginationMode::Offset(offset) => {
                if page_is_exhausted(rows_on_page, page_size) {
                    break;
                }
                let step = offset
                    .resolve_step(
                        page_size,
                        &context.client.source_schema,
                        context.target.name(),
                    )
                    .map_err(|error| {
                        provider_error(ProviderQueryError::Pagination {
                            source_schema: context.client.source_schema.clone(),
                            table: context.target.name().to_string(),
                            method: None,
                            url: None,
                            detail: error.to_string(),
                        })
                    })?;
                state.offset = state.offset.saturating_add(step);
            }
            ValidatedPaginationMode::LinkHeader | ValidatedPaginationMode::Auto => {
                match fetched_page.next_url {
                    Some(next) => state.next_url = Some(next),
                    None => break,
                }
            }
        }
    }

    Ok(all_rows)
}

#[expect(
    clippy::too_many_lines,
    reason = "One-page HTTP fetch keeps request rendering and response validation together"
)]
async fn fetch_page(
    context: FetchContext<'_>,
    pagination: &ValidatedPagination,
    page_size: Option<usize>,
    page_request: &PageFetchRequest,
) -> Result<Option<FetchedPage>> {
    let FetchContext {
        client,
        target,
        filter_values,
        arg_values,
        active_request,
    } = context;
    let state = &page_request.state;
    let resolved_inputs = client.resolved_inputs_for_request().await?;
    let state_values = pagination_state_values(state);
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
        apply_pagination_query_pairs(&mut query_pairs, target, pagination, state, page_size)
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
            pagination,
            state,
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
        return Ok(None);
    };

    validate_payload_ok(client, target, filter_values, &payload)?;
    let rows = extract_rows(target.response(), &payload);
    let rows_on_page = rows.len();
    Ok(Some(FetchedPage {
        payload,
        next_url,
        rows,
        rows_on_page,
    }))
}

fn validate_payload_ok(
    client: &HttpSourceClient,
    target: &HttpFetchTarget,
    filter_values: &HashMap<String, String>,
    payload: &Value,
) -> Result<()> {
    if target.response().ok_path.is_empty() {
        return Ok(());
    }
    let ok = get_path_value(payload, &target.response().ok_path)
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if ok {
        return Ok(());
    }
    let err = if target.response().error_path.is_empty() {
        "unknown source API error".to_string()
    } else {
        get_path_value(payload, &target.response().error_path)
            .and_then(Value::as_str)
            .unwrap_or("unknown source API error")
            .to_string()
    };
    Err(DataFusionError::External(Box::new(
        ProviderQueryError::ApiRequest {
            source_schema: client.source_schema.clone(),
            table: target.name().to_string(),
            status: None,
            method: None,
            url: None,
            filters: filter_values.clone(),
            detail: err,
        },
    )))
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
    use serde_json::json;

    use super::{HttpFetchPlan, plan_fetch};
    use crate::backends::http::test_support::{test_http_request_target, test_http_table_spec};
    use coral_spec::{
        BodySpec, HttpMethod, PageSizeSpec, PaginationMode, PaginationSpec, ParsedTemplate,
        RequestSpec,
    };

    #[test]
    fn plan_fetch_builds_sequential_plan_from_pagination_contract() {
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
        table.pagination = PaginationSpec {
            mode: PaginationMode::Offset,
            page_size: Some(PageSizeSpec {
                default: 25,
                max: 100,
                query_param: Some("limit".to_string()),
                body_path: vec![],
            }),
            offset_param: Some("start".to_string()),
            offset_start: 50,
            offset_step: Some(25),
            max_pages: Some(7),
            ..PaginationSpec::default()
        };
        let target = test_http_request_target(&table);
        let pagination = target.pagination().validated("demo", "items").unwrap();

        match plan_fetch(&target, &pagination) {
            HttpFetchPlan::Sequential(plan) => {
                assert_eq!(plan.initial_state.offset, 50);
                assert_eq!(plan.initial_state.page, 0);
                assert_eq!(plan.max_pages, 7);
            }
        }
    }
}
