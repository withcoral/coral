//! Paginated HTTP fetch orchestration.

use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result};
use futures::{StreamExt as _, stream};
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
use coral_spec::{
    RequestSpec, ValidatedPagination, ValidatedPaginationMode, ValidatedPaginationParallel,
};

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
struct PageFetchRequest {
    state: PageState,
    extra_query_pairs: Vec<(String, String)>,
}

#[derive(Debug)]
struct FetchedPage {
    payload: Value,
    next_url: Option<String>,
    rows: Vec<Value>,
    rows_on_page: usize,
}

#[derive(Debug)]
struct ParallelPage {
    index: usize,
    rows: Vec<Value>,
    rows_on_page: usize,
}

#[derive(Debug)]
enum ParallelFetchPlan {
    IndependentPages {
        start: i64,
        step: i64,
        extra_page_param: Option<String>,
        max_concurrency: usize,
    },
    IndependentOffsets {
        start: i64,
        step: i64,
        max_concurrency: usize,
    },
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

    let context = FetchContext {
        client,
        target,
        filter_values,
        arg_values,
        active_request: target.resolved_request(),
    };

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

    if let Some(plan) = resolve_parallel_fetch_plan(
        target,
        &pagination,
        &limits,
        page_size,
        sql_limit,
        max_pages,
        &client.source_schema,
    )? {
        return fetch_rows_parallel(
            context,
            limits,
            &pagination,
            page_size.expect("parallel plan requires page size"),
            max_pages,
            plan,
        )
        .await;
    }

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

        let Some(mut fetched_page) = fetch_page(
            context,
            &pagination,
            page_size,
            &PageFetchRequest {
                state: state.clone(),
                extra_query_pairs: Vec::new(),
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
                    &target.pagination().response_cursor_path,
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

async fn fetch_rows_parallel(
    context: FetchContext<'_>,
    limits: FetchLimits,
    pagination: &ValidatedPagination,
    page_size: usize,
    max_pages: usize,
    plan: ParallelFetchPlan,
) -> Result<Vec<Value>> {
    let first_request = first_parallel_page_request(&plan);
    let Some(mut first_page) =
        fetch_page(context, pagination, Some(page_size), &first_request).await?
    else {
        return Ok(Vec::new());
    };

    let mut all_rows = Vec::new();
    let first_rows_on_page = first_page.rows_on_page;
    all_rows.append(&mut first_page.rows);
    if let Some(limit) = limits.effective_limit
        && all_rows.len() >= limit
    {
        all_rows.truncate(limit);
        return Ok(all_rows);
    }
    if page_is_exhausted(first_rows_on_page, Some(page_size)) {
        return Ok(all_rows);
    }

    let Some(limit) = limits.effective_limit else {
        return Ok(all_rows);
    };
    let pages_needed = limit.div_ceil(page_size);
    if pages_needed <= 1 {
        all_rows.truncate(limit);
        return Ok(all_rows);
    }
    if pages_needed > max_pages {
        return Err(provider_error(ProviderQueryError::Pagination {
            source_schema: context.client.source_schema.clone(),
            table: context.target.name().to_string(),
            method: None,
            url: None,
            detail: format!("exceeded pagination max_pages={max_pages}"),
        }));
    }

    let requests = remaining_parallel_page_requests(&plan, pages_needed);
    let max_concurrency = plan.max_concurrency();
    let mut pages = stream::iter(requests)
        .map(|(index, request)| async move {
            let page = fetch_page(context, pagination, Some(page_size), &request).await?;
            let Some(page) = page else {
                return Ok::<_, DataFusionError>(ParallelPage {
                    index,
                    rows: Vec::new(),
                    rows_on_page: 0,
                });
            };
            Ok(ParallelPage {
                index,
                rows: page.rows,
                rows_on_page: page.rows_on_page,
            })
        })
        .buffer_unordered(max_concurrency)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

    pages.sort_by_key(|page| page.index);
    for mut page in pages {
        if page.rows_on_page == 0 {
            break;
        }
        let rows_on_page = page.rows_on_page;
        all_rows.append(&mut page.rows);
        if all_rows.len() >= limit {
            break;
        }
        if page_is_exhausted(rows_on_page, Some(page_size)) {
            break;
        }
    }
    all_rows.truncate(limit);
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

    let (mut query_pairs, body) = if following_link_header {
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
    query_pairs.extend(page_request.extra_query_pairs.iter().cloned());

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

fn resolve_parallel_fetch_plan(
    target: &HttpFetchTarget,
    pagination: &ValidatedPagination,
    limits: &FetchLimits,
    page_size: Option<usize>,
    sql_limit: Option<usize>,
    max_pages: usize,
    source_schema: &str,
) -> Result<Option<ParallelFetchPlan>> {
    if sql_limit.is_none() || limits.max_search_calls.is_some() {
        return Ok(None);
    }
    let Some(page_size) = page_size else {
        return Ok(None);
    };
    let Some(limit) = limits.effective_limit else {
        return Ok(None);
    };
    if limit <= page_size {
        return Ok(None);
    }
    if max_pages <= 1 {
        return Ok(None);
    }

    match &pagination.parallel {
        Some(ValidatedPaginationParallel::IndependentPages(page)) => {
            let extra_page_param = if matches!(pagination.mode, ValidatedPaginationMode::LinkHeader)
            {
                Some(page.param.clone())
            } else {
                None
            };
            Ok(Some(ParallelFetchPlan::IndependentPages {
                start: page.start,
                step: page.step,
                extra_page_param,
                max_concurrency: page.max_concurrency,
            }))
        }
        Some(ValidatedPaginationParallel::IndependentOffsets { max_concurrency }) => {
            let ValidatedPaginationMode::Offset(offset) = &pagination.mode else {
                return Err(provider_error(ProviderQueryError::Pagination {
                    source_schema: source_schema.to_string(),
                    table: target.name().to_string(),
                    method: None,
                    url: None,
                    detail: "independent offset pagination requires offset mode".to_string(),
                }));
            };
            let step = offset
                .resolve_step(Some(page_size), source_schema, target.name())
                .map_err(|error| {
                    provider_error(ProviderQueryError::Pagination {
                        source_schema: source_schema.to_string(),
                        table: target.name().to_string(),
                        method: None,
                        url: None,
                        detail: error.to_string(),
                    })
                })?;
            Ok(Some(ParallelFetchPlan::IndependentOffsets {
                start: offset.start,
                step,
                max_concurrency: *max_concurrency,
            }))
        }
        None => Ok(None),
    }
}

fn first_parallel_page_request(plan: &ParallelFetchPlan) -> PageFetchRequest {
    match plan {
        ParallelFetchPlan::IndependentPages {
            start,
            extra_page_param,
            ..
        } => {
            let page = *start;
            PageFetchRequest {
                state: PageState {
                    page,
                    ..PageState::default()
                },
                extra_query_pairs: extra_page_param
                    .iter()
                    .map(|param| (param.clone(), page.to_string()))
                    .collect(),
            }
        }
        ParallelFetchPlan::IndependentOffsets { start, .. } => PageFetchRequest {
            state: PageState {
                offset: *start,
                ..PageState::default()
            },
            extra_query_pairs: Vec::new(),
        },
    }
}

fn remaining_parallel_page_requests(
    plan: &ParallelFetchPlan,
    pages_needed: usize,
) -> Vec<(usize, PageFetchRequest)> {
    (1..pages_needed)
        .map(|index| (index, parallel_page_request(plan, index)))
        .collect()
}

fn parallel_page_request(plan: &ParallelFetchPlan, index: usize) -> PageFetchRequest {
    match plan {
        ParallelFetchPlan::IndependentPages {
            start,
            step,
            extra_page_param,
            ..
        } => {
            let index = i64::try_from(index).unwrap_or(i64::MAX);
            let page = start.saturating_add(step.saturating_mul(index));
            PageFetchRequest {
                state: PageState {
                    page,
                    ..PageState::default()
                },
                extra_query_pairs: extra_page_param
                    .iter()
                    .map(|param| (param.clone(), page.to_string()))
                    .collect(),
            }
        }
        ParallelFetchPlan::IndependentOffsets { start, step, .. } => {
            let index = i64::try_from(index).unwrap_or(i64::MAX);
            PageFetchRequest {
                state: PageState {
                    offset: start.saturating_add(step.saturating_mul(index)),
                    ..PageState::default()
                },
                extra_query_pairs: Vec::new(),
            }
        }
    }
}

impl ParallelFetchPlan {
    fn max_concurrency(&self) -> usize {
        match self {
            Self::IndependentPages {
                max_concurrency, ..
            }
            | Self::IndependentOffsets {
                max_concurrency, ..
            } => *max_concurrency,
        }
    }
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
