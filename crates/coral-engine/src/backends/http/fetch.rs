//! Paginated HTTP fetch orchestration.

use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result};
use serde_json::Value;

use crate::backends::http::ProviderQueryError;
use crate::backends::http::client::HttpSourceClient;
use crate::backends::http::error::{pagination_error, provider_error};
use crate::backends::http::pagination::{
    PageAdvance, PageAdvanceContext, advance_pagination_state, apply_pagination_body_fields,
    apply_pagination_query_pairs, initial_page_state, pagination_state_values, resolve_page_size,
};
use crate::backends::http::request::{build_query_pairs, build_request_body};
use crate::backends::http::target::HttpFetchTarget;
use crate::backends::http::transport::{OutgoingHttpRequest, SecretProvenance, execute_request};
use crate::backends::http::url::{join_url, normalize_base_url};
use crate::backends::shared::json_path::get_path_value;
use crate::backends::shared::response_rows::extract_rows;
use crate::backends::shared::template::{RenderContext, render_template_with_secret_provenance};
use coral_spec::{HttpMethod, ValidatedPaginationMode};

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

    let mut state = initial_page_state(&pagination);
    let mut next_link_depends_on_secret = false;

    let mut page_count = 0usize;
    let max_pages = pagination.max_pages.unwrap_or(DEFAULT_MAX_PAGES);

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
        let base_url = render_template_with_secret_provenance(
            &client.base_url,
            &render_context,
            &client.secret_input_names,
        )?;
        let normalized_base_url = normalize_base_url(&base_url.value);
        let following_link_header = matches!(
            pagination.mode,
            ValidatedPaginationMode::LinkHeader | ValidatedPaginationMode::Auto
        ) && state.next_url.is_some();

        let (url, url_depends_on_secret) = if matches!(
            pagination.mode,
            ValidatedPaginationMode::LinkHeader | ValidatedPaginationMode::Auto
        ) && let Some(next) = state.next_url.clone()
        {
            (next, next_link_depends_on_secret)
        } else {
            let rendered_path = render_template_with_secret_provenance(
                &active_request.path,
                &render_context,
                &client.secret_input_names,
            )?;
            (
                join_url(&normalized_base_url, &rendered_path.value)?,
                client.require_credential_safe_auth_transport
                    && (base_url.depends_on_secret || rendered_path.depends_on_secret),
            )
        };

        let (query_pairs, body, contains_secret_value, redact_url) = if following_link_header {
            (
                Vec::new(),
                None,
                url_depends_on_secret,
                url_depends_on_secret,
            )
        } else {
            let mut query_pairs =
                build_query_pairs(active_request, &render_context, &client.secret_input_names)?;
            let redact_url = url_depends_on_secret
                || (client.require_credential_safe_auth_transport && query_pairs.depends_on_secret);
            apply_pagination_query_pairs(&mut query_pairs.value, &pagination, &state, page_size)
                .map_err(|error| {
                    pagination_error(
                        &client.source_schema,
                        target.name(),
                        None,
                        (!redact_url).then_some(url.as_str()),
                        &error,
                    )
                })?;

            let mut body =
                build_request_body(active_request, &render_context, &client.secret_input_names)?;
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
                    (!redact_url).then_some(url.as_str()),
                    &error,
                )
            })?;
            let contains_secret_value = client.require_credential_safe_auth_transport
                && (url_depends_on_secret
                    || query_pairs.depends_on_secret
                    || body.depends_on_secret());
            (
                query_pairs.value,
                body.value,
                contains_secret_value,
                redact_url,
            )
        };
        let secret_provenance = if redact_url {
            SecretProvenance::Url
        } else if contains_secret_value {
            SecretProvenance::Request
        } else {
            SecretProvenance::Public
        };

        let request = execute_request(
            &client.http,
            client.request_timeout,
            OutgoingHttpRequest {
                auth: &client.auth,
                request_headers: &client.request_headers,
                request_authenticators: &client.request_authenticators,
                require_credential_safe_auth_transport: client
                    .require_credential_safe_auth_transport,
                secret_provenance,
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
            let error = if contains_secret_value {
                redacted_pagination_error(&error)
            } else {
                error
            };
            pagination_error(
                &client.source_schema,
                target.name(),
                Some(http_method_label(active_request.method)),
                (!redact_url).then_some(url.as_str()),
                &error,
            )
        })?;
        if page_advance == PageAdvance::Stop {
            break;
        }
        next_link_depends_on_secret = state.next_url.is_some() && contains_secret_value;
    }

    Ok(all_rows)
}

fn redacted_pagination_error(error: &DataFusionError) -> DataFusionError {
    let detail = error.to_string();
    let category = [
        (
            "next link must stay on origin",
            "pagination next link must stay on request origin",
        ),
        (
            "next URL header value must stay on origin",
            "pagination next URL header must stay on request origin",
        ),
        (
            "invalid pagination Link header item",
            "invalid pagination Link header",
        ),
        (
            "invalid pagination next link",
            "invalid pagination next link",
        ),
        (
            "invalid pagination next URL header value",
            "invalid pagination next URL header",
        ),
        (
            "invalid pagination next URL header",
            "invalid pagination next URL header",
        ),
        (
            "invalid pagination response cursor header",
            "invalid pagination response cursor header",
        ),
        (
            "invalid request URL for pagination links",
            "invalid request URL for pagination links",
        ),
    ]
    .into_iter()
    .find_map(|(needle, category)| detail.contains(needle).then_some(category))
    .unwrap_or("pagination failed for request with secret-derived URL");
    DataFusionError::Execution(category.to_string())
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
