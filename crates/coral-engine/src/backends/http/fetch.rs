//! Paginated HTTP fetch orchestration.

use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result};
use serde_json::Value;

use crate::backends::http::ProviderQueryError;
use crate::backends::http::client::HttpSourceClient;
use crate::backends::http::error::{
    REDACTED_CREDENTIAL_RESPONSE_DETAIL, pagination_error, provider_error,
};
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
    let mut provider_state_is_credential_tainted = false;

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
            (next, provider_state_is_credential_tainted)
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
                || provider_state_is_credential_tainted
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
                    || provider_state_is_credential_tainted
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
        let credential_tainted_response =
            client.require_credential_safe_auth_transport && response.credential_tainted;
        let payload = response.payload;

        if !target.response().ok_path.is_empty() {
            let ok = get_path_value(&payload, &target.response().ok_path)
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if !ok {
                let err = if credential_tainted_response {
                    REDACTED_CREDENTIAL_RESPONSE_DETAIL.to_string()
                } else if target.response().error_path.is_empty() {
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
            let error = if credential_tainted_response {
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
        if matches!(
            pagination.mode,
            ValidatedPaginationMode::LinkHeader
                | ValidatedPaginationMode::Auto
                | ValidatedPaginationMode::CursorQuery
                | ValidatedPaginationMode::CursorBody
        ) {
            provider_state_is_credential_tainted |= credential_tainted_response;
        }
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
    .unwrap_or("pagination failed for credential-bearing request");
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

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use datafusion::error::{DataFusionError, Result};
    use serde_json::{Value, json};
    use wiremock::matchers::{body_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::ProviderQueryError;
    use crate::BoundRequestIdentityHttpAuthenticator;
    use crate::backends::http::client::{HttpClients, HttpSourceClient, HttpSourceClientRuntime};
    use crate::backends::http::target::HttpFetchTarget;
    use crate::backends::http::test_support::parse_http_manifest;
    use coral_spec::backends::http::HttpSourceManifest;

    fn materialized_v4_manifest(
        base_url: &Value,
        request: &Value,
        pagination: Option<Value>,
    ) -> HttpSourceManifest {
        let mut table = json!({
            "name": "items",
            "description": "items",
            "request": request,
            "response": { "rows_path": ["data"] },
            "columns": [{ "name": "id", "type": "Utf8" }]
        });
        if let Some(pagination) = pagination {
            let table = table.as_object_mut().expect("table object");
            table.insert("pagination".to_string(), pagination);
        }
        let mut manifest = parse_http_manifest(json!({
            "dsl_version": 3,
            "name": "secret_transport",
            "version": "0.1.0",
            "backend": "http",
            "base_url": base_url,
            "inputs": { "SECRET": { "kind": "secret" } },
            "tables": [table]
        }));
        manifest.common.dsl_version = 4;
        manifest
    }

    fn credential_safe_clients(proxy: &MockServer) -> HttpClients {
        let builder = || {
            reqwest::Client::builder()
                .timeout(Duration::from_secs(1))
                .redirect(reqwest::redirect::Policy::none())
        };
        HttpClients::credential_safe(
            builder()
                .proxy(reqwest::Proxy::all(proxy.uri()).expect("proxy URL"))
                .build()
                .expect("proxy client"),
            builder().no_proxy().build().expect("direct client"),
        )
    }

    async fn fetch_manifest(
        manifest: &HttpSourceManifest,
        secret: &str,
        proxy: &MockServer,
        authenticator: Option<BoundRequestIdentityHttpAuthenticator>,
    ) -> Result<Vec<Value>> {
        let client = HttpSourceClient::from_manifest_with_source_input_resolver(
            manifest,
            &BTreeMap::from([("SECRET".to_string(), secret.to_string())]),
            &BTreeMap::new(),
            &HashMap::new(),
            HttpSourceClientRuntime::test_with_http_clients(
                None,
                credential_safe_clients(proxy),
                authenticator,
            ),
        )?;
        let table = manifest.tables.first().expect("table");
        client
            .fetch(
                &HttpFetchTarget::from_resolved_table_request(table, table.request.clone()),
                &HashMap::new(),
                &HashMap::new(),
                None,
            )
            .await
    }

    fn provider_query_error(error: &DataFusionError) -> &ProviderQueryError {
        let DataFusionError::External(inner) = error else {
            panic!("expected provider error, got {error:?}");
        };
        inner
            .downcast_ref::<ProviderQueryError>()
            .expect("provider query error")
    }

    fn assert_provider_non_egress(error: &DataFusionError, canary: &str) {
        let provider = provider_query_error(error);
        let structured = provider.to_structured();
        assert!(!provider.to_string().contains(canary), "{provider}");
        assert!(!structured.detail().contains(canary));
        assert!(
            structured
                .metadata()
                .values()
                .all(|value| !value.contains(canary))
        );
    }

    async fn assert_proxy_empty(proxy: &MockServer) {
        assert!(
            proxy
                .received_requests()
                .await
                .expect("proxy requests")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn selected_secret_surfaces_route_direct_while_public_one_of_uses_proxy() {
        let target = MockServer::start().await;
        let proxy = MockServer::start().await;
        let ok = ResponseTemplate::new(200).set_body_json(json!({"data": [{"id": "ok"}]}));
        Mock::given(method("GET"))
            .respond_with(ok.clone())
            .mount(&target)
            .await;
        Mock::given(method("POST"))
            .respond_with(ok.clone())
            .mount(&target)
            .await;
        Mock::given(method("GET"))
            .respond_with(ok)
            .mount(&proxy)
            .await;

        let cases = [
            (
                json!("{{input.SECRET}}"),
                json!({"path": "/base"}),
                target.uri(),
            ),
            (
                json!(target.uri()),
                json!({"path": "/{{input.SECRET}}"}),
                "path-secret".to_string(),
            ),
            (
                json!(target.uri()),
                json!({"path": "/query", "query": [{"name": "token", "from": "input", "key": "SECRET"}]}),
                "query-secret".to_string(),
            ),
            (
                json!(target.uri()),
                json!({"method": "POST", "path": "/body", "body": [{"path": ["token"], "from": "input", "key": "SECRET"}]}),
                "body-secret".to_string(),
            ),
        ];
        for (base_url, request, secret) in cases {
            fetch_manifest(
                &materialized_v4_manifest(&base_url, &request, None),
                &secret,
                &proxy,
                None,
            )
            .await
            .expect("selected secret request");
        }

        let public_first = materialized_v4_manifest(
            &json!(target.uri()),
            &json!({
                "path": "/public",
                "query": [{
                    "name": "selection",
                    "from": "one_of",
                    "values": [
                        {"from": "literal", "value": "public"},
                        {"from": "input", "key": "SECRET"}
                    ]
                }]
            }),
            None,
        );
        fetch_manifest(&public_first, "unused-secret", &proxy, None)
            .await
            .expect("public winner");

        let target_requests = target.received_requests().await.expect("target requests");
        let proxy_requests = proxy.received_requests().await.expect("proxy requests");
        assert_eq!((target_requests.len(), proxy_requests.len()), (4, 1));
        let direct = target_requests
            .iter()
            .map(|request| format!("{} {}", request.url, String::from_utf8_lossy(&request.body)))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            ["/base", "/path-secret", "query-secret", "body-secret"]
                .iter()
                .all(|expected| direct.contains(expected)),
            "{direct}"
        );
        let proxied = proxy_requests.first().expect("proxy request");
        let proxied = format!("{} {:?}", proxied.url, proxied.body);
        assert!(
            proxied.contains("selection=public") && !proxied.contains("unused-secret"),
            "{proxied}"
        );
    }

    #[tokio::test]
    async fn remote_cleartext_secret_fails_before_authenticator_or_network() {
        let proxy = MockServer::start().await;
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&calls);
        let authenticator: BoundRequestIdentityHttpAuthenticator = Arc::new(move |_, _| {
            observed.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(Vec::new()) })
        });
        let manifest = materialized_v4_manifest(
            &json!("http://192.0.2.1"),
            &json!({"method": "POST", "path": "/items", "body": [{"path": ["token"], "from": "input", "key": "SECRET"}]}),
            None,
        );

        let error = fetch_manifest(&manifest, "remote-secret", &proxy, Some(authenticator))
            .await
            .expect_err("remote cleartext must fail");

        let public_error = error.to_string();
        assert!(
            public_error.contains("DSL v4 secret values require HTTPS or loopback HTTP")
                && !public_error.contains("remote-secret"),
            "{public_error}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_proxy_empty(&proxy).await;
    }

    #[tokio::test]
    async fn link_and_auto_descendants_keep_secret_transport_and_redaction() {
        for mode in ["link_header", "auto"] {
            let target = MockServer::start().await;
            let proxy = MockServer::start().await;
            let canary = format!("{mode}-descendant-canary");
            let first_page = if mode == "auto" {
                json!({"ok": true, "data": [{"id": "first"}]})
            } else {
                json!({"data": [{"id": "first"}]})
            };
            Mock::given(method("POST"))
                .and(path("/start"))
                .respond_with(
                    ResponseTemplate::new(200)
                        .insert_header("link", format!("</{canary}>; rel=\"next\""))
                        .set_body_json(first_page),
                )
                .mount(&target)
                .await;
            let failure = if mode == "auto" {
                ResponseTemplate::new(200)
                    .set_body_json(json!({"ok": false, "error": canary, "data": []}))
            } else {
                ResponseTemplate::new(400).set_body_string(canary.clone())
            };
            Mock::given(method("POST"))
                .and(path(format!("/{canary}")))
                .respond_with(failure)
                .mount(&target)
                .await;
            let mut manifest = materialized_v4_manifest(
                &json!(target.uri()),
                &json!({"method": "POST", "path": "/start", "body": [{"path": ["token"], "from": "input", "key": "SECRET"}]}),
                Some(json!({"mode": mode})),
            );
            if mode == "auto" {
                let response = &mut manifest.tables.first_mut().expect("table").response;
                response.ok_path = vec!["ok".to_string()];
                response.error_path = vec!["error".to_string()];
            }

            let error = fetch_manifest(&manifest, "first-page-secret", &proxy, None)
                .await
                .expect_err("page two should fail");

            assert_provider_non_egress(&error, &canary);
            let requests = target.received_requests().await.expect("target requests");
            let [first, second] = requests.as_slice() else {
                panic!("{mode}: {requests:?}");
            };
            assert!(
                first
                    .body
                    .windows(17)
                    .any(|bytes| bytes == b"first-page-secret")
            );
            assert!(second.body.is_empty(), "{mode}: {:?}", second.body);
            assert_proxy_empty(&proxy).await;
        }
    }

    #[tokio::test]
    async fn cursor_body_descendant_from_secret_request_stays_direct() {
        let target = MockServer::start().await;
        let proxy = MockServer::start().await;
        let cursor = "provider-cursor-canary";
        Mock::given(method("POST"))
            .and(path("/items"))
            .and(body_json(json!({"cursor": "first-page-secret"})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "data": [{"id": "first"}],
                "meta": {"next_cursor": cursor}
            })))
            .mount(&target)
            .await;
        Mock::given(method("POST"))
            .and(path("/items"))
            .and(body_json(json!({"cursor": cursor})))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"data": []})))
            .mount(&target)
            .await;
        let manifest = materialized_v4_manifest(
            &json!(target.uri()),
            &json!({
                "method": "POST",
                "path": "/items",
                "body": [{"path": ["cursor"], "from": "input", "key": "SECRET"}]
            }),
            Some(json!({
                "mode": "cursor_body",
                "cursor_body_path": ["cursor"],
                "response_cursor_path": ["meta", "next_cursor"]
            })),
        );

        let rows = fetch_manifest(&manifest, "first-page-secret", &proxy, None)
            .await
            .expect("cursor request");

        assert_eq!(rows, [json!({"id": "first"})]);
        assert_eq!(
            target
                .received_requests()
                .await
                .expect("target requests")
                .len(),
            2
        );
        assert_proxy_empty(&proxy).await;
    }
}
