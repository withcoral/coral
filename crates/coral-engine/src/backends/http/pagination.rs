//! Pagination request mutation and response-link handling.

use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result};
use reqwest::header::{HeaderMap, HeaderName};
use serde_json::{Map, Value, json};

use crate::backends::http::request::{RequestBody, set_path_value};
use crate::backends::shared::json_path::get_path_value;
use coral_spec::{BodySpec, PageSizeSpec, ValidatedPagination, ValidatedPaginationMode};

#[derive(Debug, Clone, Default)]
pub(super) struct PageState {
    pub(super) cursor: Option<String>,
    pub(super) page: i64,
    pub(super) offset: i64,
    pub(super) next_url: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct ResponsePaginationHints {
    pub(super) next_url: Option<String>,
    pub(super) cursor: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PageAdvance {
    Continue,
    Stop,
}

#[derive(Clone, Copy)]
pub(super) struct PageAdvanceContext<'a> {
    pub(super) payload: &'a Value,
    pub(super) response_headers: &'a HeaderMap,
    pub(super) request_url: &'a str,
    pub(super) rows_on_page: usize,
    pub(super) page_size: Option<usize>,
    pub(super) source_schema: &'a str,
    pub(super) table_name: &'a str,
}

pub(super) fn initial_page_state(pagination: &ValidatedPagination) -> PageState {
    PageState {
        page: pagination.page_start,
        offset: match &pagination.mode {
            ValidatedPaginationMode::Offset(offset) => offset.start,
            _ => 0,
        },
        ..PageState::default()
    }
}

pub(super) fn apply_pagination_query_pairs(
    params: &mut Vec<(String, String)>,
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
        | ValidatedPaginationMode::CursorBody => {}
        ValidatedPaginationMode::LinkHeader => {
            if let Some(name) = &pagination.page_param {
                params.push((name.clone(), state.page.to_string()));
            }
        }
        ValidatedPaginationMode::CursorQuery => {
            if let Some(cursor) = &state.cursor {
                let name = pagination.cursor_param.clone().ok_or_else(|| {
                    DataFusionError::Execution(
                        "cursor_query pagination requires cursor_param".to_string(),
                    )
                })?;
                params.push((name, cursor.clone()));
            }
        }
        ValidatedPaginationMode::Page => {
            let name = pagination.page_param.clone().ok_or_else(|| {
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

pub(super) fn apply_pagination_body_fields(
    body: &mut Option<RequestBody>,
    body_spec: &BodySpec,
    pagination: &ValidatedPagination,
    state: &PageState,
    page_size: Option<usize>,
) -> Result<()> {
    let needs_page_size_body = page_size
        .zip(pagination.page_size.as_ref())
        .is_some_and(|(_, spec)| !spec.body_path.is_empty());
    let needs_cursor_body = matches!(pagination.mode, ValidatedPaginationMode::CursorBody)
        && !pagination.cursor_body_path.is_empty()
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
        if pagination.cursor_body_path.is_empty() {
            return Err(DataFusionError::Execution(
                "cursor_body pagination requires cursor_body_path".to_string(),
            ));
        }
        set_path_value(root, &pagination.cursor_body_path, json!(cursor))?;
    }

    Ok(())
}

pub(super) fn resolve_page_size(
    spec: Option<&PageSizeSpec>,
    sql_limit: Option<usize>,
) -> Option<usize> {
    let spec = spec?;
    let base = sql_limit.unwrap_or(spec.default);
    Some(base.min(spec.max).max(1))
}

pub(super) fn page_is_exhausted(rows_on_page: usize, page_size: Option<usize>) -> bool {
    rows_on_page == 0 || page_size.is_some_and(|requested| rows_on_page < requested)
}

pub(super) fn pagination_state_values(state: &PageState) -> HashMap<String, String> {
    let mut values = HashMap::new();
    values.insert("page".to_string(), state.page.to_string());
    values.insert("offset".to_string(), state.offset.to_string());
    if let Some(cursor) = &state.cursor {
        values.insert("cursor".to_string(), cursor.clone());
    }
    values
}

pub(super) fn advance_pagination_state(
    state: &mut PageState,
    pagination: &ValidatedPagination,
    context: PageAdvanceContext<'_>,
) -> Result<PageAdvance> {
    match &pagination.mode {
        ValidatedPaginationMode::None => Ok(PageAdvance::Stop),
        ValidatedPaginationMode::CursorQuery | ValidatedPaginationMode::CursorBody => {
            let hints = extract_response_pagination_hints(
                context.response_headers,
                context.request_url,
                pagination,
            )?;
            let next_cursor = hints.cursor.or_else(|| {
                get_path_value(context.payload, &pagination.response_cursor_path)
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(ToOwned::to_owned)
            });
            match next_cursor {
                Some(cursor) => {
                    state.cursor = Some(cursor);
                    Ok(PageAdvance::Continue)
                }
                None => Ok(PageAdvance::Stop),
            }
        }
        ValidatedPaginationMode::Page => {
            if page_is_exhausted(context.rows_on_page, context.page_size) {
                return Ok(PageAdvance::Stop);
            }
            state.page = state.page.saturating_add(pagination.page_step);
            Ok(PageAdvance::Continue)
        }
        ValidatedPaginationMode::Offset(offset) => {
            if page_is_exhausted(context.rows_on_page, context.page_size) {
                return Ok(PageAdvance::Stop);
            }
            let step = offset
                .resolve_step(context.page_size, context.source_schema, context.table_name)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            state.offset = state.offset.saturating_add(step);
            Ok(PageAdvance::Continue)
        }
        ValidatedPaginationMode::LinkHeader | ValidatedPaginationMode::Auto => {
            let hints = extract_response_pagination_hints(
                context.response_headers,
                context.request_url,
                pagination,
            )?;
            match hints.next_url {
                Some(next) => {
                    state.next_url = Some(next);
                    Ok(PageAdvance::Continue)
                }
                None => Ok(PageAdvance::Stop),
            }
        }
    }
}

pub(super) fn extract_response_pagination_hints(
    headers: &HeaderMap,
    request_url: &str,
    pagination: &ValidatedPagination,
) -> Result<ResponsePaginationHints> {
    let next_url = match pagination.mode {
        ValidatedPaginationMode::LinkHeader | ValidatedPaginationMode::Auto => {
            let link_next_url = extract_next_link_url(
                headers,
                request_url,
                pagination.link_header_require_results,
            )?;
            let header_next_url = extract_next_url_header(
                headers,
                request_url,
                pagination.next_url_header.as_deref(),
            )?;
            link_next_url.or(header_next_url)
        }
        _ => None,
    };

    let cursor = match pagination.mode {
        ValidatedPaginationMode::CursorQuery | ValidatedPaginationMode::CursorBody => {
            extract_response_cursor_header(headers, pagination.response_cursor_header.as_deref())?
        }
        _ => None,
    };

    Ok(ResponsePaginationHints { next_url, cursor })
}

pub(super) fn extract_next_link_url(
    headers: &HeaderMap,
    request_url: &str,
    require_results_true: bool,
) -> Result<Option<String>> {
    let base = pagination_request_url(request_url)?;

    for header in headers.get_all("link") {
        let Ok(header) = header.to_str() else {
            continue;
        };
        for part in header.split(',') {
            let item = part.trim();
            if !link_param_matches(item, "rel", "next") {
                continue;
            }
            if require_results_true && !link_param_matches(item, "results", "true") {
                continue;
            }
            let start = item.find('<').ok_or_else(|| {
                DataFusionError::Execution(format!("invalid pagination Link header item '{item}'"))
            })?;
            let end = item.find('>').ok_or_else(|| {
                DataFusionError::Execution(format!("invalid pagination Link header item '{item}'"))
            })?;
            let next_raw = item.get(start + 1..end).ok_or_else(|| {
                DataFusionError::Execution(format!("invalid pagination Link header item '{item}'"))
            })?;
            return Ok(Some(resolve_pagination_next_url(
                &base,
                next_raw,
                "next link",
            )?));
        }
    }
    Ok(None)
}

pub(super) fn extract_next_url_header(
    headers: &HeaderMap,
    request_url: &str,
    header_name: Option<&str>,
) -> Result<Option<String>> {
    let Some(header_name) = header_name else {
        return Ok(None);
    };
    let name = HeaderName::try_from(header_name).map_err(|error| {
        DataFusionError::Execution(format!(
            "invalid pagination next URL header '{header_name}': {error}"
        ))
    })?;
    let base = pagination_request_url(request_url)?;
    for header in headers.get_all(name) {
        let Ok(value) = header.to_str() else {
            continue;
        };
        let value = value.trim();
        if !value.is_empty() {
            return Ok(Some(resolve_pagination_next_url(
                &base,
                value,
                "next URL header value",
            )?));
        }
    }
    Ok(None)
}

pub(super) fn extract_response_cursor_header(
    headers: &HeaderMap,
    header_name: Option<&str>,
) -> Result<Option<String>> {
    let Some(header_name) = header_name else {
        return Ok(None);
    };
    let name = HeaderName::try_from(header_name).map_err(|error| {
        DataFusionError::Execution(format!(
            "invalid pagination response cursor header '{header_name}': {error}"
        ))
    })?;
    for header in headers.get_all(name) {
        let Ok(value) = header.to_str() else {
            continue;
        };
        let value = value.trim();
        if !value.is_empty() {
            return Ok(Some(value.to_string()));
        }
    }
    Ok(None)
}

fn pagination_request_url(request_url: &str) -> Result<reqwest::Url> {
    reqwest::Url::parse(request_url).map_err(|e| {
        DataFusionError::Execution(format!(
            "invalid request URL for pagination links '{request_url}': {e}"
        ))
    })
}

fn resolve_pagination_next_url(base: &reqwest::Url, next_raw: &str, label: &str) -> Result<String> {
    let next_url = base.join(next_raw).map_err(|e| {
        DataFusionError::Execution(format!("invalid pagination {label} '{next_raw}': {e}"))
    })?;
    if next_url.origin() != base.origin() {
        return Err(DataFusionError::Execution(format!(
            "pagination {label} must stay on origin {}: {next_raw}",
            base.origin().ascii_serialization()
        )));
    }
    Ok(next_url.to_string())
}

fn link_param_matches(item: &str, name: &str, expected: &str) -> bool {
    item.split(';').skip(1).any(|param| {
        let Some((key, value)) = param.trim().split_once('=') else {
            return false;
        };
        key.trim().eq_ignore_ascii_case(name)
            && value
                .trim()
                .trim_matches('"')
                .eq_ignore_ascii_case(expected)
    })
}

#[cfg(test)]
mod tests {
    use reqwest::header::{HeaderMap, HeaderValue};
    use serde_json::json;

    use super::{
        PageAdvance, PageAdvanceContext, PageState, advance_pagination_state,
        apply_pagination_body_fields, apply_pagination_query_pairs, extract_next_link_url,
        extract_next_url_header, extract_response_cursor_header, page_is_exhausted,
    };
    use crate::backends::http::test_support::test_http_table_spec;
    use coral_spec::{
        BodySpec, HttpMethod, PaginationMode, PaginationSpec, ParsedTemplate, RequestSpec,
        ValidatedPaginationMode, ValueSourceSpec,
    };

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
    fn extract_next_link_url_resolves_query_links_against_request_path() {
        let mut headers = HeaderMap::new();
        headers.insert("link", HeaderValue::from_static("<?page=2>; rel=\"next\""));

        let next = extract_next_link_url(
            &headers,
            "https://api.example.com/v1/resources?page=1",
            false,
        )
        .unwrap();

        assert_eq!(
            next,
            Some("https://api.example.com/v1/resources?page=2".to_string())
        );
    }

    #[test]
    fn extract_next_link_url_checks_all_link_header_values() {
        let mut headers = HeaderMap::new();
        headers.append(
            "link",
            HeaderValue::from_static("</v1/resources?page=1>; rel=\"previous\""),
        );
        headers.append(
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
    fn extract_next_link_url_accepts_token_form_link_params() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "link",
            HeaderValue::from_static("</v1/resources?page=2>; rel=next; results=true"),
        );

        let next = extract_next_link_url(&headers, "https://api.example.com", true).unwrap();

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
    fn extract_next_link_url_rejects_misordered_link_delimiters() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "link",
            HeaderValue::from_static(">/v1/resources?page=2<; rel=\"next\""),
        );

        let err = extract_next_link_url(&headers, "https://api.example.com", false).unwrap_err();

        assert!(
            err.to_string()
                .contains("invalid pagination Link header item")
        );
    }

    #[test]
    fn extract_response_cursor_header_uses_first_non_empty_value() {
        let mut headers = HeaderMap::new();
        headers.append("x-next-cursor", HeaderValue::from_static("   "));
        headers.append("x-next-cursor", HeaderValue::from_static("abc123"));

        let cursor = extract_response_cursor_header(&headers, Some("X-Next-Cursor")).unwrap();

        assert_eq!(cursor.as_deref(), Some("abc123"));
    }

    #[test]
    fn extract_response_cursor_header_rejects_invalid_config_name() {
        let headers = HeaderMap::new();

        let err = extract_response_cursor_header(&headers, Some("not a header")).unwrap_err();

        assert!(
            err.to_string()
                .contains("invalid pagination response cursor header")
        );
    }

    #[test]
    fn extract_next_url_header_resolves_relative_links_on_same_origin() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-next-page-url",
            HeaderValue::from_static("/v1/resources?page=2"),
        );

        let next =
            extract_next_url_header(&headers, "https://api.example.com", Some("X-Next-Page-Url"))
                .unwrap();

        assert_eq!(
            next,
            Some("https://api.example.com/v1/resources?page=2".to_string())
        );
    }

    #[test]
    fn extract_next_url_header_resolves_query_links_against_request_path() {
        let mut headers = HeaderMap::new();
        headers.insert("x-next-page-url", HeaderValue::from_static("?page=2"));

        let next = extract_next_url_header(
            &headers,
            "https://api.example.com/v1/resources?page=1",
            Some("X-Next-Page-Url"),
        )
        .unwrap();

        assert_eq!(
            next,
            Some("https://api.example.com/v1/resources?page=2".to_string())
        );
    }

    #[test]
    fn extract_next_url_header_rejects_cross_origin_absolute_links() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-next-page-url",
            HeaderValue::from_static("https://attacker.example/steal"),
        );

        let err =
            extract_next_url_header(&headers, "https://api.example.com", Some("X-Next-Page-Url"))
                .unwrap_err();

        assert!(
            err.to_string()
                .contains("pagination next URL header value must stay on origin")
        );
    }

    #[test]
    fn advance_cursor_pagination_ignores_unrelated_link_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "link",
            HeaderValue::from_static("<https://attacker.example/steal>; rel=\"next\""),
        );
        let pagination = PaginationSpec {
            mode: PaginationMode::CursorQuery,
            cursor_param: Some("cursor".to_string()),
            response_cursor_path: vec!["meta".to_string(), "next_cursor".to_string()],
            ..PaginationSpec::default()
        }
        .validated("demo", "items")
        .unwrap();
        let mut state = PageState::default();

        let advance = advance_pagination_state(
            &mut state,
            &pagination,
            PageAdvanceContext {
                payload: &json!({"meta": {"next_cursor": "cursor-2"}}),
                response_headers: &headers,
                request_url: "https://api.example.com/items",
                rows_on_page: 1,
                page_size: None,
                source_schema: "demo",
                table_name: "items",
            },
        )
        .unwrap();

        assert_eq!(advance, PageAdvance::Continue);
        assert_eq!(state.cursor.as_deref(), Some("cursor-2"));
    }

    #[test]
    fn apply_pagination_query_pairs_uses_typed_offset_param() {
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

        apply_pagination_query_pairs(&mut params, &pagination, &state, Some(25)).unwrap();

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
    fn apply_pagination_query_pairs_uses_link_header_initial_page_param() {
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
            mode: PaginationMode::LinkHeader,
            page_size: Some(coral_spec::PageSizeSpec {
                default: 25,
                max: 100,
                query_param: Some("per_page".to_string()),
                body_path: vec![],
            }),
            page_param: Some("page".to_string()),
            page_start: 1,
            ..PaginationSpec::default()
        };
        let pagination = table.pagination.validated("demo", "items").unwrap();
        let mut params = Vec::new();
        let state = PageState {
            page: 1,
            ..PageState::default()
        };

        apply_pagination_query_pairs(&mut params, &pagination, &state, Some(25)).unwrap();

        assert_eq!(
            params,
            vec![
                ("per_page".to_string(), "25".to_string()),
                ("page".to_string(), "1".to_string()),
            ]
        );
        assert!(matches!(
            pagination.mode,
            ValidatedPaginationMode::LinkHeader
        ));
    }

    #[test]
    fn apply_pagination_body_fields_rejects_declared_text_body_even_when_absent() {
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
}
