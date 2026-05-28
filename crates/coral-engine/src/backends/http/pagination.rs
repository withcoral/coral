//! Pagination request mutation and response-link handling.

use std::collections::HashMap;

use datafusion::error::{DataFusionError, Result};
use reqwest::header::HeaderMap;
use serde_json::{Map, Value, json};

use crate::backends::http::request::{RequestBody, set_path_value};
use crate::backends::http::target::HttpFetchTarget;
use coral_spec::{BodySpec, PageSizeSpec, ValidatedPagination, ValidatedPaginationMode};

#[derive(Debug, Clone, Default)]
pub(super) struct PageState {
    pub(super) cursor: Option<String>,
    pub(super) page: i64,
    pub(super) offset: i64,
    pub(super) next_url: Option<String>,
}

pub(super) fn apply_pagination_query_pairs(
    params: &mut Vec<(String, String)>,
    target: &HttpFetchTarget,
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
                let name = target.pagination().cursor_param.clone().ok_or_else(|| {
                    DataFusionError::Execution(
                        "cursor_query pagination requires cursor_param".to_string(),
                    )
                })?;
                params.push((name, cursor.clone()));
            }
        }
        ValidatedPaginationMode::Page => {
            let name = target.pagination().page_param.clone().ok_or_else(|| {
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
    target: &HttpFetchTarget,
    pagination: &ValidatedPagination,
    state: &PageState,
    page_size: Option<usize>,
) -> Result<()> {
    let needs_page_size_body = page_size
        .zip(pagination.page_size.as_ref())
        .is_some_and(|(_, spec)| !spec.body_path.is_empty());
    let needs_cursor_body = matches!(pagination.mode, ValidatedPaginationMode::CursorBody)
        && !target.pagination().cursor_body_path.is_empty()
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
        if target.pagination().cursor_body_path.is_empty() {
            return Err(DataFusionError::Execution(
                "cursor_body pagination requires cursor_body_path".to_string(),
            ));
        }
        set_path_value(root, &target.pagination().cursor_body_path, json!(cursor))?;
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

pub(super) fn extract_next_link_url(
    headers: &HeaderMap,
    base_url: &str,
    require_results_true: bool,
) -> Result<Option<String>> {
    let base = reqwest::Url::parse(base_url).map_err(|e| {
        DataFusionError::Execution(format!(
            "invalid base URL for pagination links '{base_url}': {e}"
        ))
    })?;

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
            let next_url = base.join(next_raw).map_err(|e| {
                DataFusionError::Execution(format!(
                    "invalid pagination next link '{next_raw}': {e}"
                ))
            })?;
            if next_url.origin() != base.origin() {
                return Err(DataFusionError::Execution(format!(
                    "pagination next link must stay on origin {}: {next_raw}",
                    base.origin().ascii_serialization()
                )));
            }
            return Ok(Some(next_url.to_string()));
        }
    }
    Ok(None)
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
        PageState, apply_pagination_body_fields, apply_pagination_query_pairs,
        extract_next_link_url, page_is_exhausted,
    };
    use crate::backends::http::test_support::{test_http_request_target, test_http_table_spec};
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

        let target = test_http_request_target(&table);
        apply_pagination_query_pairs(&mut params, &target, &pagination, &state, Some(25)).unwrap();

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
        let target = test_http_request_target(&table);

        let error = apply_pagination_body_fields(
            &mut body,
            &body_spec,
            &target,
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
