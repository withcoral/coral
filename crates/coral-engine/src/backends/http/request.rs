//! Request query and body construction for HTTP-backed sources.

use std::fmt;

use datafusion::error::{DataFusionError, Result};
use serde_json::{Map, Value, json};

use sha2::{Digest, Sha256};
use crate::backends::http::transport::build_logged_url;
use crate::backends::http::url::{join_url, normalize_base_url};
use crate::backends::shared::json_exec::JsonExecExplain;
use crate::backends::shared::template::{RenderContext, resolve_value_source, value_to_string};
use coral_spec::{BodySpec, HttpMethod, ParsedTemplate, RequestSpec};

#[derive(Debug, Clone)]
pub(super) enum RequestBody {
    Json(Value),
    Text(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct ResolvedHttpRequest {
    resolved_request: String,
    fingerprint: String,
}

impl ResolvedHttpRequest {
    #[must_use]
    pub(super) fn new(resolved_request: String, fingerprint: String) -> Self {
        Self {
            resolved_request,
            fingerprint,
        }
    }

    #[must_use]
    pub(super) fn resolved_request(&self) -> &str {
        &self.resolved_request
    }

    #[must_use]
    pub(super) fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    #[must_use]
    pub(super) fn into_json_exec_explain(self) -> JsonExecExplain {
        JsonExecExplain::new(self.resolved_request, self.fingerprint)
    }
}

impl fmt::Display for ResolvedHttpRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "resolved_request={} fingerprint={}", self.resolved_request, self.fingerprint)
    }
}

pub(super) fn build_request_explain(
    base_url: &ParsedTemplate,
    request: &RequestSpec,
    render_context: &RenderContext<'_>,
    limit: Option<usize>,
    projection: Option<&[usize]>,
) -> Result<ResolvedHttpRequest> {
    let base_url = normalize_base_url(&crate::backends::shared::template::render_template(
        base_url,
        render_context,
    )?);
    let rendered_path = crate::backends::shared::template::render_template(
        &request.path,
        render_context,
    )?;
    let url = join_url(&base_url, &rendered_path)?;
    let query_pairs = build_query_pairs(request, render_context)?;
    let logged_url = build_logged_url(&url, &query_pairs);
    let projection = projection.map(|indices| indices.to_vec());
    let resolved_request = serde_json::to_string(&json!({
        "request": request,
        "call": {
            "url": logged_url,
            "query": query_pairs
                .iter()
                .map(|(name, value)| json!({"name": name, "value": value}))
                .collect::<Vec<_>>(),
            "limit": limit,
            "projection": projection,
        },
    }))
    .map_err(|error| {
        DataFusionError::Execution(format!("failed to serialize HTTP request explain payload: {error}"))
    })?;
    let fingerprint_payload = json!({
        "url": logged_url,
        "query": query_pairs
            .iter()
            .map(|(name, value)| json!({"name": name, "value": value}))
            .collect::<Vec<_>>(),
        "limit": limit,
        "projection": projection,
    });
    let fingerprint = request_fingerprint(&fingerprint_payload)?;
    Ok(ResolvedHttpRequest::new(resolved_request, fingerprint))
}

pub(super) fn build_outgoing_request_explain(
    method: HttpMethod,
    url: &str,
    query_pairs: &[(String, String)],
    body: Option<&RequestBody>,
    limit: Option<usize>,
    projection: Option<&[usize]>,
) -> Result<ResolvedHttpRequest> {
    let logged_url = build_logged_url(url, query_pairs);
    let projection = projection.map(|indices| indices.to_vec());
    let resolved_request = serde_json::to_string(&json!({
        "method": http_method_label(method),
        "url": logged_url,
        "query": query_pairs
            .iter()
            .map(|(name, value)| json!({"name": name, "value": value}))
            .collect::<Vec<_>>(),
        "body": body.map(request_body_to_json),
        "limit": limit,
        "projection": projection,
    }))
    .map_err(|error| {
        DataFusionError::Execution(format!("failed to serialize HTTP outgoing request explain payload: {error}"))
    })?;
    let fingerprint_payload = json!({
        "url": logged_url,
        "query": query_pairs
            .iter()
            .map(|(name, value)| json!({"name": name, "value": value}))
            .collect::<Vec<_>>(),
        "limit": limit,
        "projection": projection,
    });
    let fingerprint = request_fingerprint(&fingerprint_payload)?;
    Ok(ResolvedHttpRequest::new(resolved_request, fingerprint))
}

fn request_fingerprint(payload: &Value) -> Result<String> {
    let bytes = serde_json::to_vec(payload).map_err(|error| {
        DataFusionError::Execution(format!("failed to serialize HTTP request fingerprint payload: {error}"))
    })?;
    let digest = Sha256::digest(bytes);
    Ok(format!("{digest:x}"))
}

fn http_method_label(method: HttpMethod) -> &'static str {
    match method {
        HttpMethod::GET => "GET",
        HttpMethod::POST => "POST",
    }
}

fn request_body_to_json(body: &RequestBody) -> Value {
    match body {
        RequestBody::Json(value) => value.clone(),
        RequestBody::Text(text) => Value::String(text.clone()),
    }
}
pub(super) fn build_query_pairs(
    request: &coral_spec::RequestSpec,
    render_context: &RenderContext<'_>,
) -> Result<Vec<(String, String)>> {
    let mut params = Vec::new();

    for param in &request.query {
        let value = resolve_value_source(&param.value, render_context)?;
        if let Some(value) = value {
            params.push((param.name.clone(), value_to_string(&value)));
        }
    }

    Ok(params)
}

pub(super) fn build_request_body(
    request: &coral_spec::RequestSpec,
    render_context: &RenderContext<'_>,
) -> Result<Option<RequestBody>> {
    match &request.body {
        BodySpec::Json { fields } => {
            if fields.is_empty() {
                return Ok(None);
            }
            let mut root = Value::Object(Map::new());
            let mut rendered_any_field = false;
            for field in fields {
                if field
                    .when_arg
                    .as_ref()
                    .is_some_and(|arg| !render_context.args.contains_key(arg))
                {
                    continue;
                }
                if let Some(value) = resolve_value_source(&field.value, render_context)? {
                    rendered_any_field = true;
                    set_path_value(&mut root, &field.path, value)?;
                }
            }
            if rendered_any_field {
                Ok(Some(RequestBody::Json(root)))
            } else {
                Ok(None)
            }
        }
        BodySpec::Text { content } => {
            let Some(value) = resolve_value_source(content, render_context)? else {
                return Ok(None);
            };
            Ok(Some(RequestBody::Text(value_to_string(&value))))
        }
    }
}

pub(super) fn set_path_value(root: &mut Value, path: &[String], value: Value) -> Result<()> {
    if path.is_empty() {
        *root = value;
        return Ok(());
    }

    set_path_value_at(root, path, value)
}

fn set_path_value_at(cursor: &mut Value, path: &[String], value: Value) -> Result<()> {
    let Some((head, tail)) = path.split_first() else {
        *cursor = value;
        return Ok(());
    };

    if let Ok(index) = head.parse::<usize>() {
        if !cursor.is_array() {
            *cursor = Value::Array(Vec::new());
        }
        let array = cursor.as_array_mut().ok_or_else(|| {
            DataFusionError::Execution("failed to create JSON array path".to_string())
        })?;
        if array.len() <= index {
            const MAX_JSON_ARRAY_INDEX: usize = 10_000;
            if index > MAX_JSON_ARRAY_INDEX {
                return Err(DataFusionError::Execution(format!(
                    "JSON array index {index} exceeds supported maximum {MAX_JSON_ARRAY_INDEX}"
                )));
            }
            array.resize_with(index + 1, || Value::Null);
        }
        let next = array.get_mut(index).ok_or_else(|| {
            DataFusionError::Execution("failed to access JSON array path".to_string())
        })?;
        return set_path_value_at(next, tail, value);
    }

    if !cursor.is_object() {
        *cursor = Value::Object(Map::new());
    }
    let obj = cursor.as_object_mut().ok_or_else(|| {
        DataFusionError::Execution("failed to create JSON object path".to_string())
    })?;
    let next = obj.entry(head.clone()).or_insert(Value::Null);
    set_path_value_at(next, tail, value)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};

    use serde_json::json;

    use super::{
        build_outgoing_request_explain, build_request_body, build_request_explain,
        set_path_value, RequestBody,
    };
    use crate::backends::shared::template::RenderContext;
    use coral_spec::{
        BodyFieldSpec, BodySpec, HttpMethod, ParsedTemplate, QueryParamSpec, RequestSpec,
        ValueSourceSpec,
    };

    #[test]
    fn build_request_body_omits_json_body_when_no_fields_resolve() {
        let request = RequestSpec {
            method: HttpMethod::POST,
            path: ParsedTemplate::parse("/items").expect("template"),
            query: vec![],
            body: BodySpec::Json {
                fields: vec![BodyFieldSpec {
                    path: vec!["optional".to_string()],
                    when_arg: None,
                    value: ValueSourceSpec::Filter {
                        key: "optional".to_string(),
                        default: None,
                    },
                }],
            },
            headers: vec![],
        };
        let filters = HashMap::new();
        let args = HashMap::new();
        let state = HashMap::new();
        let resolved_inputs = BTreeMap::new();
        let context = RenderContext::new(&filters, &args, &state, &resolved_inputs);

        let body = build_request_body(&request, &context).expect("request body should render");

        assert!(body.is_none());
    }

    #[test]
    fn build_request_body_keeps_json_body_when_one_field_resolves() {
        let request = RequestSpec {
            method: HttpMethod::POST,
            path: ParsedTemplate::parse("/items").expect("template"),
            query: vec![],
            body: BodySpec::Json {
                fields: vec![BodyFieldSpec {
                    path: vec!["required".to_string()],
                    when_arg: None,
                    value: ValueSourceSpec::Filter {
                        key: "required".to_string(),
                        default: None,
                    },
                }],
            },
            headers: vec![],
        };
        let filters = HashMap::from([("required".to_string(), "value".to_string())]);
        let args = HashMap::new();
        let state = HashMap::new();
        let resolved_inputs = BTreeMap::new();
        let context = RenderContext::new(&filters, &args, &state, &resolved_inputs);

        let body = build_request_body(&request, &context).expect("request body should render");

        assert!(
            matches!(body, Some(RequestBody::Json(value)) if value == json!({"required": "value"}))
        );
    }

    #[test]
    fn set_path_value_builds_arrays_from_numeric_segments() {
        let mut root = json!({});

        set_path_value(
            &mut root,
            &[
                "Dimensions".to_string(),
                "0".to_string(),
                "Name".to_string(),
            ],
            json!("ClusterName"),
        )
        .expect("path assignment should succeed");
        set_path_value(
            &mut root,
            &[
                "Dimensions".to_string(),
                "0".to_string(),
                "Value".to_string(),
            ],
            json!("titaness"),
        )
        .expect("path assignment should succeed");
        set_path_value(
            &mut root,
            &["Statistics".to_string(), "0".to_string()],
            json!("Average"),
        )
        .expect("path assignment should succeed");

        assert_eq!(
            root,
            json!({
                "Dimensions": [{
                    "Name": "ClusterName",
                    "Value": "titaness"
                }],
                "Statistics": ["Average"]
            })
        );
    }

    #[test]
    fn build_request_explain_is_stable_for_the_same_input() {
        let request = RequestSpec {
            method: HttpMethod::GET,
            path: ParsedTemplate::parse("/items").expect("template"),
            query: vec![QueryParamSpec {
                name: "limit".to_string(),
                value: ValueSourceSpec::Literal {
                    value: json!(10),
                },
            }],
            body: BodySpec::default(),
            headers: vec![],
        };
        let base_url = ParsedTemplate::parse("https://api.example.com").expect("template");
        let resolved_inputs = BTreeMap::new();
        let context = RenderContext::source_scoped(&resolved_inputs);

        let explain_one = build_request_explain(
            &base_url,
            &request,
            &context,
            Some(5),
            Some(&[0, 2]),
        )
        .expect("request explain should build");
        let explain_two = build_request_explain(
            &base_url,
            &request,
            &context,
            Some(5),
            Some(&[0, 2]),
        )
        .expect("request explain should build");
        let explain_different_limit = build_request_explain(
            &base_url,
            &request,
            &context,
            Some(6),
            Some(&[0, 2]),
        )
        .expect("request explain should build");

        assert_eq!(explain_one.resolved_request(), explain_two.resolved_request());
        assert_eq!(explain_one.fingerprint(), explain_two.fingerprint());
        assert_ne!(
            explain_one.fingerprint(),
            explain_different_limit.fingerprint()
        );
        assert!(explain_one.resolved_request().contains("https://api.example.com/items"));
    }

    #[test]
    fn build_outgoing_request_explain_reflects_page_specific_values() {
        let first = build_outgoing_request_explain(
            HttpMethod::GET,
            "https://api.example.com/items?page=1",
            &[("page".to_string(), "1".to_string())],
            Some(&RequestBody::Text("first page".to_string())),
            Some(25),
            Some(&[0, 1]),
        )
        .expect("outgoing request explain should build");
        let second = build_outgoing_request_explain(
            HttpMethod::GET,
            "https://api.example.com/items?page=2",
            &[("page".to_string(), "2".to_string())],
            Some(&RequestBody::Text("second page".to_string())),
            Some(25),
            Some(&[0, 1]),
        )
        .expect("outgoing request explain should build");

        assert_ne!(first.resolved_request(), second.resolved_request());
        assert_ne!(first.fingerprint(), second.fingerprint());
        assert!(first.resolved_request().contains("first page"));
        assert!(second.resolved_request().contains("second page"));
    }
}
