//! Request query and body construction for HTTP-backed sources.

use datafusion::error::{DataFusionError, Result};
use serde_json::{Map, Value};

use crate::backends::shared::template::{RenderContext, resolve_value_source, value_to_string};
use coral_spec::BodySpec;

#[derive(Debug, Clone)]
pub(super) enum RequestBody {
    Json(Value),
    Text(String),
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

    use super::{RequestBody, build_request_body, set_path_value};
    use crate::backends::shared::template::RenderContext;
    use coral_spec::{
        BodyFieldSpec, BodySpec, HttpMethod, ParsedTemplate, RequestSpec, ValueSourceSpec,
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
}
