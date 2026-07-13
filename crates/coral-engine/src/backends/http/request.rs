//! Request query and body construction for HTTP-backed sources.

use std::collections::BTreeSet;

use datafusion::error::{DataFusionError, Result};
use serde_json::{Map, Value};

use crate::backends::shared::template::{
    RenderContext, Resolved, resolve_value_source_with_secret_provenance, value_to_string,
};
use coral_spec::BodySpec;

#[derive(Debug, Clone)]
pub(super) enum RequestBody {
    Json(Value),
    Text(String),
}

#[derive(Debug, Clone, Default)]
pub(super) struct RenderedRequestBody {
    pub(super) value: Option<RequestBody>,
    secret_paths: Vec<Vec<String>>,
    text_depends_on_secret: bool,
}

impl RenderedRequestBody {
    pub(super) fn depends_on_secret(&self) -> bool {
        self.text_depends_on_secret || !self.secret_paths.is_empty()
    }

    pub(super) fn overwrite_with_public_path(&mut self, path: &[String]) {
        self.secret_paths
            .retain(|secret_path| !paths_overlap(secret_path, path));
    }
}

fn paths_overlap(left: &[String], right: &[String]) -> bool {
    for (left, right) in left.iter().zip(right) {
        match (left.parse::<usize>(), right.parse::<usize>()) {
            (Ok(left), Ok(right)) if left == right => {}
            (Err(_), Err(_)) if left == right => {}
            (Ok(_), Ok(_)) | (Err(_), Err(_)) => return false,
            (Ok(_), Err(_)) | (Err(_), Ok(_)) => return true,
        }
    }
    true
}

pub(super) fn build_query_pairs(
    request: &coral_spec::RequestSpec,
    render_context: &RenderContext<'_>,
    secret_input_names: &BTreeSet<String>,
) -> Result<Resolved<Vec<(String, String)>>> {
    let mut params = Vec::new();
    let mut depends_on_secret = false;

    for param in &request.query {
        if let Some(resolved) = resolve_value_source_with_secret_provenance(
            &param.value,
            render_context,
            secret_input_names,
        )? {
            depends_on_secret |= resolved.depends_on_secret;
            params.push((param.name.clone(), value_to_string(&resolved.value)));
        }
    }

    Ok(Resolved {
        value: params,
        depends_on_secret,
    })
}

pub(super) fn build_request_body(
    request: &coral_spec::RequestSpec,
    render_context: &RenderContext<'_>,
    secret_input_names: &BTreeSet<String>,
) -> Result<RenderedRequestBody> {
    match &request.body {
        BodySpec::Json { fields } => {
            if fields.is_empty() {
                return Ok(RenderedRequestBody::default());
            }
            let mut root = Value::Object(Map::new());
            let mut rendered_any_field = false;
            let mut secret_paths = Vec::<Vec<String>>::new();
            for field in fields {
                if field
                    .when_arg
                    .as_ref()
                    .is_some_and(|arg| !render_context.args.contains_key(arg))
                {
                    continue;
                }
                if let Some(resolved) = resolve_value_source_with_secret_provenance(
                    &field.value,
                    render_context,
                    secret_input_names,
                )? {
                    rendered_any_field = true;
                    secret_paths.retain(|path| !paths_overlap(path, &field.path));
                    set_path_value(&mut root, &field.path, resolved.value)?;
                    if resolved.depends_on_secret {
                        secret_paths.push(field.path.clone());
                    }
                }
            }
            let value = if rendered_any_field {
                Some(RequestBody::Json(root))
            } else {
                None
            };
            Ok(RenderedRequestBody {
                value,
                secret_paths,
                text_depends_on_secret: false,
            })
        }
        BodySpec::Text { content } => {
            let Some(resolved) = resolve_value_source_with_secret_provenance(
                content,
                render_context,
                secret_input_names,
            )?
            else {
                return Ok(RenderedRequestBody::default());
            };
            Ok(RenderedRequestBody {
                value: Some(RequestBody::Text(value_to_string(&resolved.value))),
                secret_paths: Vec::new(),
                text_depends_on_secret: resolved.depends_on_secret,
            })
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
    use std::collections::{BTreeMap, BTreeSet, HashMap};

    use serde_json::json;

    use super::{RequestBody, build_query_pairs, build_request_body, set_path_value};
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

        let body = build_request_body(&request, &context, &BTreeSet::new())
            .expect("request body should render")
            .value;

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

        let body = build_request_body(&request, &context, &BTreeSet::new())
            .expect("request body should render")
            .value;

        assert!(
            matches!(body, Some(RequestBody::Json(value)) if value == json!({"required": "value"}))
        );
    }

    #[test]
    fn build_request_body_renders_filter_string_array_as_json_array() {
        let request = RequestSpec {
            method: HttpMethod::POST,
            path: ParsedTemplate::parse("/items").expect("template"),
            query: vec![],
            body: BodySpec::Json {
                fields: vec![BodyFieldSpec {
                    path: vec!["logStreamNames".to_string()],
                    when_arg: None,
                    value: ValueSourceSpec::FilterStringArray {
                        key: "log_stream_names".to_string(),
                        default: None,
                    },
                }],
            },
            headers: vec![],
        };
        let filters = HashMap::from([(
            "log_stream_names".to_string(),
            r#"["stream-a","stream-b"]"#.to_string(),
        )]);
        let args = HashMap::new();
        let state = HashMap::new();
        let resolved_inputs = BTreeMap::new();
        let context = RenderContext::new(&filters, &args, &state, &resolved_inputs);

        let body = build_request_body(&request, &context, &BTreeSet::new())
            .expect("request body should render")
            .value;

        assert!(
            matches!(body, Some(RequestBody::Json(value)) if value == json!({
                "logStreamNames": ["stream-a", "stream-b"]
            }))
        );
    }

    #[test]
    fn request_provenance_tracks_only_emitted_secret_values() {
        let request = RequestSpec {
            method: HttpMethod::POST,
            path: ParsedTemplate::parse("/items").expect("template"),
            query: vec![QueryParamSpec {
                name: "token".to_string(),
                value: ValueSourceSpec::Input {
                    key: "SECRET".to_string(),
                },
            }],
            body: BodySpec::Json {
                fields: vec![
                    BodyFieldSpec {
                        path: vec!["secret".to_string()],
                        when_arg: Some("include_secret".to_string()),
                        value: ValueSourceSpec::Input {
                            key: "SECRET".to_string(),
                        },
                    },
                    BodyFieldSpec {
                        path: vec!["public".to_string()],
                        when_arg: None,
                        value: ValueSourceSpec::Input {
                            key: "PUBLIC".to_string(),
                        },
                    },
                ],
            },
            headers: vec![],
        };
        let inputs = BTreeMap::from([
            ("SECRET".to_string(), "hidden".to_string()),
            ("PUBLIC".to_string(), "visible".to_string()),
        ]);
        let secret_names = BTreeSet::from(["SECRET".to_string()]);
        let empty = HashMap::new();
        let context = RenderContext::new(&empty, &empty, &empty, &inputs);

        let query = build_query_pairs(&request, &context, &secret_names).expect("query");
        assert!(query.depends_on_secret);
        let body = build_request_body(&request, &context, &secret_names).expect("body");
        assert!(
            !body.depends_on_secret(),
            "skipped secret field must not taint"
        );

        let args = HashMap::from([("include_secret".to_string(), "true".to_string())]);
        let context = RenderContext::new(&empty, &args, &empty, &inputs);
        let body = build_request_body(&request, &context, &secret_names).expect("body");
        assert!(body.depends_on_secret());
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
