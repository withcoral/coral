use serde_json::{Map, Value};

use super::super::identifiers::{entity_name_from_path, entity_name_from_ref};
use super::super::ir::OutputCardinality;

pub(super) fn select_json_response(
    responses: Option<&Map<String, Value>>,
) -> Option<(u16, String, &Value)> {
    let responses = responses?;
    let mut candidates = Vec::new();
    for (status, response) in responses {
        let Ok(status_code) = status.parse::<u16>() else {
            continue;
        };
        if !(200..300).contains(&status_code) {
            continue;
        }
        let Some(content) = response.get("content").and_then(Value::as_object) else {
            continue;
        };
        let Some(json) = content.get("application/json") else {
            continue;
        };
        let schema = json.get("schema").unwrap_or(&Value::Null);
        candidates.push((status_code, "application/json".to_string(), schema));
    }
    candidates
        .iter()
        .position(|(status, _, _)| *status == 200)
        .and_then(|index| candidates.get(index).cloned())
        .or_else(|| candidates.into_iter().min_by_key(|(status, _, _)| *status))
}

pub(super) fn classify_response_schema(
    path: &str,
    schema: &Value,
) -> (OutputCardinality, Vec<String>, Value, Option<String>) {
    if schema == &Value::Null {
        return (OutputCardinality::None, Vec::new(), Value::Null, None);
    }
    if schema.get("type").and_then(Value::as_str) == Some("array") {
        let item = schema.get("items").cloned().unwrap_or(Value::Null);
        return (
            OutputCardinality::List,
            Vec::new(),
            item.clone(),
            item.get("$ref")
                .and_then(Value::as_str)
                .map(entity_name_from_ref),
        );
    }
    if schema
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("object")
        == "object"
    {
        if let Some((property_name, items)) = schema
            .get("properties")
            .and_then(Value::as_object)
            .and_then(wrapped_list_property)
        {
            let item = items.get("items").cloned().unwrap_or(Value::Null);
            return (
                OutputCardinality::WrappedList,
                vec![property_name.to_string()],
                item.clone(),
                item.get("$ref")
                    .and_then(Value::as_str)
                    .map(entity_name_from_ref),
            );
        }
        return (
            OutputCardinality::Singleton,
            Vec::new(),
            schema.clone(),
            schema
                .get("$ref")
                .and_then(Value::as_str)
                .map(entity_name_from_ref)
                .or_else(|| Some(entity_name_from_path(path))),
        );
    }
    (OutputCardinality::Unknown, Vec::new(), schema.clone(), None)
}

fn wrapped_list_property(properties: &Map<String, Value>) -> Option<(&str, &Value)> {
    ["items", "data", "results", "rows"]
        .iter()
        .find_map(|name| {
            properties
                .get(*name)
                .filter(|property| property.get("type").and_then(Value::as_str) == Some("array"))
                .map(|property| (*name, property))
        })
        .or_else(|| single_array_payload_property(properties))
}

fn single_array_payload_property(properties: &Map<String, Value>) -> Option<(&str, &Value)> {
    let array_properties = properties
        .iter()
        .filter(|(_, property)| property.get("type").and_then(Value::as_str) == Some("array"))
        .filter(|(name, _)| !is_wrapper_metadata_property(name))
        .collect::<Vec<_>>();
    match array_properties.as_slice() {
        [(name, property)] => Some((name.as_str(), *property)),
        [] | [_, _, ..] => None,
    }
}

fn is_wrapper_metadata_property(name: &str) -> bool {
    matches!(
        name,
        "total_count" | "incomplete_results" | "has_more" | "next" | "previous"
    )
}
