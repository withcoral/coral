use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::v4::ir::IrScalarType;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RefError<'a> {
    External(&'a str),
    NotFound(&'a str),
}

#[derive(Debug, Default)]
pub(crate) struct JsonObjectShape {
    pub(crate) properties: BTreeMap<String, Value>,
    pub(crate) required: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct JsonSchemaPropertyConflict {
    pub(crate) property: String,
}

pub(crate) fn resolve_local_ref<'a>(
    root: &'a Value,
    schema: &'a Value,
) -> Result<&'a Value, RefError<'a>> {
    let Some(reference) = schema.get("$ref").and_then(Value::as_str) else {
        return Ok(schema);
    };
    if !reference.starts_with("#/") {
        return Err(RefError::External(reference));
    }
    let pointer = reference.strip_prefix('#').unwrap_or(reference);
    root.pointer(pointer).ok_or(RefError::NotFound(reference))
}

pub(crate) fn direct_json_object_shape(schema: &Value) -> JsonObjectShape {
    let Some(schema) = schema.as_object() else {
        return JsonObjectShape::default();
    };
    let properties = schema
        .get("properties")
        .and_then(Value::as_object)
        .map(|properties| {
            properties
                .iter()
                .map(|(name, property)| (name.clone(), property.clone()))
                .collect()
        })
        .unwrap_or_default();
    JsonObjectShape {
        properties,
        required: json_schema_required_fields(schema),
    }
}

pub(crate) fn json_schema_required_fields(
    schema: &serde_json::Map<String, Value>,
) -> BTreeSet<String> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect()
}

pub(crate) fn json_schema_default_to_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

pub(crate) fn merge_json_schema_properties_exact(
    target: &mut BTreeMap<String, Value>,
    source: BTreeMap<String, Value>,
) -> Result<(), JsonSchemaPropertyConflict> {
    for (name, property) in source {
        if let Some(existing) = target.get(&name) {
            if existing != &property {
                return Err(JsonSchemaPropertyConflict { property: name });
            }
        } else {
            target.insert(name, property);
        }
    }
    Ok(())
}

pub(crate) fn merge_json_object_shape_annotation_insensitive(
    target: &mut JsonObjectShape,
    source: JsonObjectShape,
) -> Result<(), JsonSchemaPropertyConflict> {
    for (name, property) in source.properties {
        if let Some(existing) = target.properties.get_mut(&name) {
            if json_schema_property_schemas_conflict(existing, &property) {
                return Err(JsonSchemaPropertyConflict { property: name });
            }
            merge_json_schema_property_metadata(existing, &property);
        } else {
            target.properties.insert(name, property);
        }
    }
    target.required.extend(source.required);
    Ok(())
}

pub(crate) fn json_schema_scalar_type(schema: &Value) -> Option<IrScalarType> {
    json_schema_scalar_type_with_default(schema, None)
}

pub(crate) fn json_schema_scalar_type_or_string(schema: &Value) -> Option<IrScalarType> {
    json_schema_scalar_type_with_default(schema, Some("string"))
}

pub(crate) fn json_schema_type_contains(schema: &Value, expected: &str) -> bool {
    match schema.get("type") {
        Some(Value::String(value)) => value == expected,
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(Value::as_str)
            .any(|value| value == expected),
        _ => false,
    }
}

pub(crate) fn json_schema_type_display(schema: &Value) -> String {
    match schema.get("type") {
        Some(Value::String(value)) => value.clone(),
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join("|"),
        _ => "unknown".to_string(),
    }
}

fn json_schema_scalar_type_with_default(
    schema: &Value,
    missing_type_default: Option<&str>,
) -> Option<IrScalarType> {
    let schema_types = schema_type_values(schema);
    if schema_types.is_empty() {
        if let Some(scalar) = scalar_for_typeless_schema_format(schema) {
            return Some(scalar);
        }
        return missing_type_default
            .and_then(scalar_for_schema_type)
            .map(|scalar| apply_string_format(schema, scalar));
    }

    let mut scalar = None;
    for schema_type in schema_types {
        if schema_type == "null" {
            continue;
        }
        let candidate = scalar_for_schema_type(schema_type)?;
        if scalar.is_some_and(|existing| existing != candidate) {
            return None;
        }
        scalar = Some(candidate);
    }
    scalar.map(|scalar| apply_string_format(schema, scalar))
}

fn schema_type_values(schema: &Value) -> Vec<&str> {
    match schema.get("type") {
        Some(Value::String(value)) => vec![value.as_str()],
        Some(Value::Array(values)) => values.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    }
}

fn scalar_for_schema_type(schema_type: &str) -> Option<IrScalarType> {
    match schema_type {
        "string" => Some(IrScalarType::String),
        "integer" => Some(IrScalarType::Integer),
        "number" => Some(IrScalarType::Number),
        "boolean" => Some(IrScalarType::Boolean),
        _ => None,
    }
}

fn apply_string_format(schema: &Value, scalar: IrScalarType) -> IrScalarType {
    if scalar == IrScalarType::String
        && schema
            .get("format")
            .and_then(Value::as_str)
            .is_some_and(|format| matches!(format, "date-time" | "datetime"))
    {
        IrScalarType::Timestamp
    } else {
        scalar
    }
}

fn scalar_for_typeless_schema_format(schema: &Value) -> Option<IrScalarType> {
    schema
        .get("format")
        .and_then(Value::as_str)
        .and_then(|format| match format {
            "date-time" | "datetime" => Some(IrScalarType::Timestamp),
            _ => None,
        })
}

fn json_schema_property_schemas_conflict(existing: &Value, candidate: &Value) -> bool {
    schema_without_annotation_metadata(existing) != schema_without_annotation_metadata(candidate)
}

fn schema_without_annotation_metadata(schema: &Value) -> Value {
    schema_without_annotation_metadata_at_key(None, schema)
}

fn schema_without_annotation_metadata_at_key(key: Option<&str>, schema: &Value) -> Value {
    const ANNOTATION_KEYS: &[&str] = &["$comment", "description", "examples", "title"];
    match schema {
        Value::Object(object) => {
            let is_schema_name_map = matches!(
                key,
                Some("$defs" | "definitions" | "patternProperties" | "properties")
            );
            Value::Object(
                object
                    .iter()
                    .filter(|(key, _value)| {
                        is_schema_name_map || !ANNOTATION_KEYS.contains(&key.as_str())
                    })
                    .map(|(key, value)| {
                        (
                            key.clone(),
                            schema_without_annotation_metadata_at_key(Some(key), value),
                        )
                    })
                    .collect(),
            )
        }
        Value::Array(values) => {
            let mut values = values
                .iter()
                .map(|value| schema_without_annotation_metadata_at_key(None, value))
                .collect::<Vec<_>>();
            if key == Some("type") {
                values.sort_by_key(Value::to_string);
            }
            Value::Array(values)
        }
        other => other.clone(),
    }
}

fn merge_json_schema_property_metadata(existing: &mut Value, candidate: &Value) {
    const ANNOTATION_KEYS: &[&str] = &["$comment", "description", "examples", "title"];
    let (Some(existing), Some(candidate)) = (existing.as_object_mut(), candidate.as_object())
    else {
        return;
    };
    for key in ANNOTATION_KEYS {
        if !existing.contains_key(*key)
            && let Some(value) = candidate.get(*key)
        {
            existing.insert((*key).to_string(), value.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn scalar_type_accepts_nullable_type_arrays() {
        assert_eq!(
            json_schema_scalar_type(&json!({"type": ["integer", "null"]})),
            Some(IrScalarType::Integer)
        );
    }

    #[test]
    fn scalar_type_rejects_ambiguous_scalar_type_arrays() {
        assert_eq!(
            json_schema_scalar_type(&json!({"type": ["integer", "string"]})),
            None
        );
    }

    #[test]
    fn scalar_type_maps_string_datetime_formats_to_timestamp() {
        assert_eq!(
            json_schema_scalar_type(&json!({"type": "string", "format": "datetime"})),
            Some(IrScalarType::Timestamp)
        );
    }

    #[test]
    fn scalar_type_maps_typeless_datetime_formats_to_timestamp() {
        assert_eq!(
            json_schema_scalar_type(&json!({"format": "date-time"})),
            Some(IrScalarType::Timestamp)
        );
    }

    #[test]
    fn resolve_local_ref_returns_input_without_ref() {
        let root = json!({"$defs": {}});
        let schema = json!({"type": "string"});

        let resolved = resolve_local_ref(&root, &schema).expect("schema");

        assert!(std::ptr::eq(
            std::ptr::from_ref(resolved),
            std::ptr::from_ref(&schema)
        ));
    }

    #[test]
    fn resolve_local_ref_returns_local_pointer_target() {
        let root = json!({
            "$defs": {
                "Name": {"type": "string"}
            }
        });
        let schema = json!({"$ref": "#/$defs/Name"});

        let resolved = resolve_local_ref(&root, &schema).expect("schema");

        assert_eq!(resolved, &json!({"type": "string"}));
    }

    #[test]
    fn resolve_local_ref_rejects_external_refs() {
        let root = json!({});
        let schema = json!({"$ref": "https://example.com/schema.json#/Name"});

        assert_eq!(
            resolve_local_ref(&root, &schema),
            Err(RefError::External("https://example.com/schema.json#/Name"))
        );
    }

    #[test]
    fn resolve_local_ref_reports_missing_refs() {
        let root = json!({});
        let schema = json!({"$ref": "#/$defs/Missing"});

        assert_eq!(
            resolve_local_ref(&root, &schema),
            Err(RefError::NotFound("#/$defs/Missing"))
        );
    }

    #[test]
    fn exact_property_merge_reports_conflict() {
        let mut target = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "query": {"type": "string"}
            }
        }))
        .properties;
        let source = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "query": {"type": "integer"}
            }
        }))
        .properties;

        assert_eq!(
            merge_json_schema_properties_exact(&mut target, source),
            Err(JsonSchemaPropertyConflict {
                property: "query".to_string()
            })
        );
    }

    #[test]
    fn annotation_insensitive_object_shape_merge_keeps_metadata() {
        let mut target = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "query": {
                    "type": ["string", "null"],
                    "title": "Query"
                }
            }
        }));
        let source = direct_json_object_shape(&json!({
            "type": "object",
            "required": ["query"],
            "properties": {
                "query": {
                    "type": ["null", "string"],
                    "description": "Search query"
                }
            }
        }));

        merge_json_object_shape_annotation_insensitive(&mut target, source).expect("merge");

        let query = target.properties.get("query").expect("query property");
        assert_eq!(query.get("title").and_then(Value::as_str), Some("Query"));
        assert_eq!(
            query.get("description").and_then(Value::as_str),
            Some("Search query")
        );
        assert!(target.required.contains("query"));
    }

    #[test]
    fn default_to_string_preserves_string_values_and_serializes_other_json() {
        assert_eq!(json_schema_default_to_string(&json!("text")), "text");
        assert_eq!(json_schema_default_to_string(&json!(30)), "30");
        assert_eq!(json_schema_default_to_string(&json!(true)), "true");
        assert_eq!(
            json_schema_default_to_string(&json!({"enabled": true})),
            r#"{"enabled":true}"#
        );
    }
}
