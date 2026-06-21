use serde_json::Value;

use crate::v4::ir::IrScalarType;

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

pub(crate) fn json_schema_nullable(schema: &Value) -> bool {
    schema
        .get("nullable")
        .and_then(Value::as_bool)
        .unwrap_or(false)
        || schema_type_values(schema).contains(&"null")
}

pub(crate) fn json_schema_is_object_like(schema: &Value) -> bool {
    json_schema_type_contains(schema, "object") || schema.get("type").is_none()
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
    fn nullable_detects_openapi_30_nullable_keyword() {
        assert!(json_schema_nullable(
            &json!({"type": "string", "nullable": true})
        ));
    }

    #[test]
    fn nullable_detects_openapi_31_type_arrays() {
        assert!(json_schema_nullable(&json!({"type": ["string", "null"]})));
    }

    #[test]
    fn nullable_rejects_non_nullable_schemas() {
        assert!(!json_schema_nullable(&json!({"type": "string"})));
    }

    #[test]
    fn object_like_treats_missing_type_as_object() {
        assert!(json_schema_is_object_like(&json!({
            "properties": {"id": {"type": "string"}}
        })));
    }

    #[test]
    fn object_like_accepts_openapi_31_object_type_arrays() {
        assert!(json_schema_is_object_like(&json!({
            "type": ["object", "null"]
        })));
    }
}
