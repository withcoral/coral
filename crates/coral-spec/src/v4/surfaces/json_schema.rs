use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::v4::ir::IrScalarType;

const ANNOTATION_KEYS: &[&str] = &["$comment", "default", "description", "examples", "title"];
const WRAPPED_LIST_MAX_DEPTH: usize = 8;
const WRAPPED_LIST_PREFERRED_PROPERTIES: &[&str] = &["items", "data", "results", "rows"];
const WRAPPED_LIST_METADATA_PROPERTIES: &[&str] = &[
    "total_count",
    "incomplete_results",
    "has_more",
    "next",
    "previous",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RefError<'a> {
    External(&'a str),
    NotFound(&'a str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JsonSchemaWalkError<'a> {
    ExternalRef(&'a str),
    RefCycle(&'a str),
    RefNotFound(&'a str),
    DepthExceeded,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum JsonSchemaComparisonError {
    PropertyConflict(String),
    DepthExceeded,
}

#[derive(Debug, Default)]
pub(crate) struct JsonObjectShape {
    pub(crate) properties: BTreeMap<String, Value>,
    pub(crate) required: BTreeSet<String>,
}

/// Inputs available to wrapped-list inference.
///
/// The operation name is deliberately part of the context even though the
/// initial heuristic is schema-only. Future inference policy can add naming,
/// pagination, and other signals without changing every surface importer.
#[derive(Clone, Copy)]
pub(crate) struct WrappedListInferenceContext<'a> {
    pub(crate) operation_name: &'a str,
    pub(crate) schema_root: &'a Value,
    pub(crate) response_schema: &'a Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WrappedListInference {
    pub(crate) row_path: Vec<String>,
}

pub(crate) fn infer_wrapped_list(
    context: WrappedListInferenceContext<'_>,
) -> Option<WrappedListInference> {
    let _ = context.operation_name;
    infer_wrapped_list_path(
        context.schema_root,
        context.response_schema,
        &mut BTreeSet::new(),
        0,
    )
    .ok()
    .flatten()
    .map(|row_path| WrappedListInference { row_path })
}

fn infer_wrapped_list_path<'a>(
    root: &'a Value,
    schema: &'a Value,
    resolving_refs: &mut BTreeSet<String>,
    depth: usize,
) -> Result<Option<Vec<String>>, JsonSchemaWalkError<'a>> {
    with_resolved_json_schema(
        root,
        schema,
        resolving_refs,
        depth,
        WRAPPED_LIST_MAX_DEPTH,
        |resolved, resolving_refs, next_depth| {
            if schema_uses_composition(resolved) {
                return Ok(None);
            }
            if !json_schema_type_contains(resolved, "object") {
                return Ok(None);
            }
            let Some(properties) = resolved.get("properties").and_then(Value::as_object) else {
                return Ok(None);
            };

            // Preserve the legacy preference order for direct payload arrays.
            for name in WRAPPED_LIST_PREFERRED_PROPERTIES {
                if let Some(property) = properties.get(*name)
                    && resolved_schema_has_type(
                        root,
                        property,
                        resolving_refs,
                        next_depth,
                        "array",
                    )?
                {
                    return Ok(Some(vec![(*name).to_string()]));
                }
            }

            // Recurse only through preferred wrapper names. This adds nested
            // envelopes such as `results.data` without turning the heuristic
            // into an unrestricted walk of every resource child.
            for name in WRAPPED_LIST_PREFERRED_PROPERTIES {
                if let Some(property) = properties.get(*name)
                    && let Some(mut path) =
                        infer_wrapped_list_path(root, property, resolving_refs, next_depth)?
                {
                    path.insert(0, (*name).to_string());
                    return Ok(Some(path));
                }
            }

            let mut arrays = Vec::new();
            for (name, property) in properties {
                if WRAPPED_LIST_METADATA_PROPERTIES.contains(&name.as_str()) {
                    continue;
                }
                if resolved_schema_has_type(root, property, resolving_refs, next_depth, "array")? {
                    arrays.push(name);
                }
            }
            match arrays.as_slice() {
                [name] => Ok(Some(vec![(*name).clone()])),
                [] | [_, _, ..] => Ok(None),
            }
        },
    )
}

fn schema_uses_composition(schema: &Value) -> bool {
    ["allOf", "anyOf", "oneOf", "not"]
        .iter()
        .any(|keyword| schema.get(*keyword).is_some())
}

fn resolved_schema_has_type<'a>(
    root: &'a Value,
    schema: &'a Value,
    resolving_refs: &mut BTreeSet<String>,
    depth: usize,
    expected: &str,
) -> Result<bool, JsonSchemaWalkError<'a>> {
    with_resolved_json_schema(
        root,
        schema,
        resolving_refs,
        depth,
        WRAPPED_LIST_MAX_DEPTH,
        |resolved, _, _| {
            Ok(!schema_uses_composition(resolved) && json_schema_type_contains(resolved, expected))
        },
    )
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

pub(crate) fn with_resolved_json_schema<'a, T>(
    root: &'a Value,
    schema: &'a Value,
    resolving_refs: &mut BTreeSet<String>,
    depth: usize,
    max_depth: usize,
    visit: impl FnOnce(&'a Value, &mut BTreeSet<String>, usize) -> Result<T, JsonSchemaWalkError<'a>>,
) -> Result<T, JsonSchemaWalkError<'a>> {
    if depth > max_depth {
        return Err(JsonSchemaWalkError::DepthExceeded);
    }

    let reference = schema.get("$ref").and_then(Value::as_str);
    let guarded_reference = match reference {
        Some(reference) if reference.starts_with("#/") => {
            if !resolving_refs.insert(reference.to_string()) {
                return Err(JsonSchemaWalkError::RefCycle(reference));
            }
            Some(reference)
        }
        _ => None,
    };

    let resolved = resolve_local_ref(root, schema).map_err(json_schema_walk_error_from_ref);
    let next_depth = depth + 1;
    let result = match resolved {
        Ok(resolved) if resolved.get("$ref").is_some() => {
            with_resolved_json_schema(root, resolved, resolving_refs, next_depth, max_depth, visit)
        }
        Ok(resolved) => visit(resolved, resolving_refs, next_depth),
        Err(error) => Err(error),
    };

    if let Some(reference) = guarded_reference {
        resolving_refs.remove(reference);
    }

    result
}

pub(crate) fn resolve_json_schema_ref_with_siblings<'a>(
    root: &'a Value,
    schema: &'a Value,
    resolving_refs: &mut BTreeSet<String>,
    depth: usize,
    max_depth: usize,
) -> Result<Value, JsonSchemaWalkError<'a>> {
    with_resolved_json_schema(
        root,
        schema,
        resolving_refs,
        depth,
        max_depth,
        |resolved, resolving_refs, next_depth| {
            let mut resolved = resolve_json_schema_child_refs_allow_cycles(
                root,
                resolved,
                resolving_refs,
                next_depth,
                max_depth,
            )?;
            if let (Some(referrer), Some(resolved)) = (schema.as_object(), resolved.as_object_mut())
            {
                for (key, value) in referrer {
                    if is_ref_site_metadata_key(key) {
                        resolved.insert(key.clone(), value.clone());
                    }
                }
            }
            Ok(resolved)
        },
    )
}

fn is_ref_site_metadata_key(key: &str) -> bool {
    ANNOTATION_KEYS.contains(&key)
}

fn resolve_json_schema_refs_allow_cycles<'a>(
    root: &'a Value,
    schema: &'a Value,
    resolving_refs: &mut BTreeSet<String>,
    depth: usize,
    max_depth: usize,
) -> Result<Value, JsonSchemaWalkError<'a>> {
    match with_resolved_json_schema(
        root,
        schema,
        resolving_refs,
        depth,
        max_depth,
        |resolved, resolving_refs, next_depth| {
            resolve_json_schema_child_refs_allow_cycles(
                root,
                resolved,
                resolving_refs,
                next_depth,
                max_depth,
            )
        },
    ) {
        Ok(resolved) => Ok(resolved),
        Err(JsonSchemaWalkError::RefCycle(_reference)) => Ok(schema.clone()),
        Err(error) => Err(error),
    }
}

fn resolve_json_schema_child_refs_allow_cycles<'a>(
    root: &'a Value,
    schema: &'a Value,
    resolving_refs: &mut BTreeSet<String>,
    depth: usize,
    max_depth: usize,
) -> Result<Value, JsonSchemaWalkError<'a>> {
    let Some(object) = schema.as_object() else {
        return Ok(schema.clone());
    };

    let mut resolved = object.clone();
    for key in ["items", "additionalProperties", "not"] {
        if let Some(value) = object.get(key).filter(|value| value.is_object()) {
            resolved.insert(
                key.to_string(),
                resolve_json_schema_refs_allow_cycles(
                    root,
                    value,
                    resolving_refs,
                    depth,
                    max_depth,
                )?,
            );
        }
    }
    for key in ["allOf", "anyOf", "oneOf"] {
        if let Some(values) = object.get(key).and_then(Value::as_array) {
            resolved.insert(
                key.to_string(),
                Value::Array(
                    values
                        .iter()
                        .map(|value| {
                            resolve_json_schema_refs_allow_cycles(
                                root,
                                value,
                                resolving_refs,
                                depth,
                                max_depth,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                ),
            );
        }
    }
    for key in ["$defs", "definitions", "patternProperties", "properties"] {
        if let Some(schemas) = object.get(key).and_then(Value::as_object) {
            resolved.insert(
                key.to_string(),
                Value::Object(
                    schemas
                        .iter()
                        .map(|(name, schema)| {
                            resolve_json_schema_refs_allow_cycles(
                                root,
                                schema,
                                resolving_refs,
                                depth,
                                max_depth,
                            )
                            .map(|schema| (name.clone(), schema))
                        })
                        .collect::<Result<serde_json::Map<_, _>, _>>()?,
                ),
            );
        }
    }
    Ok(Value::Object(resolved))
}

fn json_schema_walk_error_from_ref(error: RefError<'_>) -> JsonSchemaWalkError<'_> {
    match error {
        RefError::External(reference) => JsonSchemaWalkError::ExternalRef(reference),
        RefError::NotFound(reference) => JsonSchemaWalkError::RefNotFound(reference),
    }
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
) -> Result<(), JsonSchemaComparisonError> {
    for (name, property) in source {
        if let Some(existing) = target.get(&name) {
            if existing != &property {
                return Err(JsonSchemaComparisonError::PropertyConflict(name));
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
    depth: usize,
    max_depth: usize,
) -> Result<(), JsonSchemaComparisonError> {
    for (name, property) in source.properties {
        if let Some(existing) = target.properties.get_mut(&name) {
            if json_schema_property_schemas_conflict(existing, &property, depth, max_depth)? {
                return Err(JsonSchemaComparisonError::PropertyConflict(name));
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

fn json_schema_property_schemas_conflict(
    existing: &Value,
    candidate: &Value,
    depth: usize,
    max_depth: usize,
) -> Result<bool, JsonSchemaComparisonError> {
    let Ok(left) = schema_validation_fingerprint(existing, depth, max_depth) else {
        return Err(JsonSchemaComparisonError::DepthExceeded);
    };
    let Ok(right) = schema_validation_fingerprint(candidate, depth, max_depth) else {
        return Err(JsonSchemaComparisonError::DepthExceeded);
    };
    Ok(left != right)
}

fn schema_validation_fingerprint<'a>(
    schema: &Value,
    depth: usize,
    max_depth: usize,
) -> Result<Value, JsonSchemaWalkError<'a>> {
    if depth > max_depth {
        return Err(JsonSchemaWalkError::DepthExceeded);
    }
    let next_depth = depth + 1;

    let Some(object) = schema.as_object() else {
        return Ok(schema.clone());
    };

    let mut out = serde_json::Map::new();
    for (key, value) in object
        .iter()
        .filter(|(key, _value)| !ANNOTATION_KEYS.contains(&key.as_str()))
    {
        let value = match key.as_str() {
            "$defs" | "definitions" | "dependentSchemas" | "patternProperties" | "properties" => {
                schema_map_validation_fingerprint(value, next_depth, max_depth)?
            }
            "dependencies" => {
                schema_dependency_map_validation_fingerprint(value, next_depth, max_depth)?
            }
            "additionalItems"
            | "additionalProperties"
            | "contains"
            | "contentSchema"
            | "else"
            | "if"
            | "items"
            | "not"
            | "propertyNames"
            | "then"
            | "unevaluatedItems"
            | "unevaluatedProperties" => {
                schema_or_schema_array_validation_fingerprint(value, next_depth, max_depth)?
            }
            "allOf" | "anyOf" | "oneOf" | "prefixItems" => {
                schema_array_validation_fingerprint(value, next_depth, max_depth)?
            }
            "type" => schema_type_validation_fingerprint(value),
            _ => value.clone(),
        };
        out.insert(key.clone(), value);
    }
    Ok(Value::Object(out))
}

fn schema_map_validation_fingerprint<'a>(
    schemas: &Value,
    depth: usize,
    max_depth: usize,
) -> Result<Value, JsonSchemaWalkError<'a>> {
    if depth > max_depth {
        return Err(JsonSchemaWalkError::DepthExceeded);
    }
    let next_depth = depth + 1;

    let Some(object) = schemas.as_object() else {
        return Ok(schemas.clone());
    };

    Ok(Value::Object(
        object
            .iter()
            .map(|(name, schema)| {
                schema_validation_fingerprint(schema, next_depth, max_depth)
                    .map(|schema| (name.clone(), schema))
            })
            .collect::<Result<serde_json::Map<_, _>, _>>()?,
    ))
}

fn schema_dependency_map_validation_fingerprint<'a>(
    dependencies: &Value,
    depth: usize,
    max_depth: usize,
) -> Result<Value, JsonSchemaWalkError<'a>> {
    if depth > max_depth {
        return Err(JsonSchemaWalkError::DepthExceeded);
    }
    let next_depth = depth + 1;

    let Some(object) = dependencies.as_object() else {
        return Ok(dependencies.clone());
    };

    Ok(Value::Object(
        object
            .iter()
            .map(|(name, dependency)| {
                schema_or_schema_array_validation_fingerprint(dependency, next_depth, max_depth)
                    .map(|dependency| (name.clone(), dependency))
            })
            .collect::<Result<serde_json::Map<_, _>, _>>()?,
    ))
}

fn schema_or_schema_array_validation_fingerprint<'a>(
    value: &Value,
    depth: usize,
    max_depth: usize,
) -> Result<Value, JsonSchemaWalkError<'a>> {
    match value {
        Value::Array(_values) => schema_array_validation_fingerprint(value, depth, max_depth),
        Value::Object(_) | Value::Bool(_) => schema_validation_fingerprint(value, depth, max_depth),
        other => Ok(other.clone()),
    }
}

fn schema_array_validation_fingerprint<'a>(
    schemas: &Value,
    depth: usize,
    max_depth: usize,
) -> Result<Value, JsonSchemaWalkError<'a>> {
    if depth > max_depth {
        return Err(JsonSchemaWalkError::DepthExceeded);
    }
    let next_depth = depth + 1;

    let Some(values) = schemas.as_array() else {
        return Ok(schemas.clone());
    };

    Ok(Value::Array(
        values
            .iter()
            .map(|schema| schema_validation_fingerprint(schema, next_depth, max_depth))
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

fn schema_type_validation_fingerprint(value: &Value) -> Value {
    let Value::Array(values) = value else {
        return value.clone();
    };
    let mut values = values.clone();
    values.sort_by_key(Value::to_string);
    Value::Array(values)
}

fn merge_json_schema_property_metadata(existing: &mut Value, candidate: &Value) {
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
    use std::collections::BTreeSet;

    use serde_json::json;

    use super::*;

    fn inferred_row_path(root: &Value, response_schema: &Value) -> Option<Vec<String>> {
        infer_wrapped_list(WrappedListInferenceContext {
            operation_name: "list_items",
            schema_root: root,
            response_schema,
        })
        .map(|inference| inference.row_path)
    }

    #[test]
    fn wrapped_list_prefers_named_direct_arrays_in_stable_order() {
        let schema = json!({
            "type": "object",
            "properties": {
                "data": {"type": "array", "items": {"type": "object"}},
                "items": {"type": "array", "items": {"type": "object"}}
            }
        });

        assert_eq!(
            inferred_row_path(&schema, &schema),
            Some(vec!["items".into()])
        );
    }

    #[test]
    fn wrapped_list_recurses_through_preferred_wrapper_names() {
        let schema = json!({
            "type": "object",
            "properties": {
                "total_count": {"type": "integer"},
                "results": {
                    "type": "object",
                    "properties": {
                        "data": {
                            "type": "array",
                            "items": {"type": "object"}
                        }
                    }
                }
            }
        });

        assert_eq!(
            inferred_row_path(&schema, &schema),
            Some(vec!["results".into(), "data".into()])
        );
    }

    #[test]
    fn wrapped_list_falls_back_to_the_sole_non_metadata_array() {
        let schema = json!({
            "type": "object",
            "properties": {
                "total_count": {"type": "integer"},
                "repositories": {"type": "array", "items": {"type": "object"}}
            }
        });

        assert_eq!(
            inferred_row_path(&schema, &schema),
            Some(vec!["repositories".into()])
        );
    }

    #[test]
    fn wrapped_list_abstains_for_multiple_non_preferred_arrays() {
        let schema = json!({
            "type": "object",
            "properties": {
                "warnings": {"type": "array", "items": {"type": "string"}},
                "repositories": {"type": "array", "items": {"type": "object"}}
            }
        });

        assert_eq!(inferred_row_path(&schema, &schema), None);
    }

    #[test]
    fn wrapped_list_resolves_openapi_component_references_from_document_root() {
        let document = json!({
            "components": {
                "schemas": {
                    "Envelope": {
                        "type": "object",
                        "properties": {
                            "items": {"$ref": "#/components/schemas/Items"}
                        }
                    },
                    "Items": {
                        "type": "array",
                        "items": {"type": "object"}
                    }
                }
            }
        });
        let response = json!({"$ref": "#/components/schemas/Envelope"});

        assert_eq!(
            inferred_row_path(&document, &response),
            Some(vec!["items".into()])
        );
    }

    #[test]
    fn wrapped_list_resolves_mcp_defs_from_output_schema_root() {
        let schema = json!({
            "$defs": {
                "Items": {
                    "type": "array",
                    "items": {"type": "object"}
                }
            },
            "type": "object",
            "properties": {
                "items": {"$ref": "#/$defs/Items"}
            }
        });

        assert_eq!(
            inferred_row_path(&schema, &schema),
            Some(vec!["items".into()])
        );
    }

    #[test]
    fn wrapped_list_abstains_for_unresolvable_or_composed_schemas() {
        let external = json!({"$ref": "https://example.com/schema.json#/Envelope"});
        let composed = json!({
            "type": "object",
            "properties": {
                "items": {"type": "array", "items": {"type": "object"}}
            },
            "oneOf": [
                {"type": "object", "properties": {"items": {"type": "array"}}}
            ]
        });

        assert_eq!(inferred_row_path(&external, &external), None);
        assert_eq!(inferred_row_path(&composed, &composed), None);
    }

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
    fn resolved_schema_walk_keeps_ref_guard_during_visit() {
        let root = json!({
            "$defs": {
                "Name": {"type": "string"}
            }
        });
        let schema = json!({"$ref": "#/$defs/Name"});
        let mut resolving_refs = BTreeSet::new();

        let guard_was_active = with_resolved_json_schema(
            &root,
            &schema,
            &mut resolving_refs,
            0,
            8,
            |_schema, resolving_refs, _depth| Ok(resolving_refs.contains("#/$defs/Name")),
        )
        .expect("walk");

        assert!(guard_was_active);
        assert!(resolving_refs.is_empty());
    }

    #[test]
    fn resolved_schema_ref_with_siblings_resolves_schema_bearing_children() {
        let root = json!({
            "$defs": {
                "Value": {"type": "integer"}
            },
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "properties": {
                        "value": {"$ref": "#/$defs/Value"}
                    }
                }
            }
        });
        let mut resolving_refs = BTreeSet::new();
        let filter = root.pointer("/properties/filter").expect("filter schema");

        let resolved =
            resolve_json_schema_ref_with_siblings(&root, filter, &mut resolving_refs, 0, 8)
                .expect("resolved");

        assert_eq!(
            resolved
                .pointer("/properties/value/type")
                .and_then(Value::as_str),
            Some("integer")
        );
        assert!(resolving_refs.is_empty());
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
            Err(JsonSchemaComparisonError::PropertyConflict(
                "query".to_string()
            ))
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

        merge_json_object_shape_annotation_insensitive(&mut target, source, 0, 100).expect("merge");

        let query = target.properties.get("query").expect("query property");
        assert_eq!(query.get("title").and_then(Value::as_str), Some("Query"));
        assert_eq!(
            query.get("description").and_then(Value::as_str),
            Some("Search query")
        );
        assert!(target.required.contains("query"));
    }

    #[test]
    fn annotation_insensitive_object_shape_merge_ignores_nested_schema_annotations() {
        let mut target = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "properties": {
                        "status": {
                            "type": "string",
                            "description": "Public status"
                        }
                    }
                }
            }
        }));
        let source = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "properties": {
                        "status": {
                            "type": "string",
                            "description": "Internal workflow state"
                        }
                    }
                }
            }
        }));

        merge_json_object_shape_annotation_insensitive(&mut target, source, 0, 100).expect("merge");
    }

    #[test]
    fn annotation_insensitive_object_shape_merge_keeps_const_values_opaque() {
        let mut target = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "properties": {
                        "status": {
                            "const": {
                                "description": "open"
                            }
                        }
                    }
                }
            }
        }));
        let source = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "properties": {
                        "status": {
                            "const": {}
                        }
                    }
                }
            }
        }));

        assert_eq!(
            merge_json_object_shape_annotation_insensitive(&mut target, source, 0, 100),
            Err(JsonSchemaComparisonError::PropertyConflict(
                "filter".to_string()
            ))
        );
    }

    #[test]
    fn annotation_insensitive_object_shape_merge_keeps_enum_values_opaque() {
        let mut target = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "properties": {
                        "status": {
                            "enum": [
                                {"description": "open"}
                            ]
                        }
                    }
                }
            }
        }));
        let source = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "properties": {
                        "status": {
                            "enum": [
                                {}
                            ]
                        }
                    }
                }
            }
        }));

        assert_eq!(
            merge_json_object_shape_annotation_insensitive(&mut target, source, 0, 100),
            Err(JsonSchemaComparisonError::PropertyConflict(
                "filter".to_string()
            ))
        );
    }

    #[test]
    fn annotation_insensitive_object_shape_merge_keeps_unknown_keyword_values_opaque() {
        let mut target = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "x-coral-metadata": {
                        "description": "left"
                    }
                }
            }
        }));
        let source = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "x-coral-metadata": {}
                }
            }
        }));

        assert_eq!(
            merge_json_object_shape_annotation_insensitive(&mut target, source, 0, 100),
            Err(JsonSchemaComparisonError::PropertyConflict(
                "filter".to_string()
            ))
        );
    }

    #[test]
    fn annotation_insensitive_object_shape_merge_recurses_into_schema_dependencies() {
        let mut target = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "dependencies": {
                        "status": {
                            "type": "object",
                            "properties": {
                                "reason": {
                                    "type": "string",
                                    "description": "Public reason"
                                }
                            }
                        },
                        "owner": ["team"]
                    }
                }
            }
        }));
        let source = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "dependencies": {
                        "status": {
                            "type": "object",
                            "properties": {
                                "reason": {
                                    "type": "string",
                                    "description": "Internal reason"
                                }
                            }
                        },
                        "owner": ["team"]
                    }
                }
            }
        }));

        merge_json_object_shape_annotation_insensitive(&mut target, source, 0, 100).expect("merge");
    }

    #[test]
    fn annotation_insensitive_object_shape_merge_reports_depth_exceeded() {
        let mut target = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "properties": {
                        "value": {"type": "string"}
                    }
                }
            }
        }));
        let source = direct_json_object_shape(&json!({
            "type": "object",
            "properties": {
                "filter": {
                    "type": "object",
                    "properties": {
                        "value": {"type": "string"}
                    }
                }
            }
        }));

        assert_eq!(
            merge_json_object_shape_annotation_insensitive(&mut target, source, 0, 1),
            Err(JsonSchemaComparisonError::DepthExceeded)
        );
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
