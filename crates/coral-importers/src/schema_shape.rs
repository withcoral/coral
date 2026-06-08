use std::collections::BTreeSet;

use coral_capabilities::ShapeHints;
use serde_json::Value;

pub(crate) fn shape_hints_from_json_schema(schema: &Value) -> ShapeHints {
    if json_schema_type(schema) == Some("array") {
        return ShapeHints::root_list();
    }
    if json_schema_type(schema) != Some("object") {
        return ShapeHints::unknown();
    }
    if let Some(path) = single_array_property_path(schema, schema) {
        return ShapeHints::list_at_path(path);
    }
    if let Some(path) = single_object_property_path(schema, schema) {
        return ShapeHints::singleton_at_path(path);
    }
    ShapeHints::root_singleton()
}

pub(crate) fn schema_shape_view(schema: &Value) -> Value {
    schema_shape_view_inner(schema, schema, &mut BTreeSet::new())
}

fn schema_shape_view_inner(
    root: &Value,
    schema: &Value,
    resolving: &mut BTreeSet<String>,
) -> Value {
    let Value::Object(object) = schema else {
        return schema.clone();
    };
    if let Some(reference) = object.get("$ref").and_then(Value::as_str)
        && let Some(pointer) = reference.strip_prefix('#')
        && resolving.insert(reference.to_string())
    {
        let resolved = root
            .pointer(pointer)
            .map(|value| schema_shape_view_inner(root, value, resolving));
        resolving.remove(reference);
        if let Some(resolved) = resolved {
            return schema_shape_with_root_defs(root, resolved);
        }
    }
    if let Some(all_of) = object.get("allOf").and_then(Value::as_array) {
        for value in all_of {
            let resolved = schema_shape_view_inner(root, value, resolving);
            if json_schema_type(&resolved).is_some() {
                return schema_shape_with_root_defs(root, resolved);
            }
        }
    }
    schema.clone()
}

fn schema_shape_with_root_defs(root: &Value, mut schema: Value) -> Value {
    let Some(defs) = root.get("$defs").cloned() else {
        return schema;
    };
    let Value::Object(object) = &mut schema else {
        return schema;
    };
    object.entry("$defs".to_string()).or_insert(defs);
    schema
}

fn single_array_property_path(root: &Value, schema: &Value) -> Option<Vec<String>> {
    let properties = schema.get("properties").and_then(Value::as_object)?;
    for preferred in ["items", "nodes", "edges", "data", "results"] {
        if properties.get(preferred).is_some_and(|property| {
            property_schema_type(root, property).as_deref() == Some("array")
        }) {
            return Some(vec![preferred.to_string()]);
        }
    }
    let mut array_properties = properties
        .iter()
        .filter(|(_, property)| property_schema_type(root, property).as_deref() == Some("array"))
        .map(|(name, _)| name.clone());
    let Some(first) = array_properties.next() else {
        return nested_single_array_property_path(root, schema);
    };
    if array_properties.next().is_none() {
        return Some(vec![first]);
    }
    None
}

fn nested_single_array_property_path(root: &Value, schema: &Value) -> Option<Vec<String>> {
    let properties = schema.get("properties").and_then(Value::as_object)?;
    let mut paths = properties.iter().filter_map(|(name, property)| {
        if metadata_property_name(name) {
            return None;
        }
        let property = schema_shape_view_inner(root, property, &mut BTreeSet::new());
        if json_schema_type(&property) != Some("object") {
            return None;
        }
        let mut path = single_array_property_path(root, &property)?;
        path.insert(0, name.clone());
        Some(path)
    });
    let first = paths.next()?;
    if paths.next().is_none() {
        Some(first)
    } else {
        None
    }
}

fn single_object_property_path(root: &Value, schema: &Value) -> Option<Vec<String>> {
    let properties = schema.get("properties").and_then(Value::as_object)?;
    let mut object_properties = properties.iter().filter_map(|(name, property)| {
        if metadata_property_name(name) {
            return None;
        }
        let property = schema_shape_view_inner(root, property, &mut BTreeSet::new());
        (json_schema_type(&property) == Some("object")).then_some(name.clone())
    });
    let first = object_properties.next()?;
    if object_properties.next().is_none() {
        Some(vec![first])
    } else {
        None
    }
}

fn metadata_property_name(name: &str) -> bool {
    matches!(
        name,
        "ok" | "error" | "warning" | "needed" | "provided" | "response_metadata"
    )
}

fn property_schema_type(root: &Value, property: &Value) -> Option<String> {
    let resolved = schema_shape_view_inner(root, property, &mut BTreeSet::new());
    json_schema_type(&resolved)
        .or_else(|| json_schema_type(property))
        .map(ToString::to_string)
}

fn json_schema_type(schema: &Value) -> Option<&str> {
    match schema.get("type") {
        Some(Value::String(value)) if value != "null" => Some(value.as_str()),
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(Value::as_str)
            .find(|value| *value != "null"),
        _ => None,
    }
}
