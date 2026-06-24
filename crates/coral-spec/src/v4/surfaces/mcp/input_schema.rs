use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{IrInputLocation, IrOperationInput, IrScalarType};
use crate::v4::surfaces::json_schema::json_schema_scalar_type;

use super::import::McpImporter;
use super::model::McpToolDescriptor;

impl McpImporter<'_> {
    pub(super) fn import_inputs(
        &mut self,
        tool: &McpToolDescriptor,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> ImportedInputs {
        let mut resolving_refs = BTreeSet::new();
        let mut schema_complete = true;
        let Some(shape) = input_object_shape(
            &tool.input_schema,
            &tool.input_schema,
            &self.surface.id,
            operation_id,
            &mut resolving_refs,
            diagnostics,
            &mut schema_complete,
        ) else {
            return ImportedInputs {
                inputs: Vec::new(),
                schema_complete: false,
            };
        };
        let inputs = shape
            .properties
            .iter()
            .map(|(name, property)| {
                let data_type = json_schema_scalar_type(property).unwrap_or(IrScalarType::Json);
                self.ensure_type_for_scalar(data_type);
                IrOperationInput {
                    name: name.clone(),
                    location: IrInputLocation::ToolArg,
                    required: shape.required.contains(name.as_str()),
                    data_type,
                    default_value: property_default(property),
                    description: schema_description(property),
                }
            })
            .collect();
        ImportedInputs {
            inputs,
            schema_complete,
        }
    }
}

pub(super) struct ImportedInputs {
    pub(super) inputs: Vec<IrOperationInput>,
    pub(super) schema_complete: bool,
}

#[derive(Default)]
struct InputObjectShape {
    properties: BTreeMap<String, Value>,
    required: BTreeSet<String>,
}

fn input_object_shape(
    root: &Value,
    schema: &Value,
    surface_id: &str,
    operation_id: &str,
    resolving_refs: &mut BTreeSet<String>,
    diagnostics: &mut Vec<Diagnostic>,
    schema_complete: &mut bool,
) -> Option<InputObjectShape> {
    let schema = resolve_input_schema_ref(
        root,
        schema,
        surface_id,
        operation_id,
        resolving_refs,
        diagnostics,
        schema_complete,
    )?;

    if schema.get("anyOf").is_some() || schema.get("oneOf").is_some() {
        *schema_complete = false;
        diagnostics.push(Diagnostic::warning(
            "MCP_INPUT_SCHEMA_COMPOSITION_UNSUPPORTED",
            "MCP input schema uses anyOf/oneOf, which cannot be safely imported as SQL inputs",
            surface_id.to_string(),
            Some(operation_id.to_string()),
        ));
        return None;
    }

    let mut shape = direct_input_object_shape(schema);
    if let Some(all_of) = schema.get("allOf").and_then(Value::as_array) {
        for item in all_of {
            let item_shape = input_object_shape(
                root,
                item,
                surface_id,
                operation_id,
                resolving_refs,
                diagnostics,
                schema_complete,
            )?;
            if !merge_input_object_shape(
                &mut shape,
                item_shape,
                surface_id,
                operation_id,
                diagnostics,
                schema_complete,
            ) {
                return None;
            }
        }
    }
    Some(shape)
}

fn resolve_input_schema_ref<'a>(
    root: &'a Value,
    schema: &'a Value,
    surface_id: &str,
    operation_id: &str,
    resolving_refs: &mut BTreeSet<String>,
    diagnostics: &mut Vec<Diagnostic>,
    schema_complete: &mut bool,
) -> Option<&'a Value> {
    let Some(reference) = schema.get("$ref").and_then(Value::as_str) else {
        return Some(schema);
    };
    if !reference.starts_with("#/") {
        *schema_complete = false;
        diagnostics.push(Diagnostic::warning(
            "MCP_INPUT_SCHEMA_REF_UNSUPPORTED",
            format!("MCP input schema external reference '{reference}' is unsupported"),
            surface_id.to_string(),
            Some(operation_id.to_string()),
        ));
        return None;
    }
    if !resolving_refs.insert(reference.to_string()) {
        *schema_complete = false;
        diagnostics.push(Diagnostic::warning(
            "MCP_INPUT_SCHEMA_REF_UNSUPPORTED",
            format!("MCP input schema reference cycle includes '{reference}'"),
            surface_id.to_string(),
            Some(operation_id.to_string()),
        ));
        return None;
    }
    let pointer = reference.strip_prefix('#').unwrap_or(reference);
    let resolved = root.pointer(pointer);
    resolving_refs.remove(reference);
    if let Some(resolved) = resolved {
        Some(resolved)
    } else {
        *schema_complete = false;
        diagnostics.push(Diagnostic::warning(
            "MCP_INPUT_SCHEMA_REF_NOT_FOUND",
            format!("MCP input schema reference '{reference}' was not found"),
            surface_id.to_string(),
            Some(operation_id.to_string()),
        ));
        None
    }
}

fn direct_input_object_shape(schema: &Value) -> InputObjectShape {
    let Some(schema) = schema.as_object() else {
        return InputObjectShape::default();
    };
    let required = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<BTreeSet<_>>()
        })
        .unwrap_or_default();
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
    InputObjectShape {
        properties,
        required,
    }
}

fn merge_input_object_shape(
    target: &mut InputObjectShape,
    source: InputObjectShape,
    surface_id: &str,
    operation_id: &str,
    diagnostics: &mut Vec<Diagnostic>,
    schema_complete: &mut bool,
) -> bool {
    for (name, property) in source.properties {
        if let Some(existing) = target.properties.get_mut(&name) {
            if input_property_schemas_conflict(existing, &property) {
                *schema_complete = false;
                diagnostics.push(Diagnostic::warning(
                    "MCP_INPUT_SCHEMA_CONFLICT",
                    format!("MCP input schema defines conflicting property '{name}'"),
                    surface_id.to_string(),
                    Some(operation_id.to_string()),
                ));
                return false;
            }
            merge_input_property_metadata(existing, &property);
        } else {
            target.properties.insert(name, property);
        }
    }
    target.required.extend(source.required);
    true
}

fn input_property_schemas_conflict(existing: &Value, candidate: &Value) -> bool {
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

fn merge_input_property_metadata(existing: &mut Value, candidate: &Value) {
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

pub(super) fn schema_description(schema: &Value) -> String {
    schema
        .get("description")
        .and_then(Value::as_str)
        .or_else(|| schema.get("title").and_then(Value::as_str))
        .unwrap_or_default()
        .to_string()
}

fn property_default(schema: &Value) -> Option<String> {
    schema.get("default").map(|value| match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    })
}
