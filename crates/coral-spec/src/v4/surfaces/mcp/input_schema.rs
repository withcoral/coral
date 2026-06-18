#![allow(
    clippy::too_many_arguments,
    reason = "MCP schema walking threads shared traversal state through recursive helpers."
)]

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{IrInputLocation, IrOperationInput, IrScalarType};
use crate::v4::surfaces::json_schema::json_schema_scalar_type;

use super::import::{MAX_SCHEMA_DEPTH, McpImporter};
use super::model::McpToolDescriptor;

const ANNOTATION_KEYS: &[&str] = &["$comment", "description", "examples", "title"];

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
            0,
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
    depth: usize,
) -> Option<InputObjectShape> {
    if depth > MAX_SCHEMA_DEPTH {
        *schema_complete = false;
        warn_input_schema_depth_exceeded(surface_id, operation_id, diagnostics);
        return None;
    }

    if let Some(reference) = schema.get("$ref").and_then(Value::as_str) {
        return referenced_input_object_shape(
            root,
            reference,
            surface_id,
            operation_id,
            resolving_refs,
            diagnostics,
            schema_complete,
            depth,
        );
    }

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
                depth + 1,
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

fn referenced_input_object_shape(
    root: &Value,
    reference: &str,
    surface_id: &str,
    operation_id: &str,
    resolving_refs: &mut BTreeSet<String>,
    diagnostics: &mut Vec<Diagnostic>,
    schema_complete: &mut bool,
    depth: usize,
) -> Option<InputObjectShape> {
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
    let reference = reference.to_string();
    if !resolving_refs.insert(reference.clone()) {
        *schema_complete = false;
        diagnostics.push(Diagnostic::warning(
            "MCP_INPUT_SCHEMA_REF_UNSUPPORTED",
            format!("MCP input schema reference cycle includes '{reference}'"),
            surface_id.to_string(),
            Some(operation_id.to_string()),
        ));
        return None;
    }
    let pointer = reference.strip_prefix('#').unwrap_or(&reference);
    let result = if let Some(resolved) = root.pointer(pointer) {
        input_object_shape(
            root,
            resolved,
            surface_id,
            operation_id,
            resolving_refs,
            diagnostics,
            schema_complete,
            depth + 1,
        )
    } else {
        *schema_complete = false;
        diagnostics.push(Diagnostic::warning(
            "MCP_INPUT_SCHEMA_REF_NOT_FOUND",
            format!("MCP input schema reference '{reference}' was not found"),
            surface_id.to_string(),
            Some(operation_id.to_string()),
        ));
        None
    };
    resolving_refs.remove(&reference);
    result
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
            match merge_allof_property(existing, &property) {
                AllOfPropertyMerge::Merged => {}
                AllOfPropertyMerge::Conflict => {
                    *schema_complete = false;
                    diagnostics.push(Diagnostic::warning(
                        "MCP_INPUT_SCHEMA_CONFLICT",
                        format!("MCP input schema defines conflicting property '{name}'"),
                        surface_id.to_string(),
                        Some(operation_id.to_string()),
                    ));
                    return false;
                }
                AllOfPropertyMerge::DepthExceeded => {
                    *schema_complete = false;
                    warn_input_schema_depth_exceeded(surface_id, operation_id, diagnostics);
                    return false;
                }
            }
        } else {
            target.properties.insert(name, property);
        }
    }
    target.required.extend(source.required);
    true
}

enum AllOfPropertyMerge {
    Merged,
    Conflict,
    DepthExceeded,
}

fn merge_allof_property(existing: &mut Value, candidate: &Value) -> AllOfPropertyMerge {
    let Some(matches) = property_schemas_match_without_top_level_annotations(existing, candidate)
    else {
        return AllOfPropertyMerge::DepthExceeded;
    };
    if !matches {
        return AllOfPropertyMerge::Conflict;
    }
    merge_input_property_metadata(existing, candidate);
    AllOfPropertyMerge::Merged
}

fn property_schemas_match_without_top_level_annotations(
    existing: &Value,
    candidate: &Value,
) -> Option<bool> {
    let (Some(existing), Some(candidate)) = (existing.as_object(), candidate.as_object()) else {
        return values_match_with_depth(existing, candidate, 0);
    };
    let existing_keys = existing
        .keys()
        .filter(|key| !ANNOTATION_KEYS.contains(&key.as_str()))
        .collect::<BTreeSet<_>>();
    let candidate_keys = candidate
        .keys()
        .filter(|key| !ANNOTATION_KEYS.contains(&key.as_str()))
        .collect::<BTreeSet<_>>();
    if existing_keys != candidate_keys {
        return Some(false);
    }
    for key in existing_keys {
        let Some(existing_value) = existing.get(key) else {
            return Some(false);
        };
        let Some(candidate_value) = candidate.get(key) else {
            return Some(false);
        };
        let matches = if key == "type" {
            type_values_match(existing_value, candidate_value, 1)?
        } else {
            values_match_with_depth(existing_value, candidate_value, 1)?
        };
        if !matches {
            return Some(false);
        }
    }
    Some(true)
}

fn type_values_match(existing: &Value, candidate: &Value, depth: usize) -> Option<bool> {
    let (Some(existing), Some(candidate)) = (existing.as_array(), candidate.as_array()) else {
        return values_match_with_depth(existing, candidate, depth);
    };
    let Some(mut existing_types) = existing
        .iter()
        .map(Value::as_str)
        .collect::<Option<Vec<_>>>()
    else {
        return arrays_match_with_depth(existing, candidate, depth);
    };
    let Some(mut candidate_types) = candidate
        .iter()
        .map(Value::as_str)
        .collect::<Option<Vec<_>>>()
    else {
        return arrays_match_with_depth(existing, candidate, depth);
    };
    existing_types.sort_unstable();
    candidate_types.sort_unstable();
    Some(existing_types == candidate_types)
}

fn values_match_with_depth(existing: &Value, candidate: &Value, depth: usize) -> Option<bool> {
    if depth > MAX_SCHEMA_DEPTH {
        return None;
    }
    match (existing, candidate) {
        (Value::Object(existing), Value::Object(candidate)) => {
            if existing.len() != candidate.len() {
                return Some(false);
            }
            for (key, existing_value) in existing {
                let Some(candidate_value) = candidate.get(key) else {
                    return Some(false);
                };
                let matches = if key == "type" {
                    type_values_match(existing_value, candidate_value, depth + 1)?
                } else {
                    values_match_with_depth(existing_value, candidate_value, depth + 1)?
                };
                if !matches {
                    return Some(false);
                }
            }
            Some(true)
        }
        (Value::Array(existing), Value::Array(candidate)) => {
            arrays_match_with_depth(existing, candidate, depth)
        }
        _ => Some(existing == candidate),
    }
}

fn arrays_match_with_depth(existing: &[Value], candidate: &[Value], depth: usize) -> Option<bool> {
    if existing.len() != candidate.len() {
        return Some(false);
    }
    for (existing_value, candidate_value) in existing.iter().zip(candidate) {
        if !values_match_with_depth(existing_value, candidate_value, depth + 1)? {
            return Some(false);
        }
    }
    Some(true)
}

fn warn_input_schema_depth_exceeded(
    surface_id: &str,
    operation_id: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    diagnostics.push(Diagnostic::warning(
        "MCP_INPUT_SCHEMA_DEPTH_EXCEEDED",
        format!("MCP input schema exceeds maximum depth of {MAX_SCHEMA_DEPTH}"),
        surface_id.to_string(),
        Some(operation_id.to_string()),
    ));
}

fn merge_input_property_metadata(existing: &mut Value, candidate: &Value) {
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
