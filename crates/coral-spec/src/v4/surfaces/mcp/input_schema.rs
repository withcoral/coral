use std::collections::BTreeSet;

use serde_json::Value;

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{IrInputLocation, IrOperationInput, IrScalarType};
use crate::v4::surfaces::json_schema::{
    JsonObjectShape, RefError, direct_json_object_shape, json_schema_default_to_string,
    json_schema_scalar_type, merge_json_object_shape_annotation_insensitive, resolve_local_ref,
};

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
                    default_value: property.get("default").map(json_schema_default_to_string),
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

fn input_object_shape(
    root: &Value,
    schema: &Value,
    surface_id: &str,
    operation_id: &str,
    resolving_refs: &mut BTreeSet<String>,
    diagnostics: &mut Vec<Diagnostic>,
    schema_complete: &mut bool,
) -> Option<JsonObjectShape> {
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

    let mut shape = direct_json_object_shape(schema);
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
    let reference = schema.get("$ref").and_then(Value::as_str);
    if let Some(reference) = reference
        && reference.starts_with("#/")
        && !resolving_refs.insert(reference.to_string())
    {
        *schema_complete = false;
        diagnostics.push(Diagnostic::warning(
            "MCP_INPUT_SCHEMA_REF_UNSUPPORTED",
            format!("MCP input schema reference cycle includes '{reference}'"),
            surface_id.to_string(),
            Some(operation_id.to_string()),
        ));
        return None;
    }
    let resolved = match resolve_local_ref(root, schema) {
        Ok(resolved) => Some(resolved),
        Err(RefError::External(reference)) => {
            *schema_complete = false;
            diagnostics.push(Diagnostic::warning(
                "MCP_INPUT_SCHEMA_REF_UNSUPPORTED",
                format!("MCP input schema external reference '{reference}' is unsupported"),
                surface_id.to_string(),
                Some(operation_id.to_string()),
            ));
            None
        }
        Err(RefError::NotFound(reference)) => {
            *schema_complete = false;
            diagnostics.push(Diagnostic::warning(
                "MCP_INPUT_SCHEMA_REF_NOT_FOUND",
                format!("MCP input schema reference '{reference}' was not found"),
                surface_id.to_string(),
                Some(operation_id.to_string()),
            ));
            None
        }
    };
    if let Some(reference) = reference
        && reference.starts_with("#/")
    {
        resolving_refs.remove(reference);
    }
    resolved
}

fn merge_input_object_shape(
    target: &mut JsonObjectShape,
    source: JsonObjectShape,
    surface_id: &str,
    operation_id: &str,
    diagnostics: &mut Vec<Diagnostic>,
    schema_complete: &mut bool,
) -> bool {
    match merge_json_object_shape_annotation_insensitive(target, source) {
        Ok(()) => true,
        Err(conflict) => {
            *schema_complete = false;
            diagnostics.push(Diagnostic::warning(
                "MCP_INPUT_SCHEMA_CONFLICT",
                format!(
                    "MCP input schema defines conflicting property '{}'",
                    conflict.property
                ),
                surface_id.to_string(),
                Some(operation_id.to_string()),
            ));
            false
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
