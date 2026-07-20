use std::collections::BTreeSet;

use serde_json::Value;

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{IrInputLocation, IrOperationInput, IrScalarType};
use crate::v4::surfaces::json_schema::{
    JsonObjectShape, JsonSchemaComparisonError, JsonSchemaWalkError, direct_json_object_shape,
    json_schema_default_to_string, json_schema_scalar_type,
    merge_json_object_shape_annotation_insensitive, resolve_json_schema_ref_with_siblings,
    with_resolved_json_schema,
};

use super::import::McpImporter;
use super::model::McpToolDescriptor;

const MAX_MCP_INPUT_SCHEMA_DEPTH: usize = 64;

impl McpImporter<'_> {
    pub(super) fn import_inputs(
        &mut self,
        tool: &McpToolDescriptor,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> ImportedInputs {
        let mut resolving_refs = BTreeSet::new();
        let mut schema_complete = true;
        let shape = {
            let mut context = InputSchemaContext {
                operation_id,
                diagnostics,
                schema_complete: &mut schema_complete,
            };
            let Some(shape) = input_object_shape(
                &tool.input_schema,
                &tool.input_schema,
                &mut resolving_refs,
                &mut context,
                0,
            ) else {
                return ImportedInputs {
                    inputs: Vec::new(),
                    schema_complete: false,
                };
            };
            if !validate_required_properties(&shape, &mut context) {
                let schema_complete = *context.schema_complete;
                return ImportedInputs {
                    inputs: Vec::new(),
                    schema_complete,
                };
            }
            shape
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
    resolving_refs: &mut BTreeSet<String>,
    context: &mut InputSchemaContext<'_, '_>,
    depth: usize,
) -> Option<JsonObjectShape> {
    let result = with_resolved_json_schema(
        root,
        schema,
        resolving_refs,
        depth,
        MAX_MCP_INPUT_SCHEMA_DEPTH,
        |schema, resolving_refs, next_depth| {
            if schema.get("anyOf").is_some() || schema.get("oneOf").is_some() {
                context.push_unsupported_composition();
                return Ok(None);
            }

            let mut shape = direct_json_object_shape(schema);
            if !resolve_input_property_schemas(
                root,
                &mut shape,
                resolving_refs,
                context,
                next_depth,
            ) {
                return Ok(None);
            }

            if let Some(all_of) = schema.get("allOf").and_then(Value::as_array) {
                for item in all_of {
                    let Some(item_shape) =
                        input_object_shape(root, item, resolving_refs, context, next_depth)
                    else {
                        return Ok(None);
                    };
                    if !merge_input_object_shape(
                        &mut shape,
                        item_shape,
                        context,
                        depth,
                        MAX_MCP_INPUT_SCHEMA_DEPTH,
                    ) {
                        return Ok(None);
                    }
                }
            }
            Ok(Some(shape))
        },
    );
    match result {
        Ok(shape) => shape,
        Err(error) => {
            context.push_schema_walk_diagnostic(error);
            None
        }
    }
}

fn resolve_input_property_schemas(
    root: &Value,
    shape: &mut JsonObjectShape,
    resolving_refs: &mut BTreeSet<String>,
    context: &mut InputSchemaContext<'_, '_>,
    depth: usize,
) -> bool {
    for property in shape.properties.values_mut() {
        match resolve_json_schema_ref_with_siblings(
            root,
            property,
            resolving_refs,
            depth,
            MAX_MCP_INPUT_SCHEMA_DEPTH,
        ) {
            Ok(resolved) => *property = resolved,
            Err(error) => {
                context.push_schema_walk_diagnostic(error);
                return false;
            }
        }
    }
    true
}

fn validate_required_properties(
    shape: &JsonObjectShape,
    context: &mut InputSchemaContext<'_, '_>,
) -> bool {
    let missing = shape
        .required
        .iter()
        .filter(|name| !shape.properties.contains_key(*name))
        .cloned()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return true;
    }
    context.push_warning(
        "MCP_INPUT_SCHEMA_REQUIRED_PROPERTY_MISSING",
        format!(
            "MCP input schema marks required properties that are not defined: {}",
            missing.join(", ")
        ),
    );
    false
}

fn merge_input_object_shape(
    target: &mut JsonObjectShape,
    source: JsonObjectShape,
    context: &mut InputSchemaContext<'_, '_>,
    depth: usize,
    max_depth: usize,
) -> bool {
    match merge_json_object_shape_annotation_insensitive(target, source, depth, max_depth) {
        Ok(()) => true,
        Err(JsonSchemaComparisonError::PropertyConflict(property)) => {
            context.push_warning(
                "MCP_INPUT_SCHEMA_CONFLICT",
                format!("MCP input schema defines conflicting property '{property}'"),
            );
            false
        }
        Err(JsonSchemaComparisonError::DepthExceeded) => {
            context.push_warning(
                "MCP_INPUT_SCHEMA_DEPTH_EXCEEDED",
                "MCP input schema exceeds the maximum supported nesting depth",
            );
            false
        }
    }
}

struct InputSchemaContext<'a, 'b> {
    operation_id: &'a str,
    diagnostics: &'b mut Vec<Diagnostic>,
    schema_complete: &'b mut bool,
}

impl InputSchemaContext<'_, '_> {
    fn push_unsupported_composition(&mut self) {
        self.push_warning(
            "MCP_INPUT_SCHEMA_COMPOSITION_UNSUPPORTED",
            "MCP input schema uses anyOf/oneOf, which cannot be safely imported as SQL inputs",
        );
    }

    fn push_schema_walk_diagnostic(&mut self, error: JsonSchemaWalkError<'_>) {
        let (code, message) = match error {
            JsonSchemaWalkError::ExternalRef(reference) => (
                "MCP_INPUT_SCHEMA_REF_UNSUPPORTED",
                format!("MCP input schema external reference '{reference}' is unsupported"),
            ),
            JsonSchemaWalkError::RefCycle(reference) => (
                "MCP_INPUT_SCHEMA_REF_UNSUPPORTED",
                format!("MCP input schema reference cycle includes '{reference}'"),
            ),
            JsonSchemaWalkError::RefNotFound(reference) => (
                "MCP_INPUT_SCHEMA_REF_NOT_FOUND",
                format!("MCP input schema reference '{reference}' was not found"),
            ),
            JsonSchemaWalkError::DepthExceeded => (
                "MCP_INPUT_SCHEMA_DEPTH_EXCEEDED",
                "MCP input schema exceeds the maximum supported nesting depth".to_string(),
            ),
        };
        self.push_warning(code, message);
    }

    fn push_warning(&mut self, code: &'static str, message: impl Into<String>) {
        *self.schema_complete = false;
        self.diagnostics.push(Diagnostic::warning(
            code,
            message,
            Some(self.operation_id.to_string()),
        ));
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
