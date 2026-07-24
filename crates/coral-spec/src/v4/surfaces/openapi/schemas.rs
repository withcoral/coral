use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{IrField, IrType, IrTypeShape};
use crate::v4::naming::normalize_identifier;
use crate::v4::surfaces::json_schema::{
    JsonSchemaComparisonError, direct_json_object_shape, json_schema_required_fields,
    json_schema_scalar_type, merge_json_schema_properties_exact,
};

use super::import::OpenApiImporter;

impl OpenApiImporter<'_> {
    #[expect(
        clippy::too_many_lines,
        reason = "OpenAPI schema import is deliberately kept in one local recursive routine for the first v4 slice."
    )]
    pub(super) fn import_schema(
        &mut self,
        schema: &Value,
        suggested_id: &str,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Option<String> {
        let resolved = self.resolve_ref(schema, operation_id, diagnostics)?;
        let type_id = schema.get("$ref").and_then(Value::as_str).map_or_else(
            || normalize_identifier(suggested_id, "type"),
            type_id_from_ref,
        );
        if self.types.contains_key(&type_id) {
            return Some(type_id);
        }
        let description = resolved
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let nullable = resolved
            .get("nullable")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        self.types.insert(
            type_id.clone(),
            IrType {
                id: type_id.clone(),
                shape: IrTypeShape::Json,
                nullable,
                description: description.clone(),
            },
        );
        let shape = if let Some(all_of) = resolved.get("allOf").and_then(Value::as_array) {
            let mut merged = BTreeMap::new();
            for item in all_of {
                let item = self.resolve_ref(item, operation_id, diagnostics)?;
                let properties = direct_json_object_shape(&item).properties;
                match merge_json_schema_properties_exact(&mut merged, properties) {
                    Ok(()) => {}
                    Err(JsonSchemaComparisonError::PropertyConflict(property)) => {
                        diagnostics.push(Diagnostic::warning(
                            "OPENAPI_ALLOF_CONFLICT",
                            format!("allOf property '{property}' conflicts in operation '{operation_id}'"),
                            Some(operation_id.to_string()),
                        ));
                        return None;
                    }
                    Err(JsonSchemaComparisonError::DepthExceeded) => {
                        diagnostics.push(Diagnostic::warning(
                            "OPENAPI_ALLOF_CONFLICT",
                            format!("allOf schema exceeds maximum comparison depth in operation '{operation_id}'"),
                            Some(operation_id.to_string()),
                        ));
                        return None;
                    }
                }
            }
            IrTypeShape::Object {
                fields: self.import_object_fields(
                    merged.iter(),
                    &BTreeSet::new(),
                    &type_id,
                    operation_id,
                    diagnostics,
                ),
            }
        } else if let Some(values) = resolved.get("enum").and_then(Value::as_array) {
            IrTypeShape::Enum {
                values: values.iter().map(enum_value).collect(),
            }
        } else if let Some(scalar) = json_schema_scalar_type(&resolved) {
            IrTypeShape::Scalar(scalar)
        } else {
            match resolved
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("object")
            {
                "object" => {
                    if let Some(properties) = resolved.get("properties").and_then(Value::as_object)
                    {
                        let required = resolved
                            .as_object()
                            .map(json_schema_required_fields)
                            .unwrap_or_default();
                        IrTypeShape::Object {
                            fields: self.import_object_fields(
                                properties.iter(),
                                &required,
                                &type_id,
                                operation_id,
                                diagnostics,
                            ),
                        }
                    } else if let Some(additional) = resolved.get("additionalProperties") {
                        if additional.as_bool() == Some(false) {
                            IrTypeShape::Object { fields: Vec::new() }
                        } else {
                            let value_type_ref = self
                                .import_schema(
                                    additional,
                                    &format!("{type_id}_value"),
                                    operation_id,
                                    diagnostics,
                                )
                                .unwrap_or_else(|| "json".to_string());
                            IrTypeShape::Map { value_type_ref }
                        }
                    } else {
                        IrTypeShape::Json
                    }
                }
                "array" => {
                    let item = resolved.get("items").unwrap_or(&Value::Null);
                    let item_type_ref = self
                        .import_schema(item, &format!("{type_id}_item"), operation_id, diagnostics)
                        .unwrap_or_else(|| "json".to_string());
                    IrTypeShape::List { item_type_ref }
                }
                _ => IrTypeShape::Json,
            }
        };
        self.types.insert(
            type_id.clone(),
            IrType {
                id: type_id.clone(),
                shape,
                nullable,
                description,
            },
        );
        Some(type_id)
    }

    fn import_object_fields<'a>(
        &mut self,
        properties: impl Iterator<Item = (&'a String, &'a Value)>,
        required: &BTreeSet<String>,
        parent_id: &str,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Vec<IrField> {
        properties
            .map(|(name, schema)| {
                let type_ref = self
                    .import_schema(
                        schema,
                        &format!("{parent_id}_{name}"),
                        operation_id,
                        diagnostics,
                    )
                    .unwrap_or_else(|| "json".to_string());
                IrField {
                    name: name.clone(),
                    type_ref,
                    required: required.contains(name),
                    nullable: true,
                    description: self.field_description(schema),
                }
            })
            .collect()
    }

    fn field_description(&self, schema: &Value) -> String {
        if let Some(description) = schema.get("description").and_then(Value::as_str) {
            return description.to_string();
        }
        let Some(reference) = schema.get("$ref").and_then(Value::as_str) else {
            return String::new();
        };
        let Some(pointer) = reference.strip_prefix('#') else {
            return String::new();
        };
        self.document
            .pointer(pointer)
            .and_then(|resolved| resolved.get("description"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    }
}

fn enum_value(value: &Value) -> String {
    value
        .as_str()
        .map_or_else(|| value.to_string(), ToString::to_string)
}

fn type_id_from_ref(reference: &str) -> String {
    normalize_identifier(reference.rsplit('/').next().unwrap_or(reference), "type")
}
