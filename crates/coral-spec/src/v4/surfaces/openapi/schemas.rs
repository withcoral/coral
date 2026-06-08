use std::collections::BTreeSet;

use serde_json::{Map, Value};

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{IrField, IrScalarType, IrType, IrTypeShape};
use crate::v4::naming::normalize_identifier;

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
            let mut merged = Map::new();
            for item in all_of {
                let item = self.resolve_ref(item, operation_id, diagnostics)?;
                if let Some(properties) = item.get("properties").and_then(Value::as_object) {
                    for (name, property) in properties {
                        if let Some(existing) = merged.get(name)
                            && existing != property
                        {
                            diagnostics.push(Diagnostic::warning(
                                "OPENAPI_ALLOF_CONFLICT",
                                format!("allOf property '{name}' conflicts in operation '{operation_id}'"),
                                self.surface.id.clone(),
                                Some(operation_id.to_string()),
                            ));
                            return None;
                        }
                        merged.insert(name.clone(), property.clone());
                    }
                }
            }
            IrTypeShape::Object {
                fields: self.import_object_fields(
                    &merged,
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
        } else {
            match resolved
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("object")
            {
                "object" => {
                    if let Some(properties) = resolved.get("properties").and_then(Value::as_object)
                    {
                        let required = required_fields(&resolved);
                        IrTypeShape::Object {
                            fields: self.import_object_fields(
                                properties,
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
                "string" => {
                    let scalar =
                        if resolved.get("format").and_then(Value::as_str) == Some("date-time") {
                            IrScalarType::Timestamp
                        } else {
                            IrScalarType::String
                        };
                    IrTypeShape::Scalar(scalar)
                }
                "integer" => IrTypeShape::Scalar(IrScalarType::Integer),
                "number" => IrTypeShape::Scalar(IrScalarType::Number),
                "boolean" => IrTypeShape::Scalar(IrScalarType::Boolean),
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

    fn import_object_fields(
        &mut self,
        properties: &Map<String, Value>,
        required: &BTreeSet<String>,
        parent_id: &str,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Vec<IrField> {
        properties
            .iter()
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

fn required_fields(schema: &Value) -> BTreeSet<String> {
    schema
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(ToString::to_string)
        .collect()
}

fn enum_value(value: &Value) -> String {
    value
        .as_str()
        .map_or_else(|| value.to_string(), ToString::to_string)
}

fn type_id_from_ref(reference: &str) -> String {
    normalize_identifier(reference.rsplit('/').next().unwrap_or(reference), "type")
}
