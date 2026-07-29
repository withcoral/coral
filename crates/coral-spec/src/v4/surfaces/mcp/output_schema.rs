use std::collections::BTreeSet;

use serde_json::Value;

use crate::v4::ir::{
    IrField, IrOperationOutput, IrScalarType, IrType, IrTypeShape, OutputCardinality,
};
use crate::v4::naming::{normalize_identifier, stable_suffix};
use crate::v4::surfaces::json_schema::{
    json_schema_required_fields, json_schema_scalar_type, json_schema_type_contains,
    resolve_json_schema_ref_with_siblings,
};

use super::import::McpImporter;
use super::input_schema::schema_description;

const MAX_MCP_OUTPUT_SCHEMA_DEPTH: usize = 64;

impl McpImporter<'_> {
    pub(super) fn import_output(
        &mut self,
        operation_id: &str,
        output_schema: Option<&Value>,
    ) -> IrOperationOutput {
        let row_type_id = format!("{operation_id}_row");
        let Some(root_schema) = output_schema else {
            self.insert_generic_row_type(&row_type_id);
            return IrOperationOutput {
                cardinality: OutputCardinality::Singleton,
                type_ref: row_type_id,
            };
        };
        let mut resolving_refs = BTreeSet::new();
        let Ok(schema) = resolve_json_schema_ref_with_siblings(
            root_schema,
            root_schema,
            &mut resolving_refs,
            0,
            MAX_MCP_OUTPUT_SCHEMA_DEPTH,
        ) else {
            self.insert_generic_row_type(&row_type_id);
            return IrOperationOutput {
                cardinality: OutputCardinality::Singleton,
                type_ref: row_type_id,
            };
        };
        if json_schema_type_contains(&schema, "array") {
            let item_schema = schema.get("items");
            self.insert_row_type_from_schema(&row_type_id, item_schema, root_schema);
            return IrOperationOutput {
                cardinality: OutputCardinality::List,
                type_ref: row_type_id,
            };
        }
        self.insert_row_type_from_schema(&row_type_id, Some(&schema), root_schema);
        IrOperationOutput {
            cardinality: OutputCardinality::Singleton,
            type_ref: row_type_id,
        }
    }

    fn insert_generic_row_type(&mut self, type_id: &str) {
        let json_type = self.ensure_type_for_scalar(IrScalarType::Json);
        self.types.insert(
            type_id.to_string(),
            IrType {
                id: type_id.to_string(),
                shape: IrTypeShape::Object {
                    fields: vec![
                        IrField {
                            name: "result".to_string(),
                            type_ref: json_type.clone(),
                            required: false,
                            nullable: true,
                            description: String::new(),
                            synthetic: false,
                        },
                        IrField {
                            name: "raw".to_string(),
                            type_ref: json_type,
                            required: false,
                            nullable: true,
                            description: "Raw MCP tool payload.".to_string(),
                            synthetic: true,
                        },
                    ],
                },
                nullable: false,
                description: String::new(),
            },
        );
    }

    fn insert_row_type_from_schema(
        &mut self,
        type_id: &str,
        schema: Option<&Value>,
        root_schema: &Value,
    ) {
        let Some(schema) = schema else {
            self.insert_generic_row_type(type_id);
            return;
        };
        let mut resolving_refs = BTreeSet::new();
        let Ok(schema) = resolve_json_schema_ref_with_siblings(
            root_schema,
            schema,
            &mut resolving_refs,
            0,
            MAX_MCP_OUTPUT_SCHEMA_DEPTH,
        ) else {
            self.insert_generic_row_type(type_id);
            return;
        };
        if schema
            .get("properties")
            .and_then(Value::as_object)
            .is_none()
        {
            self.insert_generic_row_type(type_id);
            return;
        }
        self.insert_output_type(type_id, &schema);
        let json_type = self.ensure_type_for_scalar(IrScalarType::Json);
        let Some(IrType {
            shape: IrTypeShape::Object { fields },
            ..
        }) = self.types.get_mut(type_id)
        else {
            return;
        };
        if fields.iter().any(|field| field.name == "raw") {
            return;
        }
        fields.push(IrField {
            name: "raw".to_string(),
            type_ref: json_type,
            required: false,
            nullable: true,
            description: "Raw MCP tool payload.".to_string(),
            synthetic: true,
        });
    }

    fn insert_output_type(&mut self, type_id: &str, schema: &Value) -> String {
        if self.types.contains_key(type_id) {
            return type_id.to_string();
        }
        if let Some(scalar) = json_schema_scalar_type(schema) {
            return self.ensure_type_for_scalar(scalar);
        }
        let nullable = schema.get("nullable").and_then(Value::as_bool) == Some(true)
            || json_schema_type_contains(schema, "null");
        if json_schema_type_contains(schema, "array") {
            let item_type_ref = if let Some(items) = schema.get("items") {
                let item_type_id = format!("{type_id}_item");
                self.insert_output_type(&item_type_id, items)
            } else {
                self.ensure_type_for_scalar(IrScalarType::Json)
            };
            self.types.insert(
                type_id.to_string(),
                IrType {
                    id: type_id.to_string(),
                    shape: IrTypeShape::List { item_type_ref },
                    nullable,
                    description: schema_description(schema),
                },
            );
            return type_id.to_string();
        }
        let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
            self.types.insert(
                type_id.to_string(),
                IrType {
                    id: type_id.to_string(),
                    shape: IrTypeShape::Json,
                    nullable,
                    description: schema_description(schema),
                },
            );
            return type_id.to_string();
        };
        let required = schema
            .as_object()
            .map(json_schema_required_fields)
            .unwrap_or_default();
        // Reserve the type id before recursion so a recursive schema degrades
        // to JSON instead of recursing forever.
        self.types.insert(
            type_id.to_string(),
            IrType {
                id: type_id.to_string(),
                shape: IrTypeShape::Json,
                nullable,
                description: schema_description(schema),
            },
        );
        let fields = properties
            .iter()
            .map(|(name, property)| {
                let normalized_name = normalize_identifier(name, "field");
                let child_type_id = format!("{type_id}_{normalized_name}_{}", stable_suffix(name));
                let type_ref = self.insert_output_type(&child_type_id, property);
                let is_required = required.contains(name.as_str());
                let type_nullable = property.get("nullable").and_then(Value::as_bool) == Some(true)
                    || json_schema_type_contains(property, "null");
                IrField {
                    name: name.clone(),
                    type_ref,
                    required: is_required,
                    nullable: !is_required || type_nullable,
                    description: schema_description(property),
                    synthetic: false,
                }
            })
            .collect::<Vec<_>>();
        self.types.insert(
            type_id.to_string(),
            IrType {
                id: type_id.to_string(),
                shape: IrTypeShape::Object { fields },
                nullable,
                description: schema_description(schema),
            },
        );
        type_id.to_string()
    }
}
