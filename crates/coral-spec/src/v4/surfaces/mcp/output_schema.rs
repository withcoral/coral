use serde_json::Value;

use crate::v4::ir::{
    IrField, IrOperationOutput, IrScalarType, IrType, IrTypeShape, OutputCardinality,
};
use crate::v4::surfaces::json_schema::{
    json_schema_required_fields, json_schema_scalar_type, json_schema_type_contains,
};

use super::import::McpImporter;
use super::input_schema::schema_description;

impl McpImporter<'_> {
    pub(super) fn import_output(
        &mut self,
        operation_id: &str,
        output_schema: Option<&Value>,
    ) -> IrOperationOutput {
        let row_type_id = format!("{operation_id}_row");
        let Some(schema) = output_schema else {
            self.insert_generic_row_type(&row_type_id);
            return IrOperationOutput {
                cardinality: OutputCardinality::Singleton,
                type_ref: row_type_id,
            };
        };
        if json_schema_type_contains(schema, "array") {
            let item_schema = schema.get("items");
            self.insert_row_type_from_schema(&row_type_id, item_schema);
            return IrOperationOutput {
                cardinality: OutputCardinality::List,
                type_ref: row_type_id,
            };
        }
        self.insert_row_type_from_schema(&row_type_id, Some(schema));
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
                            description: String::new(),
                        },
                        IrField {
                            name: "raw".to_string(),
                            type_ref: json_type,
                            required: false,
                            description: "Raw MCP tool payload.".to_string(),
                        },
                    ],
                },
                description: String::new(),
            },
        );
    }

    fn insert_row_type_from_schema(&mut self, type_id: &str, schema: Option<&Value>) {
        let Some(schema) = schema else {
            self.insert_generic_row_type(type_id);
            return;
        };
        let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
            self.insert_generic_row_type(type_id);
            return;
        };
        let required = schema
            .as_object()
            .map(json_schema_required_fields)
            .unwrap_or_default();
        let mut fields = properties
            .iter()
            .map(|(name, property)| {
                let data_type = json_schema_scalar_type(property).unwrap_or(IrScalarType::Json);
                IrField {
                    name: name.clone(),
                    type_ref: self.ensure_type_for_scalar(data_type),
                    required: required.contains(name.as_str()),
                    description: schema_description(property),
                }
            })
            .collect::<Vec<_>>();
        if !fields.iter().any(|field| field.name == "raw") {
            let json_type = self.ensure_type_for_scalar(IrScalarType::Json);
            fields.push(IrField {
                name: "raw".to_string(),
                type_ref: json_type,
                required: false,
                description: "Raw MCP tool payload.".to_string(),
            });
        }
        self.types.insert(
            type_id.to_string(),
            IrType {
                id: type_id.to_string(),
                shape: IrTypeShape::Object { fields },
                description: schema_description(schema),
            },
        );
    }
}
