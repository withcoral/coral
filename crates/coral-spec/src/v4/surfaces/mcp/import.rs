use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{
    IrEntityCandidate, IrExecutionAttachment, IrField, IrInputLocation, IrOperation,
    IrOperationInput, IrOperationOutput, IrScalarType, IrType, IrTypeShape, McpExecutionAttachment,
    OutputCardinality, SemanticIr,
};
use crate::v4::manifest::{SurfaceType, V4SourceManifest, V4Surface};
use crate::v4::naming::normalize_identifier;
use crate::v4::{MCP_IMPORTER_VERSION, V4_ARTIFACT_SCHEMA_VERSION};
use crate::{ManifestError, Result};

use super::model::{McpToolCatalog, McpToolDescriptor};

pub fn normalize_mcp_tool_catalog(catalog: &McpToolCatalog) -> Result<Vec<u8>> {
    let mut normalized = catalog.clone();
    normalized
        .tools
        .sort_by(|left, right| left.name.cmp(&right.name));
    serde_yaml::to_string(&normalized)
        .map(String::into_bytes)
        .map_err(ManifestError::parse_yaml)
}

pub fn import_mcp_surface(
    manifest: &V4SourceManifest,
    surface: &V4Surface,
    catalog: &McpToolCatalog,
) -> Result<SemanticIr> {
    if surface.surface_type != SurfaceType::Mcp {
        return Err(ManifestError::validation(format!(
            "surface '{}' is not an MCP surface",
            surface.id
        )));
    }
    let mut importer = McpImporter::new(manifest, surface);
    importer.import(catalog)
}

struct McpImporter<'a> {
    manifest: &'a V4SourceManifest,
    surface: &'a V4Surface,
    types: BTreeMap<String, IrType>,
    diagnostics: Vec<Diagnostic>,
}

impl<'a> McpImporter<'a> {
    fn new(manifest: &'a V4SourceManifest, surface: &'a V4Surface) -> Self {
        Self {
            manifest,
            surface,
            types: BTreeMap::new(),
            diagnostics: Vec::new(),
        }
    }

    fn import(&mut self, catalog: &McpToolCatalog) -> Result<SemanticIr> {
        let mut operation_ids = BTreeMap::new();
        let mut tools = catalog.tools.iter().collect::<Vec<_>>();
        tools.sort_by(|left, right| left.name.cmp(&right.name));
        let mut operations = Vec::with_capacity(tools.len());
        for tool in tools {
            let operation_id = normalize_identifier(&tool.name, "tool");
            if let Some(existing_tool_name) = operation_ids.get(&operation_id) {
                return Err(ManifestError::validation(format!(
                    "source '{}' surface '{}' imports MCP tools '{}' and '{}' that both normalize to operation id '{}'",
                    self.manifest.common.name,
                    self.surface.id,
                    existing_tool_name,
                    tool.name,
                    operation_id
                )));
            }
            operations.push(self.import_tool(tool, &operation_id));
            operation_ids.insert(operation_id, tool.name.as_str());
        }
        Ok(SemanticIr {
            artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
            source_name: self.manifest.common.name.clone(),
            surface_id: self.surface.id.clone(),
            surface_type: self.surface.surface_type,
            importer_version: MCP_IMPORTER_VERSION.to_string(),
            operations,
            types: self.types.values().cloned().collect(),
            diagnostics: self.diagnostics.clone(),
        })
    }

    fn import_tool(&mut self, tool: &McpToolDescriptor, operation_id: &str) -> IrOperation {
        let inputs = self.import_inputs(tool);
        let output = self.import_output(operation_id, tool.output_schema.as_ref());
        IrOperation {
            id: operation_id.to_string(),
            method_name: "tools/call".to_string(),
            description: tool
                .description
                .clone()
                .or_else(|| tool.title.clone())
                .unwrap_or_default(),
            deprecated: false,
            read_only: tool.read_only_hint.unwrap_or(false),
            inputs,
            output,
            entity: Some(IrEntityCandidate {
                name: operation_id.to_string(),
                type_ref: format!("{operation_id}_row"),
                identity_fields: Vec::new(),
            }),
            execution: IrExecutionAttachment::Mcp(McpExecutionAttachment {
                tool_name: tool.name.clone(),
            }),
            diagnostics: Vec::new(),
        }
    }

    fn import_inputs(&mut self, tool: &McpToolDescriptor) -> Vec<IrOperationInput> {
        let Some(schema) = tool.input_schema.as_object() else {
            return Vec::new();
        };
        let required = schema
            .get("required")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default();
        let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
            return Vec::new();
        };
        properties
            .iter()
            .map(|(name, property)| {
                let data_type = schema_scalar_type(property);
                self.ensure_type_for_scalar(data_type);
                IrOperationInput {
                    name: name.clone(),
                    location: IrInputLocation::ToolArg,
                    required: required.contains(name.as_str()),
                    data_type,
                    default_value: property_default(property),
                    description: schema_description(property),
                }
            })
            .collect()
    }

    fn import_output(
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
                row_path: Vec::new(),
            };
        };
        if schema_type_contains(schema, "array") {
            let item_schema = schema.get("items");
            self.insert_row_type_from_schema(&row_type_id, item_schema);
            return IrOperationOutput {
                cardinality: OutputCardinality::List,
                type_ref: row_type_id,
                row_path: Vec::new(),
            };
        }
        if let Some((array_property, item_schema)) = single_array_property(schema) {
            self.insert_row_type_from_schema(&row_type_id, item_schema);
            return IrOperationOutput {
                cardinality: OutputCardinality::WrappedList,
                type_ref: row_type_id,
                row_path: vec![array_property.to_string()],
            };
        }
        self.insert_row_type_from_schema(&row_type_id, Some(schema));
        IrOperationOutput {
            cardinality: OutputCardinality::Singleton,
            type_ref: row_type_id,
            row_path: Vec::new(),
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
                        },
                        IrField {
                            name: "raw".to_string(),
                            type_ref: json_type,
                            required: false,
                            nullable: true,
                            description: "Raw MCP tool payload.".to_string(),
                        },
                    ],
                },
                nullable: false,
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
            .get("required")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<BTreeSet<_>>()
            })
            .unwrap_or_default();
        let mut fields = properties
            .iter()
            .map(|(name, property)| {
                let data_type = schema_scalar_type(property);
                IrField {
                    name: name.clone(),
                    type_ref: self.ensure_type_for_scalar(data_type),
                    required: required.contains(name.as_str()),
                    nullable: !required.contains(name.as_str()),
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
                nullable: true,
                description: "Raw MCP tool payload.".to_string(),
            });
        }
        self.types.insert(
            type_id.to_string(),
            IrType {
                id: type_id.to_string(),
                shape: IrTypeShape::Object { fields },
                nullable: false,
                description: schema_description(schema),
            },
        );
    }

    fn ensure_type_for_scalar(&mut self, scalar: IrScalarType) -> String {
        let base = match scalar {
            IrScalarType::String => "string",
            IrScalarType::Integer => "integer",
            IrScalarType::Number => "number",
            IrScalarType::Boolean => "boolean",
            IrScalarType::Id => "id",
            IrScalarType::Timestamp => "timestamp",
            IrScalarType::Json => "json",
        };
        let id = format!("mcp_{base}");
        self.types.entry(id.clone()).or_insert_with(|| IrType {
            id: id.clone(),
            shape: IrTypeShape::Scalar(scalar),
            nullable: true,
            description: String::new(),
        });
        id
    }
}

fn schema_scalar_type(schema: &Value) -> IrScalarType {
    if schema_format(schema).is_some_and(|format| matches!(format, "date-time" | "datetime")) {
        return IrScalarType::Timestamp;
    }
    if schema_type_contains(schema, "integer") {
        IrScalarType::Integer
    } else if schema_type_contains(schema, "number") {
        IrScalarType::Number
    } else if schema_type_contains(schema, "boolean") {
        IrScalarType::Boolean
    } else if schema_type_contains(schema, "string") {
        IrScalarType::String
    } else {
        IrScalarType::Json
    }
}

fn schema_type_contains(schema: &Value, expected: &str) -> bool {
    match schema.get("type") {
        Some(Value::String(value)) => value == expected,
        Some(Value::Array(values)) => values
            .iter()
            .filter_map(Value::as_str)
            .any(|value| value == expected),
        _ => false,
    }
}

fn schema_format(schema: &Value) -> Option<&str> {
    schema.get("format").and_then(Value::as_str)
}

fn schema_description(schema: &Value) -> String {
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

fn single_array_property(schema: &Value) -> Option<(&str, Option<&Value>)> {
    if !schema_type_contains(schema, "object") {
        return None;
    }
    let properties = schema.get("properties").and_then(Value::as_object)?;
    let mut arrays = properties
        .iter()
        .filter(|(_name, property)| schema_type_contains(property, "array"));
    let (name, property) = arrays.next()?;
    if arrays.next().is_some() {
        return None;
    }
    Some((name.as_str(), property.get("items")))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::parse_source_manifest_yaml;
    use crate::v4::{ProjectionVisibility, generate_projection_catalog};

    fn tool(name: &str, read_only_hint: Option<bool>) -> McpToolDescriptor {
        McpToolDescriptor {
            name: name.to_string(),
            title: None,
            description: None,
            input_schema: json!({"type": "object", "properties": {}}),
            output_schema: Some(json!({
                "type": "object",
                "properties": {
                    "id": {"type": "string"}
                }
            })),
            read_only_hint,
        }
    }

    #[test]
    fn omitted_read_only_hint_keeps_mcp_projection_hidden() {
        let manifest = parse_source_manifest_yaml(
            r"
name: demo
dsl_version: 4
surfaces:
  - id: mcp
    type: mcp
    server:
      transport: stdio
      command: demo-mcp-server
",
        )
        .expect("manifest");
        let v4 = manifest.as_v4().expect("v4");
        let surface = v4.surfaces.first().expect("surface");
        let catalog = McpToolCatalog {
            tools: vec![tool("list-items", None)],
        };

        let ir = import_mcp_surface(v4, surface, &catalog).expect("import");
        let operation = ir.operations.first().expect("operation");
        assert!(!operation.read_only);

        let projections =
            generate_projection_catalog(v4, std::slice::from_ref(&ir)).expect("projections");
        let projection = projections.projections.first().expect("projection");
        assert_eq!(projection.visibility, ProjectionVisibility::Hidden);
    }

    #[test]
    fn rejects_mcp_tools_that_collide_after_operation_id_normalization() {
        let manifest = parse_source_manifest_yaml(
            r"
name: demo
dsl_version: 4
surfaces:
  - id: mcp
    type: mcp
    server:
      transport: stdio
      command: demo-mcp-server
",
        )
        .expect("manifest");
        let v4 = manifest.as_v4().expect("v4");
        let surface = v4.surfaces.first().expect("surface");
        let catalog = McpToolCatalog {
            tools: vec![tool("foo-bar", Some(true)), tool("foo_bar", Some(true))],
        };

        let error = import_mcp_surface(v4, surface, &catalog)
            .expect_err("normalized collision should fail");
        let message = error.to_string();
        assert!(message.contains("foo-bar"), "{message}");
        assert!(message.contains("foo_bar"), "{message}");
        assert!(
            message.contains("normalize to operation id 'foo_bar'"),
            "{message}"
        );
    }
}
