use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::backends::mcp::McpPaginationSpec;
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
        let pagination = infer_mcp_pagination(&inputs, &output, tool.output_schema.as_ref());
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
                pagination,
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
        if let Some((array_property, item_schema)) = wrapped_list_property(schema) {
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

fn wrapped_list_property(schema: &Value) -> Option<(&str, Option<&Value>)> {
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
    if properties.len() != 1 && find_response_cursor_path(schema).is_none() {
        return None;
    }
    Some((name.as_str(), property.get("items")))
}

fn infer_mcp_pagination(
    inputs: &[IrOperationInput],
    output: &IrOperationOutput,
    output_schema: Option<&Value>,
) -> Option<McpPaginationSpec> {
    if !matches!(
        output.cardinality,
        OutputCardinality::List | OutputCardinality::WrappedList
    ) {
        return None;
    }
    let cursor_arg = cursor_input_name(inputs)?;
    let response_cursor_path = find_response_cursor_path(output_schema?)?;
    Some(McpPaginationSpec {
        cursor_arg: cursor_arg.to_string(),
        response_cursor_path,
        max_pages: None,
    })
}

fn cursor_input_name(inputs: &[IrOperationInput]) -> Option<&str> {
    const CURSOR_INPUTS: &[&str] = &[
        "cursor",
        "after",
        "page_token",
        "pagetoken",
        "next_cursor",
        "nextcursor",
        "next_token",
        "nexttoken",
    ];
    inputs
        .iter()
        .filter(|input| !input.required)
        .find(|input| {
            let normalized = cursor_token(&input.name);
            CURSOR_INPUTS.contains(&normalized.as_str())
        })
        .map(|input| input.name.as_str())
}

fn find_response_cursor_path(schema: &Value) -> Option<Vec<String>> {
    let properties = schema.get("properties").and_then(Value::as_object)?;
    for (name, property) in properties {
        if is_response_cursor_property(name, property) {
            return Some(vec![name.clone()]);
        }
    }
    for (name, property) in properties {
        if !schema_type_contains(property, "object") {
            continue;
        }
        if let Some(mut path) = find_response_cursor_path(property) {
            path.insert(0, name.clone());
            return Some(path);
        }
    }
    None
}

fn is_response_cursor_property(name: &str, schema: &Value) -> bool {
    const RESPONSE_CURSORS: &[&str] = &["nextcursor", "nextpagetoken", "nexttoken", "endcursor"];
    RESPONSE_CURSORS.contains(&cursor_token(name).as_str())
        && (schema_type_contains(schema, "string") || schema.get("type").is_none())
}

fn cursor_token(value: &str) -> String {
    value
        .chars()
        .filter(char::is_ascii_alphanumeric)
        .flat_map(char::to_lowercase)
        .collect()
}

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::*;
    use crate::v4::{ProjectionVisibility, generate_projection_catalog};
    use crate::{ValidatedSourceManifest, parse_source_manifest_yaml};

    fn manifest() -> ValidatedSourceManifest {
        parse_source_manifest_yaml(
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
        .expect("manifest")
    }

    fn tool(name: &str, read_only_hint: Option<bool>) -> McpToolDescriptor {
        tool_with_schemas(
            name,
            json!({"type": "object", "properties": {}}),
            Some(json!({
                "type": "object",
                "properties": {
                    "id": {"type": "string"}
                }
            })),
            read_only_hint,
        )
    }

    fn tool_with_schemas(
        name: &str,
        input_schema: Value,
        output_schema: Option<Value>,
        read_only_hint: Option<bool>,
    ) -> McpToolDescriptor {
        McpToolDescriptor {
            name: name.to_string(),
            title: None,
            description: None,
            input_schema,
            output_schema,
            read_only_hint,
        }
    }

    fn import_catalog(catalog: &McpToolCatalog) -> SemanticIr {
        let manifest = manifest();
        let v4 = manifest.as_v4().expect("v4");
        let surface = v4.surfaces.first().expect("surface");
        import_mcp_surface(v4, surface, catalog).expect("import")
    }

    fn operation<'a>(ir: &'a SemanticIr, id: &str) -> &'a IrOperation {
        ir.operations
            .iter()
            .find(|operation| operation.id == id)
            .expect("operation")
    }

    fn row_fields<'a>(ir: &'a SemanticIr, type_id: &str) -> &'a [IrField] {
        let row_type = ir
            .types
            .iter()
            .find(|ty| ty.id == type_id)
            .expect("row type");
        let IrTypeShape::Object { fields } = &row_type.shape else {
            panic!("row type should be an object");
        };
        fields
    }

    fn field<'a>(fields: &'a [IrField], name: &str) -> &'a IrField {
        fields
            .iter()
            .find(|field| field.name == name)
            .expect("field")
    }

    #[test]
    fn imports_input_schema_types_required_flags_and_defaults() {
        let catalog = McpToolCatalog {
            tools: vec![tool_with_schemas(
                "search-items",
                json!({
                    "type": "object",
                    "properties": {
                        "query": {"type": ["string", "null"], "description": "Search query"},
                        "limit": {"type": "integer", "default": 10},
                        "exact": {"type": "boolean", "default": true},
                        "since": {"type": "string", "format": "date-time"}
                    },
                    "required": ["query"]
                }),
                Some(json!({"type": "object", "properties": {}})),
                Some(true),
            )],
        };

        let ir = import_catalog(&catalog);
        let operation = operation(&ir, "search_items");
        let query = operation
            .inputs
            .iter()
            .find(|input| input.name == "query")
            .expect("query input");
        assert_eq!(query.data_type, IrScalarType::String);
        assert!(query.required);
        assert_eq!(query.description, "Search query");

        let limit = operation
            .inputs
            .iter()
            .find(|input| input.name == "limit")
            .expect("limit input");
        assert_eq!(limit.data_type, IrScalarType::Integer);
        assert_eq!(limit.default_value.as_deref(), Some("10"));

        let exact = operation
            .inputs
            .iter()
            .find(|input| input.name == "exact")
            .expect("exact input");
        assert_eq!(exact.data_type, IrScalarType::Boolean);
        assert_eq!(exact.default_value.as_deref(), Some("true"));

        let since = operation
            .inputs
            .iter()
            .find(|input| input.name == "since")
            .expect("since input");
        assert_eq!(since.data_type, IrScalarType::Timestamp);
    }

    #[test]
    fn imports_output_cardinalities_and_row_types() {
        let catalog = McpToolCatalog {
            tools: vec![
                tool_with_schemas(
                    "list-items",
                    json!({"type": "object", "properties": {}}),
                    Some(json!({
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": {"type": "string", "description": "Item id"},
                                "count": {"type": ["integer", "null"]}
                            },
                            "required": ["id"]
                        }
                    })),
                    Some(true),
                ),
                tool_with_schemas(
                    "wrapped-items",
                    json!({"type": "object", "properties": {}}),
                    Some(json!({
                        "type": "object",
                        "properties": {
                            "items": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "enabled": {"type": "boolean"}
                                    }
                                }
                            }
                        }
                    })),
                    Some(true),
                ),
                tool("get-item", Some(true)),
                tool_with_schemas(
                    "no-schema",
                    json!({"type": "object", "properties": {}}),
                    None,
                    Some(true),
                ),
            ],
        };

        let ir = import_catalog(&catalog);

        let list_items = operation(&ir, "list_items");
        assert_eq!(list_items.output.cardinality, OutputCardinality::List);
        let list_fields = row_fields(&ir, "list_items_row");
        let id = field(list_fields, "id");
        assert_eq!(id.type_ref, "mcp_string");
        assert!(id.required);
        assert!(!id.nullable);
        assert_eq!(id.description, "Item id");
        let count = field(list_fields, "count");
        assert_eq!(count.type_ref, "mcp_integer");
        assert!(!count.required);
        assert!(count.nullable);
        assert!(list_fields.iter().any(|field| field.name == "raw"));

        let wrapped_items = operation(&ir, "wrapped_items");
        assert_eq!(
            wrapped_items.output.cardinality,
            OutputCardinality::WrappedList
        );
        assert_eq!(wrapped_items.output.row_path, vec!["items".to_string()]);
        let wrapped_fields = row_fields(&ir, "wrapped_items_row");
        assert_eq!(field(wrapped_fields, "enabled").type_ref, "mcp_boolean");

        let get_item = operation(&ir, "get_item");
        assert_eq!(get_item.output.cardinality, OutputCardinality::Singleton);
        let get_item_fields = row_fields(&ir, "get_item_row");
        assert_eq!(field(get_item_fields, "id").type_ref, "mcp_string");

        let no_schema = operation(&ir, "no_schema");
        assert_eq!(no_schema.output.cardinality, OutputCardinality::Singleton);
        let generic_fields = row_fields(&ir, "no_schema_row");
        assert_eq!(field(generic_fields, "result").type_ref, "mcp_json");
        assert_eq!(field(generic_fields, "raw").type_ref, "mcp_json");
    }

    #[test]
    fn infers_cursor_pagination_for_wrapped_list_envelopes() {
        let catalog = McpToolCatalog {
            tools: vec![tool_with_schemas(
                "list-items",
                json!({
                    "type": "object",
                    "properties": {
                        "cursor": {"type": "string"},
                        "limit": {"type": "integer"}
                    }
                }),
                Some(json!({
                    "type": "object",
                    "properties": {
                        "items": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "id": {"type": "string"}
                                }
                            }
                        },
                        "meta": {
                            "type": "object",
                            "properties": {
                                "nextCursor": {"type": ["string", "null"]}
                            }
                        }
                    }
                })),
                Some(true),
            )],
        };

        let ir = import_catalog(&catalog);
        let operation = operation(&ir, "list_items");
        assert_eq!(operation.output.cardinality, OutputCardinality::WrappedList);
        assert_eq!(operation.output.row_path, vec!["items".to_string()]);
        let IrExecutionAttachment::Mcp(mcp) = &operation.execution else {
            panic!("expected MCP execution");
        };
        let pagination = mcp.pagination.as_ref().expect("pagination");
        assert_eq!(pagination.cursor_arg, "cursor");
        assert_eq!(pagination.response_cursor_path, ["meta", "nextCursor"]);
        assert_eq!(pagination.max_pages, None);
    }

    #[test]
    fn object_with_sibling_array_without_cursor_stays_singleton() {
        let catalog = McpToolCatalog {
            tools: vec![tool_with_schemas(
                "get-item",
                json!({"type": "object", "properties": {}}),
                Some(json!({
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "items": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "id": {"type": "string"}
                                }
                            }
                        }
                    }
                })),
                Some(true),
            )],
        };

        let ir = import_catalog(&catalog);
        let operation = operation(&ir, "get_item");
        assert_eq!(operation.output.cardinality, OutputCardinality::Singleton);
        assert!(operation.output.row_path.is_empty());
    }

    #[test]
    fn omitted_read_only_hint_keeps_mcp_projection_hidden() {
        let manifest = manifest();
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
        let manifest = manifest();
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
