use std::collections::BTreeMap;

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{IrScalarType, IrType, IrTypeShape, SemanticIr};
use crate::v4::manifest::{SurfaceType, V4SourceManifest, V4Surface};
use crate::v4::naming::normalize_identifier;
use crate::v4::{ImportedSurface, OPERATION_METADATA_GENERATOR_VERSION, OperationMetadataCatalog};
use crate::v4::{MCP_IMPORTER_VERSION, V4_ARTIFACT_SCHEMA_VERSION};
use crate::{ManifestError, Result};

use super::model::McpToolCatalog;

pub fn normalize_mcp_tool_catalog(catalog: &McpToolCatalog) -> Result<Vec<u8>> {
    let mut normalized = catalog.clone();
    normalized
        .tools
        .sort_by(|left, right| left.name.cmp(&right.name));
    serde_yaml::to_string(&normalized)
        .map(String::into_bytes)
        .map_err(ManifestError::serialize_yaml)
}

pub fn import_mcp_surface(
    manifest: &V4SourceManifest,
    surface: &V4Surface,
    catalog: &McpToolCatalog,
) -> Result<ImportedSurface> {
    if surface.surface_type != SurfaceType::Mcp {
        return Err(ManifestError::validation("surface is not an MCP surface"));
    }
    let mut importer = McpImporter::new(manifest, surface);
    importer.import(catalog)
}

pub(super) struct McpImporter<'a> {
    pub(super) manifest: &'a V4SourceManifest,
    pub(super) surface: &'a V4Surface,
    pub(super) types: BTreeMap<String, IrType>,
    pub(super) diagnostics: Vec<Diagnostic>,
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

    fn import(&mut self, catalog: &McpToolCatalog) -> Result<ImportedSurface> {
        let mut operation_ids = BTreeMap::new();
        let mut tools = catalog.tools.iter().collect::<Vec<_>>();
        tools.sort_by(|left, right| left.name.cmp(&right.name));
        let mut operations = Vec::with_capacity(tools.len());
        let mut operation_metadata = BTreeMap::new();
        for tool in tools {
            let operation_id = normalize_identifier(&tool.name, "tool");
            if let Some(existing_tool_name) = operation_ids.get(&operation_id) {
                return Err(ManifestError::validation(format!(
                    "source '{}' surface imports MCP tools '{}' and '{}' that both normalize to operation id '{}'",
                    self.manifest.common.name, existing_tool_name, tool.name, operation_id
                )));
            }
            operation_ids.insert(operation_id.clone(), tool.name.as_str());
            if let Some((operation, metadata)) = self.import_tool(tool, &operation_id) {
                operation_metadata.insert(operation_id, metadata);
                operations.push(operation);
            }
        }
        let semantic_ir = SemanticIr {
            artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
            source_name: self.manifest.common.name.clone(),
            surface_type: self.surface.surface_type,
            importer_version: MCP_IMPORTER_VERSION.to_string(),
            operations,
            types: self.types.values().cloned().collect(),
            diagnostics: self.diagnostics.clone(),
        };
        let operation_metadata = OperationMetadataCatalog {
            artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
            source_name: self.manifest.common.name.clone(),
            generator_version: Some(OPERATION_METADATA_GENERATOR_VERSION.to_string()),
            operations: operation_metadata,
        };
        Ok(ImportedSurface {
            semantic_ir,
            operation_metadata,
        })
    }

    pub(super) fn ensure_type_for_scalar(&mut self, scalar: IrScalarType) -> String {
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

#[cfg(test)]
mod tests {
    use serde_json::{Value, json};

    use super::super::model::McpToolDescriptor;
    use super::*;
    use crate::v4::ir::{IrField, IrOperation, IrScalarType, IrTypeShape, OutputCardinality};
    use crate::v4::{
        OperationMetadata, ProjectionVisibility, SqlInputExposure, generate_projection_catalog,
    };
    use crate::{ValidatedSourceManifest, parse_source_manifest_yaml};

    fn manifest() -> ValidatedSourceManifest {
        parse_source_manifest_yaml(
            r"
name: demo
dsl_version: 4
surface:
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

    fn import_catalog(catalog: &McpToolCatalog) -> ImportedSurface {
        let manifest = manifest();
        let v4 = manifest.as_v4().expect("v4");
        let surface = &v4.surface;
        import_mcp_surface(v4, surface, catalog).expect("import")
    }

    fn operation<'a>(ir: &'a SemanticIr, id: &str) -> &'a IrOperation {
        ir.operations
            .iter()
            .find(|operation| operation.id == id)
            .expect("operation")
    }

    fn assert_no_offset_pagination_for_input_schema(input_schema: Value) {
        let catalog = McpToolCatalog {
            tools: vec![tool_with_schemas(
                "list-catalog",
                input_schema,
                Some(json!({
                    "type": "object",
                    "properties": {
                        "items": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "name": {"type": "string"}
                                }
                            }
                        },
                        "limit": {"type": "integer"},
                        "offset": {"type": "integer"},
                        "has_more": {"type": "boolean"}
                    }
                })),
                Some(true),
            )],
        };

        let ir = import_catalog(&catalog);
        let plan = ir.validated_plan().expect("plan");
        assert!(plan.mcp_pagination("list_catalog").1.is_none());
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
                        "since": {"type": "string", "format": "date-time"},
                        "updated_since": {"format": "date-time"}
                    },
                    "required": ["query"]
                }),
                Some(json!({"type": "object", "properties": {}})),
                Some(true),
            )],
        };

        let ir = import_catalog(&catalog);
        let operation = operation(&ir, "search_items");
        assert!(matches!(
            ir.operation_metadata.operations.get("search_items"),
            Some(OperationMetadata::Mcp { .. })
        ));
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

        let updated_since = operation
            .inputs
            .iter()
            .find(|input| input.name == "updated_since")
            .expect("updated_since input");
        assert_eq!(updated_since.data_type, IrScalarType::Timestamp);
    }

    #[test]
    fn imports_ref_and_all_of_input_schema_properties() {
        let catalog = McpToolCatalog {
            tools: vec![tool_with_schemas(
                "search-items",
                json!({
                    "$defs": {
                        "Pagination": {
                            "type": "object",
                            "properties": {
                                "cursor": {"type": "string"}
                            }
                        }
                    },
                    "allOf": [
                        {"$ref": "#/$defs/Pagination"},
                        {
                            "type": "object",
                            "properties": {
                                "query": {"type": "string"}
                            },
                            "required": ["query"]
                        }
                    ]
                }),
                Some(json!({"type": "object", "properties": {}})),
                Some(true),
            )],
        };

        let ir = import_catalog(&catalog);
        let operation = operation(&ir, "search_items");
        assert!(operation.diagnostics.is_empty());

        let cursor = operation
            .inputs
            .iter()
            .find(|input| input.name == "cursor")
            .expect("cursor input");
        assert_eq!(cursor.data_type, IrScalarType::String);
        assert!(!cursor.required);

        let query = operation
            .inputs
            .iter()
            .find(|input| input.name == "query")
            .expect("query input");
        assert_eq!(query.data_type, IrScalarType::String);
        assert!(query.required);
    }

    #[test]
    fn imports_property_level_input_schema_refs() {
        let catalog = McpToolCatalog {
            tools: vec![tool_with_schemas(
                "search-items",
                json!({
                    "$defs": {
                        "Query": {
                            "type": "string",
                            "description": "Search query"
                        }
                    },
                    "type": "object",
                    "properties": {
                        "query": {"$ref": "#/$defs/Query"}
                    },
                    "required": ["query"]
                }),
                Some(json!({"type": "object", "properties": {}})),
                Some(true),
            )],
        };

        let ir = import_catalog(&catalog);
        let operation = operation(&ir, "search_items");
        assert!(operation.diagnostics.is_empty());

        let query = operation
            .inputs
            .iter()
            .find(|input| input.name == "query")
            .expect("query input");
        assert_eq!(query.data_type, IrScalarType::String);
        assert!(query.required);
        assert_eq!(query.description, "Search query");
    }

    #[test]
    fn imports_recursive_object_property_refs_as_json_inputs() {
        let catalog = McpToolCatalog {
            tools: vec![tool_with_schemas(
                "search-items",
                json!({
                    "$defs": {
                        "Node": {
                            "type": "object",
                            "properties": {
                                "name": {"type": "string"},
                                "children": {
                                    "type": "array",
                                    "items": {"$ref": "#/$defs/Node"}
                                }
                            }
                        }
                    },
                    "type": "object",
                    "properties": {
                        "tree": {"$ref": "#/$defs/Node"}
                    },
                    "required": ["tree"]
                }),
                Some(json!({"type": "object", "properties": {}})),
                Some(true),
            )],
        };

        let ir = import_catalog(&catalog);
        let operation = operation(&ir, "search_items");
        assert!(operation.diagnostics.is_empty());

        let tree = operation
            .inputs
            .iter()
            .find(|input| input.name == "tree")
            .expect("tree input");
        assert_eq!(tree.data_type, IrScalarType::Json);
        assert!(tree.required);
    }

    #[test]
    fn property_level_input_schema_refs_keep_ref_site_metadata() {
        let catalog = McpToolCatalog {
            tools: vec![tool_with_schemas(
                "search-items",
                json!({
                    "$defs": {
                        "Limit": {"type": "integer"}
                    },
                    "type": "object",
                    "properties": {
                        "limit": {
                            "$ref": "#/$defs/Limit",
                            "default": 10,
                            "description": "Page size"
                        }
                    }
                }),
                Some(json!({"type": "object", "properties": {}})),
                Some(true),
            )],
        };

        let ir = import_catalog(&catalog);
        let operation = operation(&ir, "search_items");
        assert!(operation.diagnostics.is_empty());

        let limit = operation
            .inputs
            .iter()
            .find(|input| input.name == "limit")
            .expect("limit input");
        assert_eq!(limit.data_type, IrScalarType::Integer);
        assert_eq!(limit.default_value.as_deref(), Some("10"));
        assert_eq!(limit.description, "Page size");
    }

    #[test]
    fn property_level_input_schema_refs_ignore_ref_site_validation_siblings() {
        let catalog = McpToolCatalog {
            tools: vec![tool_with_schemas(
                "search-items",
                json!({
                    "$defs": {
                        "Limit": {"type": "integer"}
                    },
                    "type": "object",
                    "properties": {
                        "limit": {
                            "$ref": "#/$defs/Limit",
                            "type": "string",
                            "default": 10
                        }
                    }
                }),
                Some(json!({"type": "object", "properties": {}})),
                Some(true),
            )],
        };

        let ir = import_catalog(&catalog);
        let operation = operation(&ir, "search_items");
        assert!(operation.diagnostics.is_empty());

        let limit = operation
            .inputs
            .iter()
            .find(|input| input.name == "limit")
            .expect("limit input");
        assert_eq!(limit.data_type, IrScalarType::Integer);
        assert_eq!(limit.default_value.as_deref(), Some("10"));
    }

    #[test]
    fn unresolved_input_schema_refs_means_tool_is_not_exposed() {
        let catalog = McpToolCatalog {
            tools: vec![tool_with_schemas(
                "search-items",
                json!({
                    "$ref": "#/$defs/MissingInputSchema"
                }),
                Some(json!({"type": "object", "properties": {}})),
                Some(true),
            )],
        };

        let ir = import_catalog(&catalog);
        assert!(ir.operations.is_empty());
        assert!(
            ir.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "MCP_INPUT_SCHEMA_REF_NOT_FOUND")
        );

        let projections = generate_projection_catalog(
            manifest().as_v4().expect("v4"),
            &ir.validated_plan().expect("plan"),
        )
        .expect("projections");
        assert_eq!(projections.projections.len(), 0);
    }

    #[test]
    fn recursive_input_schema_refs_mean_tool_is_not_exposed() {
        let catalog = McpToolCatalog {
            tools: vec![tool_with_schemas(
                "search-items",
                json!({
                    "$defs": {
                        "A": {
                            "allOf": [
                                {"$ref": "#/$defs/B"}
                            ]
                        },
                        "B": {
                            "allOf": [
                                {"$ref": "#/$defs/A"}
                            ]
                        }
                    },
                    "allOf": [
                        {"$ref": "#/$defs/A"}
                    ]
                }),
                Some(json!({"type": "object", "properties": {}})),
                Some(true),
            )],
        };

        let ir = import_catalog(&catalog);
        assert!(ir.operations.is_empty());
        assert!(
            ir.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "MCP_INPUT_SCHEMA_REF_UNSUPPORTED")
        );
    }

    #[test]
    fn missing_required_input_schema_properties_mean_tool_is_not_exposed() {
        let catalog = McpToolCatalog {
            tools: vec![tool_with_schemas(
                "search-items",
                json!({
                    "type": "object",
                    "properties": {
                        "limit": {"type": "integer"}
                    },
                    "required": ["query"]
                }),
                Some(json!({"type": "object", "properties": {}})),
                Some(true),
            )],
        };

        let ir = import_catalog(&catalog);
        assert!(ir.operations.is_empty());
        assert!(
            ir.diagnostics.iter().any(|diagnostic| {
                diagnostic.code == "MCP_INPUT_SCHEMA_REQUIRED_PROPERTY_MISSING"
            })
        );
    }

    #[test]
    fn imports_all_of_properties_with_metadata_only_differences() {
        let catalog = McpToolCatalog {
            tools: vec![tool_with_schemas(
                "search-items",
                json!({
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {
                                "query": {
                                    "type": "string",
                                    "title": "Query"
                                }
                            }
                        },
                        {
                            "type": "object",
                            "properties": {
                                "query": {
                                    "type": "string",
                                    "description": "Search query"
                                }
                            },
                            "required": ["query"]
                        }
                    ]
                }),
                Some(json!({"type": "object", "properties": {}})),
                Some(true),
            )],
        };

        let ir = import_catalog(&catalog);
        let operation = operation(&ir, "search_items");
        assert!(operation.diagnostics.is_empty());

        let query = operation
            .inputs
            .iter()
            .find(|input| input.name == "query")
            .expect("query input");
        assert_eq!(query.data_type, IrScalarType::String);
        assert!(query.required);
        assert_eq!(query.description, "Search query");
    }

    #[test]
    fn imports_all_of_properties_with_equivalent_type_union_ordering() {
        let catalog = McpToolCatalog {
            tools: vec![tool_with_schemas(
                "search-items",
                json!({
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {
                                "query": {"type": ["string", "null"]}
                            }
                        },
                        {
                            "type": "object",
                            "properties": {
                                "query": {"type": ["null", "string"]}
                            },
                            "required": ["query"]
                        }
                    ]
                }),
                Some(json!({"type": "object", "properties": {}})),
                Some(true),
            )],
        };

        let ir = import_catalog(&catalog);
        let operation = operation(&ir, "search_items");
        assert!(operation.diagnostics.is_empty());

        let query = operation
            .inputs
            .iter()
            .find(|input| input.name == "query")
            .expect("query input");
        assert_eq!(query.data_type, IrScalarType::String);
        assert!(query.required);
    }

    #[test]
    fn imports_all_of_properties_with_equivalent_nested_refs() {
        let catalog = McpToolCatalog {
            tools: vec![tool_with_schemas(
                "search-items",
                json!({
                    "$defs": {
                        "Id": {"type": "string"}
                    },
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {
                                "filter": {
                                    "type": "object",
                                    "properties": {
                                        "id": {"$ref": "#/$defs/Id"}
                                    }
                                }
                            }
                        },
                        {
                            "type": "object",
                            "properties": {
                                "filter": {
                                    "type": "object",
                                    "properties": {
                                        "id": {"type": "string"}
                                    }
                                }
                            }
                        }
                    ]
                }),
                Some(json!({"type": "object", "properties": {}})),
                Some(true),
            )],
        };

        let ir = import_catalog(&catalog);
        let operation = operation(&ir, "search_items");
        assert!(operation.diagnostics.is_empty());

        let filter = operation
            .inputs
            .iter()
            .find(|input| input.name == "filter")
            .expect("filter input");
        assert_eq!(filter.data_type, IrScalarType::Json);
    }

    #[test]
    fn imports_all_of_properties_with_ref_site_default_metadata() {
        let catalog = McpToolCatalog {
            tools: vec![tool_with_schemas(
                "search-items",
                json!({
                    "$defs": {
                        "Limit": {"type": "integer"}
                    },
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {
                                "limit": {
                                    "$ref": "#/$defs/Limit",
                                    "default": 10
                                }
                            }
                        },
                        {
                            "type": "object",
                            "properties": {
                                "limit": {"type": "integer"}
                            }
                        }
                    ]
                }),
                Some(json!({"type": "object", "properties": {}})),
                Some(true),
            )],
        };

        let ir = import_catalog(&catalog);
        let operation = operation(&ir, "search_items");
        assert!(operation.diagnostics.is_empty());

        let limit = operation
            .inputs
            .iter()
            .find(|input| input.name == "limit")
            .expect("limit input");
        assert_eq!(limit.data_type, IrScalarType::Integer);
        assert_eq!(limit.default_value.as_deref(), Some("10"));
    }

    #[test]
    fn all_of_conflicts_on_nested_property_named_like_annotation() {
        let catalog = McpToolCatalog {
            tools: vec![tool_with_schemas(
                "search-items",
                json!({
                    "allOf": [
                        {
                            "type": "object",
                            "properties": {
                                "filter": {
                                    "type": "object",
                                    "properties": {
                                        "description": {"type": "string"}
                                    }
                                }
                            }
                        },
                        {
                            "type": "object",
                            "properties": {
                                "filter": {
                                    "type": "object",
                                    "properties": {
                                        "description": {"type": "integer"}
                                    }
                                }
                            }
                        }
                    ]
                }),
                Some(json!({"type": "object", "properties": {}})),
                Some(true),
            )],
        };

        let ir = import_catalog(&catalog);
        assert!(ir.operations.is_empty());
        assert!(
            ir.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "MCP_INPUT_SCHEMA_CONFLICT")
        );
    }

    #[test]
    fn hides_tools_with_unsupported_composed_input_schemas() {
        let catalog = McpToolCatalog {
            tools: vec![tool_with_schemas(
                "search-items",
                json!({
                    "anyOf": [
                        {
                            "type": "object",
                            "properties": {
                                "query": {"type": "string"}
                            },
                            "required": ["query"]
                        },
                        {
                            "type": "object",
                            "properties": {
                                "id": {"type": "string"}
                            },
                            "required": ["id"]
                        }
                    ]
                }),
                Some(json!({"type": "object", "properties": {}})),
                Some(true),
            )],
        };
        let ir = import_catalog(&catalog);
        assert!(ir.operations.is_empty());
        assert!(
            ir.diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "MCP_INPUT_SCHEMA_COMPOSITION_UNSUPPORTED")
        );

        let projections = generate_projection_catalog(
            manifest().as_v4().expect("v4"),
            &ir.validated_plan().expect("plan"),
        )
        .expect("projections");
        assert_eq!(projections.projections.len(), 0);
    }

    #[test]
    fn imports_output_cardinalities_and_row_types_without_unwrapping_objects() {
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
            OutputCardinality::Singleton
        );
        let wrapped_fields = row_fields(&ir, "wrapped_items_row");
        assert_eq!(field(wrapped_fields, "items").type_ref, "mcp_json");
        assert!(wrapped_fields.iter().any(|field| field.name == "raw"));

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
    fn does_not_infer_cursor_pagination_for_wrapped_list_envelopes() {
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
        assert_eq!(operation.output.cardinality, OutputCardinality::Singleton);
        let plan = ir.validated_plan().expect("plan");
        let (cursor, offset) = plan.mcp_pagination("list_items");
        assert!(cursor.is_none());
        assert!(offset.is_none());
    }

    #[test]
    fn does_not_infer_offset_pagination_for_wrapped_list_envelopes() {
        let catalog = McpToolCatalog {
            tools: vec![tool_with_schemas(
                "list-catalog",
                json!({
                    "type": "object",
                    "properties": {
                        "limit": {
                            "type": "integer",
                            "minimum": 1,
                            "maximum": 200,
                            "default": 50
                        },
                        "offset": {
                            "type": "integer",
                            "minimum": 0,
                            "maximum": 4_294_967_295_u64,
                            "default": 0
                        }
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
                                    "name": {"type": "string"}
                                }
                            }
                        },
                        "total": {"type": "integer"},
                        "limit": {"type": "integer"},
                        "offset": {"type": "integer"},
                        "has_more": {"type": "boolean"}
                    }
                })),
                Some(true),
            )],
        };

        let manifest = manifest();
        let v4 = manifest.as_v4().expect("v4");
        let surface = &v4.surface;
        let ir = import_mcp_surface(v4, surface, &catalog).expect("import");
        let plan = ir.validated_plan().expect("plan");
        let (cursor, offset) = plan.mcp_pagination("list_catalog");
        assert!(cursor.is_none());
        assert!(offset.is_none());

        let projections = generate_projection_catalog(v4, &ir.validated_plan().expect("plan"))
            .expect("projection catalog");
        let projection = projections
            .projections
            .iter()
            .find(|projection| projection.operation_id == "list_catalog")
            .expect("projection");
        assert!(matches!(
            projection.kind,
            crate::v4::ProjectionKind::TableFunction { .. }
        ));
        for input in &projection.inputs {
            assert_eq!(input.sql_exposure, SqlInputExposure::FunctionArg);
        }
    }

    #[test]
    fn offset_pagination_requires_positive_limit_default() {
        assert_no_offset_pagination_for_input_schema(json!({
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 200,
                    "default": 0
                },
                "offset": {
                    "type": "integer",
                    "minimum": 0,
                    "default": 0
                }
            }
        }));
    }

    #[test]
    fn offset_pagination_requires_bounded_limit_maximum() {
        assert_no_offset_pagination_for_input_schema(json!({
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "default": 50
                },
                "offset": {
                    "type": "integer",
                    "minimum": 0,
                    "default": 0
                }
            }
        }));
    }

    #[test]
    fn offset_pagination_requires_offset_zero_to_be_allowed() {
        assert_no_offset_pagination_for_input_schema(json!({
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 200,
                    "default": 50
                },
                "offset": {
                    "type": "integer",
                    "minimum": 1,
                    "default": 0
                }
            }
        }));
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
    }

    #[test]
    fn omitted_read_only_hint_keeps_mcp_projection_hidden() {
        let manifest = manifest();
        let v4 = manifest.as_v4().expect("v4");
        let surface = &v4.surface;
        let catalog = McpToolCatalog {
            tools: vec![tool("list-items", None)],
        };

        let ir = import_mcp_surface(v4, surface, &catalog).expect("import");
        let operation = ir.operations.first().expect("operation");
        assert!(!operation.read_only);

        let projections = generate_projection_catalog(v4, &ir.validated_plan().expect("plan"))
            .expect("projections");
        let projection = projections.projections.first().expect("projection");
        assert_eq!(projection.visibility, ProjectionVisibility::Hidden);
    }

    #[test]
    fn rejects_mcp_tools_that_collide_after_operation_id_normalization() {
        let manifest = manifest();
        let v4 = manifest.as_v4().expect("v4");
        let surface = &v4.surface;
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

    #[test]
    fn rejects_mcp_tools_that_collide_even_when_one_tool_is_not_exposed() {
        let manifest = manifest();
        let v4 = manifest.as_v4().expect("v4");
        let surface = &v4.surface;
        let catalog = McpToolCatalog {
            tools: vec![
                tool_with_schemas(
                    "foo-bar",
                    json!({"$ref": "#/$defs/MissingInputSchema"}),
                    Some(json!({"type": "object", "properties": {}})),
                    Some(true),
                ),
                tool("foo_bar", Some(true)),
            ],
        };

        let error = import_mcp_surface(v4, surface, &catalog)
            .expect_err("normalized collision should fail before exposure filtering");
        let message = error.to_string();
        assert!(message.contains("foo-bar"), "{message}");
        assert!(message.contains("foo_bar"), "{message}");
        assert!(
            message.contains("normalize to operation id 'foo_bar'"),
            "{message}"
        );
    }
}
