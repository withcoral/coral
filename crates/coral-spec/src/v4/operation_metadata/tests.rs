use crate::parse_source_manifest_yaml;
use crate::v4::{
    IrInputLocation, IrOperationInput, IrScalarType, McpToolCatalog, McpToolDescriptor,
    OperationMetadata, import_mcp_surface, import_openapi_surface,
};

fn imported() -> (crate::v4::V4SourceManifest, super::ImportedSurface) {
    let manifest = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
surface:
  type: openapi
  file: /tmp/openapi.yaml
  base_url: https://api.example.com
",
    )
    .expect("manifest")
    .as_v4()
    .expect("v4")
    .clone();
    let imported = import_openapi_surface(
        &manifest,
        &manifest.surface,
        br"
openapi: 3.0.3
paths:
  /items:
    get:
      operationId: items/list
      parameters:
        - {name: page, in: query, schema: {type: integer, default: 1}}
        - {name: per_page, in: query, schema: {type: integer, default: 25, maximum: 100}}
        - {name: state, in: query, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema: {type: array, items: {type: object}}
",
    )
    .expect("import");
    (manifest, imported)
}

fn imported_mcp() -> super::ImportedSurface {
    let manifest = parse_source_manifest_yaml(
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
    .expect("manifest");
    let v4 = manifest.as_v4().expect("v4");
    let catalog = McpToolCatalog {
        tools: vec![McpToolDescriptor {
            name: "search_issues".to_string(),
            title: None,
            description: None,
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {"query": {"type": "string"}}
            }),
            output_schema: Some(serde_json::json!({
                "type": "object",
                "properties": {"id": {"type": "string"}}
            })),
            read_only_hint: Some(true),
        }],
    };
    import_mcp_surface(v4, &v4.surface, &catalog).expect("import")
}

#[test]
fn plan_rejects_rest_operation_with_tool_arg_input() {
    let (_manifest, mut imported) = imported();
    imported
        .semantic_ir
        .operations
        .first_mut()
        .expect("operation")
        .inputs
        .push(IrOperationInput {
            name: "tool_input".to_string(),
            location: IrInputLocation::ToolArg,
            required: false,
            data_type: IrScalarType::String,
            default_value: None,
            description: String::new(),
        });

    let error = imported
        .validated_plan()
        .expect_err("REST operation with tool-arg input must fail");

    assert!(
        error
            .to_string()
            .contains("does not match its execution type"),
        "unexpected error: {error}"
    );
}

#[test]
fn plan_rejects_mcp_operation_with_non_tool_arg_input() {
    let mut imported = imported_mcp();
    imported
        .semantic_ir
        .operations
        .first_mut()
        .expect("operation")
        .inputs
        .iter_mut()
        .for_each(|input| input.location = IrInputLocation::Query);

    let error = imported
        .validated_plan()
        .expect_err("MCP operation with query input must fail");

    assert!(
        error
            .to_string()
            .contains("does not match its execution type"),
        "unexpected error: {error}"
    );
}

#[test]
fn plan_rejects_mcp_offset_pagination_starting_past_first_page() {
    let mut imported = imported_mcp();
    let operation = imported
        .semantic_ir
        .operations
        .first_mut()
        .expect("operation");
    for name in ["limit", "offset"] {
        operation.inputs.push(IrOperationInput {
            name: name.to_string(),
            location: IrInputLocation::ToolArg,
            required: false,
            data_type: IrScalarType::Integer,
            default_value: None,
            description: String::new(),
        });
    }
    let operation_id = operation.id.clone();
    let metadata_with_offset_start = |offset_start| OperationMetadata::Mcp {
        row_path: Vec::new(),
        pagination: crate::v4::McpOperationPagination {
            cursor: None,
            offset: Some(crate::backends::mcp::McpOffsetPaginationSpec {
                limit_arg: "limit".to_string(),
                default_limit: 50,
                max_limit: 100,
                offset_arg: "offset".to_string(),
                offset_start,
                max_pages: None,
            }),
        },
    };

    *imported
        .operation_metadata
        .operations
        .get_mut(&operation_id)
        .expect("metadata") = metadata_with_offset_start(0);
    imported
        .validated_plan()
        .expect("offset pagination starting at 0 must validate");

    *imported
        .operation_metadata
        .operations
        .get_mut(&operation_id)
        .expect("metadata") = metadata_with_offset_start(1);
    let error = imported
        .validated_plan()
        .expect_err("nonzero offset_start must fail");
    assert!(
        error.to_string().contains("offset_start must be 0"),
        "unexpected error: {error}"
    );
}

#[test]
fn semantic_ir_serialization_contains_facts_not_inferred_policy() {
    let (_manifest, imported) = imported();
    let yaml = serde_yaml::to_string(&imported.semantic_ir).expect("semantic IR YAML");

    assert!(
        !yaml.contains("pagination:"),
        "unexpected policy in IR: {yaml}"
    );
    assert!(
        !yaml.contains("lookup_keys:"),
        "unexpected policy in IR: {yaml}"
    );
    assert!(
        !yaml.contains("row_path:"),
        "unexpected policy in IR: {yaml}"
    );
    assert!(matches!(
        imported.operation_metadata.operations.values().next(),
        Some(OperationMetadata::Rest { pagination, lookup_keys, .. })
            if pagination.page_param.as_deref() == Some("page")
                && lookup_keys == &["state"]
    ));
}

#[test]
fn plan_rejects_blank_or_unresolvable_rest_row_paths() {
    let (_manifest, mut imported) = imported();
    let OperationMetadata::Rest { row_path, .. } = imported
        .operation_metadata
        .operations
        .values_mut()
        .next()
        .expect("metadata")
    else {
        panic!("expected REST metadata");
    };
    *row_path = vec![" ".to_string()];
    let error = imported
        .validated_plan()
        .expect_err("blank row path must fail");
    assert!(
        error.to_string().contains("blank or padded segment"),
        "unexpected error: {error}"
    );

    let OperationMetadata::Rest { row_path, .. } = imported
        .operation_metadata
        .operations
        .values_mut()
        .next()
        .expect("metadata")
    else {
        panic!("expected REST metadata");
    };
    // The operation returns a declared array, so no path can traverse it.
    *row_path = vec!["missing".to_string()];
    let error = imported
        .validated_plan()
        .expect_err("unresolvable row path must fail");
    assert!(
        error.to_string().contains("traverses non-object type"),
        "unexpected error: {error}"
    );
}

#[test]
fn operation_metadata_without_a_row_path_defaults_to_the_response_root() {
    let metadata: OperationMetadata = serde_yaml::from_str(
        r"
type: rest
pagination:
  mode: none
lookup_keys: []
",
    )
    .expect("metadata without a row path");

    assert!(metadata.row_path().is_empty());
}

#[test]
fn disabled_rest_pagination_serializes_only_its_mode() {
    let metadata = OperationMetadata::Rest {
        row_path: Vec::new(),
        pagination: crate::PaginationSpec::default(),
        lookup_keys: Vec::new(),
    };

    let yaml = serde_yaml::to_string(&metadata).expect("operation metadata YAML");
    let value: serde_yaml::Value = serde_yaml::from_str(&yaml).expect("metadata value");
    let pagination = value
        .get("pagination")
        .and_then(serde_yaml::Value::as_mapping)
        .expect("pagination mapping");

    assert_eq!(pagination.len(), 1, "unexpected pagination fields: {yaml}");
    assert_eq!(
        pagination.get("mode"),
        Some(&serde_yaml::Value::String("none".to_string()))
    );

    let decoded: OperationMetadata = serde_yaml::from_str(&yaml).expect("round trip");
    assert!(matches!(
        decoded,
        OperationMetadata::Rest { pagination, .. }
            if pagination.mode == crate::PaginationMode::None
                && pagination.page_step == 1
    ));
}

#[test]
fn plan_rejects_settings_for_disabled_rest_pagination() {
    let (_manifest, mut imported) = imported();
    let OperationMetadata::Rest { pagination, .. } = imported
        .operation_metadata
        .operations
        .values_mut()
        .next()
        .expect("metadata")
    else {
        panic!("REST metadata")
    };
    *pagination = crate::PaginationSpec {
        page_param: Some("page".to_string()),
        ..crate::PaginationSpec::default()
    };

    let error = imported
        .validated_plan()
        .expect_err("disabled pagination settings must fail");

    assert!(
        error
            .to_string()
            .contains("pagination.mode=none cannot define other pagination settings"),
        "unexpected error: {error}"
    );
}

#[test]
fn plan_requires_complete_exact_operation_metadata() {
    let (_manifest, imported) = imported();
    let mut missing = imported.operation_metadata.clone();
    missing.operations.clear();
    let error = super::ValidatedSurfacePlan::new(imported.semantic_ir.clone(), missing)
        .expect_err("missing metadata must fail");
    assert!(error.to_string().contains("is missing operation"));

    let mut extra = imported.operation_metadata.clone();
    let value = extra.operations.values().next().expect("metadata").clone();
    extra.operations.insert("unknown".to_string(), value);
    let error = super::ValidatedSurfacePlan::new(imported.semantic_ir, extra)
        .expect_err("unknown metadata must fail");
    assert!(error.to_string().contains("unknown operation"));
}

#[test]
fn plan_rejects_dangling_operation_output_type_reference() {
    let (_manifest, mut imported) = imported();
    imported
        .semantic_ir
        .operations
        .first_mut()
        .expect("operation")
        .output
        .type_ref = "missing_type".to_string();

    let error = imported
        .validated_plan()
        .expect_err("dangling output must fail");

    assert!(
        error
            .to_string()
            .contains("output references missing type 'missing_type'"),
        "unexpected error: {error}"
    );
}

#[test]
fn plan_rejects_dangling_nested_type_reference() {
    let (_manifest, mut imported) = imported();
    imported.semantic_ir.types.push(crate::v4::IrType {
        id: "dangling_list".to_string(),
        shape: crate::v4::IrTypeShape::List {
            item_type_ref: "missing_item_type".to_string(),
        },
        nullable: false,
        description: String::new(),
    });

    let error = imported
        .validated_plan()
        .expect_err("dangling field must fail");

    assert!(
        error
            .to_string()
            .contains("references missing type 'missing_item_type'"),
        "unexpected error: {error}"
    );
}

#[test]
fn plan_rejects_lookup_keys_owned_by_pagination() {
    let (_manifest, mut imported) = imported();
    let OperationMetadata::Rest { lookup_keys, .. } = imported
        .operation_metadata
        .operations
        .values_mut()
        .next()
        .expect("metadata")
    else {
        panic!("REST metadata")
    };
    lookup_keys.push("page".to_string());

    let error = imported.validated_plan().expect_err("overlap must fail");
    assert!(
        error
            .to_string()
            .contains("both pagination-owned and a lookup key")
    );
}
