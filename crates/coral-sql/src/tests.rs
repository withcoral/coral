use coral_capabilities::{
    Capability, EffectProfile, FileArtifactRef, FileFormatDescriptor, FileScanBinding,
    GraphqlOperationBinding, GraphqlOperationKind, GraphqlVariableBinding, HttpMethod,
    InvocationSchema, McpTaskSupport, McpToolUpstreamBinding, PaginationHint, PaginationKind,
    ProviderOrigin, ProviderOriginKind, RestUpstreamBinding, ShapeHints, SourceId, UpstreamBinding,
};
use coral_exports::{BindingBuildContext, SourceKey, SqlRowShape};
use datafusion::common::{Column, ScalarValue};
use datafusion::logical_expr::{BinaryExpr, Expr, Operator};
use serde_json::json;

use crate::projection::sql_identifier;
use crate::table_provider::provider_args_from_filters;
use crate::{
    SqlBindingContributor, SqlRuntimeBinding, generate_sql_bindings, validate_read_only_sql,
};
use coral_exports::BindingContributor;

fn file_capability() -> Capability {
    let source_id = SourceId("src_demo".to_string());
    let mut capability = Capability::new(
        source_id,
        "files",
        "issues",
        ProviderOrigin {
            kind: ProviderOriginKind::FileRelation,
            snapshot_ref: "interfaces/files/provider-snapshot.yaml#/files/issues".to_string(),
            provider_name: "issues".to_string(),
            tags: Vec::new(),
        },
        UpstreamBinding::FileRead(FileScanBinding {
            file_refs: vec![FileArtifactRef {
                id: "issues".to_string(),
                source_local_path: "interfaces/files/files/file_0".to_string(),
                display_name: None,
            }],
            format: FileFormatDescriptor::Jsonl,
            schema_ref: Some("interfaces/files/provider-snapshot.yaml#/schemas/issues".to_string()),
        }),
    );
    capability.effect_profile = EffectProfile::read();
    capability.shape_hints = ShapeHints::root_list();
    capability.output_contract = coral_capabilities::OutputContract::Single {
        schema: InvocationSchema::new(json!({
            "type": "object",
            "properties": {
                "id": { "type": "integer" },
                "title": { "type": "string" }
            }
        })),
    };
    capability
}

#[test]
fn sql_identifier_splits_camel_case_provider_names() {
    assert_eq!(sql_identifier("includeArchived"), "include_archived");
    assert_eq!(sql_identifier("orderBy"), "order_by");
    assert_eq!(sql_identifier("URLValue"), "url_value");
    assert_eq!(sql_identifier("per_page"), "per_page");
}

#[test]
fn file_read_capability_gets_sql_table_binding() {
    let source_id = SourceId("src_demo".to_string());
    let bindings = generate_sql_bindings(
        &file_capability(),
        &BindingBuildContext {
            source_id,
            display_name: "Demo".to_string(),
            source_key: SourceKey("demo".to_string()),
        },
    );
    assert_eq!(bindings.len(), 1);
    let binding = bindings.first().expect("first SQL binding");
    assert_eq!(binding.sql_reference, "demo.issues");
    assert_eq!(binding.projection.columns.len(), 2);
    assert!(binding.projection.file_scan.is_some());
}

#[test]
fn mutation_capability_gets_no_sql_binding() {
    let mut capability = file_capability();
    capability.effect_profile = EffectProfile::write();
    let source_id = SourceId("src_demo".to_string());
    let contribution = SqlBindingContributor::new()
        .contribute(
            &capability,
            &BindingBuildContext {
                source_id,
                display_name: "Demo".to_string(),
                source_key: SourceKey("demo".to_string()),
            },
        )
        .expect("contribute");
    assert!(contribution.bindings.is_empty());
}

#[test]
fn mcp_read_tool_without_output_schema_reports_sql_diagnostic() {
    let source_id = SourceId("src_demo".to_string());
    let mut capability = Capability::new(
        source_id.clone(),
        "mcp",
        "search_public",
        ProviderOrigin {
            kind: ProviderOriginKind::McpTool,
            snapshot_ref: "interfaces/mcp/provider-snapshot.yaml#/tools/search_public".to_string(),
            provider_name: "search_public".to_string(),
            tags: Vec::new(),
        },
        UpstreamBinding::McpTool(McpToolUpstreamBinding {
            server_ref: "source/src_demo/interface/mcp/server/default".to_string(),
            tool_name: "search_public".to_string(),
            task_support: McpTaskSupport::Unknown,
        }),
    );
    capability.effect_profile = EffectProfile::read();
    capability.output_contract =
        coral_capabilities::OutputContract::McpStructuredContent { schema: None };

    let contribution = SqlBindingContributor::new()
        .contribute(
            &capability,
            &BindingBuildContext {
                source_id,
                display_name: "Demo".to_string(),
                source_key: SourceKey("demo".to_string()),
            },
        )
        .expect("contribute");

    assert!(contribution.bindings.is_empty());
    assert_eq!(contribution.diagnostics.len(), 1);
    assert_eq!(
        contribution
            .diagnostics
            .first()
            .expect("missing diagnostic")
            .code,
        "MCP_SQL_OUTPUT_SCHEMA_MISSING"
    );
}

#[test]
fn mcp_read_tool_with_list_output_schema_gets_sql_binding() {
    let source_id = SourceId("src_demo".to_string());
    let mut capability = Capability::new(
        source_id.clone(),
        "mcp",
        "search_public",
        ProviderOrigin {
            kind: ProviderOriginKind::McpTool,
            snapshot_ref: "interfaces/mcp/provider-snapshot.yaml#/tools/search_public".to_string(),
            provider_name: "search_public".to_string(),
            tags: Vec::new(),
        },
        UpstreamBinding::McpTool(McpToolUpstreamBinding {
            server_ref: "source/src_demo/interface/mcp/server/default".to_string(),
            tool_name: "search_public".to_string(),
            task_support: McpTaskSupport::Unknown,
        }),
    );
    capability.effect_profile = EffectProfile::read();
    capability.shape_hints = ShapeHints::root_list();
    capability.output_contract = coral_capabilities::OutputContract::McpStructuredContent {
        schema: Some(InvocationSchema::new(json!({
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "channel": { "type": "string" },
                    "text": { "type": "string" }
                }
            }
        }))),
    };

    let contribution = SqlBindingContributor::new()
        .contribute(
            &capability,
            &BindingBuildContext {
                source_id,
                display_name: "Demo".to_string(),
                source_key: SourceKey("demo".to_string()),
            },
        )
        .expect("contribute");

    assert_eq!(contribution.bindings.len(), 1);
    assert!(contribution.diagnostics.is_empty());
}

#[test]
fn file_capability_with_inputs_gets_sql_provider_table_metadata() {
    let source_id = SourceId("src_demo".to_string());
    let mut capability = file_capability();
    capability.input_schema = InvocationSchema::object(
        serde_json::Map::from_iter([("limit".to_string(), json!({ "type": "integer" }))]),
        vec!["limit".to_string()],
        false,
    );

    let bindings = generate_sql_bindings(
        &capability,
        &BindingBuildContext {
            source_id,
            display_name: "Demo".to_string(),
            source_key: SourceKey("demo".to_string()),
        },
    );

    let binding = bindings.first().expect("sql binding");
    assert_eq!(binding.sql_reference, "demo.issues");
    assert_eq!(binding.projection.inputs.len(), 1);
    let input = binding.projection.inputs.first().expect("sql input");
    assert_eq!(input.name, "limit");
    assert!(input.required);
}

#[test]
fn importer_file_read_operation_uses_interface_name_for_sql_table() {
    let source_id = SourceId("src_demo".to_string());
    let mut capability = file_capability();
    capability.interface_id = "messages".to_string();
    capability.operation_id = "read_files".to_string();
    let binding = generate_sql_bindings(
        &capability,
        &BindingBuildContext {
            source_id,
            display_name: "Demo".to_string(),
            source_key: SourceKey("demo".to_string()),
        },
    )
    .pop()
    .expect("sql binding");
    assert_eq!(binding.sql_reference, "demo.messages");
}

#[test]
fn rest_read_capability_gets_provider_sql_table_binding() {
    let source_id = SourceId("src_demo".to_string());
    let mut capability = file_capability();
    capability.operation_id = "list_issues".to_string();
    capability.upstream_binding = UpstreamBinding::Rest(RestUpstreamBinding {
        operation_ref: "interfaces/rest/provider-snapshot.yaml#/operations/list_issues".to_string(),
        method: HttpMethod::Get,
        path_template: "/repos/{owner}/{repo}/issues".to_string(),
        parameter_bindings: vec![
            coral_capabilities::RestParameterBinding {
                name: "owner".to_string(),
                location: coral_capabilities::RestParameterLocation::Path,
                required: true,
                style: "simple".to_string(),
                explode: false,
                allow_reserved: false,
            },
            coral_capabilities::RestParameterBinding {
                name: "repo".to_string(),
                location: coral_capabilities::RestParameterLocation::Path,
                required: true,
                style: "simple".to_string(),
                explode: false,
                allow_reserved: false,
            },
            coral_capabilities::RestParameterBinding {
                name: "state".to_string(),
                location: coral_capabilities::RestParameterLocation::Query,
                required: false,
                style: "form".to_string(),
                explode: true,
                allow_reserved: false,
            },
        ],
        request_bodies: Vec::new(),
        responses: Vec::new(),
        pagination: None,
    });
    capability.input_schema = InvocationSchema::object(
        serde_json::Map::from_iter([
            ("owner".to_string(), json!({ "type": "string" })),
            ("repo".to_string(), json!({ "type": "string" })),
            ("state".to_string(), json!({ "type": "string" })),
        ]),
        vec!["owner".to_string(), "repo".to_string()],
        false,
    );
    let bindings = generate_sql_bindings(
        &capability,
        &BindingBuildContext {
            source_id,
            display_name: "Demo".to_string(),
            source_key: SourceKey("demo".to_string()),
        },
    );
    let binding = bindings.first().expect("provider sql binding");
    assert_eq!(binding.sql_reference, "demo.list_issues");
    assert!(binding.projection.file_scan.is_none());
    assert_eq!(
        binding
            .projection
            .inputs
            .iter()
            .filter(|input| input.required)
            .map(|input| input.name.as_str())
            .collect::<Vec<_>>(),
        vec!["owner", "repo"]
    );
}

#[test]
fn provider_limit_does_not_invent_per_page_argument() {
    let mut capability = file_capability();
    capability.operation_id = "search_issues".to_string();
    capability.upstream_binding = UpstreamBinding::Rest(RestUpstreamBinding {
        operation_ref: "interfaces/rest/provider-snapshot.yaml#/operations/search_issues"
            .to_string(),
        method: HttpMethod::Get,
        path_template: "/search/issues".to_string(),
        parameter_bindings: Vec::new(),
        request_bodies: Vec::new(),
        responses: Vec::new(),
        pagination: None,
    });
    let binding = runtime_binding_for(capability);

    let args = provider_args_from_filters(&binding, &[], Some(10)).expect("provider args");

    assert!(args.provider_args.is_empty());
    assert!(args.sql_args.is_empty());
}

#[test]
fn provider_limit_uses_mapped_page_size_input() {
    let mut capability = file_capability();
    capability.operation_id = "search_issues".to_string();
    capability.upstream_binding = UpstreamBinding::Rest(RestUpstreamBinding {
        operation_ref: "interfaces/rest/provider-snapshot.yaml#/operations/search_issues"
            .to_string(),
        method: HttpMethod::Get,
        path_template: "/search/issues".to_string(),
        parameter_bindings: vec![coral_capabilities::RestParameterBinding {
            name: "per_page".to_string(),
            location: coral_capabilities::RestParameterLocation::Query,
            required: false,
            style: "form".to_string(),
            explode: true,
            allow_reserved: false,
        }],
        request_bodies: Vec::new(),
        responses: Vec::new(),
        pagination: None,
    });
    capability.shape_hints.pagination_hint = Some(PaginationHint {
        kind: PaginationKind::OffsetLimit,
        cursor_arg: None,
        cursor_path: None,
        page_size_arg: Some("per_page".to_string()),
    });
    capability.input_schema = InvocationSchema::object(
        serde_json::Map::from_iter([("per_page".to_string(), json!({ "type": "integer" }))]),
        Vec::new(),
        false,
    );
    let binding = runtime_binding_for(capability);

    let args = provider_args_from_filters(&binding, &[], Some(25)).expect("provider args");

    assert_eq!(args.provider_args.get("per_page"), Some(&json!(25)));
    assert_eq!(args.sql_args.get("per_page"), Some(&json!(25)));
}

#[test]
fn provider_filters_translate_normalized_sql_names_to_rest_names() {
    let mut capability = file_capability();
    capability.operation_id = "search_issues".to_string();
    capability.upstream_binding = UpstreamBinding::Rest(RestUpstreamBinding {
        operation_ref: "interfaces/rest/provider-snapshot.yaml#/operations/search_issues"
            .to_string(),
        method: HttpMethod::Get,
        path_template: "/search/issues".to_string(),
        parameter_bindings: vec![
            coral_capabilities::RestParameterBinding {
                name: "pageSize".to_string(),
                location: coral_capabilities::RestParameterLocation::Query,
                required: false,
                style: "form".to_string(),
                explode: true,
                allow_reserved: false,
            },
            coral_capabilities::RestParameterBinding {
                name: "api-version".to_string(),
                location: coral_capabilities::RestParameterLocation::Query,
                required: false,
                style: "form".to_string(),
                explode: true,
                allow_reserved: false,
            },
        ],
        request_bodies: Vec::new(),
        responses: Vec::new(),
        pagination: None,
    });
    capability.input_schema = InvocationSchema::object(
        serde_json::Map::from_iter([
            ("pageSize".to_string(), json!({ "type": "integer" })),
            ("api-version".to_string(), json!({ "type": "string" })),
        ]),
        Vec::new(),
        false,
    );
    let binding = runtime_binding_for(capability);

    let args = provider_args_from_filters(
        &binding,
        &[
            eq_filter("page_size", ScalarValue::Int64(Some(50))),
            eq_filter(
                "api_version",
                ScalarValue::Utf8(Some("2026-01-01".to_string())),
            ),
        ],
        None,
    )
    .expect("provider args");

    assert_eq!(args.provider_args.get("pageSize"), Some(&json!(50)));
    assert_eq!(
        args.provider_args.get("api-version"),
        Some(&json!("2026-01-01"))
    );
    assert!(!args.provider_args.contains_key("page_size"));
    assert!(!args.provider_args.contains_key("api_version"));
    assert_eq!(args.sql_args.get("page_size"), Some(&json!(50)));
    assert_eq!(args.sql_args.get("api_version"), Some(&json!("2026-01-01")));
}

#[test]
fn provider_filters_translate_normalized_sql_names_to_graphql_arguments() {
    let mut capability = file_capability();
    capability.operation_id = "searchIssues".to_string();
    capability.upstream_binding = UpstreamBinding::Graphql(GraphqlOperationBinding {
        endpoint_ref: "interfaces/graphql/provider-snapshot.yaml#/endpoints/default".to_string(),
        operation_name: "SearchIssues".to_string(),
        graphql_operation_kind: GraphqlOperationKind::Query,
        document_ref: "interfaces/graphql/documents/searchIssues.graphql".to_string(),
        selection_set: None,
        variable_bindings: vec![GraphqlVariableBinding {
            variable_name: "first".to_string(),
            graphql_type: Some("Int".to_string()),
            argument_path: vec!["pageSize".to_string()],
            required: false,
        }],
        response_path: vec!["searchIssues".to_string(), "nodes".to_string()],
    });
    capability.input_schema = InvocationSchema::object(
        serde_json::Map::from_iter([("pageSize".to_string(), json!({ "type": "integer" }))]),
        Vec::new(),
        false,
    );
    let binding = runtime_binding_for(capability);

    let args = provider_args_from_filters(
        &binding,
        &[eq_filter("page_size", ScalarValue::Int64(Some(25)))],
        None,
    )
    .expect("provider args");

    assert_eq!(args.provider_args.get("pageSize"), Some(&json!(25)));
    assert!(!args.provider_args.contains_key("page_size"));
    assert_eq!(args.sql_args.get("page_size"), Some(&json!(25)));
}

#[test]
fn provider_filters_translate_boolean_input_predicates() {
    let mut capability = file_capability();
    capability.operation_id = "query_issues".to_string();
    capability.upstream_binding = UpstreamBinding::Graphql(GraphqlOperationBinding {
        endpoint_ref: "interfaces/graphql/provider-snapshot.yaml#/endpoints/default".to_string(),
        operation_name: "QueryIssues".to_string(),
        graphql_operation_kind: GraphqlOperationKind::Query,
        document_ref: "interfaces/graphql/documents/query_issues.graphql".to_string(),
        selection_set: None,
        variable_bindings: vec![GraphqlVariableBinding {
            variable_name: "includeArchived".to_string(),
            graphql_type: Some("Boolean".to_string()),
            argument_path: vec!["includeArchived".to_string()],
            required: false,
        }],
        response_path: vec!["issues".to_string(), "nodes".to_string()],
    });
    capability.input_schema = InvocationSchema::object(
        serde_json::Map::from_iter([("includeArchived".to_string(), json!({ "type": "boolean" }))]),
        Vec::new(),
        false,
    );
    let binding = runtime_binding_for(capability);

    assert_boolean_filter_args(&binding, Expr::Not(Box::new(boolean_input_column())), false);
    assert_boolean_filter_args(
        &binding,
        Expr::IsFalse(Box::new(boolean_input_column())),
        false,
    );
    assert_boolean_filter_args(
        &binding,
        not_eq_filter("include_archived", ScalarValue::Boolean(Some(true))),
        false,
    );
    assert_boolean_filter_args(&binding, boolean_input_column(), true);
    assert_boolean_filter_args(
        &binding,
        Expr::IsTrue(Box::new(boolean_input_column())),
        true,
    );
    assert_boolean_filter_args(
        &binding,
        not_eq_filter("include_archived", ScalarValue::Boolean(Some(false))),
        true,
    );
}

#[test]
fn provider_filter_conflicts_fail_loudly() {
    let mut capability = file_capability();
    capability.operation_id = "search_issues".to_string();
    capability.upstream_binding = UpstreamBinding::Rest(RestUpstreamBinding {
        operation_ref: "interfaces/rest/provider-snapshot.yaml#/operations/search_issues"
            .to_string(),
        method: HttpMethod::Get,
        path_template: "/search/issues".to_string(),
        parameter_bindings: vec![coral_capabilities::RestParameterBinding {
            name: "owner".to_string(),
            location: coral_capabilities::RestParameterLocation::Query,
            required: false,
            style: "form".to_string(),
            explode: true,
            allow_reserved: false,
        }],
        request_bodies: Vec::new(),
        responses: Vec::new(),
        pagination: None,
    });
    capability.input_schema = InvocationSchema::object(
        serde_json::Map::from_iter([("owner".to_string(), json!({ "type": "string" }))]),
        Vec::new(),
        false,
    );
    let binding = runtime_binding_for(capability);

    let error = provider_args_from_filters(
        &binding,
        &[
            eq_filter("owner", ScalarValue::Utf8(Some("octo".to_string()))),
            eq_filter("owner", ScalarValue::Utf8(Some("robot".to_string()))),
        ],
        None,
    )
    .expect_err("conflicting pushed filters must fail");

    assert!(
        error
            .to_string()
            .contains("conflicting filters for `owner`")
    );
}

#[test]
fn wrapped_items_response_selects_item_rows() {
    let source_id = SourceId("src_demo".to_string());
    let mut capability = file_capability();
    capability.operation_id = "search_issues".to_string();
    capability.upstream_binding = UpstreamBinding::Rest(RestUpstreamBinding {
        operation_ref: "interfaces/rest/provider-snapshot.yaml#/operations/search_issues"
            .to_string(),
        method: HttpMethod::Get,
        path_template: "/search/issues".to_string(),
        parameter_bindings: Vec::new(),
        request_bodies: Vec::new(),
        responses: Vec::new(),
        pagination: None,
    });
    capability.output_contract = coral_capabilities::OutputContract::RestResponseVariants {
        variants: vec![coral_capabilities::RestOutputVariant {
            status: coral_capabilities::StatusRange::Code { code: 200 },
            media_type: "application/json".to_string(),
            provider_origin: "application/json".to_string(),
            schema: InvocationSchema::new(json!({
                "type": "object",
                "properties": {
                    "total_count": { "type": "integer" },
                    "items": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "number": { "type": "integer" },
                                "title": { "type": "string" }
                            }
                        }
                    }
                }
            })),
        }],
    };
    let binding = generate_sql_bindings(
        &capability,
        &BindingBuildContext {
            source_id,
            display_name: "Demo".to_string(),
            source_key: SourceKey("demo".to_string()),
        },
    )
    .pop()
    .expect("sql binding");
    assert_eq!(
        binding
            .projection
            .response_selection
            .as_ref()
            .map(|selection| selection.path.as_slice()),
        Some(&["items".to_string()][..])
    );
    assert_eq!(
        binding
            .projection
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["number", "title"]
    );
}

#[test]
fn referenced_wrapped_rest_response_selects_item_rows() {
    let source_id = SourceId("src_demo".to_string());
    let mut capability = file_capability();
    capability.operation_id = "search_issues".to_string();
    capability.upstream_binding = UpstreamBinding::Rest(RestUpstreamBinding {
        operation_ref: "interfaces/rest/provider-snapshot.yaml#/operations/search_issues"
            .to_string(),
        method: HttpMethod::Get,
        path_template: "/search/issues".to_string(),
        parameter_bindings: Vec::new(),
        request_bodies: Vec::new(),
        responses: Vec::new(),
        pagination: None,
    });
    capability.output_contract = coral_capabilities::OutputContract::RestResponseVariants {
        variants: vec![coral_capabilities::RestOutputVariant {
            status: coral_capabilities::StatusRange::Code { code: 200 },
            media_type: "application/json".to_string(),
            provider_origin: "application/json".to_string(),
            schema: InvocationSchema::new(json!({
                "$ref": "#/$defs/SearchResponse",
                "$defs": {
                    "SearchResponse": {
                        "type": "object",
                        "properties": {
                            "total_count": { "type": "integer" },
                            "items": {
                                "type": "array",
                                "items": { "$ref": "#/$defs/Issue" }
                            }
                        }
                    },
                    "Issue": {
                        "type": "object",
                        "properties": {
                            "number": { "type": "integer" },
                            "title": { "type": "string" }
                        }
                    }
                }
            })),
        }],
    };
    let binding = generate_sql_bindings(
        &capability,
        &BindingBuildContext {
            source_id,
            display_name: "Demo".to_string(),
            source_key: SourceKey("demo".to_string()),
        },
    )
    .pop()
    .expect("sql binding");

    assert_eq!(
        binding
            .projection
            .response_selection
            .as_ref()
            .map(|selection| selection.path.as_slice()),
        Some(&["items".to_string()][..])
    );
    assert_eq!(
        binding
            .projection
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["number", "title"]
    );
}

#[test]
fn composed_wrapped_rest_response_selects_item_rows() {
    let source_id = SourceId("src_demo".to_string());
    let mut capability = file_capability();
    capability.operation_id = "search_issues".to_string();
    capability.upstream_binding = UpstreamBinding::Rest(RestUpstreamBinding {
        operation_ref: "interfaces/rest/provider-snapshot.yaml#/operations/search_issues"
            .to_string(),
        method: HttpMethod::Get,
        path_template: "/search/issues".to_string(),
        parameter_bindings: Vec::new(),
        request_bodies: Vec::new(),
        responses: Vec::new(),
        pagination: None,
    });
    capability.output_contract = coral_capabilities::OutputContract::RestResponseVariants {
        variants: vec![coral_capabilities::RestOutputVariant {
            status: coral_capabilities::StatusRange::Code { code: 200 },
            media_type: "application/json".to_string(),
            provider_origin: "application/json".to_string(),
            schema: InvocationSchema::new(json!({
                "allOf": [
                    { "$ref": "#/$defs/SearchResponse" }
                ],
                "$defs": {
                    "SearchResponse": {
                        "type": "object",
                        "properties": {
                            "total_count": { "type": "integer" },
                            "items": {
                                "type": "array",
                                "items": {
                                    "allOf": [
                                        { "$ref": "#/$defs/Issue" }
                                    ]
                                }
                            }
                        }
                    },
                    "Issue": {
                        "type": "object",
                        "properties": {
                            "number": { "type": "integer" },
                            "title": { "type": "string" }
                        }
                    }
                }
            })),
        }],
    };
    let binding = generate_sql_bindings(
        &capability,
        &BindingBuildContext {
            source_id,
            display_name: "Demo".to_string(),
            source_key: SourceKey("demo".to_string()),
        },
    )
    .pop()
    .expect("sql binding");

    assert_eq!(
        binding
            .projection
            .response_selection
            .as_ref()
            .map(|selection| selection.path.as_slice()),
        Some(&["items".to_string()][..])
    );
    assert_eq!(
        binding
            .projection
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["number", "title"]
    );
}

#[test]
fn graphql_connection_shape_hint_selects_node_rows() {
    let source_id = SourceId("src_demo".to_string());
    let mut capability = file_capability();
    capability.operation_id = "query_issues".to_string();
    capability.upstream_binding = UpstreamBinding::Graphql(GraphqlOperationBinding {
        endpoint_ref: "interfaces/graphql/provider-snapshot.yaml#/endpoints/default".to_string(),
        operation_name: "QueryIssues".to_string(),
        graphql_operation_kind: GraphqlOperationKind::Query,
        document_ref: "interfaces/graphql/documents/query_issues.graphql".to_string(),
        selection_set: None,
        variable_bindings: Vec::new(),
        response_path: vec!["issues".to_string(), "nodes".to_string()],
    });
    capability.shape_hints =
        ShapeHints::list_at_path(vec!["issues".to_string(), "nodes".to_string()]);
    capability.output_contract = coral_capabilities::OutputContract::GraphqlData {
        schema: InvocationSchema::new(json!({
            "type": "object",
            "properties": {
                "issues": {
                    "type": "object",
                    "properties": {
                        "nodes": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "identifier": { "type": "string" },
                                    "title": { "type": "string" }
                                }
                            }
                        }
                    }
                }
            }
        })),
    };
    let binding = generate_sql_bindings(
        &capability,
        &BindingBuildContext {
            source_id,
            display_name: "Demo".to_string(),
            source_key: SourceKey("demo".to_string()),
        },
    )
    .pop()
    .expect("sql binding");

    assert_eq!(
        binding
            .projection
            .response_selection
            .as_ref()
            .map(|selection| selection.path.as_slice()),
        Some(&["issues".to_string(), "nodes".to_string()][..])
    );
    assert_eq!(
        binding
            .projection
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["identifier", "title"]
    );
}

#[test]
fn graphql_singleton_shape_hint_selects_nested_object_row() {
    let source_id = SourceId("src_demo".to_string());
    let mut capability = file_capability();
    capability.operation_id = "query_issue".to_string();
    capability.upstream_binding = UpstreamBinding::Graphql(GraphqlOperationBinding {
        endpoint_ref: "interfaces/graphql/provider-snapshot.yaml#/endpoints/default".to_string(),
        operation_name: "QueryIssue".to_string(),
        graphql_operation_kind: GraphqlOperationKind::Query,
        document_ref: "interfaces/graphql/documents/query_issue.graphql".to_string(),
        selection_set: None,
        variable_bindings: Vec::new(),
        response_path: vec!["issue".to_string()],
    });
    capability.shape_hints = ShapeHints::singleton_at_path(vec!["issue".to_string()]);
    capability.output_contract = coral_capabilities::OutputContract::GraphqlData {
        schema: InvocationSchema::new(json!({
            "type": "object",
            "properties": {
                "issue": {
                    "type": "object",
                    "properties": {
                        "identifier": { "type": "string" },
                        "title": { "type": "string" }
                    }
                }
            }
        })),
    };
    let binding = generate_sql_bindings(
        &capability,
        &BindingBuildContext {
            source_id,
            display_name: "Demo".to_string(),
            source_key: SourceKey("demo".to_string()),
        },
    )
    .pop()
    .expect("sql binding");

    assert_eq!(binding.projection.row_shape, SqlRowShape::Singleton);
    assert_eq!(
        binding
            .projection
            .response_selection
            .as_ref()
            .map(|selection| selection.path.as_slice()),
        Some(&["issue".to_string()][..])
    );
    assert_eq!(
        binding
            .projection
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>(),
        vec!["identifier", "title"]
    );
}

#[test]
fn nullable_wrapped_array_response_selects_item_rows_and_preserves_column_types() {
    let source_id = SourceId("src_demo".to_string());
    let mut capability = file_capability();
    capability.operation_id = "search_issues".to_string();
    capability.upstream_binding = UpstreamBinding::Rest(RestUpstreamBinding {
        operation_ref: "interfaces/rest/provider-snapshot.yaml#/operations/search_issues"
            .to_string(),
        method: HttpMethod::Get,
        path_template: "/search/issues".to_string(),
        parameter_bindings: Vec::new(),
        request_bodies: Vec::new(),
        responses: Vec::new(),
        pagination: None,
    });
    capability.output_contract = coral_capabilities::OutputContract::RestResponseVariants {
        variants: vec![coral_capabilities::RestOutputVariant {
            status: coral_capabilities::StatusRange::Code { code: 200 },
            media_type: "application/json".to_string(),
            provider_origin: "application/json".to_string(),
            schema: InvocationSchema::new(json!({
                "type": "object",
                "properties": {
                    "items": {
                        "type": ["null", "array"],
                        "items": {
                            "type": "object",
                            "properties": {
                                "id": { "type": ["null", "integer"] },
                                "score": { "type": ["null", "number"] },
                                "active": { "type": ["boolean", "null"] },
                                "name": { "type": ["string", "null"] }
                            }
                        }
                    }
                }
            })),
        }],
    };
    let binding = generate_sql_bindings(
        &capability,
        &BindingBuildContext {
            source_id,
            display_name: "Demo".to_string(),
            source_key: SourceKey("demo".to_string()),
        },
    )
    .pop()
    .expect("sql binding");

    assert_eq!(
        binding
            .projection
            .response_selection
            .as_ref()
            .map(|selection| selection.path.as_slice()),
        Some(&["items".to_string()][..])
    );
    assert_eq!(
        binding
            .projection
            .columns
            .iter()
            .map(|column| (column.name.as_str(), column.data_type.as_str()))
            .collect::<Vec<_>>(),
        vec![
            ("active", "Boolean"),
            ("id", "Int64"),
            ("name", "Utf8"),
            ("score", "Float64"),
        ]
    );
}

fn runtime_binding_for(capability: Capability) -> SqlRuntimeBinding {
    let source_id = capability.source_id.clone();
    let binding = generate_sql_bindings(
        &capability,
        &BindingBuildContext {
            source_id,
            display_name: "Demo".to_string(),
            source_key: SourceKey("demo".to_string()),
        },
    )
    .pop()
    .expect("sql binding");
    SqlRuntimeBinding {
        capability,
        binding,
        source_materialized_dir: std::path::PathBuf::from("/tmp/source"),
    }
}

fn eq_filter(name: &str, value: ScalarValue) -> Expr {
    Expr::BinaryExpr(BinaryExpr {
        left: Box::new(Expr::Column(Column::from_name(name))),
        op: Operator::Eq,
        right: Box::new(Expr::Literal(value, None)),
    })
}

fn not_eq_filter(name: &str, value: ScalarValue) -> Expr {
    Expr::BinaryExpr(BinaryExpr {
        left: Box::new(Expr::Column(Column::from_name(name))),
        op: Operator::NotEq,
        right: Box::new(Expr::Literal(value, None)),
    })
}

fn boolean_input_column() -> Expr {
    Expr::Column(Column::from_name("include_archived"))
}

fn assert_boolean_filter_args(binding: &SqlRuntimeBinding, filter: Expr, expected: bool) {
    let args = provider_args_from_filters(binding, &[filter], None).expect("provider args");
    assert_eq!(
        args.provider_args.get("includeArchived"),
        Some(&json!(expected))
    );
    assert_eq!(
        args.sql_args.get("include_archived"),
        Some(&json!(expected))
    );
}

#[test]
fn datafusion_dependency_lives_in_sql_crate() {
    assert!(super::datafusion_runtime_type_name().contains("SessionContext"));
    assert!(super::upstream_plan_type_name().contains("UpstreamInvocationPlan"));
}

#[test]
fn describe_statements_are_read_only_sql() {
    validate_read_only_sql("DESCRIBE demo.messages").expect("describe table");
    validate_read_only_sql("DESC SELECT * FROM demo.messages").expect("describe query");
}

#[test]
fn show_discovery_statements_are_read_only_sql() {
    validate_read_only_sql("SHOW TABLES").expect("show tables");
    validate_read_only_sql("SHOW COLUMNS FROM demo.issues").expect("show columns");
    validate_read_only_sql("SHOW FUNCTIONS").expect("show functions");
}

#[test]
fn explain_dml_is_not_read_only_sql() {
    let error = validate_read_only_sql("EXPLAIN INSERT INTO demo.messages VALUES (1)")
        .expect_err("explain insert must be rejected");
    assert!(error.to_string().contains("DML not supported: INSERT"));
}

#[tokio::test]
async fn json_sql_functions_are_registered() {
    let workspace = super::SqlWorkspace::new(Vec::new());
    let execution = workspace
        .execute_sql(
            r#"select
                    json_get_str('{"country":"US"}', 'country') as country,
                    json_get_int('{"count":7}', 'count') as count,
                    json_as_text('{"active":true}', 'active') as active"#,
        )
        .await
        .expect("execute JSON SQL helpers");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({
            "country": "US",
            "count": 7,
            "active": "true",
        })]
    );
}

#[tokio::test]
async fn executes_file_sql_binding_from_installed_artifact() {
    let temp = tempfile::tempdir().expect("tempdir");
    let files_dir = temp.path().join("interfaces/files/files");
    std::fs::create_dir_all(&files_dir).expect("create files dir");
    std::fs::write(
        files_dir.join("file_0"),
        r#"{"id":2,"title":"second"}
{"id":1,"title":"first"}
"#,
    )
    .expect("write data");

    let source_id = SourceId("src_demo".to_string());
    let capability = file_capability();
    let binding = generate_sql_bindings(
        &capability,
        &BindingBuildContext {
            source_id,
            display_name: "Demo".to_string(),
            source_key: SourceKey("demo".to_string()),
        },
    )
    .pop()
    .expect("sql binding");
    let workspace = super::SqlWorkspace::new(vec![super::SqlRuntimeBinding {
        capability,
        binding,
        source_materialized_dir: temp.path().to_path_buf(),
    }]);

    let execution = workspace
        .execute_sql("select id, title from demo.issues order by id")
        .await
        .expect("execute sql");

    assert_eq!(
        execution_to_rows(&execution),
        vec![
            json!({"id": 1, "title": "first"}),
            json!({"id": 2, "title": "second"}),
        ]
    );
}

#[tokio::test]
async fn information_schema_lists_registered_sql_bindings() {
    let temp = tempfile::tempdir().expect("tempdir");
    let files_dir = temp.path().join("interfaces/files/files");
    std::fs::create_dir_all(&files_dir).expect("create files dir");
    std::fs::write(files_dir.join("file_0"), r#"{"id":1,"title":"first"}"#).expect("write data");

    let source_id = SourceId("src_demo".to_string());
    let capability = file_capability();
    let binding = generate_sql_bindings(
        &capability,
        &BindingBuildContext {
            source_id,
            display_name: "Demo".to_string(),
            source_key: SourceKey("demo".to_string()),
        },
    )
    .pop()
    .expect("sql binding");
    let workspace = super::SqlWorkspace::new(vec![super::SqlRuntimeBinding {
        capability,
        binding,
        source_materialized_dir: temp.path().to_path_buf(),
    }]);

    let execution = workspace
        .execute_sql(
            "select table_schema, table_name \
                 from information_schema.tables \
                 where table_schema = 'demo' and table_name = 'issues'",
        )
        .await
        .expect("query information_schema");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({"table_schema": "demo", "table_name": "issues"})]
    );
}

#[tokio::test]
async fn show_discovery_statements_execute() {
    let temp = tempfile::tempdir().expect("tempdir");
    let files_dir = temp.path().join("interfaces/files/files");
    std::fs::create_dir_all(&files_dir).expect("create files dir");
    std::fs::write(files_dir.join("file_0"), r#"{"id":1,"title":"first"}"#).expect("write data");

    let source_id = SourceId("src_demo".to_string());
    let capability = file_capability();
    let binding = generate_sql_bindings(
        &capability,
        &BindingBuildContext {
            source_id,
            display_name: "Demo".to_string(),
            source_key: SourceKey("demo".to_string()),
        },
    )
    .pop()
    .expect("sql binding");
    let workspace = super::SqlWorkspace::new(vec![super::SqlRuntimeBinding {
        capability,
        binding,
        source_materialized_dir: temp.path().to_path_buf(),
    }]);

    let tables = workspace
        .execute_sql("SHOW TABLES")
        .await
        .expect("show tables");
    let table_rows = execution_to_rows(&tables);
    assert!(
        table_rows
            .iter()
            .any(|row| row.get("table_name").and_then(serde_json::Value::as_str) == Some("issues")),
        "SHOW TABLES should list demo.issues: {table_rows:#?}"
    );

    let columns = workspace
        .execute_sql("SHOW COLUMNS FROM demo.issues")
        .await
        .expect("show columns");
    let column_rows = execution_to_rows(&columns);
    assert!(
        column_rows
            .iter()
            .any(|row| row.get("column_name").and_then(serde_json::Value::as_str) == Some("title")),
        "SHOW COLUMNS should list title column: {column_rows:#?}"
    );

    let functions = workspace
        .execute_sql("SHOW FUNCTIONS")
        .await
        .expect("show functions");
    assert!(
        !functions.schema().is_empty(),
        "SHOW FUNCTIONS should return a result schema"
    );
}

#[tokio::test]
async fn executes_json_array_file_sql_binding_from_installed_artifact() {
    let temp = tempfile::tempdir().expect("tempdir");
    let files_dir = temp.path().join("interfaces/files/files");
    std::fs::create_dir_all(&files_dir).expect("create files dir");
    std::fs::write(
        files_dir.join("file_0"),
        r#"[{"id":2,"title":"second"},{"id":1,"title":"first"}]"#,
    )
    .expect("write data");

    let source_id = SourceId("src_demo".to_string());
    let mut capability = file_capability();
    let UpstreamBinding::FileRead(file_binding) = &mut capability.upstream_binding else {
        panic!("expected file binding");
    };
    file_binding.format = FileFormatDescriptor::Json;
    let binding = generate_sql_bindings(
        &capability,
        &BindingBuildContext {
            source_id,
            display_name: "Demo".to_string(),
            source_key: SourceKey("demo".to_string()),
        },
    )
    .pop()
    .expect("sql binding");
    let workspace = super::SqlWorkspace::new(vec![super::SqlRuntimeBinding {
        capability,
        binding,
        source_materialized_dir: temp.path().to_path_buf(),
    }]);

    let execution = workspace
        .execute_sql("select id, title from demo.issues order by id")
        .await
        .expect("execute sql");

    assert_eq!(
        execution_to_rows(&execution),
        vec![
            json!({"id": 1, "title": "first"}),
            json!({"id": 2, "title": "second"}),
        ]
    );
}

fn execution_to_rows(execution: &super::QueryExecution) -> Vec<serde_json::Value> {
    let mut bytes = Vec::new();
    {
        let mut writer = arrow::json::ArrayWriter::new(&mut bytes);
        for batch in execution.batches() {
            writer.write(batch).expect("batch should encode to json");
        }
        writer.finish().expect("json writer should finish");
    }
    serde_json::from_slice(&bytes).expect("json rows should decode")
}
