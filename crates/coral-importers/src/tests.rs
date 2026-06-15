use std::collections::BTreeMap;

use coral_capabilities::{
    Capability, CapabilityKind, EffectKind, IdempotencyKind, OutputContract, ProviderOriginKind,
    ResultShapeHint, ShapeHints, StatusRange, SupportStatus, UpstreamBinding,
};
use coral_spec::{SourceInterface, parse_source_manifest_yaml};
use serde_json::{Value, json};

use crate::graphql::{REACHABLE_INPUT_DEFS_LIMIT, graphql_type_ref_from_sdl};
use crate::mcp::mcp_effect_profile;
use crate::naming::{OperationIdAllocator, normalize_operation_id};
use crate::{RawInterfaceInput, import_source};

fn spec() -> coral_spec::SourceSpec {
    parse_source_manifest_yaml(
        r#"
spec_version: 1
kind: source
name: demo
inputs:
  - key: token
    kind: secret
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
    base_url: https://api.example.com
    auth:
      kind: bearer_input
      key: token
  - id: mcp
    type: mcp
    server:
      transport:
        type: streamable_http
        url: https://example.com/mcp
  - id: graph
    type: graphql
    endpoint: https://example.com/graphql
    schema:
      kind: introspection_query
  - id: files
    type: file
    files: ["./issues.jsonl"]
    format:
      kind: jsonl
"#,
    )
    .expect("parse")
}

fn graphql_output_schema_value(capability: &Capability) -> &Value {
    let OutputContract::GraphqlData { schema } = &capability.output_contract else {
        panic!("expected GraphQL output contract");
    };
    &schema.schema
}

fn demo_rest_spec() -> coral_spec::SourceSpec {
    parse_source_manifest_yaml(
        r"
spec_version: 1
kind: source
name: demo
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
    base_url: https://api.example.com
",
    )
    .expect("parse rest source spec")
}

fn linear_graphql_spec(name: &str) -> coral_spec::SourceSpec {
    parse_source_manifest_yaml(&format!(
        r"
spec_version: 1
kind: source
name: {name}
interfaces:
  - id: graph
    type: graphql
    endpoint: https://api.linear.app/graphql
    schema:
      kind: introspection_query
"
    ))
    .expect("parse source spec")
}

fn import_demo(
    source_id: &str,
    spec: &coral_spec::SourceSpec,
    raw: &BTreeMap<String, RawInterfaceInput>,
) -> crate::ImportResult {
    import_source(
        coral_capabilities::SourceId(source_id.to_string()),
        spec,
        raw,
    )
    .expect("import")
}

fn find_op<'a>(capabilities: &'a [Capability], operation_id: &str) -> &'a Capability {
    capabilities
        .iter()
        .find(|capability| capability.operation_id == operation_id)
        .unwrap_or_else(|| panic!("missing capability: {operation_id}"))
}

#[test]
fn mcp_read_only_hint_wins_over_destructive_description_terms() {
    let profile = mcp_effect_profile(&json!({
        "name": "slack_search_public",
        "description": "Search deleted channels, execute query modifiers, and find invited users.",
        "annotations": {
            "readOnlyHint": true,
            "destructiveHint": false,
            "idempotentHint": true
        }
    }));

    assert_eq!(profile.capability_kind, CapabilityKind::Query);
    assert_eq!(profile.effects, vec![EffectKind::Read]);
    assert_eq!(profile.idempotency, IdempotencyKind::Idempotent);
}

#[test]
fn mcp_non_read_only_non_destructive_hint_maps_to_write() {
    let profile = mcp_effect_profile(&json!({
        "name": "slack_create_conversation",
        "description": "Create a channel, DM, or group DM.",
        "annotations": {
            "readOnlyHint": false,
            "destructiveHint": false,
            "idempotentHint": false
        }
    }));

    assert_eq!(profile.capability_kind, CapabilityKind::Mutation);
    assert_eq!(profile.effects, vec![EffectKind::Write]);
    assert_eq!(profile.idempotency, IdempotencyKind::NonIdempotent);
}

#[test]
fn mcp_destructive_hint_maps_to_delete() {
    let profile = mcp_effect_profile(&json!({
        "name": "slack_update_canvas",
        "description": "Replace existing canvas content.",
        "annotations": {
            "readOnlyHint": false,
            "destructiveHint": true,
            "idempotentHint": true
        }
    }));

    assert_eq!(profile.capability_kind, CapabilityKind::Mutation);
    assert_eq!(profile.effects, vec![EffectKind::Delete]);
    assert_eq!(profile.idempotency, IdempotencyKind::Idempotent);
}

#[test]
fn mcp_missing_annotations_use_mcp_tool_annotation_defaults() {
    let profile = mcp_effect_profile(&json!({
        "name": "list_channel_members",
        "description": "List channel members."
    }));

    assert_eq!(profile.capability_kind, CapabilityKind::Mutation);
    assert_eq!(profile.effects, vec![EffectKind::Delete]);
    assert_eq!(profile.idempotency, IdempotencyKind::NonIdempotent);
}

#[test]
fn mcp_output_schema_root_ref_produces_list_shape_hints() {
    let spec = parse_source_manifest_yaml(
        r"
spec_version: 1
kind: source
name: mcp_ref_output
interfaces:
  - id: mcp
    type: mcp
    server:
      transport:
        type: streamable_http
        url: https://example.com/mcp
",
    )
    .expect("parse source spec");
    let raw = BTreeMap::from([(
        "mcp".to_string(),
        RawInterfaceInput::McpToolsList {
            value: json!({
                "tools": [{
                    "name": "list_members",
                    "description": "List channel members",
                    "inputSchema": {"type": "object"},
                    "outputSchema": {
                        "$ref": "#/$defs/Members",
                        "$defs": {
                            "Members": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "id": {"type": "string"}
                                    }
                                }
                            }
                        }
                    },
                    "annotations": {"readOnlyHint": true}
                }]
            }),
        },
    )]);

    let result = import_demo("src_mcp_ref", &spec, &raw);

    let capability = find_op(&result.capabilities.capabilities, "list_members");
    assert_eq!(capability.shape_hints, ShapeHints::root_list());
}

#[expect(
    clippy::too_many_lines,
    reason = "vertical slice fixture intentionally covers all interface importers together"
)]
#[test]
fn imports_vertical_capability_slice() {
    let spec = spec();
    assert!(matches!(
        spec.interfaces.first(),
        Some(SourceInterface::OpenApi(_))
    ));
    let mut raw = BTreeMap::new();
    raw.insert(
            "rest".to_string(),
            RawInterfaceInput::OpenApiDocument {
                bytes: br#"{
                  "openapi":"3.0.3",
                  "paths":{
                    "/issues":{
                      "get":{
                        "operationId":"listIssues",
                        "parameters":[{"name":"state","in":"query","schema":{"type":"string"}}],
                        "responses":{"200":{"content":{"application/json":{"schema":{"type":"object","properties":{"id":{"type":"integer"}}}}}}}
                      },
                      "post":{
                        "operationId":"createIssue",
                        "requestBody":{"content":{"application/json":{"schema":{"type":"object"}},"application/x-www-form-urlencoded":{"schema":{"type":"object"}}}},
                        "responses":{"201":{"content":{"application/json":{"schema":{"type":"object"}}}}}
                      }
                    }
                  }
                }"#.to_vec(),
            },
        );
    raw.insert(
        "mcp".to_string(),
        RawInterfaceInput::McpToolsList {
            value: json!({
                "protocol_version": "2025-06-18",
                "list_changed": true,
                "tools": [{
                    "name": "list_issues",
                    "description": "List issues",
                    "inputSchema": {"type":"object"},
                    "outputSchema": {"type":"object","properties":{"id":{"type":"integer"}}},
                    "annotations": {"readOnlyHint": true}
                }]
            }),
        },
    );
    raw.insert(
        "files".to_string(),
        RawInterfaceInput::FileListing {
            schema: json!({
                "type": "object",
                "properties": {
                    "id": { "type": "integer" },
                    "title": { "type": "string" }
                }
            }),
        },
    );
    raw.insert(
            "graph".to_string(),
            RawInterfaceInput::GraphqlIntrospection {
                value: json!({
                    "data": {"__schema": {
                        "queryType": {"name": "Query"},
                        "mutationType": {"name": "Mutation"},
                        "types": [
                            {"name":"Query","fields":[
                                {"name":"repository","args":[{"name":"owner"}],"type":{"kind":"OBJECT","name":"Repository"}},
                                {"name":"rateLimit","args":[],"type":{"kind":"SCALAR","name":"Int"}},
                                {
                                    "name":"issueSearch",
                                    "args":[],
                                    "type":{"kind":"OBJECT","name":"Repository"},
                                    "isDeprecated": true,
                                    "deprecationReason": "This endpoint deprecated."
                                }
                            ]},
                            {"name":"Mutation","fields":[
                                {"name":"addLabel","args":[],"type":{"kind":"SCALAR","name":"Boolean"}}
                            ]},
                            {"kind":"OBJECT","name":"Repository","fields":[
                                {"name":"id","args":[],"type":{"kind":"NON_NULL","ofType":{"kind":"SCALAR","name":"ID"}}},
                                {"name":"name","args":[],"type":{"kind":"SCALAR","name":"String"}}
                            ]}
                        ]
                    }}
                }),
            },
        );

    let result = import_demo("src_demo", &spec, &raw);
    assert_eq!(result.provider_snapshots.len(), 4);
    assert_eq!(result.capabilities.capabilities.len(), 8);
    let deprecated_issue_search = find_op(&result.capabilities.capabilities, "query_issue_search");
    assert!(deprecated_issue_search.display.deprecated);
    assert_eq!(
        deprecated_issue_search.display.support_status,
        SupportStatus::Deprecated
    );
    let graph_snapshot = result
        .provider_snapshots
        .iter()
        .find(|snapshot| snapshot.interface_id == "graph")
        .expect("graph snapshot");
    let issue_search = graph_snapshot
        .snapshot
        .get("root_fields")
        .and_then(Value::as_array)
        .and_then(|fields| {
            fields
                .iter()
                .find(|field| field.get("name").and_then(Value::as_str) == Some("issueSearch"))
        })
        .expect("deprecated field snapshot");
    assert_eq!(issue_search.get("deprecated"), Some(&json!(true)));
    assert_eq!(issue_search.get("unsupported"), Some(&json!(false)));
    assert_eq!(
        issue_search.get("deprecation_reason"),
        Some(&json!("This endpoint deprecated."))
    );
    let repository = find_op(&result.capabilities.capabilities, "query_repository");
    let UpstreamBinding::Graphql(repository_binding) = &repository.upstream_binding else {
        panic!("expected GraphQL binding");
    };
    assert_eq!(repository_binding.selection_set.as_deref(), Some("id name"));
    assert_eq!(
        repository_binding
            .variable_bindings
            .first()
            .and_then(|binding| binding.graphql_type.as_deref()),
        Some("String")
    );
    assert!(
        result
            .capabilities
            .capabilities
            .iter()
            .any(|capability| matches!(
                capability.provider_origin.kind,
                ProviderOriginKind::McpTool
            ))
    );
    let rest_post = find_op(&result.capabilities.capabilities, "create_issue");
    assert!(matches!(
        rest_post.upstream_binding,
        UpstreamBinding::Rest(_)
    ));
    assert_eq!(
        rest_post.effect_profile.capability_kind,
        CapabilityKind::Mutation
    );
    let mcp = find_op(&result.capabilities.capabilities, "list_issues");
    assert!(mcp.effect_profile.effects.contains(&EffectKind::Read));
    assert_eq!(mcp.shape_hints, ShapeHints::root_singleton());
    let files = find_op(&result.capabilities.capabilities, "read_files");
    let UpstreamBinding::FileRead(file_binding) = &files.upstream_binding else {
        panic!("expected file read binding");
    };
    let file_ref = file_binding.file_refs.first().expect("file artifact ref");
    assert_eq!(file_ref.source_local_path, "interfaces/files/files/file_0");
    assert_eq!(file_ref.display_name.as_deref(), Some("issues.jsonl"));
    assert!(!file_ref.source_local_path.contains("issues.jsonl"));
    let OutputContract::Single { schema } = &files.output_contract else {
        panic!("expected file output schema");
    };
    assert_eq!(
        schema
            .schema
            .pointer("/properties/id/type")
            .and_then(Value::as_str),
        Some("integer")
    );
}

#[test]
fn linear_issue_search_is_suppressed_even_when_introspection_marks_it_active() {
    let spec = linear_graphql_spec("renamed_linear_workspace");
    let raw = BTreeMap::from([(
        "graph".to_string(),
        RawInterfaceInput::GraphqlIntrospection {
            value: json!({
                "data": {"__schema": {
                    "queryType": {"name": "Query"},
                    "types": [
                        {"kind":"OBJECT","name":"Query","fields":[{
                            "name":"issueSearch",
                            "args":[{"name":"query","type":{"kind":"SCALAR","name":"String"}}],
                            "type":{"kind":"OBJECT","name":"IssueConnection"},
                            "isDeprecated": false,
                            "deprecationReason": null
                        }]},
                        {"kind":"OBJECT","name":"IssueConnection","fields":[
                            {"name":"nodes","args":[],"type":{"kind":"LIST","ofType":{"kind":"OBJECT","name":"Issue"}}}
                        ]},
                        {"kind":"OBJECT","name":"Issue","fields":[
                            {"name":"id","args":[],"type":{"kind":"SCALAR","name":"ID"}},
                            {"name":"identifier","args":[],"type":{"kind":"SCALAR","name":"String"}}
                        ]}
                    ]
                }}
            }),
        },
    )]);

    let result = import_demo("src_linear", &spec, &raw);

    assert!(result.capabilities.capabilities.is_empty());
    let graph_snapshot = result
        .provider_snapshots
        .iter()
        .find(|snapshot| snapshot.interface_id == "graph")
        .expect("graph snapshot");
    let issue_search = graph_snapshot
        .snapshot
        .get("root_fields")
        .and_then(Value::as_array)
        .and_then(|fields| {
            fields
                .iter()
                .find(|field| field.get("name").and_then(Value::as_str) == Some("issueSearch"))
        })
        .expect("issueSearch snapshot");
    assert_eq!(issue_search.get("deprecated"), Some(&json!(true)));
    assert_eq!(issue_search.get("unsupported"), Some(&json!(true)));
    assert_eq!(
        issue_search.get("unsupported_reason"),
        Some(&json!("Provider returns a deprecation error at runtime"))
    );
}

#[expect(
    clippy::too_many_lines,
    reason = "Linear-style GraphQL connection fixture keeps nested type metadata visible"
)]
#[test]
fn graphql_import_generates_connection_capability_with_variables() {
    let spec = linear_graphql_spec("linear_graphql");
    let raw = BTreeMap::from([(
        "graph".to_string(),
        RawInterfaceInput::GraphqlIntrospection {
            value: json!({
                "data": {"__schema": {
                    "queryType": {"name": "Query"},
                    "types": [
                        {"kind":"OBJECT","name":"Query","fields":[{
                            "name":"issues",
                            "args":[
                                {"name":"filter","type":{"kind":"INPUT_OBJECT","name":"IssueFilter"}},
                                {"name":"first","type":{"kind":"SCALAR","name":"Int"}},
                                {"name":"includeArchived","type":{"kind":"SCALAR","name":"Boolean"}},
                                {"name":"orderBy","type":{"kind":"ENUM","name":"PaginationOrderBy"}}
                            ],
                            "type":{"kind":"NON_NULL","ofType":{"kind":"OBJECT","name":"IssueConnection"}}
                        }]},
                        {"kind":"OBJECT","name":"IssueConnection","fields":[
                            {"name":"nodes","args":[],"type":{"kind":"NON_NULL","ofType":{"kind":"LIST","ofType":{"kind":"NON_NULL","ofType":{"kind":"OBJECT","name":"Issue"}}}}},
                            {"name":"pageInfo","args":[],"type":{"kind":"NON_NULL","ofType":{"kind":"OBJECT","name":"PageInfo"}}}
                        ]},
                        {"kind":"OBJECT","name":"Issue","fields":[
                            {"name":"id","args":[],"type":{"kind":"NON_NULL","ofType":{"kind":"SCALAR","name":"ID"}}},
                            {"name":"identifier","args":[],"type":{"kind":"SCALAR","name":"String"}},
                            {"name":"title","args":[],"type":{"kind":"SCALAR","name":"String"}},
                            {"name":"url","args":[],"type":{"kind":"SCALAR","name":"String"}},
                            {"name":"state","args":[],"type":{"kind":"OBJECT","name":"WorkflowState"}},
                            {"name":"team","args":[],"type":{"kind":"OBJECT","name":"Team"}},
                            {"name":"assignee","args":[],"type":{"kind":"OBJECT","name":"User"}}
                        ]},
                        {"kind":"OBJECT","name":"WorkflowState","fields":[
                            {"name":"id","args":[],"type":{"kind":"NON_NULL","ofType":{"kind":"SCALAR","name":"ID"}}},
                            {"name":"name","args":[],"type":{"kind":"SCALAR","name":"String"}},
                            {"name":"type","args":[],"type":{"kind":"SCALAR","name":"String"}}
                        ]},
                        {"kind":"OBJECT","name":"Team","fields":[
                            {"name":"id","args":[],"type":{"kind":"NON_NULL","ofType":{"kind":"SCALAR","name":"ID"}}},
                            {"name":"name","args":[],"type":{"kind":"SCALAR","name":"String"}},
                            {"name":"key","args":[],"type":{"kind":"SCALAR","name":"String"}}
                        ]},
                        {"kind":"OBJECT","name":"User","fields":[
                            {"name":"id","args":[],"type":{"kind":"NON_NULL","ofType":{"kind":"SCALAR","name":"ID"}}},
                            {"name":"name","args":[],"type":{"kind":"SCALAR","name":"String"}},
                            {"name":"displayName","args":[],"type":{"kind":"SCALAR","name":"String"}},
                            {"name":"email","args":[],"type":{"kind":"SCALAR","name":"String"}}
                        ]},
                        {"kind":"OBJECT","name":"PageInfo","fields":[
                            {"name":"hasPreviousPage","args":[],"type":{"kind":"SCALAR","name":"Boolean"}},
                            {"name":"hasNextPage","args":[],"type":{"kind":"SCALAR","name":"Boolean"}},
                            {"name":"startCursor","args":[],"type":{"kind":"SCALAR","name":"String"}},
                            {"name":"endCursor","args":[],"type":{"kind":"SCALAR","name":"String"}}
                        ]},
                        {"kind":"INPUT_OBJECT","name":"IssueFilter","inputFields":[
                            {"name":"team","type":{"kind":"INPUT_OBJECT","name":"TeamFilter"}},
                            {"name":"title","type":{"kind":"INPUT_OBJECT","name":"StringComparator"}}
                        ]},
                        {"kind":"INPUT_OBJECT","name":"StringComparator","inputFields":[
                            {"name":"eq","type":{"kind":"SCALAR","name":"String"}},
                            {"name":"contains","type":{"kind":"SCALAR","name":"String"}}
                        ]},
                        {"kind":"ENUM","name":"PaginationOrderBy","enumValues":[
                            {"name":"createdAt"},
                            {"name":"updatedAt"}
                        ]}
                    ]
                }}
            }),
        },
    )]);

    let result = import_demo("src_linear_graphql", &spec, &raw);
    let issues = find_op(&result.capabilities.capabilities, "query_issues");
    let UpstreamBinding::Graphql(binding) = &issues.upstream_binding else {
        panic!("expected GraphQL binding");
    };
    assert_eq!(
        binding.selection_set.as_deref(),
        Some(
            "nodes { id identifier title url state { id name type } assignee { id name displayName } team { id name key } } pageInfo { hasPreviousPage hasNextPage startCursor endCursor }"
        )
    );
    assert_eq!(
        binding
            .variable_bindings
            .iter()
            .find(|binding| binding.variable_name == "orderBy")
            .and_then(|binding| binding.graphql_type.as_deref()),
        Some("PaginationOrderBy")
    );
    assert_eq!(
        issues
            .input_schema
            .schema
            .pointer("/properties/first/type")
            .and_then(Value::as_str),
        Some("integer")
    );
    assert_eq!(
        issues
            .input_schema
            .schema
            .pointer("/properties/orderBy/enum/0")
            .and_then(Value::as_str),
        Some("createdAt")
    );
    assert_eq!(
        issues
            .input_schema
            .schema
            .pointer("/properties/filter/$ref")
            .and_then(Value::as_str),
        Some("#/$defs/IssueFilter")
    );
    assert_eq!(
        issues
            .input_schema
            .schema
            .pointer("/$defs/IssueFilter/properties/title/$ref")
            .and_then(Value::as_str),
        Some("#/$defs/StringComparator")
    );
    assert_eq!(
        issues
            .input_schema
            .schema
            .pointer("/$defs/StringComparator/properties/eq/type")
            .and_then(Value::as_str),
        Some("string")
    );
    assert!(
        issues
            .input_schema
            .schema
            .pointer("/$defs/IssueFilter/properties/team/$ref")
            .is_none(),
        "TeamFilter is missing from the schema index, so its field falls back to the any-schema"
    );
    assert_eq!(
        issues
            .input_schema
            .schema
            .pointer("/$defs/IssueFilter/properties/team/type/0")
            .and_then(Value::as_str),
        Some("object")
    );
    assert_eq!(binding.response_path, ["issues", "nodes"]);
    assert_eq!(issues.shape_hints.result_shape, ResultShapeHint::List);
    assert_eq!(
        issues.shape_hints.row_path_candidates,
        vec![vec!["issues".to_string(), "nodes".to_string()]]
    );
    assert_eq!(
        graphql_output_schema_value(issues)
            .pointer("/properties/issues/properties/nodes/type")
            .and_then(Value::as_str),
        Some("array")
    );
    assert_eq!(
        graphql_output_schema_value(issues)
            .pointer("/properties/issues/properties/nodes/items/properties/identifier/type")
            .and_then(Value::as_str),
        Some("string")
    );
    assert!(
        graphql_output_schema_value(issues)
            .pointer(
                "/properties/issues/properties/nodes/items/properties/assignee/properties/email"
            )
            .is_none(),
        "output schema should not advertise fields absent from the generated selection set"
    );
}

#[expect(
    clippy::too_many_lines,
    reason = "Linear-style recursive input filter graph keeps the full type fixture visible"
)]
#[test]
fn graphql_recursive_input_filters_define_each_named_type_once() {
    let spec = linear_graphql_spec("linear_recursive_filters");
    let raw = BTreeMap::from([(
        "graph".to_string(),
        RawInterfaceInput::GraphqlIntrospection {
            value: json!({
                "data": {"__schema": {
                    "queryType": {"name": "Query"},
                    "types": [
                        {"kind":"OBJECT","name":"Query","fields":[{
                            "name":"issues",
                            "args":[
                                {"name":"filter","type":{"kind":"INPUT_OBJECT","name":"IssueFilter"}},
                                {"name":"first","type":{"kind":"SCALAR","name":"Int"}}
                            ],
                            "type":{"kind":"SCALAR","name":"Int"}
                        }]},
                        {"kind":"INPUT_OBJECT","name":"IssueFilter","inputFields":[
                            {"name":"and","type":{"kind":"LIST","ofType":{"kind":"NON_NULL","ofType":{"kind":"INPUT_OBJECT","name":"IssueFilter"}}}},
                            {"name":"or","type":{"kind":"LIST","ofType":{"kind":"NON_NULL","ofType":{"kind":"INPUT_OBJECT","name":"IssueFilter"}}}},
                            {"name":"title","type":{"kind":"INPUT_OBJECT","name":"StringComparator"}},
                            {"name":"createdAt","type":{"kind":"INPUT_OBJECT","name":"DateComparator"}},
                            {"name":"assignee","type":{"kind":"INPUT_OBJECT","name":"UserFilter"}},
                            {"name":"team","type":{"kind":"INPUT_OBJECT","name":"TeamFilter"}}
                        ]},
                        {"kind":"INPUT_OBJECT","name":"UserFilter","inputFields":[
                            {"name":"and","type":{"kind":"LIST","ofType":{"kind":"NON_NULL","ofType":{"kind":"INPUT_OBJECT","name":"UserFilter"}}}},
                            {"name":"name","type":{"kind":"INPUT_OBJECT","name":"StringComparator"}},
                            {"name":"id","type":{"kind":"INPUT_OBJECT","name":"IDComparator"}},
                            {"name":"active","type":{"kind":"SCALAR","name":"Boolean"}}
                        ]},
                        {"kind":"INPUT_OBJECT","name":"TeamFilter","inputFields":[
                            {"name":"name","type":{"kind":"INPUT_OBJECT","name":"StringComparator"}},
                            {"name":"key","type":{"kind":"INPUT_OBJECT","name":"StringComparator"}},
                            {"name":"id","type":{"kind":"INPUT_OBJECT","name":"IDComparator"}},
                            {"name":"issues","type":{"kind":"INPUT_OBJECT","name":"IssueCollectionFilter"}}
                        ]},
                        {"kind":"INPUT_OBJECT","name":"IssueCollectionFilter","inputFields":[
                            {"name":"some","type":{"kind":"INPUT_OBJECT","name":"IssueFilter"}},
                            {"name":"every","type":{"kind":"INPUT_OBJECT","name":"IssueFilter"}},
                            {"name":"length","type":{"kind":"INPUT_OBJECT","name":"NumberComparator"}}
                        ]},
                        {"kind":"INPUT_OBJECT","name":"StringComparator","inputFields":[
                            {"name":"eq","type":{"kind":"SCALAR","name":"String"}},
                            {"name":"neq","type":{"kind":"SCALAR","name":"String"}},
                            {"name":"in","type":{"kind":"LIST","ofType":{"kind":"NON_NULL","ofType":{"kind":"SCALAR","name":"String"}}}},
                            {"name":"contains","type":{"kind":"SCALAR","name":"String"}},
                            {"name":"startsWith","type":{"kind":"SCALAR","name":"String"}}
                        ]},
                        {"kind":"INPUT_OBJECT","name":"IDComparator","inputFields":[
                            {"name":"eq","type":{"kind":"SCALAR","name":"ID"}},
                            {"name":"in","type":{"kind":"LIST","ofType":{"kind":"NON_NULL","ofType":{"kind":"SCALAR","name":"ID"}}}}
                        ]},
                        {"kind":"INPUT_OBJECT","name":"DateComparator","inputFields":[
                            {"name":"eq","type":{"kind":"SCALAR","name":"String"}},
                            {"name":"lt","type":{"kind":"SCALAR","name":"String"}},
                            {"name":"gt","type":{"kind":"SCALAR","name":"String"}}
                        ]},
                        {"kind":"INPUT_OBJECT","name":"NumberComparator","inputFields":[
                            {"name":"eq","type":{"kind":"SCALAR","name":"Float"}},
                            {"name":"lt","type":{"kind":"SCALAR","name":"Float"}},
                            {"name":"gt","type":{"kind":"SCALAR","name":"Float"}}
                        ]}
                    ]
                }}
            }),
        },
    )]);

    let result = import_demo("src_linear_recursive", &spec, &raw);
    let issues = find_op(&result.capabilities.capabilities, "query_issues");
    let schema = &issues.input_schema.schema;

    assert_eq!(
        schema
            .pointer("/properties/filter/$ref")
            .and_then(Value::as_str),
        Some("#/$defs/IssueFilter")
    );
    let defs = schema
        .get("$defs")
        .and_then(Value::as_object)
        .expect("input schema defs");
    let expected_defs = [
        "IssueFilter",
        "UserFilter",
        "TeamFilter",
        "IssueCollectionFilter",
        "StringComparator",
        "IDComparator",
        "DateComparator",
        "NumberComparator",
    ];
    assert_eq!(defs.len(), expected_defs.len());
    let serialized = serde_json::to_string(schema).expect("serialize input schema");
    for name in expected_defs {
        assert!(defs.contains_key(name), "missing def for {name}");
        assert_eq!(
            serialized.matches(&format!("\"title\":\"{name}\"")).count(),
            1,
            "type {name} must be defined exactly once"
        );
    }
    // Cycles are plain self-refs instead of depth-limited stub objects.
    assert_eq!(
        schema
            .pointer("/$defs/IssueFilter/properties/and/items/$ref")
            .and_then(Value::as_str),
        Some("#/$defs/IssueFilter")
    );
    assert_eq!(
        schema
            .pointer("/$defs/UserFilter/properties/and/items/$ref")
            .and_then(Value::as_str),
        Some("#/$defs/UserFilter")
    );
    // Mutual recursion (IssueFilter -> TeamFilter -> IssueCollectionFilter ->
    // IssueFilter) also resolves through refs rather than re-inlining.
    assert_eq!(
        schema
            .pointer("/$defs/IssueCollectionFilter/properties/some/$ref")
            .and_then(Value::as_str),
        Some("#/$defs/IssueFilter")
    );
    // Comparator types are referenced from many sites but defined once, so the
    // serialized schema stays small where naive depth-limited inlining of this
    // fixture re-expanded each comparator at every reference site.
    assert!(
        serialized.matches("\"#/$defs/StringComparator\"").count() >= 4,
        "expected StringComparator to be shared across reference sites: {serialized}"
    );
    assert!(
        serialized.len() < 4096,
        "recursive filter input schema should stay compact, got {} bytes",
        serialized.len()
    );
    // Scalar list fields stay inline.
    assert_eq!(
        schema
            .pointer("/$defs/StringComparator/properties/in/items/type")
            .and_then(Value::as_str),
        Some("string")
    );
    // The executable Code Mode schema keeps every local ref resolvable.
    let executable_schema = coral_capabilities::code_mode_tool_input_schema(issues);
    assert_eq!(
        coral_capabilities::executable_schema_unresolved_refs(&executable_schema),
        Vec::<String>::new()
    );
}

#[test]
fn graphql_input_defs_cap_truncates_overflow_types() {
    let type_count = REACHABLE_INPUT_DEFS_LIMIT + 6;
    let mut types = vec![json!({
        "kind": "OBJECT",
        "name": "Query",
        "fields": [{
            "name": "items",
            "args": [{"name": "filter", "type": {"kind": "INPUT_OBJECT", "name": "Filter0"}}],
            "type": {"kind": "SCALAR", "name": "Int"}
        }]
    })];
    for index in 0..type_count {
        let next_type = if index + 1 < type_count {
            json!({"kind": "INPUT_OBJECT", "name": format!("Filter{}", index + 1)})
        } else {
            json!({"kind": "SCALAR", "name": "String"})
        };
        types.push(json!({
            "kind": "INPUT_OBJECT",
            "name": format!("Filter{index}"),
            "inputFields": [{"name": "next", "type": next_type}]
        }));
    }
    let spec = linear_graphql_spec("graph_defs_cap");
    let raw = BTreeMap::from([(
        "graph".to_string(),
        RawInterfaceInput::GraphqlIntrospection {
            value: json!({
                "data": {"__schema": {
                    "queryType": {"name": "Query"},
                    "types": types
                }}
            }),
        },
    )]);

    let result = import_demo("src_defs_cap", &spec, &raw);
    let items = find_op(&result.capabilities.capabilities, "query_items");
    let schema = &items.input_schema.schema;

    let defs = schema
        .get("$defs")
        .and_then(Value::as_object)
        .expect("input schema defs");
    assert_eq!(defs.len(), REACHABLE_INPUT_DEFS_LIMIT);
    let last_included = format!("Filter{}", REACHABLE_INPUT_DEFS_LIMIT - 1);
    let first_truncated = format!("Filter{REACHABLE_INPUT_DEFS_LIMIT}");
    assert_eq!(
        schema
            .pointer(&format!(
                "/$defs/Filter{}/properties/next/$ref",
                REACHABLE_INPUT_DEFS_LIMIT - 2
            ))
            .and_then(Value::as_str),
        Some(format!("#/$defs/{last_included}").as_str())
    );
    assert_eq!(
        schema
            .pointer(&format!(
                "/$defs/{last_included}/properties/next/x-coral-truncated"
            ))
            .and_then(Value::as_bool),
        Some(true)
    );
    assert_eq!(
        schema
            .pointer(&format!(
                "/$defs/{last_included}/properties/next/description"
            ))
            .and_then(Value::as_str),
        Some(format!("type {first_truncated} omitted").as_str())
    );
    let serialized = serde_json::to_string(schema).expect("serialize input schema");
    assert!(
        !serialized.contains(&format!("#/$defs/{first_truncated}")),
        "types beyond the cap must not be referenced: {serialized}"
    );
    let executable_schema = coral_capabilities::code_mode_tool_input_schema(items);
    assert_eq!(
        coral_capabilities::executable_schema_unresolved_refs(&executable_schema),
        Vec::<String>::new()
    );
}

/// Checked-in Linear-style recursive introspection fixture. The MCP payload
/// gates in `crates/coral-mcp/src/tests.rs` install the same file as a
/// `introspection_json_file` source, so importer-level and end-to-end byte
/// budgets are pinned against one fixture.
const LINEAR_RECURSIVE_INTROSPECTION_JSON: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/linear_recursive_introspection.json"
));

fn fixture_input_object_names(introspection: &Value) -> Vec<String> {
    introspection
        .pointer("/data/__schema/types")
        .and_then(Value::as_array)
        .expect("fixture types")
        .iter()
        .filter(|ty| ty.get("kind").and_then(Value::as_str) == Some("INPUT_OBJECT"))
        .filter_map(|ty| ty.get("name").and_then(Value::as_str).map(str::to_string))
        .collect()
}

/// Payload regression gate (fix pipeline gate 1): the realistic recursive
/// Linear-style filter graph must keep define-once `$defs` semantics with a
/// hard serialized-size cap. A return to inlining `INPUT_OBJECT` types at
/// reference sites fails the once-per-type and byte-cap assertions loudly.
#[test]
fn payload_gate_recursive_fixture_defines_each_input_type_once_within_byte_cap() {
    // Measured at ~17.0KB with define-once `$defs`; the old depth-2 inlining
    // re-expands every comparator at each of its dozens of reference sites and
    // lands several times over this cap.
    const INPUT_SCHEMA_HARD_CAP_BYTES: usize = 20 * 1024;
    let introspection: Value = serde_json::from_str(LINEAR_RECURSIVE_INTROSPECTION_JSON)
        .expect("parse fixture introspection");
    let input_object_names = fixture_input_object_names(&introspection);
    assert!(
        input_object_names.len() >= 30,
        "fixture must stay a realistic deep filter graph (~30+ named input types), got {}",
        input_object_names.len()
    );
    let spec = linear_graphql_spec("linear_payload_gate");
    let raw = BTreeMap::from([(
        "graph".to_string(),
        RawInterfaceInput::GraphqlIntrospection {
            value: introspection,
        },
    )]);

    let result = import_demo("src_linear_payload_gate", &spec, &raw);

    let issues = find_op(&result.capabilities.capabilities, "query_issues");
    let schema = &issues.input_schema.schema;
    let serialized = serde_json::to_string(schema).expect("serialize input schema");
    let defs = schema
        .get("$defs")
        .and_then(Value::as_object)
        .expect("input schema defs");
    for name in &input_object_names {
        let definitions = serialized.matches(&format!("\"title\":\"{name}\"")).count();
        if name == "IssueUpdateInput" {
            assert_eq!(
                definitions, 0,
                "mutation-only input type leaked into the query input schema"
            );
            assert!(!defs.contains_key(name.as_str()));
        } else {
            assert!(defs.contains_key(name.as_str()), "missing def for {name}");
            assert_eq!(
                definitions, 1,
                "type {name} must be defined exactly once, found {definitions} definitions"
            );
        }
    }
    assert_eq!(
        defs.len(),
        input_object_names.len() - 1,
        "query defs must cover exactly the filter graph"
    );
    assert!(
        serialized.len() < INPUT_SCHEMA_HARD_CAP_BYTES,
        "recursive fixture input schema regressed to {} bytes (hard cap {INPUT_SCHEMA_HARD_CAP_BYTES})",
        serialized.len()
    );
    // The fixture intentionally overflows the compact describe budget so the
    // MCP describe gates keep exercising schema bounding end to end.
    assert!(
        serialized.len() > coral_capabilities::COMPACT_INPUT_SCHEMA_BUDGET_BYTES,
        "fixture input schema shrank to {} bytes and no longer exercises compact bounding",
        serialized.len()
    );
    // Self- and mutual recursion stay plain `$ref`s instead of re-inlining.
    assert_eq!(
        schema
            .pointer("/$defs/IssueFilter/properties/and/items/$ref")
            .and_then(Value::as_str),
        Some("#/$defs/IssueFilter")
    );
    assert_eq!(
        schema
            .pointer("/$defs/CommentFilter/properties/issue/$ref")
            .and_then(Value::as_str),
        Some("#/$defs/NullableIssueFilter")
    );
    let executable_schema = coral_capabilities::code_mode_tool_input_schema(issues);
    assert_eq!(
        coral_capabilities::executable_schema_unresolved_refs(&executable_schema),
        Vec::<String>::new()
    );

    let update = find_op(&result.capabilities.capabilities, "mutation_issue_update");
    assert!(update.effect_profile.effects.contains(&EffectKind::Write));
    assert_eq!(
        update
            .input_schema
            .schema
            .pointer("/properties/input/$ref")
            .and_then(Value::as_str),
        Some("#/$defs/IssueUpdateInput")
    );
}

#[test]
fn graphql_sdl_return_type_ignores_argument_type_colons() {
    let count = graphql_type_ref_from_sdl("count(id: ID!): Int!");
    assert_eq!(count.named_type.as_deref(), Some("Int"));
    assert_eq!(count.graphql_type.as_deref(), Some("Int!"));
    assert_eq!(count.kind.as_deref(), Some("SCALAR"));
    assert!(count.required);

    let search =
        graphql_type_ref_from_sdl("search(query: String!, filter: SearchFilter): [SearchResult!]!");
    assert_eq!(search.named_type.as_deref(), Some("SearchResult"));
    assert_eq!(search.graphql_type.as_deref(), Some("SearchResult!"));
    assert!(search.is_list);
}

#[test]
fn operation_ids_split_camel_case_provider_names() {
    assert_eq!(normalize_operation_id("issueSearch"), "issue_search");
    assert_eq!(
        normalize_operation_id("usersGetAuthenticated"),
        "users_get_authenticated"
    );
    assert_eq!(
        normalize_operation_id("pulls/list-reviews"),
        "pulls_list_reviews"
    );
}

#[test]
fn operation_id_allocator_suffixes_normalized_collisions() {
    let mut allocator = OperationIdAllocator::default();

    assert_eq!(allocator.allocate("issueSearch"), "issue_search");
    assert_eq!(allocator.allocate("issue_search"), "issue_search_2");
    assert_eq!(allocator.allocate("issue_search_2"), "issue_search_2_2");
}

#[test]
fn openapi_import_suffixes_normalized_operation_id_collisions() {
    let spec = demo_rest_spec();
    let raw = BTreeMap::from([(
        "rest".to_string(),
        RawInterfaceInput::OpenApiDocument {
            bytes: serde_json::to_vec(&json!({
                "openapi": "3.0.3",
                "paths": {
                    "/camel": {
                        "get": {
                            "operationId": "issueSearch",
                            "responses": { "200": { "description": "ok" } }
                        }
                    },
                    "/snake": {
                        "get": {
                            "operationId": "issue_search",
                            "responses": { "200": { "description": "ok" } }
                        }
                    }
                }
            }))
            .expect("serialize openapi fixture"),
        },
    )]);

    let result = import_demo("src_demo", &spec, &raw);
    result
        .capabilities
        .validate()
        .expect("collision-safe capability ids");
    let operation_ids = result
        .capabilities
        .capabilities
        .iter()
        .map(|capability| capability.operation_id.as_str())
        .collect::<Vec<_>>();
    assert_eq!(operation_ids, vec!["issue_search", "issue_search_2"]);
    let suffixed = find_op(&result.capabilities.capabilities, "issue_search_2");
    assert_eq!(suffixed.provider_origin.provider_name, "issue_search");
}

#[test]
fn openapi_import_preserves_provider_operation_id_and_tags() {
    let spec = parse_source_manifest_yaml(
        r"
spec_version: 1
kind: source
name: datadog
interfaces:
  - id: rest_v1
    type: openapi
    url: https://example.com/openapi.json
    base_url: https://api.datadoghq.com
",
    )
    .expect("parse rest source spec");
    let raw = BTreeMap::from([(
        "rest_v1".to_string(),
        RawInterfaceInput::OpenApiDocument {
            bytes: serde_json::to_vec(&json!({
                "openapi": "3.0.3",
                "paths": {
                    "/api/v1/monitor/validate": {
                        "post": {
                            "operationId": "ValidateMonitor",
                            "tags": ["Monitors"],
                            "responses": { "200": { "description": "ok" } }
                        }
                    }
                }
            }))
            .expect("serialize openapi fixture"),
        },
    )]);

    let result = import_demo("src_datadog", &spec, &raw);
    let capability = find_op(&result.capabilities.capabilities, "validate_monitor");
    assert_eq!(capability.provider_origin.provider_name, "ValidateMonitor");
    assert_eq!(
        capability.provider_origin.tags,
        vec!["Monitors".to_string()]
    );
    let snapshot = result
        .provider_snapshots
        .first()
        .expect("provider snapshot");
    assert_eq!(
        snapshot
            .snapshot
            .pointer("/operations/0/provider_operation_id")
            .and_then(Value::as_str),
        Some("ValidateMonitor")
    );
    assert_eq!(
        snapshot
            .snapshot
            .pointer("/operations/0/tags/0")
            .and_then(Value::as_str),
        Some("Monitors")
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "OpenAPI fixture keeps singleton, root-list, and nested-list shape hints together"
)]
fn openapi_get_shape_hints_distinguish_singletons_and_lists() {
    let spec = demo_rest_spec();
    let raw = BTreeMap::from([(
        "rest".to_string(),
        RawInterfaceInput::OpenApiDocument {
            bytes: serde_json::to_vec(&json!({
                "openapi": "3.0.3",
                "paths": {
                    "/user": {
                        "get": {
                            "operationId": "users/get-authenticated",
                            "responses": {
                                "200": {
                                    "content": {
                                        "application/json": {
                                            "schema": {
                                                "$ref": "#/components/schemas/User"
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "/issues": {
                        "get": {
                            "operationId": "issues/list",
                            "responses": {
                                "200": {
                                    "content": {
                                        "application/json": {
                                            "schema": {
                                                "type": "array",
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "id": { "type": "integer" }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    },
                    "/search/issues": {
                        "get": {
                            "operationId": "search/issues-and-pull-requests",
                            "responses": {
                                "200": {
                                    "content": {
                                        "application/json": {
                                            "schema": {
                                                "type": "object",
                                                "properties": {
                                                    "total_count": { "type": "integer" },
                                                    "items": {
                                                        "type": "array",
                                                        "items": {
                                                            "type": "object",
                                                            "properties": {
                                                                "id": { "type": "integer" }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "components": {
                    "schemas": {
                        "User": {
                            "type": "object",
                            "properties": {
                                "login": { "type": "string" },
                                "id": { "type": "integer" }
                            }
                        }
                    }
                }
            }))
            .expect("serialize openapi fixture"),
        },
    )]);

    let result = import_demo("src_demo", &spec, &raw);
    let authenticated = find_op(&result.capabilities.capabilities, "users_get_authenticated");
    assert_eq!(
        authenticated.shape_hints.result_shape,
        ResultShapeHint::Singleton
    );
    assert_eq!(
        authenticated.shape_hints.row_path_candidates,
        vec![Vec::<String>::new()]
    );
    let issues = find_op(&result.capabilities.capabilities, "issues_list");
    assert_eq!(issues.shape_hints.result_shape, ResultShapeHint::List);
    assert_eq!(
        issues.shape_hints.row_path_candidates,
        vec![Vec::<String>::new()]
    );
    let search = find_op(
        &result.capabilities.capabilities,
        "search_issues_and_pull_requests",
    );
    assert_eq!(search.shape_hints.result_shape, ResultShapeHint::List);
    assert_eq!(
        search.shape_hints.row_path_candidates,
        vec![vec!["items".to_string()]]
    );
}

#[test]
fn openapi_shape_hints_resolve_referenced_wrapped_array_properties() {
    let spec = demo_rest_spec();
    let raw = BTreeMap::from([(
        "rest".to_string(),
        RawInterfaceInput::OpenApiDocument {
            bytes: serde_json::to_vec(&json!({
                "openapi": "3.0.3",
                "paths": {
                    "/issues/search": {
                        "get": {
                            "operationId": "searchIssues",
                            "responses": {
                                "200": {
                                    "content": {
                                        "application/json": {
                                            "schema": {
                                                "type": "object",
                                                "properties": {
                                                    "items": {
                                                        "$ref": "#/components/schemas/IssueArray"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                },
                "components": {
                    "schemas": {
                        "IssueArray": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "id": { "type": "string" }
                                }
                            }
                        }
                    }
                }
            }))
            .expect("serialize openapi fixture"),
        },
    )]);

    let result = import_demo("src_demo", &spec, &raw);
    let capability = find_op(&result.capabilities.capabilities, "search_issues");

    assert_eq!(capability.shape_hints.result_shape, ResultShapeHint::List);
    assert_eq!(
        capability.shape_hints.row_path_candidates,
        vec![vec!["items".to_string()]]
    );
}

#[test]
fn openapi_shape_hints_detect_nested_wrapped_array_properties() {
    let spec = demo_rest_spec();
    let raw = BTreeMap::from([(
        "rest".to_string(),
        RawInterfaceInput::OpenApiDocument {
            bytes: serde_json::to_vec(&json!({
                "openapi": "3.0.3",
                "paths": {
                    "/search/messages": {
                        "get": {
                            "operationId": "searchMessages",
                            "responses": {
                                "200": {
                                    "content": {
                                        "application/json": {
                                            "schema": {
                                                "type": "object",
                                                "properties": {
                                                    "ok": { "type": "boolean" },
                                                    "messages": {
                                                        "type": "object",
                                                        "properties": {
                                                            "total": { "type": "integer" },
                                                            "matches": {
                                                                "type": "array",
                                                                "items": {
                                                                    "type": "object",
                                                                    "properties": {
                                                                        "id": { "type": "string" }
                                                                    }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }))
            .expect("serialize openapi fixture"),
        },
    )]);

    let result = import_demo("src_demo", &spec, &raw);
    let capability = find_op(&result.capabilities.capabilities, "search_messages");

    assert_eq!(capability.shape_hints.result_shape, ResultShapeHint::List);
    assert_eq!(
        capability.shape_hints.row_path_candidates,
        vec![vec!["messages".to_string(), "matches".to_string()]]
    );
}

#[test]
fn openapi_shape_hints_detect_wrapped_singleton_object_properties() {
    let spec = demo_rest_spec();
    let raw = BTreeMap::from([(
        "rest".to_string(),
        RawInterfaceInput::OpenApiDocument {
            bytes: serde_json::to_vec(&json!({
                "openapi": "3.0.3",
                "paths": {
                    "/files/info": {
                        "get": {
                            "operationId": "filesInfo",
                            "responses": {
                                "200": {
                                    "content": {
                                        "application/json": {
                                            "schema": {
                                                "type": "object",
                                                "properties": {
                                                    "ok": { "type": "boolean" },
                                                    "file": {
                                                        "type": "object",
                                                        "properties": {
                                                            "id": { "type": "string" },
                                                            "title": { "type": "string" }
                                                        }
                                                    },
                                                    "response_metadata": {
                                                        "type": "object"
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }))
            .expect("serialize openapi fixture"),
        },
    )]);

    let result = import_demo("src_demo", &spec, &raw);
    let capability = find_op(&result.capabilities.capabilities, "files_info");

    assert_eq!(
        capability.shape_hints.result_shape,
        ResultShapeHint::Singleton
    );
    assert_eq!(
        capability.shape_hints.row_path_candidates,
        vec![vec!["file".to_string()]]
    );
}

#[expect(
    clippy::too_many_lines,
    reason = "OpenAPI request-body component fixture keeps the regression visible"
)]
#[test]
fn openapi_import_resolves_referenced_request_body_and_response() {
    let spec = demo_rest_spec();
    let raw = BTreeMap::from([(
        "rest".to_string(),
        RawInterfaceInput::OpenApiDocument {
            bytes: br##"{
                  "openapi":"3.0.3",
	                  "components":{
	                    "parameters":{
	                      "State":{
	                        "name":"state",
	                        "in":"query",
	                        "schema":{"$ref":"#/components/schemas/State"}
	                      }
	                    },
	                    "requestBodies":{
	                      "CreateIssue":{
	                        "required":true,
	                        "content":{
	                          "application/vnd.example+json":{
	                            "schema":{"$ref":"#/components/schemas/IssueInput"}
	                          }
	                        }
	                      }
	                    },
                    "responses":{
	                      "IssueCreated":{
	                        "content":{
	                          "application/json":{
	                            "schema":{"$ref":"#/components/schemas/Issue"}
	                          }
	                        }
	                      }
	                    },
	                    "schemas":{
	                      "IssueInput":{
	                        "type":"object",
	                        "required":["title"],
	                        "properties":{
	                          "title":{"type":"string"},
	                          "description":{"type":"string","nullable":true},
	                          "parent":{"$ref":"#/components/schemas/IssueInput"}
	                        }
	                      },
	                      "Issue":{
	                        "type":"object",
	                        "properties":{"id":{"type":"integer"}}
	                      },
	                      "State":{
	                        "type":"string",
	                        "nullable":true,
	                        "enum":["open","closed"]
	                      }
	                    }
	                  },
	                  "paths":{
	                    "/issues":{
	                      "post":{
	                        "operationId":"createIssue",
	                        "parameters":[{"$ref":"#/components/parameters/State"}],
	                        "requestBody":{"$ref":"#/components/requestBodies/CreateIssue"},
	                        "responses":{"201":{"$ref":"#/components/responses/IssueCreated"}}
	                      }
                    }
                  }
                }"##
            .to_vec(),
        },
    )]);

    let result = import_demo("src_demo", &spec, &raw);
    let capability = find_op(&result.capabilities.capabilities, "create_issue");
    let UpstreamBinding::Rest(rest) = &capability.upstream_binding else {
        panic!("expected REST binding");
    };
    assert_eq!(rest.request_bodies.len(), 1);
    let request_body = rest.request_bodies.first().expect("request body");
    assert_eq!(request_body.media_type, "application/vnd.example+json");
    assert!(request_body.required);
    let request_body_defs = request_body
        .schema
        .schema
        .get("$defs")
        .and_then(Value::as_object)
        .expect("request body schema defs");
    let issue_input = request_body_defs
        .values()
        .find(|schema| {
            schema
                .pointer("/properties/title/type")
                .and_then(Value::as_str)
                == Some("string")
        })
        .expect("issue input definition");
    assert_eq!(
        issue_input
            .pointer("/properties/title/type")
            .and_then(Value::as_str),
        Some("string")
    );
    assert!(
        issue_input
            .pointer("/properties/description/type")
            .and_then(Value::as_array)
            .is_some_and(|types| types.iter().any(|value| value.as_str() == Some("null")))
    );
    assert!(
        issue_input
            .pointer("/properties/parent/$ref")
            .and_then(Value::as_str)
            .is_some_and(|reference| reference.starts_with("#/$defs/IssueInput_"))
    );
    let state_schema = capability
        .input_schema
        .schema
        .pointer("/properties/query/properties/state")
        .expect("state parameter schema");
    assert!(state_schema.pointer("/$defs").is_none());
    let input_defs = capability
        .input_schema
        .schema
        .get("$defs")
        .and_then(Value::as_object)
        .expect("capability input schema defs");
    let state_def = input_defs
        .values()
        .find(|schema| schema.pointer("/enum/0").and_then(Value::as_str) == Some("open"))
        .expect("state definition");
    assert!(
        state_schema
            .pointer("/$ref")
            .and_then(Value::as_str)
            .is_some()
    );
    assert!(
        state_def
            .pointer("/type")
            .and_then(Value::as_array)
            .is_some_and(|types| types.iter().any(|value| value.as_str() == Some("null")))
    );
    assert!(
        state_def
            .pointer("/enum")
            .and_then(Value::as_array)
            .is_some_and(|values| values.iter().any(Value::is_null))
    );
    assert!(
        capability
            .input_schema
            .schema
            .pointer("/required")
            .and_then(Value::as_array)
            .is_some_and(|required| required.contains(&Value::String("body".to_string()))),
        "input schema should require body: {:?}",
        capability.input_schema.schema
    );
    assert!(
        capability
            .input_schema
            .schema
            .pointer("/properties/body/$defs")
            .is_none()
    );
    assert_eq!(rest.responses.len(), 1);
    let response = rest.responses.first().expect("response");
    assert_eq!(response.status, StatusRange::Code { code: 201 });
    assert_eq!(response.media_type, "application/json");
    let response_defs = response
        .schema
        .schema
        .get("$defs")
        .and_then(Value::as_object)
        .expect("response schema defs");
    assert_eq!(
        response_defs
            .values()
            .find_map(|schema| schema.pointer("/properties/id/type"))
            .and_then(Value::as_str),
        Some("integer")
    );
    let executable_schema = coral_capabilities::code_mode_tool_input_schema(capability);
    assert_eq!(
        coral_capabilities::executable_schema_unresolved_refs(&executable_schema),
        Vec::<String>::new()
    );
}

#[test]
fn openapi_without_runtime_base_url_does_not_emit_rest_capabilities() {
    let spec = parse_source_manifest_yaml(
        r"
spec_version: 1
kind: source
name: github_like
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
",
    )
    .expect("parse source spec");
    let raw = BTreeMap::from([(
            "rest".to_string(),
            RawInterfaceInput::OpenApiDocument {
                bytes: br#"{
                  "openapi":"3.0.3",
                  "paths":{
                    "/items":{
                      "get":{
                        "operationId":"listItems",
                        "responses":{"200":{"content":{"application/json":{"schema":{"type":"object"}}}}}
                      }
                    }
                  }
                }"#
                .to_vec(),
            },
        )]);

    let result = import_demo("src_demo", &spec, &raw);

    assert!(result.capabilities.capabilities.is_empty());
    assert!(
        result.diagnostics.iter().any(|diagnostic| {
            diagnostic.code == "OPENAPI_RUNTIME_BASE_URL_MISSING"
                && diagnostic.interface_id.as_deref() == Some("rest")
        }),
        "diagnostics: {:?}",
        result.diagnostics
    );
}

#[test]
fn openapi_remote_http_server_url_does_not_emit_rest_capabilities() {
    let spec = parse_source_manifest_yaml(
        r"
spec_version: 1
kind: source
name: github_like
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
",
    )
    .expect("parse source spec");
    let raw = BTreeMap::from([(
            "rest".to_string(),
            RawInterfaceInput::OpenApiDocument {
                bytes: br#"{
                  "openapi":"3.0.3",
                  "servers":[{"url":"http://api.example.com"}],
                  "paths":{
                    "/items":{
                      "get":{
                        "operationId":"listItems",
                        "responses":{"200":{"content":{"application/json":{"schema":{"type":"object"}}}}}
                      }
                    }
                  }
                }"#
                .to_vec(),
            },
        )]);

    let result = import_demo("src_demo", &spec, &raw);

    assert!(result.capabilities.capabilities.is_empty());
    assert!(
        result
            .diagnostics
            .iter()
            .any(|diagnostic| diagnostic.code == "OPENAPI_RUNTIME_BASE_URL_MISSING")
    );
}

#[test]
fn openapi_import_resolves_referenced_parameters_for_rest_capabilities() {
    let spec = parse_source_manifest_yaml(
        r"
spec_version: 1
kind: source
name: github_like
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
    base_url: https://api.example.com
",
    )
    .expect("parse source spec");
    let mut raw = BTreeMap::new();
    raw.insert(
        "rest".to_string(),
        RawInterfaceInput::OpenApiDocument {
            bytes: br##"{
                  "openapi": "3.0.3",
                  "paths": {
                    "/search/issues": {
                      "get": {
                        "operationId": "search/issues-and-pull-requests",
                        "parameters": [
                          { "$ref": "#/components/parameters/search_query" },
                          { "$ref": "#/components/parameters/page" },
                          { "$ref": "#/components/parameters/per_page" }
                        ],
                        "responses": {
                          "200": {
                            "content": {
                              "application/json": {
                                "schema": { "type": "object" }
                              }
                            }
                          }
                        }
                      }
                    }
                  },
                  "components": {
                    "parameters": {
                      "search_query": {
                        "name": "q",
                        "in": "query",
                        "required": true,
                        "schema": { "type": "string" }
                      },
                      "page": {
                        "name": "page",
                        "in": "query",
                        "schema": { "type": "integer" }
                      },
                      "per_page": {
                        "name": "per_page",
                        "in": "query",
                        "schema": { "type": "integer", "default": 30 }
                      }
                    }
                  }
                }"##
            .to_vec(),
        },
    );

    let result = import_demo("src_github_like", &spec, &raw);
    let capability = find_op(
        &result.capabilities.capabilities,
        "search_issues_and_pull_requests",
    );
    let schema = &capability.input_schema.schema;
    assert_eq!(
        schema
            .pointer("/properties/query/properties/q/type")
            .and_then(Value::as_str),
        Some("string")
    );
    assert_eq!(
        schema
            .pointer("/properties/query/properties/per_page/type")
            .and_then(Value::as_str),
        Some("integer")
    );

    let UpstreamBinding::Rest(binding) = &capability.upstream_binding else {
        panic!("expected REST binding");
    };
    assert!(
        binding
            .parameter_bindings
            .iter()
            .any(|parameter| parameter.name == "per_page")
    );
}

#[test]
fn openapi_import_resolves_path_level_parameters_for_github_review_calls() {
    let spec = parse_source_manifest_yaml(
        r"
spec_version: 1
kind: source
name: github_like
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
    base_url: https://api.github.test
",
    )
    .expect("parse source spec");
    let mut raw = BTreeMap::new();
    raw.insert(
        "rest".to_string(),
        RawInterfaceInput::OpenApiDocument {
            bytes: github_reviews_openapi(),
        },
    );

    let result = import_demo("src_github_like", &spec, &raw);
    let capability = find_op(&result.capabilities.capabilities, "pulls_list_reviews");
    let schema = &capability.input_schema.schema;
    assert_eq!(
        schema
            .pointer("/properties/path/properties/owner/type")
            .and_then(Value::as_str),
        Some("string")
    );
    assert_eq!(
        schema
            .pointer("/properties/path/properties/pull_number/type")
            .and_then(Value::as_str),
        Some("integer")
    );
    assert!(
        schema
            .pointer("/required")
            .and_then(Value::as_array)
            .is_some_and(|required| required.iter().any(|value| value == "path"))
    );

    let UpstreamBinding::Rest(binding) = &capability.upstream_binding else {
        panic!("expected REST binding");
    };
    assert_eq!(
        binding
            .parameter_bindings
            .iter()
            .filter(|parameter| {
                parameter.location == coral_capabilities::RestParameterLocation::Path
            })
            .map(|parameter| parameter.name.as_str())
            .collect::<Vec<_>>(),
        vec!["owner", "repo", "pull_number"]
    );
}

#[test]
fn openapi_import_resolves_authenticated_user_events_path_parameter() {
    let spec = parse_source_manifest_yaml(
        r"
spec_version: 1
kind: source
name: github_like
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
    base_url: https://api.github.test
",
    )
    .expect("parse source spec");
    let mut raw = BTreeMap::new();
    raw.insert(
        "rest".to_string(),
        RawInterfaceInput::OpenApiDocument {
            bytes: github_user_events_openapi(),
        },
    );

    let result = import_demo("src_github_like", &spec, &raw);
    let capability = find_op(
        &result.capabilities.capabilities,
        "activity_list_events_for_authenticated_user",
    );
    let schema = &capability.input_schema.schema;
    assert_eq!(
        schema
            .pointer("/properties/path/properties/username/type")
            .and_then(Value::as_str),
        Some("string")
    );

    let UpstreamBinding::Rest(binding) = &capability.upstream_binding else {
        panic!("expected REST binding");
    };
    assert!(binding.parameter_bindings.iter().any(|parameter| {
        parameter.location == coral_capabilities::RestParameterLocation::Path
            && parameter.name == "username"
            && parameter.required
    }));
}

fn github_reviews_openapi() -> Vec<u8> {
    br##"{
          "openapi": "3.0.3",
          "paths": {
            "/repos/{owner}/{repo}/pulls/{pull_number}/reviews": {
              "parameters": [
                { "$ref": "#/components/parameters/owner" },
                { "$ref": "#/components/parameters/repo" },
                { "$ref": "#/components/parameters/pull_number" }
              ],
              "get": {
                "operationId": "pulls/list-reviews",
                "responses": {
                  "200": {
                    "content": {
                      "application/json": {
                        "schema": {
                          "type": "array",
                          "items": { "type": "object" }
                        }
                      }
                    }
                  }
                }
              }
            }
          },
          "components": {
            "parameters": {
              "owner": {
                "name": "owner",
                "in": "path",
                "required": true,
                "schema": { "type": "string" }
              },
              "repo": {
                "name": "repo",
                "in": "path",
                "required": true,
                "schema": { "type": "string" }
              },
              "pull_number": {
                "name": "pull_number",
                "in": "path",
                "required": true,
                "schema": { "type": "integer" }
              }
            }
          }
        }"##
    .to_vec()
}

fn github_user_events_openapi() -> Vec<u8> {
    br##"{
          "openapi": "3.0.3",
          "paths": {
            "/users/{username}/events": {
              "parameters": [
                { "$ref": "#/components/parameters/username" }
              ],
              "get": {
                "operationId": "activity/list-events-for-authenticated-user",
                "responses": {
                  "200": {
                    "content": {
                      "application/json": {
                        "schema": {
                          "type": "array",
                          "items": { "type": "object" }
                        }
                      }
                    }
                  }
                }
              }
            }
          },
          "components": {
            "parameters": {
              "username": {
                "name": "username",
                "in": "path",
                "required": true,
                "schema": { "type": "string" }
              }
            }
          }
        }"##
    .to_vec()
}
