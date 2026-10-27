//! MCP Materialized Tool Catalog acceptance at the public Module seam.
#![allow(
    unused_crate_dependencies,
    reason = "integration targets inherit the library's direct dependency set"
)]
#![expect(
    clippy::indexing_slicing,
    reason = "acceptance fixtures assert known JSON members"
)]

use coral_catalog::AuthenticationConfiguration;
use coral_catalog::{
    ExecutionBinding, MaterializationOutcome, MaterializedTool, McpDiscoverySnapshot,
    McpExecutionBinding, McpInvocationMethod, McpTaskSupport, ProviderSetupInput, ToolAvailability,
    ToolHints, materialize,
};
use coral_outbound::DestinationPolicy;

const MCP_ARCHITECTURE_CORPUS: &str = include_str!("fixtures/mcp-tool-corpus.json");

fn ready(snapshot: &str) -> coral_catalog::PreparedProviderCatalog {
    let snapshot = McpDiscoverySnapshot::parse(snapshot).expect("bounded MCP discovery snapshot");
    match materialize(
        DestinationPolicy::default(),
        &ProviderSetupInput::from(snapshot),
        &[],
    ) {
        MaterializationOutcome::Ready(prepared) => prepared,
        MaterializationOutcome::Rejected(diagnostic) => panic!(
            "fixture should materialize: {}: {}",
            diagnostic.code(),
            diagnostic.message()
        ),
    }
}

fn mcp_binding(tool: &MaterializedTool) -> &McpExecutionBinding {
    let ExecutionBinding::Mcp(binding) = tool.execution_binding() else {
        panic!("MCP materialization must produce an MCP Execution Binding");
    };
    binding
}

#[test]
fn corpus_keeps_exactly_the_declared_hints() {
    let prepared = ready(MCP_ARCHITECTURE_CORPUS);
    for (key, expected) in [
        (
            "list_issues",
            ToolHints::new(Some(true), Some(false), Some(true), Some(true)),
        ),
        (
            "delete_file",
            ToolHints::new(Some(false), Some(true), Some(false), Some(true)),
        ),
        // No annotations: an undeclared hint never takes the protocol default.
        ("get-structured-content", ToolHints::default()),
    ] {
        let tool = prepared
            .tools()
            .iter()
            .find(|tool| tool.key().as_str() == key)
            .expect("corpus Tool");
        assert_eq!(tool.hints(), expected, "{key}");
    }
}

#[test]
fn corpus_preserves_exact_schemas_annotations_and_result_field_presence() {
    let prepared = ready(MCP_ARCHITECTURE_CORPUS);

    let list = prepared
        .tools()
        .iter()
        .find(|tool| tool.key().as_str() == "list_issues")
        .expect("list_issues Tool");
    let list_binding = mcp_binding(list);
    assert_eq!(
        list_binding.transport(),
        coral_catalog::McpTransport::StreamableHttp
    );
    assert_eq!(list_binding.server_url(), "https://mcp.example.test/mcp");
    assert_eq!(
        list_binding
            .annotations()
            .expect("declared annotations")
            .read_only_hint(),
        Some(true),
        "the binding keeps the hint that made the Tool read-only"
    );
    assert!(list_binding.output_schema().is_none());
    let list_input = prepared
        .schema_graph()
        .schema(list_binding.input_schema())
        .expect("authoritative list input schema");
    assert_eq!(list_input["properties"]["owner"]["minLength"], 1);
    assert_eq!(
        list_input["properties"]["labels"]["items"]["additionalProperties"],
        false
    );
    assert!(list.typescript_declaration().contains("providerAnnotated"));
    assert!(list.typescript_declaration().contains("binaryToken"));
    assert!(!list.typescript_declaration().contains("BinaryValue"));

    let delete = prepared
        .tools()
        .iter()
        .find(|tool| tool.key().as_str() == "delete_file")
        .expect("delete_file Tool");
    assert_eq!(
        mcp_binding(delete)
            .annotations()
            .expect("explicit negative annotation")
            .read_only_hint(),
        Some(false),
        "an explicit false hint remains distinct from an absent hint"
    );

    let structured = prepared
        .tools()
        .iter()
        .find(|tool| tool.key().as_str() == "get-structured-content")
        .expect("structured-content Tool");
    let structured_binding = mcp_binding(structured);
    let output_schema = prepared
        .schema_graph()
        .schema(
            structured_binding
                .output_schema()
                .expect("declared outputSchema presence"),
        )
        .expect("authoritative output schema");
    assert_eq!(
        output_schema["properties"]["conditions"]["enum"],
        serde_json::json!(["sunny", "cloudy", "rain"])
    );
    assert!(
        structured
            .typescript_declaration()
            .contains("providerSecret")
    );
    for field in [
        "content: Array<McpContentBlock>",
        "isError?: false",
        "_meta?: McpSafeMeta",
    ] {
        assert!(structured.typescript_declaration().contains(field));
    }
    assert!(
        structured
            .typescript_declaration()
            .contains("structuredContent?"),
        "the authoring contract keeps content and structured content as distinct channels"
    );
    for retained_field in ["annotations?", "title?", "icons?", "_meta?"] {
        assert!(
            structured.typescript_declaration().contains(retained_field),
            "MCP content contract should retain {retained_field}"
        );
    }
}

#[test]
fn standalone_mcp_schema_references_remain_exact_and_projectable() {
    let snapshot = serde_json::json!({
        "protocolVersion": "2025-11-25",
        "serverInfo": { "name": "schema-references", "version": "1" },
        "server": { "transport": "streamable_http", "url": "https://refs.example.test/mcp" },
        "capabilities": { "tools": {} },
        "tools": [{
            "name": "referenced",
            "inputSchema": {
                "$defs": {
                    "Query": {
                        "type": "object",
                        "required": ["text"],
                        "properties": {
                            "text": { "type": "string" },
                            "next": { "$ref": "#/$defs/Query" },
                            "opaque": { "const": { "$ref": "not-a-schema", "$id": "opaque-data" } }
                        },
                        "additionalProperties": false
                    }
                },
                "$ref": "#/$defs/Query"
            }
        }]
    });
    let prepared = ready(&snapshot.to_string());
    let tool = &prepared.tools()[0];
    assert_eq!(tool.availability(), ToolAvailability::Available);
    let binding = mcp_binding(tool);
    assert_eq!(
        binding.tool_definition().expect("exact tool definition")["inputSchema"]["$ref"],
        "#/$defs/Query"
    );
    assert!(
        prepared
            .schema_graph()
            .schema(binding.input_schema())
            .expect("normalized authoritative schema")["$ref"]
            .as_str()
            .is_some_and(|reference| reference.starts_with("schema_"))
    );
    assert!(tool.typescript_declaration().contains("text"));
}

#[test]
fn duplicated_required_entries_are_deduplicated_in_the_schema_graph() {
    let snapshot = serde_json::json!({
        "protocolVersion": "2025-11-25",
        "serverInfo": { "name": "duplicate-required", "version": "1" },
        "server": { "transport": "streamable_http", "url": "https://dupes.example.test/mcp" },
        "capabilities": { "tools": {} },
        "tools": [{
            "name": "lookup",
            "inputSchema": {
                "$defs": {
                    "Filter": {
                        "type": "object",
                        "required": ["field", "field"],
                        "properties": { "field": { "type": "string" } }
                    }
                },
                "type": "object",
                "required": ["query", "filter", "query"],
                "properties": {
                    "query": { "type": "string" },
                    "filter": { "$ref": "#/$defs/Filter" }
                }
            }
        }]
    });
    let prepared = ready(&snapshot.to_string());
    let tool = &prepared.tools()[0];
    assert_eq!(tool.availability(), ToolAvailability::Available);
    let graph = prepared.schema_graph();
    let input = graph
        .schema(mcp_binding(tool).input_schema())
        .expect("input schema");
    assert_eq!(input["required"], serde_json::json!(["query", "filter"]));
    let filter: coral_catalog::SchemaId =
        serde_json::from_value(input["properties"]["filter"]["$ref"].clone())
            .expect("Filter reference names a package-local schema");
    assert_eq!(
        graph.schema(&filter).expect("Filter schema")["required"],
        serde_json::json!(["field"])
    );
}

#[test]
fn tool_scoped_schema_names_do_not_collide_and_non_object_roots_are_unavailable() {
    let snapshot = serde_json::json!({
        "protocolVersion": "2025-11-25",
        "serverInfo": { "name": "schema-names", "version": "1" },
        "server": { "transport": "streamable_http", "url": "https://schemas.example.test/mcp" },
        "capabilities": { "tools": {} },
        "tools": [
            {
                "name": "text_value",
                "inputSchema": {
                    "$defs": { "Shared": { "type": "string" } },
                    "type": "object",
                    "properties": { "value": { "$ref": "#/$defs/Shared" } }
                }
            },
            {
                "name": "numeric_value",
                "inputSchema": {
                    "$defs": { "Shared": { "type": "number" } },
                    "type": "object",
                    "properties": { "value": { "$ref": "#/$defs/Shared" } }
                }
            },
            { "name": "scalar_root", "inputSchema": { "type": "string" } },
            {
                "name": "nested_id_scope",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "nested": { "$id": "nested", "type": "object" }
                    }
                }
            }
        ]
    });
    let prepared = ready(&snapshot.to_string());
    let available = prepared
        .tools()
        .iter()
        .filter(|tool| tool.availability() == ToolAvailability::Available)
        .collect::<Vec<_>>();
    assert_eq!(available.len(), 2);
    let declarations = available
        .iter()
        .map(|tool| prepared.authoring_declaration(tool))
        .collect::<Vec<_>>();
    let aliases = declarations
        .iter()
        .flat_map(|declaration| declaration.lines())
        .filter(|line| line.starts_with("type SharedSchema"))
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(aliases.len(), 2, "tool-local schema names stay distinct");
    let unsupported = prepared
        .tools()
        .iter()
        .find(|tool| tool.key().as_str() == "scalar_root")
        .expect("unsupported schema Tool");
    assert_eq!(unsupported.availability(), ToolAvailability::Unavailable);
    assert_eq!(
        unsupported.diagnostic().expect("safe diagnostic").code(),
        "unsupported_mcp_input_schema"
    );
    let scoped = prepared
        .tools()
        .iter()
        .find(|tool| tool.key().as_str() == "nested_id_scope")
        .expect("unsupported scoped schema Tool");
    assert_eq!(scoped.availability(), ToolAvailability::Unavailable);
}

#[test]
fn malformed_server_capability_rejects_the_snapshot() {
    let snapshot = serde_json::json!({
        "protocolVersion": "2025-11-25",
        "serverInfo": { "name": "capabilities", "version": "1" },
        "server": { "transport": "streamable_http", "url": "https://capabilities.example.test/mcp" },
        "capabilities": { "resources": { "subscribe": "yes" } }
    });
    let snapshot = McpDiscoverySnapshot::parse(&snapshot.to_string()).expect("bounded snapshot");
    let MaterializationOutcome::Rejected(diagnostic) = materialize(
        DestinationPolicy::default(),
        &ProviderSetupInput::from(snapshot),
        &[],
    ) else {
        panic!("malformed capability should reject the package root");
    };
    assert_eq!(diagnostic.code(), "invalid_mcp_discovery_snapshot");
}

#[test]
fn invalid_presentation_metadata_warns_without_changing_structural_availability() {
    let snapshot = serde_json::json!({
        "protocolVersion": "2025-11-25",
        "serverInfo": { "name": "metadata", "version": "1" },
        "server": { "transport": "streamable_http", "url": "https://metadata.example.test/mcp" },
        "capabilities": { "tools": {} },
        "tools": [{
            "name": "still_callable",
            "title": 42,
            "description": "Safe fallback",
            "inputSchema": { "type": "object", "additionalProperties": false }
        }]
    });
    let prepared = ready(&snapshot.to_string());
    let tool = &prepared.tools()[0];
    assert_eq!(tool.availability(), ToolAvailability::Available);
    assert!(tool.diagnostic().is_none());
    assert!(
        tool.diagnostics()
            .iter()
            .any(|diagnostic| diagnostic.code() == "invalid_mcp_tool_metadata")
    );
}

#[test]
fn malformed_annotation_member_retains_valid_effect_evidence_but_blocks_the_tool() {
    let snapshot = serde_json::json!({
        "protocolVersion": "2025-11-25",
        "serverInfo": { "name": "annotations", "version": "1" },
        "server": { "transport": "streamable_http", "url": "https://annotations.example.test/mcp" },
        "capabilities": { "tools": {} },
        "tools": [{
            "name": "partly_invalid_annotations",
            "inputSchema": { "type": "object", "additionalProperties": false },
            "annotations": { "readOnlyHint": true, "destructiveHint": "invalid" }
        }]
    });
    let prepared = ready(&snapshot.to_string());
    let tool = &prepared.tools()[0];
    assert_eq!(tool.availability(), ToolAvailability::Unavailable);
    assert_eq!(
        tool.hints(),
        ToolHints::new(Some(true), None, None, None),
        "an invalid hint is undeclared; valid hints remain"
    );
    assert_eq!(
        tool.diagnostic().expect("safe diagnostic").code(),
        "invalid_mcp_tool_annotations"
    );
    assert_eq!(
        mcp_binding(tool)
            .annotations()
            .expect("valid annotation evidence remains present")
            .read_only_hint(),
        Some(true)
    );
}

#[test]
fn required_tasks_are_unavailable_while_optional_tasks_stay_direct() {
    let prepared = ready(MCP_ARCHITECTURE_CORPUS);
    let required = prepared
        .tools()
        .iter()
        .find(|tool| tool.key().as_str() == "simulate-research-query")
        .expect("task-required corpus Tool");
    assert_eq!(required.availability(), ToolAvailability::Unavailable);
    assert_eq!(
        required.diagnostic().expect("safe task diagnostic").code(),
        "upstream_mcp_task_required"
    );
    let binding = mcp_binding(required);
    assert_eq!(
        binding.declared_task_support(),
        Some(McpTaskSupport::Required)
    );
    assert_eq!(
        binding
            .server_task_capability()
            .expect("declared server Task capability")["requests"]["tools"]["call"],
        serde_json::json!({})
    );
    assert_eq!(binding.invocation_method(), McpInvocationMethod::CallTool);

    let optional = serde_json::json!({
        "protocolVersion": "2025-11-25",
        "serverInfo": { "name": "optional-tasks", "version": "1" },
        "server": { "transport": "streamable_http", "url": "https://tasks.example.test" },
        "capabilities": { "tools": {} },
        "tools": [{
            "name": "optional_task",
            "inputSchema": { "type": "object", "additionalProperties": false },
            "execution": { "taskSupport": "optional" }
        }]
    });
    let prepared = ready(&optional.to_string());
    let optional = &prepared.tools()[0];
    assert_eq!(optional.availability(), ToolAvailability::Available);
    let binding = mcp_binding(optional);
    assert_eq!(binding.server_url(), "https://tasks.example.test");
    assert_eq!(
        binding.declared_task_support(),
        Some(McpTaskSupport::Optional)
    );
    assert_eq!(binding.invocation_method(), McpInvocationMethod::CallTool);
}

#[test]
fn resource_capability_synthesizes_one_opaque_provider_bound_read_tool() {
    let prepared = ready(MCP_ARCHITECTURE_CORPUS);
    let reads = prepared
        .tools()
        .iter()
        .filter(|tool| tool.relative_path() == "resources.read")
        .collect::<Vec<_>>();
    assert_eq!(reads.len(), 1);
    let read = reads[0];
    assert_eq!(read.key().as_str(), "resources/read");
    assert_eq!(read.hints(), ToolHints::new(Some(true), None, None, None));
    let binding = mcp_binding(read);
    assert_eq!(
        binding.invocation_method(),
        McpInvocationMethod::ReadResource
    );
    assert!(binding.tool_name().is_none());
    assert!(binding.tool_definition().is_none());
    let input = prepared
        .schema_graph()
        .schema(binding.input_schema())
        .expect("synthesized opaque input schema");
    assert_eq!(
        input["properties"]["reference"]["x-coral-opaque-type"],
        "McpResourceRef"
    );
    assert_eq!(
        input["properties"]["reference"]["not"],
        serde_json::json!({}),
        "no ordinary JSON value can satisfy the opaque host reference schema"
    );
    assert!(input["properties"]["reference"].get("uri").is_none());
    assert!(!read.typescript_declaration().contains("structuredContent"));
    assert!(!read.typescript_declaration().contains("isError"));
    assert!(!read.typescript_declaration().contains("uri"));
    assert!(read.typescript_declaration().contains("contents:"));
    assert!(
        read.typescript_declaration()
            .contains("McpResourceContents = { readonly reference: McpResourceRef")
    );
    assert!(
        read.typescript_declaration()
            .contains("export default function (input: { reference: McpResourceRef })")
    );

    let resources_only = serde_json::json!({
        "protocolVersion": "2025-11-25",
        "serverInfo": { "name": "resources-only", "version": "1" },
        "server": { "transport": "streamable_http", "url": "https://resources.example.test/mcp" },
        "capabilities": { "resources": {} }
    });
    let resources_only = ready(&resources_only.to_string());
    assert_eq!(resources_only.tools().len(), 1);
    assert_eq!(resources_only.tools()[0].relative_path(), "resources.read");

    let without_resources = serde_json::json!({
        "protocolVersion": "2025-11-25",
        "serverInfo": { "name": "tools-only", "version": "1" },
        "server": { "transport": "streamable_http", "url": "https://tools.example.test/mcp" },
        "capabilities": { "tools": {} },
        "tools": [{
            "name": "ping",
            "inputSchema": { "type": "object", "additionalProperties": false }
        }]
    });
    assert!(
        ready(&without_resources.to_string())
            .tools()
            .iter()
            .all(|tool| tool.relative_path() != "resources.read")
    );
}

#[test]
fn multiline_tool_summaries_survive_materialization() {
    let prepared = ready(
        r#"{
  "protocolVersion": "2025-11-25",
  "serverInfo": {"name": "split-dns", "version": "1.0.0"},
  "server": {"transport": "streamable_http", "url": "https://mcp.example.test/mcp"},
  "capabilities": {"tools": {}},
  "tools": [
    {
      "name": "set_split_dns",
      "description": "Replaces the entire split DNS configuration.\n\nAny domain absent from the request body loses its nameservers.\n",
      "inputSchema": {"type": "object", "additionalProperties": {"type": "array", "items": {"type": "string"}}}
    }
  ]
}"#,
    );
    let tool = prepared.tools().first().expect("Tool");
    let summary = tool.summary();

    // The tool declares no `title`, so its description is also the summary.
    // Rejecting the newlines left only the bare tool name.
    assert!(
        summary.contains("Any domain absent from the request body"),
        "the multiline summary must not fall back to the tool name: {summary}"
    );
    assert_eq!(summary, summary.trim());
    assert!(summary.contains('\n'));
}

#[test]
fn snapshot_credential_contract_is_retained_on_every_mcp_binding() {
    let expected: coral_catalog::ProviderCredentialContract =
        AuthenticationConfiguration::OAuthAuthorizationCode {
            scope_format: coral_catalog::OAuthScopeFormat::Comma,
            authorization_url: "https://auth.example.test/authorize".into(),
            token_url: "https://auth.example.test/token".into(),
            scopes: vec!["read".into()],
            resource: None,
            issuer: None,
            client: None,
        }
        .into();
    let snapshot = serde_json::json!({
        "protocolVersion": "2025-11-25",
        "serverInfo": { "name": "auth", "version": "1" },
        "server": { "transport": "streamable_http", "url": "https://auth.example.test/mcp" },
        "capabilities": { "tools": {}, "resources": {} },
        "credentialContract": expected,
        "tools": [{ "name": "lookup", "inputSchema": { "type": "object" } }]
    });
    let prepared = ready(&snapshot.to_string());
    assert_eq!(prepared.credential_contract(), expected);
    for tool in prepared.tools() {
        assert_eq!(mcp_binding(tool).credential_contract(), &expected);
    }
}

#[test]
fn absent_snapshot_credential_contract_is_credential_free_and_headers_are_rejected() {
    let credential_free = r#"{
  "protocolVersion": "2025-11-25",
  "serverInfo": { "name": "open", "version": "1" },
  "server": { "transport": "streamable_http", "url": "https://open.example.test/mcp" },
  "capabilities": { "tools": {} },
  "tools": [{ "name": "lookup", "inputSchema": { "type": "object" } }]
}"#;
    assert_eq!(
        ready(credential_free).credential_contract(),
        coral_catalog::ProviderCredentialContract::NONE
    );
    // MCP has no recipe for a credential a person types in.
    for authentication in [
        r#"{"method":"header_credentials","headers":{"x-key":{"source":"field","field":"key"}}}"#,
        r#"{"method":"basic_auth","username":{"source":"field","field":"email"},"password":{"source":"field","field":"token"}}"#,
        r#"{"method":"query_credentials","parameters":{"key":{"source":"field","field":"key"}}}"#,
    ] {
        let contract = credential_free.replace(
            r#""capabilities": { "tools": {} },"#,
            &format!(
                r#""capabilities": {{ "tools": {{}} }}, "credentialContract": {{"authentication":{authentication},"base_urls":[]}},"#
            ),
        );
        let snapshot = McpDiscoverySnapshot::parse(&contract).expect("snapshot");
        let MaterializationOutcome::Rejected(diagnostic) = materialize(
            DestinationPolicy::default(),
            &ProviderSetupInput::from(snapshot),
            &[],
        ) else {
            panic!("an MCP snapshot cannot select {authentication}");
        };
        assert_eq!(diagnostic.code(), "invalid_credential_contract");
    }
}
