use coral_engine::QueryExecution;
use serde_json::{Value, json};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::QueryManager;
use crate::sources::SourceName;
use crate::sources::manager::{ImportSourceCommand, SourceBindings, SourceManager};
use crate::workspaces::WorkspaceName;

pub(super) fn execution_to_rows(execution: &QueryExecution) -> Vec<Value> {
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

pub(super) async fn mount_v4_openapi_catalog_server(server: &MockServer) {
    for (path_value, id, title) in [
        ("/tagged", 1, "Tagged"),
        ("/public", 2, "Public"),
        ("/search", 3, "Search"),
    ] {
        Mock::given(method("GET"))
            .and(path(path_value))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(json!([{"id": id, "title": title}])),
            )
            .mount(server)
            .await;
    }
}

pub(super) fn import_v4_openapi_catalog_source(
    manager: &QueryManager,
    workspace_name: &WorkspaceName,
    source_name: &str,
    server_uri: &str,
) -> SourceName {
    let descriptor_temp = tempfile::tempdir().expect("descriptor temp dir");
    let openapi_file = descriptor_temp.path().join("catalog-openapi.yaml");
    std::fs::write(
        &openapi_file,
        format!(
            r"
openapi: 3.0.3
info:
  title: Catalog runtime
servers:
  - url: {server_uri}
paths:
  /tagged:
    get:
      tags: [issues]
      operationId: issues/list_tagged
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: '#/components/schemas/Item'
  /public:
    get:
      operationId: list_public
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: '#/components/schemas/Item'
  /search:
    get:
      operationId: search_public
      parameters:
        - name: query
          in: query
          required: true
          schema: {{type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: '#/components/schemas/Item'
components:
  schemas:
    Item:
      type: object
      properties:
        id: {{type: integer}}
        title: {{type: string}}
"
        ),
    )
    .expect("write OpenAPI fixture");
    import_v4_source(
        manager,
        workspace_name,
        source_name,
        format!(
            r"
name: {source_name}
dsl_version: 4
surface:
  type: openapi
  file: {}
",
            openapi_file.display()
        ),
        "OpenAPI",
    )
}

fn v4_mcp_rpc_result(request: &wiremock::Request, result: &Value) -> ResponseTemplate {
    let body: Value = request.body_json().expect("JSON-RPC request body");
    let id = body.get("id").cloned().expect("JSON-RPC request id");
    ResponseTemplate::new(200)
        .append_header("Content-Type", "application/json")
        .set_body_json(json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result
        }))
}

pub(super) async fn mount_v4_mcp_catalog_server(server: &MockServer) {
    Mock::given(method("POST"))
        .respond_with(|request: &wiremock::Request| {
            let body: Value = request.body_json().expect("JSON-RPC request body");
            match body.get("method").and_then(Value::as_str) {
                Some("initialize") => v4_mcp_rpc_result(
                    request,
                    &json!({
                        "protocolVersion": "2025-03-26",
                        "capabilities": {"tools": {}},
                        "serverInfo": {"name": "catalog-runtime", "version": "1.0.0"}
                    }),
                ),
                Some("notifications/initialized") => ResponseTemplate::new(202),
                Some("tools/list") => v4_mcp_rpc_result(
                    request,
                    &json!({
                        "tools": [
                            {
                                "name": "list_items",
                                "description": "List items",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {}
                                },
                                "outputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "items": {
                                            "type": "array",
                                            "items": {"type": "object"}
                                        }
                                    }
                                },
                                "annotations": {"readOnlyHint": true}
                            },
                            {
                                "name": "search_items",
                                "description": "Search items",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "query": {"type": "string"}
                                    },
                                    "required": ["query"]
                                },
                                "outputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "items": {
                                            "type": "array",
                                            "items": {"type": "object"}
                                        }
                                    }
                                },
                                "annotations": {"readOnlyHint": true}
                            }
                        ]
                    }),
                ),
                Some("tools/call") => {
                    let tool_name = body
                        .pointer("/params/name")
                        .and_then(Value::as_str)
                        .expect("tool name");
                    let arguments = body
                        .pointer("/params/arguments")
                        .cloned()
                        .unwrap_or_else(|| json!({}));
                    v4_mcp_rpc_result(
                        request,
                        &json!({
                            "structuredContent": {
                                "tool": tool_name,
                                "arguments": arguments
                            }
                        }),
                    )
                }
                other => ResponseTemplate::new(404)
                    .set_body_string(format!("unexpected MCP method {other:?}")),
            }
        })
        .mount(server)
        .await;
}

pub(super) fn import_v4_mcp_catalog_source(
    manager: &QueryManager,
    workspace_name: &WorkspaceName,
    source_name: &str,
    server_uri: &str,
) -> SourceName {
    import_v4_source(
        manager,
        workspace_name,
        source_name,
        format!(
            r#"
name: {source_name}
dsl_version: 4
surface:
  type: mcp
  server:
    transport: streamable_http
    url: "{server_uri}"
"#
        ),
        "MCP",
    )
}

fn import_v4_source(
    manager: &QueryManager,
    workspace_name: &WorkspaceName,
    source_name: &str,
    manifest_yaml: String,
    surface_name: &str,
) -> SourceName {
    let source_manager = SourceManager::new_for_tests(
        manager.config_store.clone(),
        manager.credential_manager.clone(),
        manager.layout.clone(),
    );
    let source_name = SourceName::parse(source_name).expect("source name");
    source_manager
        .import_source(
            workspace_name,
            &ImportSourceCommand {
                manifest_yaml,
                bindings: SourceBindings::default(),
            },
        )
        .unwrap_or_else(|error| panic!("import v4 {surface_name} source: {error:#}"));
    source_name
}
