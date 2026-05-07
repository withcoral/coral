#![allow(
    missing_docs,
    unused_crate_dependencies,
    reason = "Integration tests only use a subset of the package dependency graph."
)]
#![cfg(feature = "cli-test-server")]

mod harness;

use harness::MockServer;
use rmcp::{
    RoleClient, ServiceExt,
    model::{CallToolRequestParams, ReadResourceRequestParams},
    service::RunningService,
    transport::{ConfigureCommandExt, TokioChildProcess},
};
use serde_json::{Map, Value, json};

fn json_object(value: &Value) -> Map<String, Value> {
    value.as_object().cloned().expect("json object")
}

async fn start_mcp_client(
    server: &MockServer,
) -> Result<RunningService<RoleClient, ()>, Box<dyn std::error::Error>> {
    start_mcp_client_with_args(server, &[]).await
}

async fn start_mcp_client_with_args(
    server: &MockServer,
    args: &[&str],
) -> Result<RunningService<RoleClient, ()>, Box<dyn std::error::Error>> {
    let transport = TokioChildProcess::new(
        tokio::process::Command::new(env!("CARGO_BIN_EXE_coral")).configure(|cmd| {
            cmd.arg("mcp-stdio")
                .args(args)
                .env("CORAL_ENDPOINT", server.endpoint_uri());
        }),
    )?;
    let client = ().serve(transport).await?;
    Ok(client)
}

fn text_content(result: &rmcp::model::ReadResourceResult) -> &str {
    match &result.contents[0] {
        rmcp::model::ResourceContents::TextResourceContents { text, .. } => text,
        other @ rmcp::model::ResourceContents::BlobResourceContents { .. } => {
            panic!("unexpected resource contents: {other:?}")
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_lists_tools_and_resources() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let client = start_mcp_client(&server).await?;

    let tools = client.list_all_tools().await?;
    assert_eq!(
        tools
            .iter()
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>(),
        vec!["sql", "list_tables"]
    );
    assert!(
        tools[0]
            .description
            .as_deref()
            .expect("sql description")
            .contains("3 table(s) are currently visible")
    );
    assert!(
        tools[1]
            .description
            .as_deref()
            .expect("list_tables description")
            .contains("3 table(s) are currently visible")
    );

    let resources = client.list_all_resources().await?;
    assert_eq!(
        resources
            .iter()
            .map(|resource| resource.uri.as_str())
            .collect::<Vec<_>>(),
        vec!["coral://guide", "coral://tables"]
    );

    let guide = client
        .read_resource(ReadResourceRequestParams::new("coral://guide"))
        .await?;
    let guide_text = text_content(&guide);
    assert!(guide_text.contains("## Available Schemas"));
    assert!(guide_text.contains("- local_messages"));
    assert!(guide_text.contains(
        "FROM coral.columns WHERE schema_name = 'local_messages' AND table_name = 'events'"
    ));

    let tables = client
        .read_resource(ReadResourceRequestParams::new("coral://tables"))
        .await?;
    let tables_json: Value = serde_json::from_str(text_content(&tables))?;
    assert_eq!(tables_json["tables"][0]["name"], "local_messages.events");

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_enable_feedback_lists_feedback_tool() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let client = start_mcp_client_with_args(&server, &["--enable-feedback"]).await?;

    let tools = client.list_all_tools().await?;
    assert_eq!(
        tools
            .iter()
            .map(|tool| tool.name.as_ref())
            .collect::<Vec<_>>(),
        vec!["sql", "list_tables", "feedback"]
    );

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_sql_and_list_tables_return_structured_content()
-> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let client = start_mcp_client(&server).await?;

    let tables = client
        .call_tool(CallToolRequestParams::new("list_tables"))
        .await?;
    assert_eq!(tables.is_error, Some(false));
    let structured_tables = tables.structured_content.expect("structured content");
    assert_eq!(structured_tables["total"], 3);
    assert_eq!(structured_tables["limit"], 50);
    assert_eq!(structured_tables["offset"], 0);
    assert_eq!(structured_tables["has_more"], false);
    assert_eq!(
        structured_tables["tables"][0]["name"],
        "local_messages.events"
    );
    assert!(structured_tables["tables"][0]["columns"].is_null());
    let requests = server.list_tables_requests();
    let request = requests.last().expect("list tables request");
    assert_eq!(request.schema_name, "");
    let request_pagination = request.pagination.as_ref().expect("request pagination");
    assert_eq!(request_pagination.limit, 50);
    assert_eq!(request_pagination.offset, 0);
    assert!(request.omit_columns);

    let paginated = client
        .call_tool(
            CallToolRequestParams::new("list_tables").with_arguments(json_object(&json!({
                "schema": "local_messages",
                "limit": 2,
                "offset": 0
            }))),
        )
        .await?;
    let paginated = paginated.structured_content.expect("structured content");
    assert_eq!(paginated["total"], 3);
    assert_eq!(paginated["has_more"], true);
    assert_eq!(paginated["next_offset"], 2);

    let sql = client
        .call_tool(
            CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                "sql": "SELECT text FROM local_messages.messages ORDER BY text"
            }))),
        )
        .await?;
    assert_eq!(sql.is_error, Some(false));
    assert_eq!(
        sql.structured_content.expect("structured content")["rows"][0]["text"],
        "hello"
    );

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn mcp_stdio_tool_errors_do_not_end_the_session() -> Result<(), Box<dyn std::error::Error>> {
    let server = MockServer::start().await;
    let client = start_mcp_client(&server).await?;

    let invalid_sql = client
        .call_tool(
            CallToolRequestParams::new("sql").with_arguments(json_object(&json!({
                "sql": "DELETE FROM local_messages.messages"
            }))),
        )
        .await?;
    assert_eq!(invalid_sql.is_error, Some(true));
    assert_eq!(
        invalid_sql.structured_content.expect("structured content")["error"]["summary"],
        "Query request is invalid"
    );

    let tables = client
        .call_tool(CallToolRequestParams::new("list_tables"))
        .await?;
    assert_eq!(tables.is_error, Some(false));
    assert_eq!(
        tables.structured_content.expect("structured content")["tables"][0]["name"],
        "local_messages.events"
    );

    client.cancel().await?;
    server.shutdown().await;
    Ok(())
}
