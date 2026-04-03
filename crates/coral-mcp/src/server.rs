//! RMCP server implementation for Coral's stdio MCP surface.

use coral_api::v1::{ExecuteSqlRequest, ListSourcesRequest, ListTablesRequest, Source, Table};
use coral_client::{
    AppClient, QueryClient, SourceClient, batches_to_json_rows, decode_execute_sql_response,
    default_workspace,
};
use rmcp::{
    ErrorData, ServerHandler,
    model::{
        CallToolRequestParams, CallToolResult, Implementation, ListResourcesResult,
        ListToolsResult, PaginatedRequestParams, ReadResourceRequestParams, ReadResourceResult,
        ResourceContents, ServerCapabilities, ServerInfo,
    },
    service::{RequestContext, RoleServer},
};
use serde_json::Value;
use tonic::Request;

use crate::surface::{
    build_tool_result, guide_resource, guide_resource_content, initial_instructions,
    internal_status, list_tables_tool, list_tables_value, required_string_argument, sql_tool,
    status_to_error_data, tables_resource, tables_resource_content, tool_error_from_status,
    tool_error_result,
};

#[derive(Clone)]
pub(crate) struct CoralMcpServer {
    source_client: SourceClient,
    query_client: QueryClient,
}

impl CoralMcpServer {
    pub(crate) fn new(app: &AppClient) -> Self {
        Self {
            source_client: app.source_client(),
            query_client: app.query_client(),
        }
    }

    async fn load_sources(&self) -> Result<Vec<Source>, tonic::Status> {
        let mut source_client = self.source_client.clone();
        Ok(source_client
            .list_sources(Request::new(ListSourcesRequest {
                workspace: Some(default_workspace()),
            }))
            .await?
            .into_inner()
            .sources)
    }

    async fn load_tables(&self) -> Result<Vec<Table>, tonic::Status> {
        let mut query_client = self.query_client.clone();
        Ok(query_client
            .list_tables(Request::new(ListTablesRequest {
                workspace: Some(default_workspace()),
            }))
            .await?
            .into_inner()
            .tables)
    }

    async fn load_sources_and_tables(&self) -> Result<(Vec<Source>, Vec<Table>), tonic::Status> {
        tokio::try_join!(self.load_sources(), self.load_tables())
    }

    async fn query_rows(&self, sql: &str) -> Result<Vec<Value>, tonic::Status> {
        let mut query_client = self.query_client.clone();
        let response = query_client
            .execute_sql(Request::new(ExecuteSqlRequest {
                workspace: Some(default_workspace()),
                sql: sql.to_string(),
            }))
            .await?
            .into_inner();
        let result = decode_execute_sql_response(&response)
            .map_err(|error| tonic::Status::internal(error.to_string()))?;
        batches_to_json_rows(result.batches())
            .map_err(|error| tonic::Status::internal(error.to_string()))
    }

    async fn execute_sql_value(&self, sql: &str) -> Result<Value, tonic::Status> {
        self.query_rows(sql)
            .await
            .map(|rows| serde_json::json!({ "rows": rows }))
    }
}

impl ServerHandler for CoralMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_resources()
                .enable_tools()
                .build(),
        )
        .with_server_info(Implementation::new("coral", env!("CARGO_PKG_VERSION")))
        .with_instructions(initial_instructions())
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let (sources, tables) = self
            .load_sources_and_tables()
            .await
            .map_err(|status| status_to_error_data(&status))?;
        Ok(ListToolsResult::with_all_items(vec![
            sql_tool(&sources, &tables),
            list_tables_tool(&tables),
        ]))
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        match request.name.as_ref() {
            "sql" => {
                let sql = required_string_argument(request.arguments.as_ref(), "sql")?;
                match self.execute_sql_value(&sql).await {
                    Ok(value) => build_tool_result(value),
                    Err(status) => Ok(tool_error_result(tool_error_from_status("Query", &status))),
                }
            }
            "list_tables" => match self.load_tables().await {
                Ok(tables) => build_tool_result(list_tables_value(&tables)),
                Err(status) => Ok(tool_error_result(tool_error_from_status(
                    "Table listing",
                    &status,
                ))),
            },
            _ => Err(ErrorData::invalid_params(
                format!("tool '{}' not found", request.name),
                None,
            )),
        }
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        let (sources, tables) = self
            .load_sources_and_tables()
            .await
            .map_err(|status| status_to_error_data(&status))?;
        Ok(ListResourcesResult::with_all_items(vec![
            guide_resource(&sources, &tables),
            tables_resource(&tables),
        ]))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, ErrorData> {
        match request.uri.as_str() {
            "coral://guide" => {
                let (sources, tables) = self
                    .load_sources_and_tables()
                    .await
                    .map_err(|status| status_to_error_data(&status))?;
                Ok(ReadResourceResult::new(vec![
                    ResourceContents::text(guide_resource_content(&sources, &tables), request.uri)
                        .with_mime_type("text/markdown"),
                ]))
            }
            "coral://tables" => {
                let tables = self
                    .load_tables()
                    .await
                    .map_err(|status| status_to_error_data(&status))?;
                let text = tables_resource_content(&tables)
                    .map_err(|error| internal_status(&error))
                    .map_err(|status| status_to_error_data(&status))?;
                Ok(ReadResourceResult::new(vec![
                    ResourceContents::text(text, request.uri).with_mime_type("application/json"),
                ]))
            }
            _ => Err(ErrorData::resource_not_found(
                format!("resource '{}' not found", request.uri),
                None,
            )),
        }
    }
}
