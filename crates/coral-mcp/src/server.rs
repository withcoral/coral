//! RMCP server implementation for Coral's stdio MCP surface.

use coral_api::v1::{
    ExecuteSqlRequest, ListSourcesRequest, ListTablesRequest, ListTablesResponse,
    PaginationRequest, Source, SubmitFeedbackRequest, TableSummary,
};
use coral_client::{
    AppClient, FeedbackClient, QueryClient, SourceClient, batches_to_json_rows,
    decode_execute_sql_response, default_workspace,
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

use crate::{
    McpOptions,
    surface::{
        build_tool_result, feedback_tool, guide_resource, guide_resource_content,
        initial_instructions, internal_status, list_tables_arguments, list_tables_tool,
        list_tables_value, required_string_argument, sql_tool, status_to_error_data,
        tables_resource, tables_resource_content, tool_error_from_status, tool_error_result,
    },
};

const LIST_TABLES_COUNT_LIMIT: u32 = 1;
const LIST_TABLES_UNBOUNDED_LIMIT: u32 = 0;

struct LoadTablesParams<'a> {
    schema_name: Option<&'a str>,
    pagination: PaginationRequest,
    omit_columns: bool,
}

#[derive(Clone)]
pub(crate) struct CoralMcpServer {
    source: SourceClient,
    query: QueryClient,
    feedback: FeedbackClient,
    options: McpOptions,
}

impl CoralMcpServer {
    pub(crate) fn new(app: &AppClient, options: McpOptions) -> Self {
        Self {
            source: app.source_client(),
            query: app.query_client(),
            feedback: app.feedback_client(),
            options,
        }
    }

    async fn load_sources(&self) -> Result<Vec<Source>, tonic::Status> {
        let mut source_client = self.source.clone();
        Ok(source_client
            .list_sources(Request::new(ListSourcesRequest {
                workspace: Some(default_workspace()),
            }))
            .await?
            .into_inner()
            .sources)
    }

    async fn load_tables(
        &self,
        params: LoadTablesParams<'_>,
    ) -> Result<ListTablesResponse, tonic::Status> {
        let mut query_client = self.query.clone();
        Ok(query_client
            .list_tables(Request::new(ListTablesRequest {
                workspace: Some(default_workspace()),
                schema_name: params.schema_name.unwrap_or_default().to_string(),
                pagination: Some(params.pagination),
                omit_columns: params.omit_columns,
            }))
            .await?
            .into_inner())
    }

    async fn load_all_table_summaries(&self) -> Result<Vec<TableSummary>, tonic::Status> {
        Ok(self
            .load_tables(LoadTablesParams {
                schema_name: None,
                pagination: PaginationRequest {
                    limit: LIST_TABLES_UNBOUNDED_LIMIT,
                    offset: 0,
                },
                omit_columns: true,
            })
            .await?
            .table_summaries)
    }

    async fn load_table_count(&self) -> Result<usize, tonic::Status> {
        self.load_tables(LoadTablesParams {
            schema_name: None,
            pagination: PaginationRequest {
                limit: LIST_TABLES_COUNT_LIMIT,
                offset: 0,
            },
            omit_columns: true,
        })
        .await
        .map(|response| {
            response
                .pagination
                .map_or(0, |pagination| pagination.total_count as usize)
        })
    }

    async fn load_sources_and_table_count(&self) -> Result<(Vec<Source>, usize), tonic::Status> {
        tokio::try_join!(self.load_sources(), self.load_table_count())
    }

    async fn load_sources_and_table_summaries(
        &self,
    ) -> Result<(Vec<Source>, Vec<TableSummary>), tonic::Status> {
        tokio::try_join!(self.load_sources(), self.load_all_table_summaries())
    }

    async fn query_rows(&self, sql: &str) -> Result<Vec<Value>, tonic::Status> {
        let mut query_client = self.query.clone();
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

    async fn submit_feedback_value(
        &self,
        trying_to_do: &str,
        tried: &str,
        stuck: &str,
    ) -> Result<Value, tonic::Status> {
        let mut feedback_client = self.feedback.clone();
        let response = feedback_client
            .submit_feedback(Request::new(SubmitFeedbackRequest {
                workspace: Some(default_workspace()),
                trying_to_do: trying_to_do.to_string(),
                tried: tried.to_string(),
                stuck: stuck.to_string(),
            }))
            .await?
            .into_inner();
        let report = response
            .report
            .ok_or_else(|| tonic::Status::internal("feedback response missing report"))?;
        Ok(serde_json::json!({
            "feedback_id": report.id,
            "created_at": report.created_at,
            "message": "Feedback report stored.",
        }))
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
        let (sources, visible_table_count) = self
            .load_sources_and_table_count()
            .await
            .map_err(|status| status_to_error_data(&status))?;
        let mut tools = vec![
            sql_tool(&sources, visible_table_count),
            list_tables_tool(visible_table_count),
        ];
        if self.options.feedback_enabled {
            tools.push(feedback_tool());
        }
        Ok(ListToolsResult::with_all_items(tools))
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
            "list_tables" => {
                let arguments = list_tables_arguments(request.arguments.as_ref())?;
                match self
                    .load_tables(LoadTablesParams {
                        schema_name: arguments.schema.as_deref(),
                        pagination: PaginationRequest {
                            limit: arguments.limit,
                            offset: arguments.offset,
                        },
                        omit_columns: true,
                    })
                    .await
                {
                    Ok(response) => build_tool_result(list_tables_value(&response)),
                    Err(status) => Ok(tool_error_result(tool_error_from_status(
                        "Table listing",
                        &status,
                    ))),
                }
            }
            "feedback" if self.options.feedback_enabled => {
                let trying_to_do =
                    required_string_argument(request.arguments.as_ref(), "trying_to_do")?;
                let tried = required_string_argument(request.arguments.as_ref(), "tried")?;
                let stuck = required_string_argument(request.arguments.as_ref(), "stuck")?;
                match self
                    .submit_feedback_value(&trying_to_do, &tried, &stuck)
                    .await
                {
                    Ok(value) => build_tool_result(value),
                    Err(status) => Ok(tool_error_result(tool_error_from_status(
                        "Feedback submission",
                        &status,
                    ))),
                }
            }
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
        let (sources, visible_table_count) = self
            .load_sources_and_table_count()
            .await
            .map_err(|status| status_to_error_data(&status))?;
        Ok(ListResourcesResult::with_all_items(vec![
            guide_resource(&sources, visible_table_count),
            tables_resource(visible_table_count),
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
                    .load_sources_and_table_summaries()
                    .await
                    .map_err(|status| status_to_error_data(&status))?;
                Ok(ReadResourceResult::new(vec![
                    ResourceContents::text(guide_resource_content(&sources, &tables), request.uri)
                        .with_mime_type("text/markdown"),
                ]))
            }
            "coral://tables" => {
                let tables = self
                    .load_all_table_summaries()
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
