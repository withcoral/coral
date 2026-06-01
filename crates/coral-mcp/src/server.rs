//! RMCP server implementation for Coral's stdio MCP surface.

use coral_api::v1::{
    CatalogItemKind as ProtoCatalogItemKind, DescribeTableRequest, DescribeTableResponse,
    ExecuteSqlRequest, ListCatalogRequest, ListCatalogResponse, ListColumnsRequest,
    ListSourcesRequest, PaginationRequest, SearchCatalogRequest, Source, SubmitFeedbackRequest,
    TableSummary as ProtoTableSummary, catalog_item,
};
use coral_client::{
    AppClient, CatalogClient, FeedbackClient, QueryClient, SourceClient,
    batches_to_json_rows_json_safe_numbers, decode_execute_sql_response, default_workspace,
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
use serde::Serialize;
use serde_json::{Map, Value};
use tonic::Request;

use crate::{
    McpOptions,
    surface::{
        CatalogToolKind, build_tool_result, describe_table_arguments, describe_table_tool,
        describe_table_value, feedback_tool, guide_resource, guide_resource_content,
        initial_instructions, list_catalog_arguments, list_catalog_tool, list_catalog_value,
        list_columns_arguments, list_columns_tool, list_columns_value, required_string_argument,
        search_catalog_arguments, search_catalog_tool, search_catalog_value, sql_tool,
        status_to_error_data, tables_resource, tables_resource_content, tool_error_from_status,
        tool_error_result,
    },
    telemetry,
};

const LIST_TABLES_COUNT_LIMIT: u32 = 1;
const LIST_TABLE_FUNCTIONS_COUNT_LIMIT: u32 = 1;
const LIST_CATALOG_UNBOUNDED_LIMIT: u32 = 0;
const CATALOG_KIND_ALL: ProtoCatalogItemKind = ProtoCatalogItemKind::Unspecified;
const CATALOG_KIND_TABLE: ProtoCatalogItemKind = ProtoCatalogItemKind::Table;
const CATALOG_KIND_TABLE_FUNCTION: ProtoCatalogItemKind = ProtoCatalogItemKind::TableFunction;

enum ToolCallOutcome {
    Success(Value),
    ToolError {
        operation: &'static str,
        status: tonic::Status,
    },
}

#[derive(Serialize)]
struct SqlRowsValue {
    rows: Vec<Value>,
}

#[derive(Serialize)]
struct FeedbackStoredValue {
    feedback_id: String,
    created_at: String,
    message: &'static str,
}

fn serialize_tool_value(value: impl Serialize) -> Result<Value, tonic::Status> {
    serde_json::to_value(value).map_err(|error| tonic::Status::internal(error.to_string()))
}

impl ToolCallOutcome {
    fn from_value_result(operation: &'static str, result: Result<Value, tonic::Status>) -> Self {
        match result {
            Ok(value) => Self::Success(value),
            Err(status) => Self::ToolError { operation, status },
        }
    }
}

#[derive(Clone)]
pub(crate) struct CoralMcpServer {
    source: SourceClient,
    catalog: CatalogClient,
    query: QueryClient,
    feedback: FeedbackClient,
    options: McpOptions,
}

impl CoralMcpServer {
    pub(crate) fn new(app: &AppClient, options: McpOptions) -> Self {
        Self {
            source: app.source_client(),
            catalog: app.catalog_client(),
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

    async fn load_catalog(
        &self,
        schema_name: Option<&str>,
        kind: ProtoCatalogItemKind,
        pagination: PaginationRequest,
    ) -> Result<ListCatalogResponse, tonic::Status> {
        let mut catalog_client = self.catalog.clone();
        Ok(catalog_client
            .list_catalog(Request::new(ListCatalogRequest {
                workspace: Some(default_workspace()),
                schema_name: schema_name.unwrap_or_default().to_string(),
                kind: kind as i32,
                pagination: Some(pagination),
            }))
            .await?
            .into_inner())
    }

    async fn load_all_table_summaries(&self) -> Result<Vec<ProtoTableSummary>, tonic::Status> {
        self.load_catalog(
            None,
            CATALOG_KIND_TABLE,
            PaginationRequest {
                limit: LIST_CATALOG_UNBOUNDED_LIMIT,
                offset: 0,
            },
        )
        .await
        .map(|response| {
            response
                .items
                .into_iter()
                .filter_map(|item| match item.item {
                    Some(catalog_item::Item::Table(table)) => Some(table),
                    Some(catalog_item::Item::TableFunction(_)) | None => None,
                })
                .collect()
        })
    }

    async fn load_guide_catalog(
        &self,
    ) -> Result<(Vec<ProtoTableSummary>, Vec<String>), tonic::Status> {
        self.load_catalog(
            None,
            CATALOG_KIND_ALL,
            PaginationRequest {
                limit: LIST_CATALOG_UNBOUNDED_LIMIT,
                offset: 0,
            },
        )
        .await
        .map(guide_catalog_from_response)
    }

    async fn load_table_description(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<DescribeTableResponse, tonic::Status> {
        let mut catalog_client = self.catalog.clone();
        Ok(catalog_client
            .describe_table(Request::new(DescribeTableRequest {
                workspace: Some(default_workspace()),
                schema_name: schema_name.to_string(),
                table_name: table_name.to_string(),
            }))
            .await?
            .into_inner())
    }

    async fn load_table_count(&self) -> Result<usize, tonic::Status> {
        self.load_catalog(
            None,
            CATALOG_KIND_TABLE,
            PaginationRequest {
                limit: LIST_TABLES_COUNT_LIMIT,
                offset: 0,
            },
        )
        .await
        .map(|response| {
            response
                .pagination
                .map_or(0, |pagination| pagination.total_count as usize)
        })
    }

    async fn load_table_function_count(&self) -> Result<usize, tonic::Status> {
        self.load_catalog(
            None,
            CATALOG_KIND_TABLE_FUNCTION,
            PaginationRequest {
                limit: LIST_TABLE_FUNCTIONS_COUNT_LIMIT,
                offset: 0,
            },
        )
        .await
        .map(|response| {
            response
                .pagination
                .map_or(0, |pagination| pagination.total_count as usize)
        })
    }

    async fn load_sources_and_catalog_counts(
        &self,
    ) -> Result<(Vec<Source>, usize, usize), tonic::Status> {
        tokio::try_join!(
            self.load_sources(),
            self.load_table_count(),
            self.load_table_function_count()
        )
    }

    async fn load_sources_and_guide_catalog(
        &self,
    ) -> Result<(Vec<Source>, Vec<ProtoTableSummary>, Vec<String>), tonic::Status> {
        let (sources, (tables, table_function_schema_names)) =
            tokio::try_join!(self.load_sources(), self.load_guide_catalog())?;
        Ok((sources, tables, table_function_schema_names))
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
        batches_to_json_rows_json_safe_numbers(result.batches())
            .map_err(|error| tonic::Status::internal(error.to_string()))
    }

    async fn execute_sql_value(&self, sql: &str) -> Result<Value, tonic::Status> {
        serialize_tool_value(SqlRowsValue {
            rows: self.query_rows(sql).await?,
        })
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
        serialize_tool_value(FeedbackStoredValue {
            feedback_id: report.id,
            created_at: report.created_at,
            message: "Feedback report stored.",
        })
    }

    async fn search_catalog_tool_result(
        &self,
        request_arguments: Option<&Map<String, Value>>,
    ) -> Result<ToolCallOutcome, ErrorData> {
        let arguments = search_catalog_arguments(request_arguments)?;
        let mut catalog_client = self.catalog.clone();
        match catalog_client
            .search_catalog(Request::new(SearchCatalogRequest {
                workspace: Some(default_workspace()),
                pattern: arguments.pattern,
                ignore_case: arguments.ignore_case,
                schema_name: arguments.schema.unwrap_or_default(),
                kind: catalog_item_kind_from_tool(arguments.kind) as i32,
                pagination: Some(PaginationRequest {
                    limit: arguments.pagination.limit,
                    offset: arguments.pagination.offset,
                }),
            }))
            .await
            .map(|response| search_catalog_value(&response.into_inner()))
        {
            Ok(value) => Ok(ToolCallOutcome::Success(value)),
            Err(status) if status.code() == tonic::Code::InvalidArgument => {
                Err(status_to_error_data(&status))
            }
            Err(status) => Ok(ToolCallOutcome::ToolError {
                operation: "Catalog search",
                status,
            }),
        }
    }

    async fn list_catalog_tool_result(
        &self,
        request_arguments: Option<&Map<String, Value>>,
    ) -> Result<ToolCallOutcome, ErrorData> {
        let arguments = list_catalog_arguments(request_arguments)?;
        let mut catalog_client = self.catalog.clone();
        let result = catalog_client
            .list_catalog(Request::new(ListCatalogRequest {
                workspace: Some(default_workspace()),
                schema_name: arguments.schema.unwrap_or_default(),
                kind: catalog_item_kind_from_tool(arguments.kind) as i32,
                pagination: Some(PaginationRequest {
                    limit: arguments.pagination.limit,
                    offset: arguments.pagination.offset,
                }),
            }))
            .await
            .map(|response| list_catalog_value(&response.into_inner()));
        Ok(ToolCallOutcome::from_value_result(
            "Catalog listing",
            result,
        ))
    }

    async fn describe_table_tool_result(
        &self,
        request_arguments: Option<&Map<String, Value>>,
    ) -> Result<ToolCallOutcome, ErrorData> {
        let arguments = describe_table_arguments(request_arguments)?;
        match self
            .load_table_description(&arguments.schema, &arguments.table)
            .await
        {
            Ok(response) => Ok(ToolCallOutcome::Success(describe_table_value(
                &arguments.schema,
                &arguments.table,
                &response,
            ))),
            Err(status) => Ok(ToolCallOutcome::ToolError {
                operation: "Table description",
                status,
            }),
        }
    }

    async fn dispatch_tool(
        &self,
        request: CallToolRequestParams,
    ) -> Result<ToolCallOutcome, ErrorData> {
        match request.name.as_ref() {
            "sql" => {
                let sql = required_string_argument(request.arguments.as_ref(), "sql")?;
                Ok(ToolCallOutcome::from_value_result(
                    "Query",
                    self.execute_sql_value(&sql).await,
                ))
            }
            "list_catalog" => {
                self.list_catalog_tool_result(request.arguments.as_ref())
                    .await
            }
            "search_catalog" => {
                self.search_catalog_tool_result(request.arguments.as_ref())
                    .await
            }
            "describe_table" => {
                self.describe_table_tool_result(request.arguments.as_ref())
                    .await
            }
            "list_columns" => {
                self.list_columns_tool_result(request.arguments.as_ref())
                    .await
            }
            "feedback" if self.options.feedback_enabled => {
                let trying_to_do =
                    required_string_argument(request.arguments.as_ref(), "trying_to_do")?;
                let tried = required_string_argument(request.arguments.as_ref(), "tried")?;
                let stuck = required_string_argument(request.arguments.as_ref(), "stuck")?;
                Ok(ToolCallOutcome::from_value_result(
                    "Feedback submission",
                    self.submit_feedback_value(&trying_to_do, &tried, &stuck)
                        .await,
                ))
            }
            _ => Err(ErrorData::invalid_params(
                format!("tool '{}' not found", request.name),
                None,
            )),
        }
    }

    async fn list_columns_tool_result(
        &self,
        request_arguments: Option<&Map<String, Value>>,
    ) -> Result<ToolCallOutcome, ErrorData> {
        let arguments = list_columns_arguments(request_arguments)?;
        let mut catalog_client = self.catalog.clone();
        match catalog_client
            .list_columns(Request::new(ListColumnsRequest {
                workspace: Some(default_workspace()),
                schema_name: arguments.schema.clone(),
                table_name: arguments.table.clone(),
                pattern: arguments.pattern.clone(),
                ignore_case: arguments.ignore_case,
                required_only: arguments.required_only,
                pagination: Some(PaginationRequest {
                    limit: arguments.pagination.limit,
                    offset: arguments.pagination.offset,
                }),
            }))
            .await
        {
            Ok(response) => Ok(ToolCallOutcome::Success(list_columns_value(
                &arguments.schema,
                &arguments.table,
                &response.into_inner(),
            ))),
            Err(status) if status.code() == tonic::Code::InvalidArgument => {
                Err(status_to_error_data(&status))
            }
            Err(status) if status.code() == tonic::Code::NotFound => {
                match self
                    .load_table_description(&arguments.schema, &arguments.table)
                    .await
                {
                    Ok(response) => Ok(ToolCallOutcome::Success(describe_table_value(
                        &arguments.schema,
                        &arguments.table,
                        &response,
                    ))),
                    Err(status) => Ok(ToolCallOutcome::ToolError {
                        operation: "Column listing",
                        status,
                    }),
                }
            }
            Err(status) => Ok(ToolCallOutcome::ToolError {
                operation: "Column listing",
                status,
            }),
        }
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
        let span = telemetry::list_tools_span(self.options.trace_parent.as_deref());
        telemetry::instrument_protocol(span, async {
            let (visible_table_count, visible_function_count) =
                tokio::try_join!(self.load_table_count(), self.load_table_function_count())
                    .map_err(|status| status_to_error_data(&status))?;
            let mut tools = vec![
                sql_tool(visible_table_count),
                list_catalog_tool(visible_table_count, visible_function_count),
                search_catalog_tool(visible_table_count, visible_function_count),
                describe_table_tool(),
                list_columns_tool(),
            ];
            if self.options.feedback_enabled {
                tools.push(feedback_tool());
            }
            Ok(ListToolsResult::with_all_items(tools))
        })
        .await
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let span =
            telemetry::call_tool_span(request.name.as_ref(), self.options.trace_parent.as_deref());
        let outcome = telemetry::instrument(span.clone(), self.dispatch_tool(request)).await;
        finish_tool_call(&span, outcome)
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        let span = telemetry::list_resources_span(self.options.trace_parent.as_deref());
        telemetry::instrument_protocol(span, async {
            let (sources, visible_table_count, visible_function_count) = self
                .load_sources_and_catalog_counts()
                .await
                .map_err(|status| status_to_error_data(&status))?;
            Ok(ListResourcesResult::with_all_items(vec![
                guide_resource(&sources, visible_table_count, visible_function_count),
                tables_resource(visible_table_count),
            ]))
        })
        .await
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, ErrorData> {
        let span = telemetry::read_resource_span(
            request.uri.as_str(),
            self.options.trace_parent.as_deref(),
        );
        telemetry::instrument_protocol(span, async {
            match request.uri.as_str() {
                "coral://guide" => {
                    let (sources, tables, table_function_schema_names) = self
                        .load_sources_and_guide_catalog()
                        .await
                        .map_err(|status| status_to_error_data(&status))?;
                    Ok(ReadResourceResult::new(vec![
                        ResourceContents::text(
                            guide_resource_content(&sources, &tables, &table_function_schema_names),
                            request.uri,
                        )
                        .with_mime_type("text/markdown"),
                    ]))
                }
                "coral://tables" => {
                    let tables = self
                        .load_all_table_summaries()
                        .await
                        .map_err(|status| status_to_error_data(&status))?;
                    let text = tables_resource_content(&tables)
                        .map_err(|error| ErrorData::internal_error(error.to_string(), None))?;
                    Ok(ReadResourceResult::new(vec![
                        ResourceContents::text(text, request.uri)
                            .with_mime_type("application/json"),
                    ]))
                }
                _ => Err(ErrorData::resource_not_found(
                    format!("resource '{}' not found", request.uri),
                    None,
                )),
            }
        })
        .await
    }
}

fn finish_tool_call(
    span: &tracing::Span,
    outcome: Result<ToolCallOutcome, ErrorData>,
) -> Result<CallToolResult, ErrorData> {
    match outcome {
        Ok(ToolCallOutcome::Success(value)) => {
            let result = build_tool_result(value);
            telemetry::record_protocol_result(span, &result);
            result
        }
        Ok(ToolCallOutcome::ToolError { operation, status }) => {
            telemetry::record_tonic_status(span, &status);
            Ok(tool_error_result(tool_error_from_status(
                operation, &status,
            )))
        }
        Err(error) => {
            telemetry::record_protocol_error(span, &error);
            Err(error)
        }
    }
}

fn catalog_item_kind_from_tool(kind: Option<CatalogToolKind>) -> ProtoCatalogItemKind {
    match kind {
        None => CATALOG_KIND_ALL,
        Some(CatalogToolKind::Table) => CATALOG_KIND_TABLE,
        Some(CatalogToolKind::TableFunction) => CATALOG_KIND_TABLE_FUNCTION,
    }
}

fn guide_catalog_from_response(
    response: ListCatalogResponse,
) -> (Vec<ProtoTableSummary>, Vec<String>) {
    let mut tables = Vec::new();
    let mut table_function_schema_names = Vec::new();
    for item in response.items {
        match item.item {
            Some(catalog_item::Item::Table(table)) => tables.push(table),
            Some(catalog_item::Item::TableFunction(function)) => {
                table_function_schema_names.push(function.schema_name);
            }
            None => {}
        }
    }
    (tables, table_function_schema_names)
}
