//! RMCP server implementation for Coral's stdio MCP surface.

use std::collections::BTreeSet;

use coral_api::v1::{
    ExecuteSqlRequest, ListSourcesRequest, ListTablesRequest, ListTablesResponse,
    PaginationRequest, Source, SubmitFeedbackRequest, Table as ProtoTable,
    TableSummary as ProtoTableSummary,
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
use serde_json::{Map, Value};
use tonic::Request;

use crate::{
    McpOptions,
    surface::{
        ColumnSummary, TableSummary, build_tool_result, compile_metadata_regex,
        describe_table_arguments, describe_table_tool, feedback_tool, guide_resource,
        guide_resource_content, initial_instructions, internal_status, list_columns_arguments,
        list_columns_tool, list_tables_arguments, list_tables_tool, list_tables_value, page_items,
        paged_value, required_string_argument, search_tables_arguments, search_tables_tool,
        sql_tool, status_to_error_data, tables_resource, tables_resource_content,
        tool_error_from_status, tool_error_result,
    },
    telemetry,
};

const LIST_TABLES_COUNT_LIMIT: u32 = 1;
const LIST_TABLES_UNBOUNDED_LIMIT: u32 = 0;

struct LoadTablesParams<'a> {
    schema_name: Option<&'a str>,
    table_name: Option<&'a str>,
    pagination: PaginationRequest,
    omit_columns: bool,
}

enum ToolCallOutcome {
    Success(Value),
    ToolError {
        operation: &'static str,
        status: tonic::Status,
    },
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
                table_name: params.table_name.unwrap_or_default().to_string(),
                pagination: Some(params.pagination),
                omit_columns: params.omit_columns,
            }))
            .await?
            .into_inner())
    }

    async fn load_all_table_summaries(&self) -> Result<Vec<ProtoTableSummary>, tonic::Status> {
        self.load_table_summaries(None).await
    }

    async fn load_table_summaries(
        &self,
        schema_name: Option<&str>,
    ) -> Result<Vec<ProtoTableSummary>, tonic::Status> {
        Ok(self
            .load_tables(LoadTablesParams {
                schema_name,
                table_name: None,
                pagination: PaginationRequest {
                    limit: LIST_TABLES_UNBOUNDED_LIMIT,
                    offset: 0,
                },
                omit_columns: true,
            })
            .await?
            .table_summaries)
    }

    async fn load_exact_table(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> Result<Option<ProtoTable>, tonic::Status> {
        Ok(self
            .load_tables(LoadTablesParams {
                schema_name: Some(schema_name),
                table_name: Some(table_name),
                pagination: PaginationRequest {
                    limit: LIST_TABLES_COUNT_LIMIT,
                    offset: 0,
                },
                omit_columns: false,
            })
            .await?
            .tables
            .into_iter()
            .find(|table| table.schema_name == schema_name && table.name == table_name))
    }

    async fn load_table_count(&self) -> Result<usize, tonic::Status> {
        self.load_tables(LoadTablesParams {
            schema_name: None,
            table_name: None,
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
    ) -> Result<(Vec<Source>, Vec<ProtoTableSummary>), tonic::Status> {
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

    async fn search_tables_tool_result(
        &self,
        request_arguments: Option<&Map<String, Value>>,
    ) -> Result<ToolCallOutcome, ErrorData> {
        let arguments = search_tables_arguments(request_arguments)?;
        let regex = compile_metadata_regex(&arguments.pattern, arguments.ignore_case)?;
        match self.load_table_summaries(arguments.schema.as_deref()).await {
            Ok(tables) => {
                let mut matches = Vec::new();
                for table in &tables {
                    let summary = TableSummary::from_proto(table);
                    let matched_fields = summary.matched_fields(&regex);
                    if !matched_fields.is_empty() {
                        matches.push(summary.search_result_value(&matched_fields));
                    }
                }
                Ok(ToolCallOutcome::Success(paged_value(
                    "tables",
                    page_items(matches, arguments.pagination),
                )))
            }
            Err(status) => Ok(ToolCallOutcome::ToolError {
                operation: "Table search",
                status,
            }),
        }
    }

    async fn describe_table_tool_result(
        &self,
        request_arguments: Option<&Map<String, Value>>,
    ) -> Result<ToolCallOutcome, ErrorData> {
        let arguments = describe_table_arguments(request_arguments)?;
        match self
            .load_exact_table(&arguments.schema, &arguments.table)
            .await
        {
            Ok(Some(table)) => Ok(ToolCallOutcome::Success(describe_found_table_value(&table))),
            Ok(None) => {
                let all_tables = match self.load_all_table_summaries().await {
                    Ok(tables) => tables,
                    Err(status) => {
                        return Ok(ToolCallOutcome::ToolError {
                            operation: "Table description",
                            status,
                        });
                    }
                };
                let all_summaries = table_summaries_from_proto(&all_tables);
                Ok(ToolCallOutcome::Success(describe_missing_table_value(
                    &arguments.schema,
                    &arguments.table,
                    &all_summaries,
                )))
            }
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
            "list_tables" => {
                let arguments = list_tables_arguments(request.arguments.as_ref())?;
                let result = self
                    .load_tables(LoadTablesParams {
                        schema_name: arguments.schema.as_deref(),
                        table_name: None,
                        pagination: PaginationRequest {
                            limit: arguments.limit,
                            offset: arguments.offset,
                        },
                        omit_columns: true,
                    })
                    .await
                    .map(|response| list_tables_value(&response));
                Ok(ToolCallOutcome::from_value_result("Table listing", result))
            }
            "search_tables" => {
                self.search_tables_tool_result(request.arguments.as_ref())
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
        let columns = match self
            .load_exact_table(&arguments.schema, &arguments.table)
            .await
        {
            Ok(Some(table)) => table
                .columns
                .iter()
                .map(|column| ColumnSummary::from_proto(&table, column))
                .collect(),
            Ok(None) => {
                let all_tables = match self.load_all_table_summaries().await {
                    Ok(tables) => tables,
                    Err(status) => {
                        return Ok(ToolCallOutcome::ToolError {
                            operation: "Column listing",
                            status,
                        });
                    }
                };
                let all_summaries = table_summaries_from_proto(&all_tables);
                return Ok(ToolCallOutcome::Success(describe_missing_table_value(
                    &arguments.schema,
                    &arguments.table,
                    &all_summaries,
                )));
            }
            Err(status) => {
                return Ok(ToolCallOutcome::ToolError {
                    operation: "Column listing",
                    status,
                });
            }
        };
        Ok(ToolCallOutcome::Success(list_columns_value(
            &arguments.schema,
            &arguments.table,
            columns,
            arguments.pattern.as_deref(),
            arguments.ignore_case,
            arguments.required_only,
            arguments.pagination,
        )?))
    }
}

fn table_summaries_from_proto(tables: &[ProtoTableSummary]) -> Vec<TableSummary> {
    tables.iter().map(TableSummary::from_proto).collect()
}

fn describe_found_table_value(table: &ProtoTable) -> Value {
    serde_json::json!({
        "found": true,
        "schema_name": table.schema_name,
        "table_name": table.name,
        "name": format!("{}.{}", table.schema_name, table.name),
        "description": table.description,
        "guide": table.guide,
        "required_filters": table.required_filters,
        "column_count": table.columns.len(),
        "columns_hint": "Use list_columns with this schema/table to inspect columns.",
    })
}

fn describe_missing_table_value(schema: &str, table: &str, summaries: &[TableSummary]) -> Value {
    let available_schemas = summaries
        .iter()
        .map(|summary| summary.schema_name.as_str())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let same_schema_tables = summaries
        .iter()
        .filter(|summary| summary.schema_name == schema)
        .take(10)
        .map(TableSummary::summary_value)
        .collect::<Vec<_>>();
    let escaped_table = regex::escape(table);
    let search_arguments = if same_schema_tables.is_empty() {
        serde_json::json!({
            "pattern": escaped_table,
        })
    } else {
        serde_json::json!({
            "pattern": escaped_table,
            "schema": schema,
        })
    };
    let mut suggested_calls = vec![serde_json::json!({
        "tool": "search_tables",
        "arguments": search_arguments,
    })];
    if !same_schema_tables.is_empty() {
        suggested_calls.push(serde_json::json!({
            "tool": "list_tables",
            "arguments": {
                "schema": schema,
                "limit": 10,
            }
        }));
    }
    serde_json::json!({
        "found": false,
        "requested": {
            "schema": schema,
            "table": table,
        },
        "available_schemas": available_schemas,
        "same_schema_tables": same_schema_tables,
        "suggested_calls": suggested_calls,
    })
}

fn list_columns_value(
    schema: &str,
    table: &str,
    columns: Vec<ColumnSummary>,
    pattern: Option<&str>,
    ignore_case: bool,
    required_only: bool,
    pagination: crate::surface::Pagination,
) -> Result<Value, ErrorData> {
    let regex = pattern
        .map(|pattern| compile_metadata_regex(pattern, ignore_case))
        .transpose()?;
    let mut values = Vec::new();
    for column in columns {
        if column.schema_name != schema || column.table_name != table {
            continue;
        }
        if required_only && !column.is_required_filter {
            continue;
        }
        let matched_fields = regex.as_ref().map(|regex| column.matched_fields(regex));
        if matched_fields.as_ref().is_some_and(std::vec::Vec::is_empty) {
            continue;
        }
        values.push(column.value(matched_fields));
    }
    let page = page_items(values, pagination);
    let mut value = Map::from_iter([
        ("schema_name".to_string(), serde_json::json!(schema)),
        ("table_name".to_string(), serde_json::json!(table)),
        ("columns".to_string(), serde_json::json!(page.items)),
        ("total".to_string(), serde_json::json!(page.total)),
        ("limit".to_string(), serde_json::json!(page.limit)),
        ("offset".to_string(), serde_json::json!(page.offset)),
        ("has_more".to_string(), serde_json::json!(page.has_more)),
    ]);
    if let Some(next_offset) = page.next_offset {
        value.insert("next_offset".to_string(), serde_json::json!(next_offset));
    }
    Ok(Value::Object(value))
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
            let (sources, visible_table_count) = self
                .load_sources_and_table_count()
                .await
                .map_err(|status| status_to_error_data(&status))?;
            let mut tools = vec![
                sql_tool(&sources, visible_table_count),
                list_tables_tool(visible_table_count),
                search_tables_tool(visible_table_count),
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
            let (sources, visible_table_count) = self
                .load_sources_and_table_count()
                .await
                .map_err(|status| status_to_error_data(&status))?;
            Ok(ListResourcesResult::with_all_items(vec![
                guide_resource(&sources, visible_table_count),
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
                    let (sources, tables) = self
                        .load_sources_and_table_summaries()
                        .await
                        .map_err(|status| status_to_error_data(&status))?;
                    Ok(ReadResourceResult::new(vec![
                        ResourceContents::text(
                            guide_resource_content(&sources, &tables),
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
                        .map_err(|error| internal_status(&error))
                        .map_err(|status| status_to_error_data(&status))?;
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
