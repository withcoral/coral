//! RMCP server implementation for Coral's stdio MCP surface.

use coral_api::v1::{
    CatalogItemKind as ProtoCatalogItemKind, DescribeTableRequest, DescribeTableResponse,
    ExecuteSqlRequest, ListCatalogRequest, ListCatalogResponse, ListColumnsRequest,
    ListSourcesRequest, OpenEpisodeRequest, PaginationRequest, SearchCatalogRequest, Source,
    SubmitFeedbackRequest, TableSummary as ProtoTableSummary, catalog_item,
};
use coral_client::{
    AppClient, CatalogClient, EpisodeClient, FeedbackClient, QueryClient, SourceClient,
    batches_to_json_rows_json_safe_numbers, decode_execute_sql_response, default_workspace,
    with_episode_metadata,
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
use tonic::{
    Request,
    metadata::{Ascii, MetadataValue},
};

use crate::{
    McpOptions, McpQueryExample,
    surface::{
        CatalogToolKind, ToolDescriptionContext, build_tool_result, describe_table_arguments,
        describe_table_tool, describe_table_value, feedback_tool, guide_resource,
        guide_resource_content, initial_instructions, list_catalog_arguments, list_catalog_tool,
        list_catalog_value, list_columns_arguments, list_columns_tool, list_columns_value,
        open_episode_arguments, open_episode_tool, optional_episode_id_argument,
        required_string_argument, search_catalog_arguments, search_catalog_tool,
        search_catalog_value, sql_tool, status_to_error_data, tables_resource,
        tables_resource_content, tool_error_from_status, tool_error_result,
        with_episode_id_argument,
    },
    telemetry,
};

const LIST_CATALOG_UNBOUNDED_LIMIT: u32 = 0;
const LIST_CATALOG_COUNT_LIMIT: u32 = 1;
const CATALOG_KIND_ALL: ProtoCatalogItemKind = ProtoCatalogItemKind::Unspecified;
const CATALOG_KIND_TABLE: ProtoCatalogItemKind = ProtoCatalogItemKind::Table;
const CATALOG_KIND_TABLE_FUNCTION: ProtoCatalogItemKind = ProtoCatalogItemKind::TableFunction;
const MAX_INITIAL_QUERY_EXAMPLES: usize = 5;

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

#[derive(Serialize)]
struct EpisodeOpenedValue {
    episode_id: String,
    parent_episode_id: Option<String>,
    message: &'static str,
    instructions: &'static str,
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

#[derive(Debug, Default)]
struct EpisodeTag {
    episode_id: Option<String>,
    episode_id_metadata: Option<MetadataValue<Ascii>>,
}

impl EpisodeTag {
    fn from_tool_request(
        options: &McpOptions,
        arguments: Option<&Map<String, Value>>,
    ) -> Result<Self, ErrorData> {
        if !options.episodes_enabled {
            return Ok(Self::default());
        }
        let episode_id = optional_episode_id_argument(arguments, "episode_id")?;
        let episode_id_metadata = episode_id
            .as_deref()
            .map(|episode_id| {
                episode_id.parse().map_err(|error| {
                    ErrorData::invalid_params(
                        format!("argument 'episode_id' is not valid metadata: {error}"),
                        None,
                    )
                })
            })
            .transpose()?;
        Ok(Self {
            episode_id,
            episode_id_metadata,
        })
    }

    fn record_telemetry(&self, span: &tracing::Span) {
        if let Some(episode_id) = self.episode_id.as_deref() {
            telemetry::record_episode_id(span, episode_id);
        }
    }

    fn into_metadata(self) -> Option<MetadataValue<Ascii>> {
        self.episode_id_metadata
    }
}

#[derive(Clone)]
pub(crate) struct CoralMcpServer {
    source: SourceClient,
    catalog: CatalogClient,
    query: QueryClient,
    feedback: FeedbackClient,
    episode: EpisodeClient,
    startup_context: McpStartupContext,
    options: McpOptions,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct McpStartupContext {
    source_names: Vec<String>,
    query_examples: Vec<McpQueryExample>,
}

impl McpStartupContext {
    fn new(
        source_names: impl IntoIterator<Item = String>,
        query_examples: impl IntoIterator<Item = McpQueryExample>,
    ) -> Self {
        let mut source_names = source_names
            .into_iter()
            .map(|name| name.trim().to_string())
            .filter(|name| !name.is_empty())
            .collect::<Vec<_>>();
        source_names.sort_unstable();
        source_names.dedup();
        let query_examples = normalize_query_examples(query_examples);
        Self {
            source_names,
            query_examples,
        }
    }

    fn from_options(options: &McpOptions) -> Self {
        Self::new(options.source_names.clone(), options.query_examples.clone())
    }

    fn source_names(&self) -> &[String] {
        &self.source_names
    }

    fn query_examples(&self) -> &[McpQueryExample] {
        &self.query_examples
    }
}

impl CoralMcpServer {
    pub(crate) fn new(app: &AppClient, options: McpOptions) -> Self {
        let startup_context = McpStartupContext::from_options(&options);
        Self::new_with_startup_context(app, options, startup_context)
    }

    pub(crate) fn new_with_startup_context(
        app: &AppClient,
        options: McpOptions,
        startup_context: McpStartupContext,
    ) -> Self {
        Self {
            source: app.source_client(),
            catalog: app.catalog_client(),
            query: app.query_client(),
            feedback: app.feedback_client(),
            episode: app.episode_client(),
            startup_context,
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

    async fn load_catalog_counts(&self) -> Result<(usize, usize), tonic::Status> {
        // One item is enough: the app returns per-kind counts before pagination.
        let response = self
            .load_catalog(
                None,
                CATALOG_KIND_ALL,
                PaginationRequest {
                    limit: LIST_CATALOG_COUNT_LIMIT,
                    offset: 0,
                },
            )
            .await?;
        let counts = response
            .counts
            .ok_or_else(|| tonic::Status::internal("catalog response missing counts"))?;
        Ok((
            usize::try_from(counts.table_count).unwrap_or(usize::MAX),
            usize::try_from(counts.table_function_count).unwrap_or(usize::MAX),
        ))
    }

    async fn load_sources_and_catalog_counts(
        &self,
    ) -> Result<(Vec<Source>, usize, usize), tonic::Status> {
        let (sources, (table_count, table_function_count)) =
            tokio::try_join!(self.load_sources(), self.load_catalog_counts())?;
        Ok((sources, table_count, table_function_count))
    }

    async fn load_sources_and_guide_catalog(
        &self,
    ) -> Result<(Vec<Source>, Vec<ProtoTableSummary>, Vec<String>), tonic::Status> {
        let (sources, (tables, table_function_schema_names)) =
            tokio::try_join!(self.load_sources(), self.load_guide_catalog())?;
        Ok((sources, tables, table_function_schema_names))
    }

    async fn query_rows(
        &self,
        request: Request<ExecuteSqlRequest>,
    ) -> Result<Vec<Value>, tonic::Status> {
        let mut query_client = self.query.clone();
        let response = query_client.execute_sql(request).await?.into_inner();
        let result = decode_execute_sql_response(&response)
            .map_err(|error| tonic::Status::internal(error.to_string()))?;
        batches_to_json_rows_json_safe_numbers(result.batches())
            .map_err(|error| tonic::Status::internal(error.to_string()))
    }

    async fn execute_sql_value(
        &self,
        request: Request<ExecuteSqlRequest>,
    ) -> Result<Value, tonic::Status> {
        serialize_tool_value(SqlRowsValue {
            rows: self.query_rows(request).await?,
        })
    }

    async fn open_episode(
        &self,
        intent: &str,
        parent_episode_id: Option<&str>,
    ) -> Result<EpisodeOpenedValue, tonic::Status> {
        let episode_id = format!("ep_{}", uuid::Uuid::new_v4().simple());
        let mut episode_client = self.episode.clone();
        episode_client
            .open_episode(Request::new(OpenEpisodeRequest {
                workspace: Some(default_workspace()),
                episode_id: episode_id.clone(),
                intent: intent.to_string(),
                parent_episode_id: parent_episode_id.unwrap_or_default().to_string(),
            }))
            .await?;
        Ok(EpisodeOpenedValue {
            episode_id,
            parent_episode_id: parent_episode_id.map(str::to_string),
            message: "Episode opened.",
            instructions: "Pass this episode_id as episode_id on subsequent Coral MCP tool calls for this work.",
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
        span: &tracing::Span,
    ) -> Result<ToolCallOutcome, ErrorData> {
        match request.name.as_ref() {
            "sql" => {
                let sql = required_string_argument(request.arguments.as_ref(), "sql")?;
                let request = Request::new(ExecuteSqlRequest {
                    workspace: Some(default_workspace()),
                    sql,
                });
                Ok(ToolCallOutcome::from_value_result(
                    "Query",
                    self.execute_sql_value(request).await,
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
            "open_episode" if self.options.episodes_enabled => {
                let arguments = open_episode_arguments(request.arguments.as_ref())?;
                match self
                    .open_episode(&arguments.intent, arguments.parent_episode_id.as_deref())
                    .await
                    .and_then(|episode| {
                        telemetry::record_episode_id(span, &episode.episode_id);
                        serialize_tool_value(episode)
                    }) {
                    Ok(value) => Ok(ToolCallOutcome::Success(value)),
                    Err(status) if status.code() == tonic::Code::InvalidArgument => {
                        Err(status_to_error_data(&status))
                    }
                    Err(status) => Ok(ToolCallOutcome::ToolError {
                        operation: "Episode opening",
                        status,
                    }),
                }
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
        .with_instructions(initial_instructions(
            self.startup_context.source_names(),
            self.startup_context.query_examples(),
        ))
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let span = telemetry::list_tools_span(self.options.trace_parent.as_deref());
        telemetry::instrument_protocol(span, async {
            let (visible_table_count, visible_function_count) = self
                .load_catalog_counts()
                .await
                .map_err(|status| status_to_error_data(&status))?;
            let source_names = match self.load_sources().await {
                Ok(sources) => sources.into_iter().map(|source| source.name).collect(),
                Err(status) => {
                    tracing::warn!(
                        error = %status,
                        "failed to load source names for MCP tool descriptions"
                    );
                    Vec::new()
                }
            };
            let tool_context = ToolDescriptionContext::new(
                visible_table_count,
                visible_function_count,
                source_names,
            );
            let mut tools = vec![
                sql_tool(&tool_context),
                list_catalog_tool(&tool_context),
                search_catalog_tool(&tool_context),
                describe_table_tool(),
                list_columns_tool(),
            ];
            if self.options.episodes_enabled {
                tools = tools.into_iter().map(with_episode_id_argument).collect();
                tools.push(open_episode_tool());
            }
            if self.options.feedback_enabled {
                let feedback = feedback_tool();
                let feedback = if self.options.episodes_enabled {
                    with_episode_id_argument(feedback)
                } else {
                    feedback
                };
                tools.push(feedback);
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
        let inject_episode_metadata = request.name.as_ref() != "open_episode";
        let episode_tag = EpisodeTag::from_tool_request(&self.options, request.arguments.as_ref());
        let outcome = match episode_tag {
            Ok(episode_tag) => {
                episode_tag.record_telemetry(&span);
                let episode_id_metadata = inject_episode_metadata
                    .then(|| episode_tag.into_metadata())
                    .flatten();
                telemetry::instrument(
                    span.clone(),
                    with_episode_metadata(episode_id_metadata, self.dispatch_tool(request, &span)),
                )
                .await
            }
            Err(error) => Err(error),
        };
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

fn normalize_query_examples(
    query_examples: impl IntoIterator<Item = McpQueryExample>,
) -> Vec<McpQueryExample> {
    let mut normalized = Vec::new();
    for example in query_examples {
        let sql = example.sql().trim();
        if sql.is_empty() {
            continue;
        }
        let mut normalized_example =
            McpQueryExample::new(sql.to_string()).with_sources(example.sources().iter().cloned());
        if let Some(row_count) = example.row_count() {
            normalized_example = normalized_example.with_row_count(row_count);
        }
        normalized.push(normalized_example);
        if normalized.len() >= MAX_INITIAL_QUERY_EXAMPLES {
            break;
        }
    }
    normalized
}

#[cfg(test)]
mod startup_context_tests {
    use super::{MAX_INITIAL_QUERY_EXAMPLES, McpStartupContext};
    use crate::McpQueryExample;

    #[test]
    fn startup_context_sorts_and_dedupes_source_names() {
        let context = McpStartupContext::new(
            [
                "slack".to_string(),
                "github".to_string(),
                "linear".to_string(),
                "github".to_string(),
            ],
            [],
        );

        assert_eq!(
            context
                .source_names()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["github", "linear", "slack"]
        );
    }

    #[test]
    fn startup_context_keeps_query_examples_in_order() {
        let context = McpStartupContext::new(
            [],
            [
                McpQueryExample::new(" SELECT * FROM github.issues "),
                McpQueryExample::new(""),
                McpQueryExample::new("SELECT * FROM github.issues"),
                McpQueryExample::new("SELECT * FROM linear.issues"),
            ],
        );

        assert_eq!(
            context
                .query_examples()
                .iter()
                .map(McpQueryExample::sql)
                .collect::<Vec<_>>(),
            vec![
                "SELECT * FROM github.issues",
                "SELECT * FROM github.issues",
                "SELECT * FROM linear.issues",
            ]
        );
    }

    #[test]
    fn startup_context_keeps_query_example_metadata() {
        let context = McpStartupContext::new(
            [],
            [McpQueryExample::new(" SELECT * FROM github.issues ")
                .with_sources(["github".to_string(), "github".to_string()])
                .with_row_count(15)],
        );

        let example = context.query_examples().first().expect("query example");
        assert_eq!(example.sql(), "SELECT * FROM github.issues");
        assert_eq!(example.sources(), &["github".to_string()]);
        assert_eq!(example.row_count(), Some(15));
    }

    #[test]
    fn startup_context_caps_query_examples() {
        let context = McpStartupContext::new(
            [],
            (0..MAX_INITIAL_QUERY_EXAMPLES + 2)
                .map(|index| McpQueryExample::new(format!("SELECT {index}"))),
        );

        assert_eq!(context.query_examples().len(), MAX_INITIAL_QUERY_EXAMPLES);
        assert_eq!(
            context.query_examples().last().map(McpQueryExample::sql),
            Some("SELECT 4")
        );
    }
}
