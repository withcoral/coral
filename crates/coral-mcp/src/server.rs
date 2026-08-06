//! RMCP server implementation for Coral's stdio MCP surface.

use std::sync::Arc;

use coral_api::v1::{
    AddFunctionRequest, CatalogItemKind as ProtoCatalogItemKind, DescribeCatalogSurfaceRequest,
    DescribeCatalogSurfaceResponse, EndTaskRequest, ExecuteSqlRequest, FunctionWriteSurface,
    ListCatalogRequest, ListCatalogResponse, ListColumnsRequest, ListSourcesRequest,
    PaginationRequest, QueryGuideReadContext, SearchRequest, Source, StartTaskRequest,
    SubmitFeedbackRequest, TableSummary as ProtoTableSummary, TaskStatus as ProtoTaskStatus,
    catalog_item,
};
use coral_client::{
    AppClient, CatalogClient, FeedbackClient, FunctionClient, QueryClient, SearchClient,
    SourceClient, TaskClient, batches_to_json_rows_json_safe_numbers, decode_execute_sql_response,
    default_workspace, search_response_json_value, with_task_metadata,
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
use tracing::Instrument as _;

use crate::{
    McpOptions, McpQueryExample,
    guide_block::GuideBlockState,
    surface::{
        AddFunctionArguments, CatalogToolKind, EndTaskArguments, FeedbackStoredValue,
        SqlBatchValue, SqlGuideBlockValue, SqlGuideValue, SqlQueryResultValue, StartTaskArguments,
        TaskEndedValue, TaskId, TaskStartedValue, TaskStatus, ToolAvailability,
        ToolDescriptionContext, ToolName, add_function_arguments, available_tools,
        build_tool_result, describe_arguments, describe_value, end_task_arguments,
        feedback_arguments, function_added_value, guide_resource, guide_resource_content,
        initial_instructions, list_catalog_arguments, list_catalog_value, list_columns_arguments,
        list_columns_value, render_function_artifact, required_task_id_argument,
        required_tool_intent_argument, search_arguments, sql_arguments, start_task_arguments,
        status_to_error_data, tables_resource, tables_resource_content, tool_error_from_status,
        tool_error_result,
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
    Payload(Value),
    Sql(SqlBatchExecution),
    ToolError {
        operation: &'static str,
        status: tonic::Status,
    },
}

enum SqlBatchOutput {
    Executed(SqlBatchValue),
    GuideBlocked(SqlGuideBlockValue),
}

struct SqlBatchExecution {
    output: SqlBatchOutput,
    task_gate: tokio::sync::OwnedMutexGuard<()>,
}

fn serialize_tool_value(value: impl Serialize) -> Result<Value, tonic::Status> {
    serde_json::to_value(value).map_err(|error| tonic::Status::internal(error.to_string()))
}

enum QueryRows {
    Rows(Vec<Value>),
    GuideRequired(Vec<SqlGuideValue>),
}

fn task_id_from_backend_response(
    operation: &'static str,
    value: &str,
) -> Result<TaskId, tonic::Status> {
    if value.is_empty() {
        return Err(tonic::Status::internal(format!(
            "{operation} response missing task_id"
        )));
    }
    TaskId::from_uuid_str(value)
        .map_err(|_err| tonic::Status::internal(format!("{operation} response invalid task_id")))
}

fn task_started_value(task: &coral_api::v1::Task) -> Result<TaskStartedValue, tonic::Status> {
    let task_id = task_id_from_backend_response("start task", &task.task_id)?;
    Ok(TaskStartedValue {
        task_id,
        message: "Task started.",
        instructions: "Pass this task_id plus a concise intent for the specific operation on each subsequent Coral data or enabled-feedback call, then call end_task when the task is complete.",
    })
}

fn task_status_to_proto(status: TaskStatus) -> ProtoTaskStatus {
    match status {
        TaskStatus::Success => ProtoTaskStatus::Success,
        TaskStatus::Failure => ProtoTaskStatus::Failure,
    }
}

fn task_status_from_proto(status: i32) -> Result<TaskStatus, tonic::Status> {
    match ProtoTaskStatus::try_from(status) {
        Ok(ProtoTaskStatus::Success) => Ok(TaskStatus::Success),
        Ok(ProtoTaskStatus::Failure) => Ok(TaskStatus::Failure),
        Ok(ProtoTaskStatus::Unspecified) | Err(_) => Err(tonic::Status::internal(
            "end task response missing task status",
        )),
    }
}

impl ToolCallOutcome {
    fn success(value: Value) -> Self {
        Self::Payload(value)
    }

    fn from_value_result(operation: &'static str, result: Result<Value, tonic::Status>) -> Self {
        match result {
            Ok(value) => Self::Payload(value),
            Err(status) => Self::ToolError { operation, status },
        }
    }
}

#[derive(Debug, Default)]
struct TaskCallContext {
    task_id: Option<TaskId>,
    task_id_metadata: Option<MetadataValue<Ascii>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TaskContextRequirement {
    None,
    Intent,
    TaskId,
    TaskIdAndIntent,
}

impl TaskContextRequirement {
    fn requires_intent(self) -> bool {
        matches!(self, Self::Intent | Self::TaskIdAndIntent)
    }

    fn requires_task_id(self) -> bool {
        matches!(self, Self::TaskId | Self::TaskIdAndIntent)
    }
}

impl TaskCallContext {
    fn from_tool_request(
        options: &McpOptions,
        tool_name: Option<ToolName>,
        arguments: Option<&Map<String, Value>>,
    ) -> Result<Self, ErrorData> {
        let Some(tool_name) = tool_name else {
            return Ok(Self::default());
        };
        let requirement = task_context_requirement(options, tool_name);
        if requirement == TaskContextRequirement::None {
            return Ok(Self::default());
        }
        if requirement.requires_intent() {
            required_tool_intent_argument(arguments, "intent")?;
        }
        let task_id = requirement
            .requires_task_id()
            .then(|| required_task_id_argument(arguments, "task_id"))
            .transpose()?;
        let task_id_metadata = task_id
            .as_ref()
            .map(ToString::to_string)
            .map(|task_id| {
                task_id.parse().map_err(|error| {
                    ErrorData::invalid_params(
                        format!("argument 'task_id' is not valid metadata: {error}"),
                        None,
                    )
                })
            })
            .transpose()?;
        Ok(Self {
            task_id,
            task_id_metadata,
        })
    }

    fn record_telemetry(&self, span: &tracing::Span) {
        if let Some(task_id) = self.task_id.as_ref() {
            telemetry::record_task_id(span, &task_id.to_string());
        }
    }

    fn task_id(&self) -> Option<TaskId> {
        self.task_id
    }

    fn into_metadata(self) -> Option<MetadataValue<Ascii>> {
        self.task_id_metadata
    }
}

fn task_context_requirement(options: &McpOptions, tool_name: ToolName) -> TaskContextRequirement {
    match tool_name {
        ToolName::Sql
        | ToolName::AddFunction
        | ToolName::Search
        | ToolName::ListCatalog
        | ToolName::Describe
        | ToolName::ListColumns => TaskContextRequirement::TaskIdAndIntent,
        ToolName::StartTask => TaskContextRequirement::Intent,
        ToolName::EndTask => TaskContextRequirement::TaskId,
        ToolName::Feedback if options.feedback_enabled => TaskContextRequirement::TaskIdAndIntent,
        ToolName::Feedback => TaskContextRequirement::None,
    }
}

/// Cloneable factory for constructing an independent MCP handler per session.
#[derive(Clone)]
pub(crate) struct CoralMcpServerFactory {
    app: AppClient,
    options: McpOptions,
    guide_block: Arc<GuideBlockState>,
}

impl CoralMcpServerFactory {
    /// Creates a handler factory with the tools selected by `options`.
    ///
    /// The caller must ensure that `app` is authorized for every session
    /// created by this factory. A transport may share one factory only when
    /// those sessions share the same authorization context. An authenticated
    /// transport must use a client bound to the validated session and must not
    /// fall back to a shared unauthenticated client.
    #[must_use]
    pub(crate) fn new(app: AppClient, options: McpOptions) -> Self {
        Self {
            app,
            options,
            guide_block: Arc::new(GuideBlockState::default()),
        }
    }

    /// Constructs a fresh handler for one MCP session.
    ///
    /// Handlers from this factory share task-scoped guide-block state.
    #[must_use]
    pub(crate) fn create(&self) -> impl ServerHandler + Clone + use<> {
        CoralMcpServer::new(
            &self.app,
            self.options.clone(),
            Arc::clone(&self.guide_block),
        )
    }
}

#[derive(Clone)]
pub(crate) struct CoralMcpServer {
    source: SourceClient,
    catalog: CatalogClient,
    query: QueryClient,
    search: SearchClient,
    function: FunctionClient,
    feedback: FeedbackClient,
    task: TaskClient,
    guide_block: Arc<GuideBlockState>,
    startup_context: McpStartupContext,
    options: McpOptions,
}

#[derive(Clone, Debug, Default)]
struct McpStartupContext {
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
    fn new(app: &AppClient, options: McpOptions, guide_block: Arc<GuideBlockState>) -> Self {
        let startup_context = McpStartupContext::from_options(&options);
        Self::new_with_startup_context(app, options, startup_context, guide_block)
    }

    fn new_with_startup_context(
        app: &AppClient,
        options: McpOptions,
        startup_context: McpStartupContext,
        guide_block: Arc<GuideBlockState>,
    ) -> Self {
        Self {
            source: app.source_client(),
            catalog: app.catalog_client(),
            query: app.query_client(),
            search: app.search_client(),
            function: app.function_client(),
            feedback: app.feedback_client(),
            task: app.task_client(),
            guide_block,
            startup_context,
            options,
        }
    }

    fn tool_allowed(&self, tool: ToolName) -> bool {
        match tool {
            ToolName::Feedback => self.options.feedback_enabled,
            _ => true,
        }
    }

    fn workspace(&self) -> coral_api::v1::Workspace {
        self.options
            .workspace
            .clone()
            .unwrap_or_else(default_workspace)
    }

    async fn load_sources(&self) -> Result<Vec<Source>, tonic::Status> {
        let mut source_client = self.source.clone();
        Ok(source_client
            .list_sources(Request::new(ListSourcesRequest {
                workspace: Some(self.workspace()),
            }))
            .await?
            .into_inner()
            .sources)
    }

    async fn load_catalog(
        &self,
        catalog_name: Option<&str>,
        schema_name: Option<&str>,
        kind: ProtoCatalogItemKind,
        pagination: PaginationRequest,
    ) -> Result<ListCatalogResponse, tonic::Status> {
        let mut catalog_client = self.catalog.clone();
        Ok(catalog_client
            .list_catalog(Request::new(ListCatalogRequest {
                workspace: Some(self.workspace()),
                catalog_name: catalog_name.unwrap_or_default().to_string(),
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

    async fn load_catalog_surface_description(
        &self,
        catalog_name: Option<&str>,
        schema_name: &str,
        surface_name: &str,
    ) -> Result<DescribeCatalogSurfaceResponse, tonic::Status> {
        let mut catalog_client = self.catalog.clone();
        Ok(catalog_client
            .describe_catalog_surface(Request::new(DescribeCatalogSurfaceRequest {
                workspace: Some(self.workspace()),
                catalog_name: catalog_name.unwrap_or_default().to_string(),
                schema_name: schema_name.to_string(),
                surface_name: surface_name.to_string(),
            }))
            .await?
            .into_inner())
    }

    async fn load_catalog_counts(&self) -> Result<(usize, usize), tonic::Status> {
        // One item is enough: the app returns per-kind counts before pagination.
        let response = self
            .load_catalog(
                None,
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
    ) -> Result<QueryRows, tonic::Status> {
        let mut query_client = self.query.clone();
        let mut response = query_client.execute_sql(request).await?.into_inner();
        if let Some(required) = response.guide_required.take() {
            if required.guides.is_empty() {
                return Err(tonic::Status::internal(
                    "guide-required response contained no guides",
                ));
            }
            return Ok(QueryRows::GuideRequired(
                required
                    .guides
                    .into_iter()
                    .map(|guide| {
                        SqlGuideValue::new(
                            guide.schema_name,
                            guide.resource_name,
                            guide.guide,
                            guide.guide_id,
                        )
                    })
                    .collect(),
            ));
        }
        let result = decode_execute_sql_response(&response)
            .map_err(|error| tonic::Status::internal(error.to_string()))?;
        batches_to_json_rows_json_safe_numbers(result.batches())
            .map(QueryRows::Rows)
            .map_err(|error| tonic::Status::internal(error.to_string()))
    }

    async fn execute_one_sql_query(
        &self,
        index: usize,
        sql: String,
        shown_guide_ids: Vec<String>,
    ) -> SqlQueryResultValue {
        let request = Request::new(ExecuteSqlRequest {
            workspace: Some(self.workspace()),
            sql,
            guide_read_context: Some(QueryGuideReadContext { shown_guide_ids }),
        });
        match self.query_rows(request).await {
            Ok(QueryRows::Rows(rows)) => SqlQueryResultValue::Success { index, rows },
            Ok(QueryRows::GuideRequired(guides)) => {
                SqlQueryResultValue::GuideRequired { index, guides }
            }
            Err(status) => SqlQueryResultValue::Error {
                index,
                error: tool_error_from_status("Query", &status),
            },
        }
    }

    async fn execute_sql_batch(
        &self,
        queries: Vec<String>,
        task_id: TaskId,
        task_id_metadata: Option<MetadataValue<Ascii>>,
    ) -> Result<SqlBatchExecution, tonic::Status> {
        let task_gate = self.guide_block.lock_task(task_id).await?;
        let shown_guide_ids = self.guide_block.shown_guide_ids(task_id)?;

        let mut tasks = tokio::task::JoinSet::new();
        for (index, sql) in queries.into_iter().enumerate() {
            let server = self.clone();
            let task_id_metadata = task_id_metadata.clone();
            let shown_guide_ids = shown_guide_ids.clone();
            tasks.spawn(
                async move {
                    with_task_metadata(
                        task_id_metadata,
                        server.execute_one_sql_query(index, sql, shown_guide_ids),
                    )
                    .await
                }
                .in_current_span(),
            );
        }

        let mut results = Vec::new();
        while let Some(joined) = tasks.join_next().await {
            results.push(joined.map_err(|error| tonic::Status::internal(error.to_string()))?);
        }

        let guides = results
            .iter()
            .flat_map(SqlQueryResultValue::required_guides)
            .cloned()
            .collect::<Vec<_>>();
        self.guide_block.record_guides(task_id, &guides)?;
        if let [SqlQueryResultValue::GuideRequired { guides, .. }] = results.as_slice() {
            return Ok(SqlBatchExecution {
                output: SqlBatchOutput::GuideBlocked(SqlGuideBlockValue::new(guides.clone())),
                task_gate,
            });
        }

        Ok(SqlBatchExecution {
            output: SqlBatchOutput::Executed(SqlBatchValue::from_unordered(results)),
            task_gate,
        })
    }

    async fn start_task_value(
        &self,
        arguments: StartTaskArguments,
    ) -> Result<Value, tonic::Status> {
        let mut task_client = self.task.clone();
        let task = task_client
            .start_task(Request::new(StartTaskRequest {
                workspace: Some(self.workspace()),
                intent: arguments.intent,
            }))
            .await?
            .into_inner()
            .task
            .ok_or_else(|| tonic::Status::internal("start task response missing task"))?;
        serialize_tool_value(task_started_value(&task)?)
    }

    async fn end_task_value(&self, arguments: EndTaskArguments) -> Result<Value, tonic::Status> {
        let mut task_client = self.task.clone();
        let task_end = task_client
            .end_task(Request::new(EndTaskRequest {
                workspace: Some(self.workspace()),
                task_id: arguments.task_id.to_string(),
                task_status: task_status_to_proto(arguments.task_status) as i32,
            }))
            .await?
            .into_inner()
            .task_end
            .ok_or_else(|| tonic::Status::internal("end task response missing task_end"))?;
        let task_id = task_id_from_backend_response("end task", &task_end.task_id)?;
        if task_id != arguments.task_id {
            return Err(tonic::Status::internal(
                "end task response task_id did not match request",
            ));
        }
        self.guide_block.clear_task(task_id)?;
        serialize_tool_value(TaskEndedValue {
            task_id,
            task_status: task_status_from_proto(task_end.task_status)?,
            note: "Task status recorded. Before responding, you MUST call `add_function` if it would improve future discovery or compress this task’s useful query workflow into fewer or simpler future calls. Do not add a duplicate or simple rename of an existing function.",
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
                workspace: Some(self.workspace()),
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

    async fn add_function_value(
        &self,
        arguments: AddFunctionArguments,
    ) -> Result<Value, tonic::Status> {
        let artifact_sql = render_function_artifact(&arguments).map_err(|error| {
            tonic::Status::internal(format!("failed to render function artifact: {error}"))
        })?;
        let mut function_client = self.function.clone();
        let response = function_client
            .add_function(Request::new(AddFunctionRequest {
                workspace: Some(self.workspace()),
                sql: artifact_sql,
                fail_if_exists: true,
                write_surface: FunctionWriteSurface::Mcp as i32,
            }))
            .await?
            .into_inner();
        let function = response
            .function
            .ok_or_else(|| tonic::Status::internal("add function response missing function"))?;
        function_added_value(&function)
    }

    async fn search_tool_result(
        &self,
        request_arguments: Option<&Map<String, Value>>,
    ) -> Result<ToolCallOutcome, ErrorData> {
        let arguments = search_arguments(request_arguments)?;
        let mut search_client = self.search.clone();
        match search_client
            .search(Request::new(SearchRequest {
                workspace: Some(self.workspace()),
                query: arguments.query,
                limit: arguments.limit,
            }))
            .await
            .map(|response| search_response_json_value(&response.into_inner()))
        {
            Ok(value) => Ok(ToolCallOutcome::success(value)),
            Err(status) if status.code() == tonic::Code::InvalidArgument => {
                Err(status_to_error_data(&status))
            }
            Err(status) => Ok(ToolCallOutcome::ToolError {
                operation: "Search",
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
                workspace: Some(self.workspace()),
                catalog_name: arguments.catalog.unwrap_or_default(),
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

    async fn describe_tool_result(
        &self,
        request_arguments: Option<&Map<String, Value>>,
    ) -> Result<ToolCallOutcome, ErrorData> {
        let arguments = describe_arguments(request_arguments)?;
        let result = self
            .load_catalog_surface_description(
                arguments.catalog.as_deref(),
                &arguments.schema,
                &arguments.surface,
            )
            .await
            .and_then(|response| describe_value(&response));
        Ok(ToolCallOutcome::from_value_result(
            "Catalog item description",
            result,
        ))
    }

    async fn dispatch_tool(
        &self,
        request: CallToolRequestParams,
        task_id: Option<TaskId>,
        task_id_metadata: Option<MetadataValue<Ascii>>,
        tool_name: Option<ToolName>,
    ) -> Result<ToolCallOutcome, ErrorData> {
        let Some(tool) = tool_name.filter(|tool| self.tool_allowed(*tool)) else {
            return Err(ErrorData::invalid_params(
                format!("tool '{}' not found", request.name),
                None,
            ));
        };

        match tool {
            ToolName::Sql => {
                let arguments = sql_arguments(request.arguments.as_ref())?;
                let task_id = task_id.ok_or_else(|| {
                    ErrorData::internal_error("SQL call missing validated task id", None)
                })?;
                match self
                    .execute_sql_batch(arguments.queries, task_id, task_id_metadata)
                    .await
                {
                    Ok(execution) => Ok(ToolCallOutcome::Sql(execution)),
                    Err(status) => Ok(ToolCallOutcome::ToolError {
                        operation: "Query",
                        status,
                    }),
                }
            }
            ToolName::AddFunction => {
                let arguments = add_function_arguments(request.arguments.as_ref())?;
                Ok(ToolCallOutcome::from_value_result(
                    "Function add",
                    self.add_function_value(arguments).await,
                ))
            }
            ToolName::ListCatalog => {
                self.list_catalog_tool_result(request.arguments.as_ref())
                    .await
            }
            ToolName::Search => self.search_tool_result(request.arguments.as_ref()).await,
            ToolName::Describe => self.describe_tool_result(request.arguments.as_ref()).await,
            ToolName::ListColumns => {
                self.list_columns_tool_result(request.arguments.as_ref())
                    .await
            }
            ToolName::StartTask => {
                let arguments = start_task_arguments(request.arguments.as_ref())?;
                match self.start_task_value(arguments).await {
                    Ok(value) => Ok(ToolCallOutcome::success(value)),
                    Err(status) if status.code() == tonic::Code::InvalidArgument => {
                        Err(status_to_error_data(&status))
                    }
                    Err(status) => Ok(ToolCallOutcome::ToolError {
                        operation: "Task start",
                        status,
                    }),
                }
            }
            ToolName::EndTask => {
                let arguments = end_task_arguments(request.arguments.as_ref())?;
                Ok(ToolCallOutcome::from_value_result(
                    "Task end",
                    self.end_task_value(arguments).await,
                ))
            }
            ToolName::Feedback => {
                let arguments = feedback_arguments(request.arguments.as_ref())?;
                Ok(ToolCallOutcome::from_value_result(
                    "Feedback submission",
                    self.submit_feedback_value(
                        &arguments.trying_to_do,
                        &arguments.tried,
                        &arguments.stuck,
                    )
                    .await,
                ))
            }
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
                workspace: Some(self.workspace()),
                catalog_name: arguments.catalog.clone().unwrap_or_default(),
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
            Ok(response) => Ok(ToolCallOutcome::success(list_columns_value(
                arguments.catalog.as_deref(),
                &arguments.schema,
                &arguments.table,
                &response.into_inner(),
            ))),
            Err(status) if status.code() == tonic::Code::InvalidArgument => {
                Err(status_to_error_data(&status))
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
            &self.workspace().name,
            self.startup_context.source_names(),
            self.startup_context.query_examples(),
            self.options.observed_values_search_enabled,
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
            let tools = available_tools(
                &tool_context,
                ToolAvailability {
                    feedback_enabled: self.options.feedback_enabled,
                    observed_values_search_enabled: self.options.observed_values_search_enabled,
                },
            );
            Ok(ListToolsResult::with_all_items(tools))
        })
        .await
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let tool_name = request.name.as_ref().parse::<ToolName>().ok();
        let span = telemetry::call_tool_span(
            tool_name,
            &self.workspace().name,
            self.options.trace_parent.as_deref(),
        );
        let inject_task_metadata =
            !matches!(tool_name, Some(ToolName::StartTask | ToolName::EndTask));
        let task_context = TaskCallContext::from_tool_request(
            &self.options,
            tool_name,
            request.arguments.as_ref(),
        );
        match task_context {
            Ok(task_context) => {
                task_context.record_telemetry(&span);
                let task_id = task_context.task_id();
                let task_id_metadata = inject_task_metadata
                    .then(|| task_context.into_metadata())
                    .flatten();
                let dispatch_task_id_metadata = task_id_metadata.clone();
                let outcome = telemetry::instrument(
                    span.clone(),
                    with_task_metadata(
                        task_id_metadata,
                        self.dispatch_tool(request, task_id, dispatch_task_id_metadata, tool_name),
                    ),
                )
                .await;
                finish_tool_call(&span, outcome)
            }
            Err(error) => finish_tool_call(&span, Err(error)),
        }
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
                            guide_resource_content(
                                &sources,
                                &tables,
                                &table_function_schema_names,
                                self.options.observed_values_search_enabled,
                            ),
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
        Ok(ToolCallOutcome::Payload(value)) => {
            telemetry::record_success(span);
            Ok(build_tool_result(value))
        }
        Ok(ToolCallOutcome::Sql(execution)) => finish_sql_tool_call(span, execution),
        Ok(ToolCallOutcome::ToolError { operation, status }) => {
            telemetry::record_tonic_status(span, &status);
            Ok(tool_error_result(
                tool_error_from_status(operation, &status),
                None,
            ))
        }
        Err(error) => {
            telemetry::record_protocol_error(span, &error);
            Err(error)
        }
    }
}

fn finish_sql_tool_call(
    span: &tracing::Span,
    execution: SqlBatchExecution,
) -> Result<CallToolResult, ErrorData> {
    let SqlBatchExecution { output, task_gate } = execution;
    let result = match output {
        SqlBatchOutput::Executed(batch) => {
            let serialized = match serialize_tool_value(&batch) {
                Ok(value) => value,
                Err(status) => {
                    telemetry::record_tonic_status(span, &status);
                    return Ok(tool_error_result(
                        tool_error_from_status("Query", &status),
                        None,
                    ));
                }
            };
            if batch.has_errors() {
                telemetry::record_sql_batch_partial_failure(span);
                Ok(tool_error_result(
                    batch.partial_failure_error(),
                    Some(serialized),
                ))
            } else {
                telemetry::record_success(span);
                Ok(build_tool_result(serialized))
            }
        }
        SqlBatchOutput::GuideBlocked(block) => {
            let serialized = serialize_tool_value(block).map_err(|status| {
                telemetry::record_tonic_status(span, &status);
                ErrorData::internal_error(status.message().to_string(), None)
            })?;
            telemetry::record_success(span);
            Ok(build_tool_result(serialized))
        }
    };
    drop(task_gate);
    result
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
    use super::{MAX_INITIAL_QUERY_EXAMPLES, McpStartupContext, task_id_from_backend_response};
    use crate::McpQueryExample;

    #[test]
    fn task_id_from_backend_response_canonicalizes_uuid() {
        let task_id =
            task_id_from_backend_response("start task", "550e8400e29b41d4a716446655440000")
                .expect("task id should parse");

        assert_eq!(task_id.to_string(), "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn task_id_from_backend_response_rejects_malformed_uuid() {
        let status = task_id_from_backend_response("end task", "not-a-uuid")
            .expect_err("malformed backend task id should fail");

        assert_eq!(status.code(), tonic::Code::Internal);
        assert_eq!(status.message(), "end task response invalid task_id");
    }

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

#[cfg(test)]
mod tool_call_telemetry_tests {
    use opentelemetry::trace::{Status as OtelStatus, TracerProvider as _};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
    use serde_json::{Map, Value};
    use tracing_subscriber::layer::SubscriberExt as _;

    use super::finish_tool_call;
    use crate::surface::{ToolName, list_catalog_arguments};
    use crate::telemetry::{self, MCP_PROTOCOL_ERROR_MESSAGE};

    #[test]
    fn finish_tool_call_returns_details_without_recording_them() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("mcp-tool-error-privacy-test");
        let subscriber = tracing_subscriber::Registry::default()
            .with(tracing_opentelemetry::layer().with_tracer(tracer));
        let _guard = tracing::subscriber::set_default(subscriber);

        let sentinel = "SENSITIVE_LIST_CATALOG_KIND_MARKER";
        let arguments = Map::from_iter([("kind".to_string(), Value::String(sentinel.to_string()))]);
        let Err(error) = list_catalog_arguments(Some(&arguments)) else {
            panic!("unknown list_catalog kind should fail argument parsing");
        };
        let span = telemetry::call_tool_span(Some(ToolName::ListCatalog), "default", None);
        let returned = finish_tool_call(&span, Err(error))
            .expect_err("invalid list_catalog kind should remain a caller-visible protocol error");
        assert!(returned.message.contains(sentinel));
        drop(span);

        provider.force_flush().expect("flush spans");
        let spans = exporter.get_finished_spans().expect("finished spans");
        let tool_call = spans
            .iter()
            .find(|span| span.name == "coral.mcp.call_tool")
            .expect("tool call span");

        assert_eq!(
            string_attribute(tool_call, "error.type"),
            Some("INVALID_PARAMS".to_string())
        );
        assert_eq!(
            string_attribute(tool_call, "exception.message"),
            Some(MCP_PROTOCOL_ERROR_MESSAGE.to_string())
        );
        assert_eq!(
            tool_call.status,
            OtelStatus::error(MCP_PROTOCOL_ERROR_MESSAGE)
        );
        assert!(!format!("{tool_call:?}").contains(sentinel));

        provider.shutdown().expect("provider shutdown");
    }

    fn string_attribute(span: &SpanData, name: &str) -> Option<String> {
        span.attributes
            .iter()
            .find(|attribute| attribute.key.as_str() == name)
            .map(|attribute| attribute.value.as_str().into_owned())
    }
}
