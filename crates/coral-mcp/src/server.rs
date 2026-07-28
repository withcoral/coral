//! RMCP server implementation for Coral's stdio MCP surface.

use std::sync::Arc;

use coral_api::CORAL_GUIDE_STATE_FINGERPRINT_METADATA_KEY;
use coral_api::v1::{
    CatalogItemKind as ProtoCatalogItemKind, DescribeTableRequest, DescribeTableResponse,
    EndTaskRequest, ExecuteSqlRequest, ListCatalogRequest, ListCatalogResponse, ListColumnsRequest,
    ListSourcesRequest, PaginationRequest, QueryGuideRequirement,
    ResolveSqlGuideRequirementsRequest, SearchRequest, Source, StartTaskRequest,
    SubmitFeedbackRequest, TableSummary as ProtoTableSummary, TaskStatus as ProtoTaskStatus,
    catalog_item,
};
use coral_client::{
    AppClient, CatalogClient, DecodedStatusError, FeedbackClient, QueryClient, SearchClient,
    SourceClient, TaskClient, batches_to_json_rows_json_safe_numbers, decode_execute_sql_response,
    decode_status_error, default_workspace, search_response_json_value, with_task_metadata,
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
    guide_block::GuideBlockState,
    surface::{
        CatalogToolKind, EndTaskArguments, FeedbackStoredValue, SqlBatchValue, SqlGuideBlockValue,
        SqlQueryResultValue, StartTaskArguments, TaskEndedValue, TaskId, TaskStartedValue,
        TaskStatus, ToolAvailability, ToolDescriptionContext, ToolName, available_tools,
        build_tool_result, describe_table_arguments, describe_table_value, end_task_arguments,
        feedback_arguments, guide_resource, guide_resource_content, initial_instructions,
        list_catalog_arguments, list_catalog_value, list_columns_arguments,
        list_columns_table_fallback_value, list_columns_value, required_task_id_argument,
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
const QUERY_ANALYSIS_FAILED_FINGERPRINT: &str = "v1:analysis-failed";

enum ToolCallOutcome {
    Payload(Value),
    SqlBatch(SqlBatchValue),
    SqlGuideBlock(SqlGuideBlockValue),
    ToolError {
        operation: &'static str,
        status: tonic::Status,
    },
}

enum SqlBatchExecution {
    Executed(SqlBatchValue),
    GuideBlocked(SqlGuideBlockValue),
}

struct SqlQueryGuideState {
    required_guides: Vec<QueryGuideRequirement>,
    fingerprint: String,
}

fn serialize_tool_value(value: impl Serialize) -> Result<Value, tonic::Status> {
    serde_json::to_value(value).map_err(|error| tonic::Status::internal(error.to_string()))
}

fn is_query_analysis_failure(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::InvalidArgument
        || matches!(
            decode_status_error(status),
            DecodedStatusError::Structured(error) if error.reason == "TABLE_NOT_FOUND"
        )
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
        | ToolName::Search
        | ToolName::ListCatalog
        | ToolName::DescribeTable
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
        schema_name: Option<&str>,
        kind: ProtoCatalogItemKind,
        pagination: PaginationRequest,
    ) -> Result<ListCatalogResponse, tonic::Status> {
        let mut catalog_client = self.catalog.clone();
        Ok(catalog_client
            .list_catalog(Request::new(ListCatalogRequest {
                workspace: Some(self.workspace()),
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
                workspace: Some(self.workspace()),
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

    async fn load_query_guide_state(
        &self,
        sql: String,
    ) -> Result<SqlQueryGuideState, tonic::Status> {
        let mut query_client = self.query.clone();
        let response = query_client
            .resolve_sql_guide_requirements(Request::new(ResolveSqlGuideRequirementsRequest {
                workspace: Some(self.workspace()),
                sql,
            }))
            .await?
            .into_inner();
        if response.guide_state_fingerprint.is_empty() {
            return Err(tonic::Status::failed_precondition(
                "SQL guide preflight response missing guide state fingerprint",
            ));
        }
        Ok(SqlQueryGuideState {
            required_guides: response.required_guides,
            fingerprint: response.guide_state_fingerprint,
        })
    }

    async fn preflight_sql_batch(
        &self,
        queries: &[String],
        task_id_metadata: Option<MetadataValue<Ascii>>,
    ) -> Result<Vec<SqlQueryGuideState>, tonic::Status> {
        let mut tasks = tokio::task::JoinSet::new();
        for (index, sql) in queries.iter().enumerate() {
            let server = self.clone();
            let sql = sql.clone();
            let task_id_metadata = task_id_metadata.clone();
            tasks.spawn(async move {
                (
                    index,
                    with_task_metadata(task_id_metadata, server.load_query_guide_state(sql)).await,
                )
            });
        }

        let mut guide_states = Vec::with_capacity(queries.len());
        while let Some(joined) = tasks.join_next().await {
            // Execution returns the per-query error if logical analysis fails.
            // Successful siblings still need to gate the whole batch. Operational
            // failures must stop execution because Coral could not prove that the
            // query does not require a guide.
            let (index, result) =
                joined.map_err(|error| tonic::Status::internal(error.to_string()))?;
            match result {
                Ok(guide_state) => guide_states.push((index, guide_state)),
                Err(status) if is_query_analysis_failure(&status) => {
                    guide_states.push((
                        index,
                        SqlQueryGuideState {
                            required_guides: Vec::new(),
                            fingerprint: QUERY_ANALYSIS_FAILED_FINGERPRINT.to_string(),
                        },
                    ));
                }
                Err(status) => return Err(status),
            }
        }
        guide_states.sort_unstable_by_key(|(index, _)| *index);
        Ok(guide_states
            .into_iter()
            .map(|(_, guide_state)| guide_state)
            .collect())
    }

    async fn execute_one_sql_query(
        &self,
        index: usize,
        sql: String,
        guide_state_fingerprint: String,
    ) -> SqlQueryResultValue {
        let mut request = Request::new(ExecuteSqlRequest {
            workspace: Some(self.workspace()),
            sql,
        });
        let Ok(fingerprint) = MetadataValue::<Ascii>::try_from(guide_state_fingerprint) else {
            return SqlQueryResultValue::Error {
                index,
                error: tool_error_from_status(
                    "Query",
                    &tonic::Status::internal(
                        "SQL guide state fingerprint is not valid gRPC metadata",
                    ),
                ),
            };
        };
        request
            .metadata_mut()
            .insert(CORAL_GUIDE_STATE_FINGERPRINT_METADATA_KEY, fingerprint);
        match self.query_rows(request).await {
            Ok(rows) => SqlQueryResultValue::Success { index, rows },
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
        let guide_states = self
            .preflight_sql_batch(&queries, task_id_metadata.clone())
            .await?;
        let requirements = guide_states
            .iter()
            .flat_map(|state| state.required_guides.iter().cloned())
            .collect();
        let guides = self
            .guide_block
            .newly_required_guides(task_id, requirements)?;
        if !guides.is_empty() {
            return Ok(SqlBatchExecution::GuideBlocked(SqlGuideBlockValue::new(
                guides,
            )));
        }

        let mut tasks = tokio::task::JoinSet::new();
        for (index, (sql, guide_state)) in queries.into_iter().zip(guide_states).enumerate() {
            let server = self.clone();
            let task_id_metadata = task_id_metadata.clone();
            tasks.spawn(async move {
                with_task_metadata(
                    task_id_metadata,
                    server.execute_one_sql_query(index, sql, guide_state.fingerprint),
                )
                .await
            });
        }

        let mut results = Vec::new();
        while let Some(joined) = tasks.join_next().await {
            results.push(joined.map_err(|error| tonic::Status::internal(error.to_string()))?);
        }
        Ok(SqlBatchExecution::Executed(SqlBatchValue::from_unordered(
            results,
        )))
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
            note: "Task status recorded.",
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
            Ok(response) => Ok(ToolCallOutcome::success(describe_table_value(
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
        task_id: Option<TaskId>,
        task_id_metadata: Option<MetadataValue<Ascii>>,
    ) -> Result<ToolCallOutcome, ErrorData> {
        let Some(tool) = request
            .name
            .as_ref()
            .parse::<ToolName>()
            .ok()
            .filter(|tool| self.tool_allowed(*tool))
        else {
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
                    Ok(SqlBatchExecution::Executed(batch)) => Ok(ToolCallOutcome::SqlBatch(batch)),
                    Ok(SqlBatchExecution::GuideBlocked(block)) => {
                        Ok(ToolCallOutcome::SqlGuideBlock(block))
                    }
                    Err(status) => Ok(ToolCallOutcome::ToolError {
                        operation: "Query",
                        status,
                    }),
                }
            }
            ToolName::ListCatalog => {
                self.list_catalog_tool_result(request.arguments.as_ref())
                    .await
            }
            ToolName::Search => self.search_tool_result(request.arguments.as_ref()).await,
            ToolName::DescribeTable => {
                self.describe_table_tool_result(request.arguments.as_ref())
                    .await
            }
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
                    Ok(response) => {
                        Ok(ToolCallOutcome::success(list_columns_table_fallback_value(
                            &arguments.schema,
                            &arguments.table,
                            &response,
                        )))
                    }
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
        let span =
            telemetry::call_tool_span(request.name.as_ref(), self.options.trace_parent.as_deref());
        let tool_name = request.name.as_ref().parse::<ToolName>().ok();
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
                let _sql_gate = match (tool_name, task_id) {
                    (Some(ToolName::Sql), Some(task_id)) => Some(
                        self.guide_block
                            .sql_gate(task_id)
                            .map_err(|status| status_to_error_data(&status))?
                            .lock_owned()
                            .await,
                    ),
                    _ => None,
                };
                let task_id_metadata = inject_task_metadata
                    .then(|| task_context.into_metadata())
                    .flatten();
                let dispatch_task_id_metadata = task_id_metadata.clone();
                let outcome = telemetry::instrument(
                    span.clone(),
                    with_task_metadata(
                        task_id_metadata,
                        self.dispatch_tool(request, task_id, dispatch_task_id_metadata),
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
        Ok(ToolCallOutcome::SqlBatch(batch)) => {
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
        Ok(ToolCallOutcome::SqlGuideBlock(block)) => {
            let serialized = serialize_tool_value(block).map_err(|status| {
                telemetry::record_tonic_status(span, &status);
                ErrorData::internal_error(status.message().to_string(), None)
            })?;
            telemetry::record_success(span);
            Ok(build_tool_result(serialized))
        }
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
    use crate::surface::list_catalog_arguments;
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
        let span = telemetry::call_tool_span("list_catalog", None);
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
