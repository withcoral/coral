//! RMCP server implementation for Coral's stdio MCP surface.

use std::sync::{Arc, Mutex};

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
    McpOptions,
    surface::{
        CatalogToolKind, ToolDescriptionContext, build_tool_result, describe_table_arguments,
        describe_table_tool, describe_table_value, feedback_tool, guide_resource,
        guide_resource_content, initial_instructions, list_catalog_arguments, list_catalog_tool,
        list_catalog_value, list_columns_arguments, list_columns_tool, list_columns_value,
        open_episode_arguments, open_episode_tool, optional_episode_id_argument,
        optional_intent_argument, required_string_argument, search_catalog_arguments,
        search_catalog_tool, search_catalog_value, sql_tool, status_to_error_data, tables_resource,
        tables_resource_content, tool_error_from_status, tool_error_result,
        with_episode_id_argument, with_intent_argument,
    },
    telemetry,
};

const LIST_CATALOG_UNBOUNDED_LIMIT: u32 = 0;
const LIST_CATALOG_COUNT_LIMIT: u32 = 1;
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

/// The episode a session is currently attributing intent-bearing tool calls to.
///
/// Held per MCP session and mutated only inside the synchronous critical section of
/// [`CoralMcpServer::resolve_episode_metadata`]. `normalized_intent` is the segmentation key —
/// a call whose normalized intent matches reuses `episode_id`; a change mints a new one.
#[derive(Clone, Debug)]
struct CurrentEpisode {
    normalized_intent: String,
    episode_id: String,
}

/// Outcome of the segmentation decision; computed under the lock, acted on after it is dropped.
enum EpisodeResolution {
    /// No attribution: episodes off, `open_episode`, or no intent and no current episode.
    Untagged,
    /// Tag with an already-known episode id; no `OpenEpisode` RPC needed.
    Reuse(String),
    /// A new id was minted and made current; `OpenEpisode(id, intent)` must be issued.
    Open { episode_id: String, intent: String },
}

/// Normalize intent for boundary detection: trim and collapse internal ASCII whitespace runs to a
/// single space. Case-sensitive — a v1 heuristic; fuzzy/embedding matching is a separate issue.
fn normalize_intent(intent: &str) -> String {
    intent.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn episode_id_to_metadata(episode_id: &str) -> Result<MetadataValue<Ascii>, ErrorData> {
    episode_id.parse().map_err(|error| {
        ErrorData::invalid_params(
            format!("argument 'episode_id' is not valid metadata: {error}"),
            None,
        )
    })
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
    /// The session's current episode, used to segment intent-bearing tool calls. Shared via `Arc`
    /// (one server instance per stdio session; rmcp dispatches calls on concurrent tasks) and
    /// guarded by a `std::sync::Mutex` so the decide+mint critical section holds no `.await`.
    current_episode: Arc<Mutex<Option<CurrentEpisode>>>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct McpStartupContext {
    source_names: Vec<String>,
}

impl McpStartupContext {
    fn from_source_names(source_names: impl IntoIterator<Item = String>) -> Self {
        let mut source_names = source_names
            .into_iter()
            .map(|name| name.trim().to_string())
            .filter(|name| !name.is_empty())
            .collect::<Vec<_>>();
        source_names.sort_unstable();
        source_names.dedup();
        Self { source_names }
    }

    fn source_names(&self) -> &[String] {
        &self.source_names
    }
}

impl CoralMcpServer {
    pub(crate) fn new(app: &AppClient, options: McpOptions) -> Self {
        let startup_context = McpStartupContext::from_source_names(options.source_names.clone());
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
            current_episode: Arc::new(Mutex::new(None)),
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

    /// Issue a single `OpenEpisode` RPC. Shared by the explicit `open_episode` tool and by
    /// intent-driven segmentation in [`Self::resolve_episode_metadata`].
    async fn open_episode_request(
        &self,
        episode_id: &str,
        intent: &str,
        parent_episode_id: Option<&str>,
    ) -> Result<(), tonic::Status> {
        let mut episode_client = self.episode.clone();
        episode_client
            .open_episode(Request::new(OpenEpisodeRequest {
                workspace: Some(default_workspace()),
                episode_id: episode_id.to_string(),
                intent: intent.to_string(),
                parent_episode_id: parent_episode_id.unwrap_or_default().to_string(),
            }))
            .await?;
        Ok(())
    }

    async fn open_episode(
        &self,
        intent: &str,
        parent_episode_id: Option<&str>,
    ) -> Result<EpisodeOpenedValue, tonic::Status> {
        let episode_id = format!("ep_{}", uuid::Uuid::new_v4().simple());
        self.open_episode_request(&episode_id, intent, parent_episode_id)
            .await?;
        Ok(EpisodeOpenedValue {
            episode_id,
            parent_episode_id: parent_episode_id.map(str::to_string),
            message: "Episode opened.",
            instructions: "Pass this episode_id as episode_id on subsequent Coral MCP tool calls for this work.",
        })
    }

    /// Resolve the `coral-episode-id` metadata to attribute a tool call to, segmenting episodes from
    /// the optional per-call `intent`. Returns `Ok(None)` when nothing should be tagged.
    ///
    /// Precedence (episodes on, data tools only):
    /// 1. explicit `episode_id` arg wins (no segmentation);
    /// 2. else `intent` matching the current episode reuses it; a changed/new intent mints a fresh
    ///    `ep_<uuid>`, makes it current, and opens it via `OpenEpisode`;
    /// 3. else (no intent) reuse the current episode if one exists, otherwise no tag.
    ///
    async fn resolve_episode_metadata(
        &self,
        name: &str,
        arguments: Option<&Map<String, Value>>,
        intent: Option<&str>,
        span: &tracing::Span,
    ) -> Result<Option<MetadataValue<Ascii>>, ErrorData> {
        if !self.options.episodes_enabled {
            return Ok(None);
        }
        // Validate a stray `episode_id` even for `open_episode` (preserves the "reject bad
        // episode_id" behavior), but never tag the open call itself.
        let explicit_episode_id = optional_episode_id_argument(arguments, "episode_id")?;
        if name == "open_episode" {
            return Ok(None);
        }

        // Option B: an explicit episode id wins; do not segment.
        if let Some(explicit) = explicit_episode_id {
            let metadata = episode_id_to_metadata(&explicit)?;
            telemetry::record_episode_id(span, &explicit);
            return Ok(Some(metadata));
        }

        // Synchronous decide+mint critical section: holds no `.await` (the rmcp-spawned future must
        // be `Send`, so a held `std::sync::MutexGuard` across an await would not compile).
        let resolution = {
            let mut current = self
                .current_episode
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match intent {
                Some(intent) => {
                    let normalized = normalize_intent(intent);
                    match current.as_ref() {
                        Some(existing) if existing.normalized_intent == normalized => {
                            EpisodeResolution::Reuse(existing.episode_id.clone())
                        }
                        _ => {
                            let episode_id = format!("ep_{}", uuid::Uuid::new_v4().simple());
                            *current = Some(CurrentEpisode {
                                normalized_intent: normalized,
                                episode_id: episode_id.clone(),
                            });
                            EpisodeResolution::Open {
                                episode_id,
                                intent: intent.to_string(),
                            }
                        }
                    }
                }
                None => match current.as_ref() {
                    Some(existing) => EpisodeResolution::Reuse(existing.episode_id.clone()),
                    None => EpisodeResolution::Untagged,
                },
            }
        };

        let episode_id = match resolution {
            EpisodeResolution::Untagged => return Ok(None),
            EpisodeResolution::Reuse(episode_id) => episode_id,
            EpisodeResolution::Open { episode_id, intent } => {
                // Segmented episodes are roots (lineage comes from explicit open_episode). Best
                // effort: a failed open must not fail the user's data call — log and still tag, so
                // attribution telemetry stays meaningful and we honor "open once per intent".
                if let Err(status) = self.open_episode_request(&episode_id, &intent, None).await {
                    tracing::warn!(
                        error = %status,
                        episode_id = %episode_id,
                        "failed to open episode for intent segmentation"
                    );
                }
                episode_id
            }
        };
        let metadata = episode_id_to_metadata(&episode_id)?;
        telemetry::record_episode_id(span, &episode_id);
        Ok(Some(metadata))
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
                telemetry::record_tool_intent(span, &arguments.intent);
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
        .with_instructions(initial_instructions(self.startup_context.source_names()))
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
            ]
            .into_iter()
            .map(with_intent_argument)
            .collect::<Vec<_>>();
            if self.options.episodes_enabled {
                // `episode_id` is explicit attribution and is only useful when episodes are on.
                // Regular tools always advertise `intent`; with episodes enabled, intent also
                // drives automatic episode segmentation.
                tools = tools.into_iter().map(with_episode_id_argument).collect();
                tools.push(open_episode_tool());
            }
            if self.options.feedback_enabled {
                let mut feedback = with_intent_argument(feedback_tool());
                if self.options.episodes_enabled {
                    feedback = with_episode_id_argument(feedback);
                }
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
        let tool_intent = if self.tool_accepts_optional_intent(request.name.as_ref()) {
            optional_intent_argument(request.arguments.as_ref(), "intent")?
        } else {
            None
        };
        if let Some(intent) = tool_intent.as_deref() {
            telemetry::record_tool_intent(&span, intent);
        }
        let outcome = match self
            .resolve_episode_metadata(
                request.name.as_ref(),
                request.arguments.as_ref(),
                tool_intent.as_deref(),
                &span,
            )
            .await
        {
            Ok(episode_id_metadata) => {
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

impl CoralMcpServer {
    fn tool_accepts_optional_intent(&self, tool_name: &str) -> bool {
        matches!(
            tool_name,
            "sql" | "list_catalog" | "search_catalog" | "describe_table" | "list_columns"
        ) || (tool_name == "feedback" && self.options.feedback_enabled)
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

#[cfg(test)]
mod startup_context_tests {
    use super::McpStartupContext;

    #[test]
    fn startup_context_sorts_and_dedupes_source_names() {
        let context = McpStartupContext::from_source_names([
            "slack".to_string(),
            "github".to_string(),
            "linear".to_string(),
            "github".to_string(),
        ]);

        assert_eq!(
            context
                .source_names()
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["github", "linear", "slack"]
        );
    }
}
