//! RMCP server implementation for Coral's stdio MCP surface.

use std::sync::{Arc, Mutex};

use coral_api::v1::{
    CatalogItemKind as ProtoCatalogItemKind, DescribeTableRequest, DescribeTableResponse,
    ExecuteSqlRequest, ListCatalogRequest, ListCatalogResponse, ListColumnsRequest,
    ListRecipeMcpToolsRequest, ListSourcesRequest, OpenEpisodeRequest, PaginationRequest, Recipe,
    RecipeArgument, SearchCatalogRequest, Source, SubmitFeedbackRequest,
    TableSummary as ProtoTableSummary, catalog_item, recipe_published_surface,
};
use coral_client::{
    AppClient, CatalogClient, EpisodeClient, FeedbackClient, QueryClient, RecipeClient,
    SourceClient, batches_to_json_rows_json_safe_numbers, decode_execute_sql_response,
    default_workspace, with_episode_metadata,
};
use rmcp::{
    ErrorData, ServerHandler,
    model::{
        CallToolRequestParams, CallToolResult, Implementation, ListResourcesResult,
        ListToolsResult, PaginatedRequestParams, ReadResourceRequestParams, ReadResourceResult,
        ResourceContents, ServerCapabilities, ServerInfo, Tool, ToolAnnotations,
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
    McpOptions, RECIPE_MCP_TOOL_PREFIX, recipe_mcp_tool_name,
    surface::{
        CatalogToolKind, ToolDescriptionContext, build_tool_result, describe_table_arguments,
        describe_table_tool, describe_table_value, feedback_tool, guide_resource,
        guide_resource_content, initial_instructions, json_object_schema, list_catalog_arguments,
        list_catalog_tool, list_catalog_value, list_columns_arguments, list_columns_tool,
        list_columns_value, open_episode_arguments, open_episode_tool,
        optional_episode_id_argument, required_string_argument, search_catalog_arguments,
        search_catalog_tool, search_catalog_value, sql_tool, status_to_error_data, tables_resource,
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

#[derive(Debug, PartialEq, Eq)]
enum RecipeMcpToolSurfaceChange {
    FirstObservation,
    Unchanged,
    Changed,
}

#[derive(Debug, PartialEq)]
struct RecipeMcpToolSurface {
    tools: Vec<Tool>,
}

#[derive(Debug)]
enum RecipeMcpToolSurfaceTracker {
    Unobserved,
    Observed(RecipeMcpToolSurface),
}

impl RecipeMcpToolSurface {
    fn from_recipes(recipes: &[Recipe]) -> Self {
        let mut tools = recipes
            .iter()
            .flat_map(recipe_mcp_tools)
            .collect::<Vec<_>>();
        tools.sort_by(|left, right| left.name.cmp(&right.name));
        Self { tools }
    }
}

impl RecipeMcpToolSurfaceTracker {
    fn observe(&mut self, surface: RecipeMcpToolSurface) -> RecipeMcpToolSurfaceChange {
        match self {
            Self::Unobserved => {
                *self = Self::Observed(surface);
                RecipeMcpToolSurfaceChange::FirstObservation
            }
            Self::Observed(previous) if *previous == surface => {
                RecipeMcpToolSurfaceChange::Unchanged
            }
            Self::Observed(previous) => {
                *previous = surface;
                RecipeMcpToolSurfaceChange::Changed
            }
        }
    }
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
    recipe: RecipeClient,
    feedback: FeedbackClient,
    episode: EpisodeClient,
    options: McpOptions,
    observed_recipe_mcp_tool_surface: Arc<Mutex<RecipeMcpToolSurfaceTracker>>,
}

impl CoralMcpServer {
    pub(crate) fn new(app: &AppClient, options: McpOptions) -> Self {
        Self {
            source: app.source_client(),
            catalog: app.catalog_client(),
            query: app.query_client(),
            recipe: app.recipe_client(),
            feedback: app.feedback_client(),
            episode: app.episode_client(),
            options,
            observed_recipe_mcp_tool_surface: Arc::new(Mutex::new(
                RecipeMcpToolSurfaceTracker::Unobserved,
            )),
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

    async fn load_recipe_mcp_tools(&self) -> Result<Vec<Recipe>, tonic::Status> {
        let mut recipe_client = self.recipe.clone();
        Ok(recipe_client
            .list_recipe_mcp_tools(Request::new(ListRecipeMcpToolsRequest {
                workspace: Some(default_workspace()),
            }))
            .await?
            .into_inner()
            .recipes)
    }

    fn observe_recipe_mcp_tool_surface(&self, recipes: &[Recipe]) -> RecipeMcpToolSurfaceChange {
        let surface = RecipeMcpToolSurface::from_recipes(recipes);
        let mut tracker = self
            .observed_recipe_mcp_tool_surface
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        tracker.observe(surface)
    }

    async fn load_recipe_mcp_tools_and_notify_if_changed(
        &self,
        context: &RequestContext<RoleServer>,
    ) -> Result<Vec<Recipe>, tonic::Status> {
        let recipes = match self.load_recipe_mcp_tools().await {
            Ok(recipes) => recipes,
            Err(status) => {
                tracing::warn!(
                    detail = %status,
                    "failed to refresh recipe MCP tool surface"
                );
                return Err(status);
            }
        };
        if self.observe_recipe_mcp_tool_surface(&recipes) != RecipeMcpToolSurfaceChange::Changed {
            return Ok(recipes);
        }
        if let Err(error) = context.peer.notify_tool_list_changed().await {
            tracing::debug!(
                detail = %error,
                "failed to send MCP tool-list changed notification"
            );
        }
        Ok(recipes)
    }

    async fn notify_if_recipe_mcp_tool_surface_changed(
        &self,
        context: &RequestContext<RoleServer>,
    ) {
        if self
            .load_recipe_mcp_tools_and_notify_if_changed(context)
            .await
            .is_err()
        {
            // The refresh path already logged the status. Non-tool calls should
            // keep serving their primary resource even if recipe inventory is
            // temporarily unavailable.
        }
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
        recipe_mcp_tools: Result<Vec<Recipe>, tonic::Status>,
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
            name => {
                let Some(recipe) = recipe_mcp_tools
                    .map_err(|status| status_to_error_data(&status))?
                    .into_iter()
                    .find(|recipe| recipe_has_mcp_tool(recipe, name))
                else {
                    return Err(ErrorData::invalid_params(
                        format!("tool '{}' not found", request.name),
                        None,
                    ));
                };
                let sql = recipe_tool_sql(&recipe, name, request.arguments.as_ref())?;
                Ok(ToolCallOutcome::from_value_result(
                    "Recipe execution",
                    self.execute_sql_value(&sql).await,
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

fn recipe_mcp_tools(recipe: &Recipe) -> Vec<Tool> {
    if recipe_table_function(recipe).is_none() {
        return Vec::new();
    }
    recipe
        .publish
        .iter()
        .filter_map(|publish| match publish.target.as_ref()? {
            recipe_published_surface::Target::McpTool(target) => Some(Tool::new(
                recipe_mcp_tool_name(&target.name),
                recipe_tool_description(recipe, &target.description),
                recipe_input_schema(recipe),
            )),
            recipe_published_surface::Target::TableFunction(_) => None,
        })
        .map(|tool| {
            tool.with_raw_output_schema(recipe_output_schema())
                .with_annotations(
                    ToolAnnotations::with_title("Run Recipe")
                        .read_only(true)
                        .destructive(false)
                        .idempotent(true)
                        .open_world(true),
                )
        })
        .collect()
}

fn recipe_has_mcp_tool(recipe: &Recipe, tool_name: &str) -> bool {
    let Some(authored_name) = tool_name.strip_prefix(RECIPE_MCP_TOOL_PREFIX) else {
        return false;
    };
    recipe_table_function(recipe).is_some()
        && recipe.publish.iter().any(|publish| {
            matches!(
                publish.target.as_ref(),
                Some(recipe_published_surface::Target::McpTool(target))
                    if target.name == authored_name
            )
        })
}

fn recipe_tool_description(recipe: &Recipe, publish_description: &str) -> String {
    if publish_description.trim().is_empty() {
        recipe.description.clone()
    } else {
        publish_description.to_string()
    }
}

fn recipe_input_schema(recipe: &Recipe) -> std::sync::Arc<Map<String, Value>> {
    let mut required = Vec::new();
    let mut properties = Map::new();
    for argument in &recipe.arguments {
        if argument.required {
            required.push(Value::String(argument.name.clone()));
        }
        let mut property = Map::new();
        let schema_type = if argument.required {
            Value::String(argument.data_type.clone())
        } else {
            Value::Array(vec![
                Value::String(argument.data_type.clone()),
                Value::String("null".to_string()),
            ])
        };
        property.insert("type".to_string(), schema_type);
        if !argument.description.is_empty() {
            property.insert(
                "description".to_string(),
                Value::String(argument.description.clone()),
            );
        }
        properties.insert(argument.name.clone(), Value::Object(property));
    }
    json_object_schema(&serde_json::json!({
        "type": "object",
        "required": required,
        "additionalProperties": false,
        "properties": properties
    }))
}

fn recipe_output_schema() -> std::sync::Arc<Map<String, Value>> {
    json_object_schema(&serde_json::json!({
        "type": "object",
        "required": ["rows"],
        "additionalProperties": false,
        "properties": {
            "rows": {
                "type": "array",
                "items": { "type": "object" }
            }
        }
    }))
}

fn recipe_table_function(recipe: &Recipe) -> Option<(&str, &str)> {
    recipe
        .publish
        .iter()
        .find_map(|publish| match publish.target.as_ref()? {
            recipe_published_surface::Target::TableFunction(target) => {
                Some((target.schema.as_str(), target.name.as_str()))
            }
            recipe_published_surface::Target::McpTool(_) => None,
        })
}

fn recipe_tool_sql(
    recipe: &Recipe,
    tool_name: &str,
    arguments: Option<&Map<String, Value>>,
) -> Result<String, ErrorData> {
    let (schema, function) = recipe_table_function(recipe).ok_or_else(|| {
        ErrorData::invalid_params(format!("recipe tool '{tool_name}' is not executable"), None)
    })?;
    let supplied = arguments.cloned().unwrap_or_default();
    reject_unknown_recipe_arguments(recipe, &supplied)?;

    let mut sql_arguments = Vec::new();
    for argument in &recipe.arguments {
        let Some(value) = supplied.get(&argument.name) else {
            if argument.required {
                return Err(ErrorData::invalid_params(
                    format!("missing recipe argument '{}'", argument.name),
                    None,
                ));
            }
            continue;
        };
        if argument.required && value.is_null() {
            return Err(ErrorData::invalid_params(
                format!(
                    "recipe argument '{}' is required and cannot be null",
                    argument.name
                ),
                None,
            ));
        }
        sql_arguments.push(format!(
            "{} => {}",
            quote_sql_identifier(&argument.name),
            recipe_sql_literal(argument, value)?
        ));
    }

    Ok(format!(
        "select * from {}.{}({})",
        quote_sql_identifier(schema),
        quote_sql_identifier(function),
        sql_arguments.join(", ")
    ))
}

fn reject_unknown_recipe_arguments(
    recipe: &Recipe,
    supplied: &Map<String, Value>,
) -> Result<(), ErrorData> {
    let known = recipe
        .arguments
        .iter()
        .map(|argument| argument.name.as_str())
        .collect::<std::collections::HashSet<_>>();
    for key in supplied.keys() {
        if !known.contains(key.as_str()) {
            return Err(ErrorData::invalid_params(
                format!("unknown recipe argument '{key}'"),
                None,
            ));
        }
    }
    Ok(())
}

fn recipe_sql_literal(argument: &RecipeArgument, value: &Value) -> Result<String, ErrorData> {
    if value.is_null() {
        return Ok("NULL".to_string());
    }
    match argument.data_type.as_str() {
        "string" => value
            .as_str()
            .map(sql_string_literal)
            .ok_or_else(|| recipe_argument_type_error(&argument.name, &argument.data_type)),
        "integer" => value
            .as_i64()
            .map(|value| value.to_string())
            .ok_or_else(|| recipe_argument_type_error(&argument.name, &argument.data_type)),
        "boolean" => value
            .as_bool()
            .map(|value| value.to_string())
            .ok_or_else(|| recipe_argument_type_error(&argument.name, &argument.data_type)),
        other => Err(ErrorData::invalid_params(
            format!(
                "recipe argument '{}' has unsupported type '{other}'",
                argument.name
            ),
            None,
        )),
    }
}

fn sql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn quote_sql_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn recipe_argument_type_error(name: &str, data_type: &str) -> ErrorData {
    ErrorData::invalid_params(
        format!("recipe argument '{name}' must be {data_type}"),
        None,
    )
}

impl ServerHandler for CoralMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_resources()
                .enable_tools()
                .enable_tool_list_changed()
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
            let recipes = self
                .load_recipe_mcp_tools()
                .await
                .map_err(|status| status_to_error_data(&status))?;
            self.observe_recipe_mcp_tool_surface(&recipes);
            let mut seen_tools = tools
                .iter()
                .map(|tool| tool.name.to_string())
                .collect::<std::collections::HashSet<_>>();
            for tool in recipes.iter().flat_map(recipe_mcp_tools) {
                if seen_tools.insert(tool.name.to_string()) {
                    tools.push(tool);
                }
            }
            Ok(ListToolsResult::with_all_items(tools))
        })
        .await
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let recipe_mcp_tools = self
            .load_recipe_mcp_tools_and_notify_if_changed(&context)
            .await;
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
                    with_episode_metadata(
                        episode_id_metadata,
                        self.dispatch_tool(request, &span, recipe_mcp_tools),
                    ),
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
        context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        self.notify_if_recipe_mcp_tool_surface_changed(&context)
            .await;
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
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResult, ErrorData> {
        self.notify_if_recipe_mcp_tool_surface_changed(&context)
            .await;
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
