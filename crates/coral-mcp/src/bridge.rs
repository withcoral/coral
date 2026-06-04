//! Finite Coral function bridge shared by direct MCP and Code Mode calls.

use coral_api::v1::{
    CatalogItemKind as ProtoCatalogItemKind, DescribeTableRequest, DescribeTableResponse,
    ExecuteSqlRequest, ListCatalogRequest, ListCatalogResponse, ListColumnsRequest,
    ListSourcesRequest, PaginationRequest, SearchCatalogRequest, Source, SqlNamedParameters,
    SqlNullParameterValue, SqlParameterValue as ProtoSqlParameterValue, SqlPositionalParameters,
    SubmitFeedbackRequest, TableSummary as ProtoTableSummary, catalog_item, execute_sql_request,
    sql_parameter_value,
};
use coral_client::{
    AppClient, CatalogClient, FeedbackClient, QueryClient, SourceClient,
    batches_to_json_rows_json_safe_numbers, decode_execute_sql_response, default_workspace,
    schema_to_json_columns,
};
use rmcp::ErrorData;
#[cfg(feature = "code-mode")]
use rmcp::model::Content;
use serde::Serialize;
use serde_json::{Map, Value};
use tonic::Request;

use crate::surface::{
    CatalogToolKind, SqlParameterArgument, SqlParametersArgument, describe_table_arguments,
    describe_table_value, list_catalog_arguments, list_catalog_value, list_columns_arguments,
    list_columns_value, required_string_argument, search_catalog_arguments, search_catalog_value,
    status_to_error_data, tool_error_from_status, tool_error_result,
};

const LIST_CATALOG_COUNT_LIMIT: u32 = 1;
const LIST_CATALOG_UNBOUNDED_LIMIT: u32 = 0;
const CATALOG_KIND_ALL: ProtoCatalogItemKind = ProtoCatalogItemKind::Unspecified;
const CATALOG_KIND_TABLE: ProtoCatalogItemKind = ProtoCatalogItemKind::Table;
const CATALOG_KIND_TABLE_FUNCTION: ProtoCatalogItemKind = ProtoCatalogItemKind::TableFunction;

/// Bridge-level options for finite Coral functions.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct BridgeOptions {
    pub(crate) feedback_enabled: bool,
}

/// Result from a finite Coral function before projection into MCP or Code Mode.
pub(crate) enum BridgeCallOutcome {
    Success(Value),
    #[cfg(feature = "code-mode")]
    SuccessWithContent {
        value: Value,
        content: Vec<Content>,
    },
    ToolError {
        operation: &'static str,
        status: tonic::Status,
    },
}

#[derive(Serialize)]
struct SqlResultValue {
    columns: Vec<Value>,
    rows: Vec<Value>,
    row_count: usize,
}

#[derive(Serialize)]
struct FeedbackStoredValue {
    feedback_id: String,
    created_at: String,
    message: &'static str,
}

/// Function bridge over Coral's local app client.
#[derive(Clone)]
pub(crate) struct CoralToolBridge {
    source: SourceClient,
    catalog: CatalogClient,
    query: QueryClient,
    feedback: FeedbackClient,
    options: BridgeOptions,
}

impl CoralToolBridge {
    pub(crate) fn new(app: &AppClient, options: BridgeOptions) -> Self {
        Self {
            source: app.source_client(),
            catalog: app.catalog_client(),
            query: app.query_client(),
            feedback: app.feedback_client(),
            options,
        }
    }

    pub(crate) async fn load_sources(&self) -> Result<Vec<Source>, tonic::Status> {
        let mut source_client = self.source.clone();
        Ok(source_client
            .list_sources(Request::new(ListSourcesRequest {
                workspace: Some(default_workspace()),
            }))
            .await?
            .into_inner()
            .sources)
    }

    pub(crate) async fn load_catalog(
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

    pub(crate) async fn load_all_table_summaries(
        &self,
    ) -> Result<Vec<ProtoTableSummary>, tonic::Status> {
        self.load_table_summaries(None, LIST_CATALOG_UNBOUNDED_LIMIT)
            .await
    }

    #[cfg(feature = "code-mode")]
    pub(crate) async fn load_limited_table_summaries(
        &self,
        limit: u32,
    ) -> Result<Vec<ProtoTableSummary>, tonic::Status> {
        self.load_table_summaries(None, limit).await
    }

    async fn load_table_summaries(
        &self,
        schema_name: Option<&str>,
        limit: u32,
    ) -> Result<Vec<ProtoTableSummary>, tonic::Status> {
        self.load_catalog(
            schema_name,
            CATALOG_KIND_TABLE,
            PaginationRequest { limit, offset: 0 },
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

    pub(crate) async fn load_guide_catalog(
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

    pub(crate) async fn load_table_description(
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

    pub(crate) async fn load_catalog_counts(&self) -> Result<(usize, usize), tonic::Status> {
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

    pub(crate) async fn load_sources_and_catalog_counts(
        &self,
    ) -> Result<(Vec<Source>, usize, usize), tonic::Status> {
        let (sources, (table_count, table_function_count)) =
            tokio::try_join!(self.load_sources(), self.load_catalog_counts())?;
        Ok((sources, table_count, table_function_count))
    }

    pub(crate) async fn load_sources_and_guide_catalog(
        &self,
    ) -> Result<(Vec<Source>, Vec<ProtoTableSummary>, Vec<String>), tonic::Status> {
        let (sources, (tables, table_function_schema_names)) =
            tokio::try_join!(self.load_sources(), self.load_guide_catalog())?;
        Ok((sources, tables, table_function_schema_names))
    }

    pub(crate) async fn call(
        &self,
        name: &str,
        request_arguments: Option<&Map<String, Value>>,
    ) -> Result<BridgeCallOutcome, ErrorData> {
        match name {
            "sql" => {
                let arguments = crate::surface::sql_arguments(request_arguments)?;
                Ok(BridgeCallOutcome::from_value_result(
                    "Query",
                    self.execute_sql_value(&arguments.sql, arguments.params)
                        .await,
                ))
            }
            "list_catalog" => self.list_catalog_call_result(request_arguments).await,
            "search_catalog" => self.search_catalog_call_result(request_arguments).await,
            "describe_table" => self.describe_table_call_result(request_arguments).await,
            "list_columns" => self.list_columns_call_result(request_arguments).await,
            "feedback" if self.options.feedback_enabled => {
                let trying_to_do = required_string_argument(request_arguments, "trying_to_do")?;
                let tried = required_string_argument(request_arguments, "tried")?;
                let stuck = required_string_argument(request_arguments, "stuck")?;
                Ok(BridgeCallOutcome::from_value_result(
                    "Feedback submission",
                    self.submit_feedback_value(&trying_to_do, &tried, &stuck)
                        .await,
                ))
            }
            _ => Err(ErrorData::invalid_params(
                format!("tool '{name}' not found"),
                None,
            )),
        }
    }

    async fn execute_sql_value(
        &self,
        sql: &str,
        params: Option<SqlParametersArgument>,
    ) -> Result<Value, tonic::Status> {
        let mut query_client = self.query.clone();
        let response = query_client
            .execute_sql(Request::new(ExecuteSqlRequest {
                workspace: Some(default_workspace()),
                sql: sql.to_string(),
                parameters: params.map(proto_sql_parameters),
            }))
            .await?
            .into_inner();
        let result = decode_execute_sql_response(&response)
            .map_err(|error| tonic::Status::internal(error.to_string()))?;
        let columns = schema_to_json_columns(result.schema());
        let rows = batches_to_json_rows_json_safe_numbers(result.batches())
            .map_err(|error| tonic::Status::internal(error.to_string()))?;
        serialize_tool_value(SqlResultValue {
            columns,
            rows,
            row_count: result.row_count(),
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

    async fn search_catalog_call_result(
        &self,
        request_arguments: Option<&Map<String, Value>>,
    ) -> Result<BridgeCallOutcome, ErrorData> {
        let arguments = search_catalog_arguments(request_arguments)?;
        let schema_name =
            search_catalog_schema_name(arguments.schema, &arguments.pattern, arguments.ignore_case);
        let mut catalog_client = self.catalog.clone();
        match catalog_client
            .search_catalog(Request::new(SearchCatalogRequest {
                workspace: Some(default_workspace()),
                pattern: arguments.pattern,
                ignore_case: arguments.ignore_case,
                schema_name,
                kind: catalog_item_kind_from_tool(arguments.kind) as i32,
                pagination: Some(PaginationRequest {
                    limit: arguments.pagination.limit,
                    offset: arguments.pagination.offset,
                }),
            }))
            .await
            .map(|response| search_catalog_value(&response.into_inner()))
        {
            Ok(value) => Ok(BridgeCallOutcome::Success(value)),
            Err(status) if status.code() == tonic::Code::InvalidArgument => {
                Err(status_to_error_data(&status))
            }
            Err(status) => Ok(BridgeCallOutcome::ToolError {
                operation: "Catalog search",
                status,
            }),
        }
    }

    async fn list_catalog_call_result(
        &self,
        request_arguments: Option<&Map<String, Value>>,
    ) -> Result<BridgeCallOutcome, ErrorData> {
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
        Ok(BridgeCallOutcome::from_value_result(
            "Catalog listing",
            result,
        ))
    }

    async fn describe_table_call_result(
        &self,
        request_arguments: Option<&Map<String, Value>>,
    ) -> Result<BridgeCallOutcome, ErrorData> {
        let arguments = describe_table_arguments(request_arguments)?;
        match self
            .load_table_description(&arguments.schema, &arguments.table)
            .await
        {
            Ok(response) => Ok(BridgeCallOutcome::Success(describe_table_value(
                &arguments.schema,
                &arguments.table,
                &response,
            ))),
            Err(status) => Ok(BridgeCallOutcome::ToolError {
                operation: "Table description",
                status,
            }),
        }
    }

    async fn list_columns_call_result(
        &self,
        request_arguments: Option<&Map<String, Value>>,
    ) -> Result<BridgeCallOutcome, ErrorData> {
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
            Ok(response) => Ok(BridgeCallOutcome::Success(list_columns_value(
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
                    Ok(response) => Ok(BridgeCallOutcome::Success(describe_table_value(
                        &arguments.schema,
                        &arguments.table,
                        &response,
                    ))),
                    Err(status) => Ok(BridgeCallOutcome::ToolError {
                        operation: "Column listing",
                        status,
                    }),
                }
            }
            Err(status) => Ok(BridgeCallOutcome::ToolError {
                operation: "Column listing",
                status,
            }),
        }
    }
}

impl BridgeCallOutcome {
    pub(crate) fn from_value_result(
        operation: &'static str,
        result: Result<Value, tonic::Status>,
    ) -> Self {
        match result {
            Ok(value) => Self::Success(value),
            Err(status) => Self::ToolError { operation, status },
        }
    }
}

pub(crate) fn finish_bridge_call(
    span: &tracing::Span,
    outcome: Result<BridgeCallOutcome, ErrorData>,
) -> Result<rmcp::model::CallToolResult, ErrorData> {
    match outcome {
        Ok(BridgeCallOutcome::Success(value)) => {
            let result = crate::surface::build_tool_result(value);
            crate::telemetry::record_protocol_result(span, &result);
            result
        }
        #[cfg(feature = "code-mode")]
        Ok(BridgeCallOutcome::SuccessWithContent { value, content }) => {
            let mut result = rmcp::model::CallToolResult::structured(value);
            result.content = content;
            let result = Ok(result);
            crate::telemetry::record_protocol_result(span, &result);
            result
        }
        Ok(BridgeCallOutcome::ToolError { operation, status }) => {
            crate::telemetry::record_tonic_status(span, &status);
            Ok(tool_error_result(tool_error_from_status(
                operation, &status,
            )))
        }
        Err(error) => {
            crate::telemetry::record_protocol_error(span, &error);
            Err(error)
        }
    }
}

#[cfg(feature = "code-mode")]
pub(crate) fn bridge_outcome_result(
    outcome: Result<BridgeCallOutcome, ErrorData>,
) -> Result<Value, String> {
    match outcome {
        Ok(BridgeCallOutcome::Success(value)) => Ok(value),
        #[cfg(feature = "code-mode")]
        Ok(BridgeCallOutcome::SuccessWithContent { value, .. }) => Ok(value),
        Ok(BridgeCallOutcome::ToolError { operation, status }) => {
            let result = tool_error_result(tool_error_from_status(operation, &status));
            let structured = result
                .structured_content
                .unwrap_or_else(|| Value::String(format!("{operation} failed")));
            Err(structured.to_string())
        }
        Err(error) => Err(error.message.to_string()),
    }
}

fn serialize_tool_value(value: impl Serialize) -> Result<Value, tonic::Status> {
    serde_json::to_value(value).map_err(|error| tonic::Status::internal(error.to_string()))
}

fn proto_sql_parameters(params: SqlParametersArgument) -> execute_sql_request::Parameters {
    match params {
        SqlParametersArgument::Positional(values) => {
            execute_sql_request::Parameters::PositionalParams(SqlPositionalParameters {
                values: values.into_iter().map(proto_sql_parameter_value).collect(),
            })
        }
        SqlParametersArgument::Named(values) => {
            execute_sql_request::Parameters::NamedParams(SqlNamedParameters {
                values: values
                    .into_iter()
                    .map(|(name, value)| (name, proto_sql_parameter_value(value)))
                    .collect(),
            })
        }
    }
}

fn proto_sql_parameter_value(value: SqlParameterArgument) -> ProtoSqlParameterValue {
    ProtoSqlParameterValue {
        kind: Some(match value {
            SqlParameterArgument::Null => {
                sql_parameter_value::Kind::NullValue(SqlNullParameterValue {})
            }
            SqlParameterArgument::Boolean(value) => sql_parameter_value::Kind::BoolValue(value),
            SqlParameterArgument::Int64(value) => sql_parameter_value::Kind::Int64Value(value),
            SqlParameterArgument::Float64(value) => sql_parameter_value::Kind::Float64Value(value),
            SqlParameterArgument::String(value) => sql_parameter_value::Kind::StringValue(value),
        }),
    }
}

fn catalog_item_kind_from_tool(kind: Option<CatalogToolKind>) -> ProtoCatalogItemKind {
    match kind {
        None => CATALOG_KIND_ALL,
        Some(CatalogToolKind::Table) => CATALOG_KIND_TABLE,
        Some(CatalogToolKind::TableFunction) => CATALOG_KIND_TABLE_FUNCTION,
    }
}

fn search_catalog_schema_name(
    schema_name: Option<String>,
    pattern: &str,
    ignore_case: bool,
) -> String {
    if let Some(schema_name) = schema_name {
        return schema_name;
    }

    let normalized_pattern;
    let pattern = pattern.trim_start_matches('^');
    let pattern = if ignore_case {
        normalized_pattern = pattern.to_ascii_lowercase();
        normalized_pattern.as_str()
    } else {
        pattern
    };
    if pattern.starts_with("coral\\.")
        || pattern.starts_with("coral[.]")
        || pattern.starts_with("coral.")
    {
        "coral".to_string()
    } else {
        String::new()
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
mod tests {
    use super::search_catalog_schema_name;

    #[test]
    fn search_catalog_schema_name_infers_system_schema_for_system_patterns() {
        assert_eq!(search_catalog_schema_name(None, "^coral\\.", true), "coral");
        assert_eq!(
            search_catalog_schema_name(None, "coral[.]tables", true),
            "coral"
        );
        assert_eq!(
            search_catalog_schema_name(None, "coral.tables", true),
            "coral"
        );
        assert_eq!(search_catalog_schema_name(None, "^CORAL\\.", true), "coral");
        assert_eq!(search_catalog_schema_name(None, "^CORAL\\.", false), "");
    }

    #[test]
    fn search_catalog_schema_name_preserves_explicit_schema_or_default() {
        assert_eq!(
            search_catalog_schema_name(Some("local_messages".to_string()), "^coral\\.", true),
            "local_messages"
        );
        assert_eq!(search_catalog_schema_name(None, "messages", true), "");
    }
}
