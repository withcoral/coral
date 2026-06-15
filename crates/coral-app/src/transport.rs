//! Shared gRPC transport helpers for app-owned services.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tokio::sync::mpsc;
use tokio::task;
use tokio_stream::Stream;

use coral_api::{
    CORAL_EPISODE_ID_METADATA_KEY, CORAL_ERROR_DOMAIN, grpc_response_status_code,
    v1::{
        CatalogItem as ProtoCatalogItem, CatalogSearchResult as ProtoCatalogSearchResult, Column,
        ColumnSearchResult as ProtoColumnSearchResult,
        DescribeTableResponse as ProtoDescribeTableResponse, PaginationResponse, QueryTestFailure,
        QueryTestResult, QueryTestSuccess, Source, Table, TableFunction, TableFunctionArgument,
        TableFunctionResultColumn, TableSummary, ValidateSourceResponse, Workspace, catalog_item,
        query_test_result,
    },
};
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::Status as OtelStatus;
use tonic::codegen::{Service, http};
use tonic::{Code, Request, Response, Status};
use tonic_types::{ErrorDetail, StatusExt as _};
use tracing::{Instrument as _, field};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::bootstrap::{AppError, app_status, core_status};
use crate::catalog::discovery::{
    CatalogItem, CatalogMetadataField, CatalogSearchResult, ColumnMetadataField,
    ColumnSearchResult, DescribeTableResult,
};
use crate::credentials::oauth::{
    OAuthProgressEvent, OAuthProgressEventSender, PendingOAuthProgressEvent,
};
use crate::episode::EpisodeId;
use crate::identity::{UserPrincipal, UserPrincipalError, UserPrincipalProvider};
use crate::query::manager::QueryManagerError;
use crate::workspaces::WorkspaceName;

struct MetadataExtractor<'a>(&'a tonic::metadata::MetadataMap);

impl Extractor for MetadataExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        use tonic::metadata::KeyRef;
        self.0
            .keys()
            .filter_map(|k| match k {
                KeyRef::Ascii(key) => Some(key.as_str()),
                KeyRef::Binary(_) => None,
            })
            .collect()
    }
}

/// Extracts a W3C trace context from incoming gRPC request metadata.
pub(crate) fn extract_trace_context(
    metadata: &tonic::metadata::MetadataMap,
) -> opentelemetry::Context {
    opentelemetry::global::get_text_map_propagator(|p| p.extract(&MetadataExtractor(metadata)))
}

/// Wraps a generated tonic service and stores the inbound gRPC path on the request.
///
/// Tonic preserves `http::Request` extensions when it decodes the protobuf
/// message into a `tonic::Request`, but generated server wrappers do not insert
/// `tonic::GrpcMethod` the way generated clients do. This keeps the method
/// data at the transport boundary and lets handlers read it from the request.
#[derive(Clone)]
pub(crate) struct GrpcMethodAnnotatedService<S> {
    inner: S,
}

impl<S> GrpcMethodAnnotatedService<S> {
    pub(crate) fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S, B> Service<http::Request<B>> for GrpcMethodAnnotatedService<S>
where
    S: Service<http::Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<B>) -> Self::Future {
        if let Some(method) = GrpcServerMethod::from_path(request.uri().path()) {
            request.extensions_mut().insert(method);
        }
        self.inner.call(request)
    }
}

impl<S> tonic::server::NamedService for GrpcMethodAnnotatedService<S>
where
    S: tonic::server::NamedService,
{
    const NAME: &'static str = S::NAME;
}

/// Creates a span parented to the trace context extracted from a gRPC request.
pub(crate) fn grpc_span<T>(request: &Request<T>) -> tracing::Span {
    let parent_cx = extract_trace_context(request.metadata());
    let metadata = grpc_method(request);
    let span_name = format!("{}/{}", metadata.service, metadata.method);
    let span = tracing::info_span!(
        "grpc",
        error.type = tracing::field::Empty,
        exception.message = tracing::field::Empty,
        otel.kind = "server",
        otel.name = span_name.as_str(),
        rpc.system = "grpc",
        rpc.system.name = "grpc",
        rpc.service = metadata.service.as_str(),
        rpc.method = metadata.method.as_str(),
        rpc.response.status_code = tracing::field::Empty,
        grpc.method = metadata.method.as_str(),
        grpc.status_code = tracing::field::Empty,
        grpc.code = tracing::field::Empty,
        status = tracing::field::Empty,
    );
    drop(span.set_parent(parent_cx));
    span
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct GrpcServerMethod {
    service: String,
    method: String,
}

impl GrpcServerMethod {
    fn from_path(path: &str) -> Option<Self> {
        let trimmed = path.strip_prefix('/').unwrap_or(path);
        let (service, method) = trimmed.split_once('/')?;
        if service.is_empty() || method.is_empty() || method.contains('/') {
            return None;
        }
        Some(Self {
            service: service.to_string(),
            method: method.to_string(),
        })
    }
}

#[derive(Debug, Eq, PartialEq)]
struct GrpcMethodMetadata {
    service: String,
    method: String,
}

impl GrpcMethodMetadata {
    fn new(service: impl Into<String>, method: impl Into<String>) -> Self {
        Self {
            service: service.into(),
            method: method.into(),
        }
    }
}

fn grpc_method<T>(request: &Request<T>) -> GrpcMethodMetadata {
    if let Some(method) = request.extensions().get::<tonic::GrpcMethod<'static>>() {
        return GrpcMethodMetadata::new(method.service(), method.method());
    }
    request.extensions().get::<GrpcServerMethod>().map_or_else(
        || GrpcMethodMetadata::new("coral.v1.UnknownService", "Unknown"),
        |method| GrpcMethodMetadata::new(method.service.as_str(), method.method.as_str()),
    )
}

pub(crate) async fn instrument_grpc<T, F>(span: tracing::Span, future: F) -> Result<T, Status>
where
    F: Future<Output = Result<T, Status>>,
{
    let result = future.instrument(span.clone()).await;
    match &result {
        Ok(_) => record_grpc_status(&span, Code::Ok, None),
        Err(status) => record_grpc_status(&span, status.code(), Some(status)),
    }
    result
}

/// Runs a blocking `operation` on the blocking thread pool while preserving the
/// current tracing span, mapping a join failure or [`AppError`] to a [`Status`].
/// `label` names the operation in the join-failure message.
pub(crate) async fn run_blocking_operation<T, F>(label: &str, operation: F) -> Result<T, Status>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, AppError> + Send + 'static,
{
    let span = tracing::Span::current();
    task::spawn_blocking(move || span.in_scope(operation))
        .await
        .map_err(|error| Status::internal(format!("{label} task failed: {error}")))?
        .map_err(app_status)
}

/// Creates the request span, authenticates the caller through the provider,
/// and runs `handler` with the authenticated principal and decoded message,
/// recording the gRPC status on the span.
///
/// Handlers that need the request span (for example to instrument a response
/// stream) can read it with `tracing::Span::current()`.
pub(crate) async fn instrument_authenticated_grpc<Req, Res, F, Fut>(
    user_principal_provider: &Arc<dyn UserPrincipalProvider>,
    request: Request<Req>,
    handler: F,
) -> Result<Response<Res>, Status>
where
    F: FnOnce(UserPrincipal, Req) -> Fut,
    Fut: Future<Output = Result<Response<Res>, Status>>,
{
    let span = grpc_span(&request);
    let user_principal_provider = Arc::clone(user_principal_provider);
    instrument_grpc(span, async move {
        let principal = user_principal_provider
            .principal_for_metadata(request.metadata())
            .await
            .map_err(user_principal_status)?;
        handler(principal, request.into_inner()).await
    })
    .await
}

fn user_principal_status(error: UserPrincipalError) -> Status {
    match error {
        UserPrincipalError::Unauthenticated(message) => Status::unauthenticated(message),
        UserPrincipalError::InvalidInput(message) => {
            Status::invalid_argument(format!("invalid user principal metadata: {message}"))
        }
        UserPrincipalError::Internal(message) => Status::internal(message),
    }
}

fn record_grpc_status(span: &tracing::Span, code: Code, status: Option<&Status>) {
    let response_status_code = grpc_response_status_code(code);
    span.record("grpc.status_code", code as i64);
    span.record("grpc.code", response_status_code);
    span.record("rpc.response.status_code", response_status_code);
    if code == Code::Ok {
        span.record("status", "ok");
        span.set_status(OtelStatus::Ok);
    } else {
        let error = status.map_or_else(
            || GrpcErrorTelemetry {
                error_type: response_status_code.to_string(),
                message: response_status_code.to_string(),
            },
            decode_grpc_error,
        );
        span.record("status", "error");
        span.record("error.type", error.error_type.as_str());
        span.record("exception.message", field::display(error.message.as_str()));
        span.set_status(OtelStatus::error(error.message));
    }
}

struct GrpcErrorTelemetry {
    error_type: String,
    message: String,
}

fn decode_grpc_error(status: &Status) -> GrpcErrorTelemetry {
    for detail in status.get_error_details_vec() {
        if let ErrorDetail::ErrorInfo(info) = detail
            && info.domain == CORAL_ERROR_DOMAIN
        {
            return GrpcErrorTelemetry {
                error_type: info.reason,
                message: status.message().to_string(),
            };
        }
    }

    GrpcErrorTelemetry {
        error_type: grpc_response_status_code(status.code()).to_string(),
        message: status.message().to_string(),
    }
}

/// One OAuth progress event mapped onto the shared credential proto pair.
pub enum OAuthProgressProto {
    /// Authorization URL or device-code details.
    Authorization(coral_api::v1::OAuthCredentialAuthorization),
    /// OAuth credential retrieval completion metadata.
    Completed(coral_api::v1::OAuthCredentialCompleted),
}

impl From<OAuthProgressEvent> for OAuthProgressProto {
    fn from(event: OAuthProgressEvent) -> Self {
        match event {
            OAuthProgressEvent::OAuthAuthorization {
                input_key,
                authorization_url,
                expires_in_seconds,
                user_code,
                verification_uri,
                verification_uri_complete,
            } => Self::Authorization(coral_api::v1::OAuthCredentialAuthorization {
                input_key,
                authorization_url,
                expires_in_seconds,
                user_code: user_code.unwrap_or_default(),
                verification_uri: verification_uri.unwrap_or_default(),
                verification_uri_complete: verification_uri_complete.unwrap_or_default(),
            }),
            OAuthProgressEvent::OAuthCompleted {
                input_key,
                metadata,
            } => Self::Completed(coral_api::v1::OAuthCredentialCompleted {
                input_key,
                metadata: metadata
                    .into_iter()
                    .map(|(key, value)| coral_api::v1::CredentialMetadata { key, value })
                    .collect(),
            }),
        }
    }
}

/// Builds a gRPC response stream that forwards acknowledged OAuth progress
/// events while `operation` runs, then yields the operation's mapped result.
///
/// `closed_message` is the error reported to the operation when it emits an
/// event after the stream consumer went away.
pub fn oauth_operation_response_stream<T, R, F, Fut>(
    closed_message: &'static str,
    operation: F,
    event_to_response: fn(OAuthProgressEvent) -> R,
    operation_to_response: impl FnOnce(T) -> R + Send + 'static,
) -> Pin<Box<dyn Stream<Item = Result<R, Status>> + Send>>
where
    T: 'static,
    R: Send + Unpin + 'static,
    F: FnOnce(OAuthProgressEventSender) -> Fut,
    Fut: Future<Output = Result<T, Status>> + Send + 'static,
{
    let (event_tx, event_rx) = mpsc::channel(8);
    let sender = OAuthProgressEventSender::new(event_tx, closed_message);
    Box::pin(OAuthOperationResponseStream {
        events: event_rx,
        operation: Some((
            Box::pin(operation(sender)) as Pin<Box<dyn Future<Output = _> + Send>>,
            Box::new(operation_to_response),
        )),
        completion: None,
        event_to_response,
    })
}

struct OAuthOperationResponseStream<T, R> {
    events: mpsc::Receiver<PendingOAuthProgressEvent>,
    #[expect(clippy::type_complexity, reason = "private one-shot operation slot")]
    operation: Option<(
        Pin<Box<dyn Future<Output = Result<T, Status>> + Send>>,
        Box<dyn FnOnce(T) -> R + Send>,
    )>,
    completion: Option<Result<R, Status>>,
    event_to_response: fn(OAuthProgressEvent) -> R,
}

impl<T, R> OAuthOperationResponseStream<T, R> {
    fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<Option<R>> {
        Pin::new(&mut self.events)
            .poll_recv(cx)
            .map(|event| event.map(|event| (self.event_to_response)(event.into_event())))
    }
}

impl<T, R: Unpin> Stream for OAuthOperationResponseStream<T, R> {
    type Item = Result<R, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Poll::Ready(Some(event)) = this.poll_event(cx) {
                return Poll::Ready(Some(Ok(event)));
            }
            if let Some(completion) = this.completion.take() {
                return Poll::Ready(Some(completion));
            }
            let Some((operation, _)) = this.operation.as_mut() else {
                return Poll::Ready(None);
            };
            match operation.as_mut().poll(cx) {
                Poll::Ready(result) => {
                    if let Some((_, operation_to_response)) = this.operation.take() {
                        this.completion = Some(result.map(operation_to_response));
                    }
                }
                Poll::Pending => {
                    return match this.poll_event(cx) {
                        Poll::Ready(Some(event)) => Poll::Ready(Some(Ok(event))),
                        Poll::Ready(None) | Poll::Pending => Poll::Pending,
                    };
                }
            }
        }
    }
}

pub(crate) fn query_status(error: QueryManagerError) -> Status {
    match error {
        QueryManagerError::App(error) => app_status(error),
        QueryManagerError::Core(error) => core_status(error),
    }
}

pub(crate) fn workspace_name_from_proto(
    workspace: Option<&Workspace>,
) -> Result<WorkspaceName, Status> {
    let workspace = workspace
        .ok_or_else(|| app_status(AppError::InvalidInput("missing workspace".to_string())))?;
    WorkspaceName::parse(&workspace.name).map_err(app_status)
}

pub(crate) fn workspace_to_proto(workspace_name: &WorkspaceName) -> Workspace {
    Workspace {
        name: workspace_name.as_str().to_string(),
    }
}

/// Extracts and validates the originating episode from request metadata.
///
/// Episode attribution is best-effort: a missing `coral-episode-id` yields
/// `None`, and a present-but-malformed value is ignored (debug-logged) rather
/// than failing the call — the query is valid regardless of its trajectory tag.
pub(crate) fn episode_id_from_metadata(
    metadata: &tonic::metadata::MetadataMap,
) -> Option<EpisodeId> {
    let value = metadata.get(CORAL_EPISODE_ID_METADATA_KEY)?.to_str().ok()?;
    match EpisodeId::parse(value) {
        Ok(episode_id) => Some(episode_id),
        Err(error) => {
            tracing::debug!(%error, "ignoring malformed coral-episode-id metadata");
            None
        }
    }
}

pub(crate) fn table_to_proto(
    workspace_name: &WorkspaceName,
    table: coral_engine::TableInfo,
) -> Table {
    let columns = table.columns.into_iter().map(column_to_proto).collect();

    Table {
        workspace: Some(workspace_to_proto(workspace_name)),
        schema_name: table.schema_name,
        name: table.table_name,
        description: table.description,
        columns,
        required_filters: table.required_filters,
        guide: table.guide,
    }
}

pub(crate) fn table_summary_to_proto(
    workspace_name: &WorkspaceName,
    table: coral_engine::TableInfo,
) -> TableSummary {
    TableSummary {
        workspace: Some(workspace_to_proto(workspace_name)),
        schema_name: table.schema_name,
        name: table.table_name,
        description: table.description,
        required_filters: table.required_filters,
        guide: table.guide,
    }
}

pub(crate) fn catalog_item_to_proto(
    workspace_name: &WorkspaceName,
    item: CatalogItem,
) -> ProtoCatalogItem {
    match item {
        CatalogItem::Table(table) => ProtoCatalogItem {
            item: Some(catalog_item::Item::Table(table_summary_to_proto(
                workspace_name,
                table,
            ))),
        },
        CatalogItem::TableFunction(function) => ProtoCatalogItem {
            item: Some(catalog_item::Item::TableFunction(table_function_to_proto(
                workspace_name,
                function,
            ))),
        },
    }
}

pub(crate) fn catalog_search_result_to_proto(
    workspace_name: &WorkspaceName,
    result: CatalogSearchResult,
) -> ProtoCatalogSearchResult {
    ProtoCatalogSearchResult {
        item: Some(catalog_item_to_proto(workspace_name, result.item)),
        matched_fields: result
            .matched_fields
            .into_iter()
            .map(CatalogMetadataField::as_proto_name)
            .map(str::to_string)
            .collect(),
    }
}

pub(crate) fn table_function_to_proto(
    workspace_name: &WorkspaceName,
    function: coral_engine::TableFunctionInfo,
) -> TableFunction {
    TableFunction {
        workspace: Some(workspace_to_proto(workspace_name)),
        schema_name: function.schema_name,
        name: function.function_name,
        description: function.description,
        arguments: function
            .arguments
            .into_iter()
            .map(|argument| TableFunctionArgument {
                name: argument.name,
                required: argument.required,
                values: argument.values,
            })
            .collect(),
        result_columns: function
            .result_columns
            .into_iter()
            .map(|column| TableFunctionResultColumn {
                name: column.name,
                data_type: column.data_type,
                nullable: column.nullable,
                description: column.description,
            })
            .collect(),
    }
}

pub(crate) fn describe_table_response_to_proto(
    workspace_name: &WorkspaceName,
    result: DescribeTableResult,
) -> ProtoDescribeTableResponse {
    match result {
        DescribeTableResult::Found(table) => ProtoDescribeTableResponse {
            table: Some(table_to_proto(workspace_name, table)),
            suggestions: Vec::new(),
            available_schemas: Vec::new(),
            same_schema_tables: Vec::new(),
        },
        DescribeTableResult::Missing(context) => ProtoDescribeTableResponse {
            table: None,
            suggestions: context
                .suggestions
                .into_iter()
                .map(|table| table_summary_to_proto(workspace_name, table))
                .collect(),
            available_schemas: context.available_schemas,
            same_schema_tables: context
                .same_schema_tables
                .into_iter()
                .map(|table| table_summary_to_proto(workspace_name, table))
                .collect(),
        },
    }
}

pub(crate) fn column_search_result_to_proto(result: ColumnSearchResult) -> ProtoColumnSearchResult {
    ProtoColumnSearchResult {
        column: Some(column_to_proto(result.column)),
        matched_fields: result
            .matched_fields
            .into_iter()
            .map(ColumnMetadataField::as_proto_name)
            .map(str::to_string)
            .collect(),
    }
}

pub(crate) fn pagination_to_proto(
    total_count: u32,
    limit: u32,
    offset: u32,
    has_more: bool,
    next_offset: Option<u32>,
) -> PaginationResponse {
    PaginationResponse {
        total_count,
        limit,
        offset,
        has_more,
        next_offset: next_offset.unwrap_or(0),
    }
}

fn column_to_proto(column: coral_engine::ColumnInfo) -> Column {
    Column {
        name: column.name,
        data_type: column.data_type,
        nullable: column.nullable,
        is_virtual: column.is_virtual,
        is_required_filter: column.is_required_filter,
        description: column.description,
        ordinal_position: column.ordinal_position,
    }
}

pub(crate) fn query_test_result_to_proto(
    result: &coral_engine::QueryTestResult,
) -> QueryTestResult {
    let outcome = match result.result() {
        Ok(success) => Some(query_test_result::Outcome::Success(QueryTestSuccess {
            row_count: success.row_count(),
        })),
        Err(failure) => Some(query_test_result::Outcome::Failure(QueryTestFailure {
            error_message: failure.error_message().to_string(),
        })),
    };
    QueryTestResult {
        sql: result.sql().to_string(),
        outcome,
    }
}

pub(crate) fn validate_source_response_to_proto(
    source: Source,
    workspace_name: &WorkspaceName,
    report: coral_engine::SourceValidationReport,
) -> ValidateSourceResponse {
    let coral_engine::SourceValidationReport {
        tables,
        table_functions,
        query_tests,
    } = report;
    ValidateSourceResponse {
        source: Some(source),
        tables: tables
            .into_iter()
            .map(|table| table_to_proto(workspace_name, table))
            .collect(),
        query_tests: query_tests.iter().map(query_test_result_to_proto).collect(),
        table_functions: table_functions
            .into_iter()
            .map(|function| table_function_to_proto(workspace_name, function))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::indexing_slicing,
        reason = "proto shape assertions intentionally fail loudly in tests"
    )]

    use coral_api::{
        grpc_response_status_code,
        v1::{QueryTestFailure, Workspace, query_test_result},
    };
    use tonic::{Code, Request};

    use super::{
        GrpcMethodMetadata, GrpcServerMethod, episode_id_from_metadata, grpc_method, query_status,
        query_test_result_to_proto, table_summary_to_proto, table_to_proto,
        workspace_name_from_proto, workspace_to_proto,
    };
    use crate::bootstrap::AppError;
    use crate::query::manager::QueryManagerError;
    use crate::workspaces::WorkspaceName;
    use coral_engine::{
        ColumnInfo, CoreError, QueryTestResult as EngineQueryTestResult, TableInfo,
    };

    #[test]
    fn query_status_maps_app_errors() {
        let status = query_status(QueryManagerError::App(AppError::SourceNotFound(
            "users".to_string(),
        )));

        assert_eq!(status.code(), Code::NotFound);
        assert_eq!(status.message(), "source 'users' not found");
    }

    #[test]
    fn query_status_maps_core_errors() {
        let status = query_status(QueryManagerError::Core(CoreError::Unavailable(
            "backend down".to_string(),
        )));

        assert_eq!(status.code(), Code::Unavailable);
        assert_eq!(status.message(), "unavailable: backend down");
    }

    #[test]
    fn grpc_response_status_codes_use_otel_names() {
        for (code, name) in [
            (Code::Ok, "OK"),
            (Code::InvalidArgument, "INVALID_ARGUMENT"),
            (Code::Unavailable, "UNAVAILABLE"),
        ] {
            assert_eq!(grpc_response_status_code(code), name, "{name}");
        }
    }

    #[test]
    fn grpc_server_method_derives_from_uri_path() {
        assert_eq!(
            GrpcServerMethod::from_path("/coral.v1.QueryService/ExecuteSql"),
            Some(GrpcServerMethod {
                service: "coral.v1.QueryService".to_string(),
                method: "ExecuteSql".to_string(),
            })
        );
        assert_eq!(GrpcServerMethod::from_path("/missing-method"), None);
        assert_eq!(
            GrpcServerMethod::from_path("/coral.v1.QueryService/Extra/Path"),
            None
        );
    }

    #[test]
    fn grpc_method_reads_server_method_from_request_extensions() {
        let mut request = Request::new(());
        request
            .extensions_mut()
            .insert(GrpcServerMethod::from_path("/coral.v1.QueryService/ExecuteSql").unwrap());

        assert_eq!(
            grpc_method(&request),
            GrpcMethodMetadata::new("coral.v1.QueryService", "ExecuteSql")
        );
    }

    #[test]
    fn workspace_name_from_proto_rejects_missing_workspace() {
        let status = workspace_name_from_proto(None).expect_err("workspace should be required");

        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(status.message(), "invalid input: missing workspace");
    }

    #[test]
    fn workspace_name_from_proto_parses_valid_workspace() {
        let workspace = Workspace {
            name: "default".to_string(),
        };

        let workspace_name =
            workspace_name_from_proto(Some(&workspace)).expect("workspace should parse");

        assert_eq!(workspace_name.as_str(), "default");
    }

    #[test]
    fn episode_id_from_metadata_extracts_valid_id() {
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert("coral-episode-id", "ep_123".parse().expect("ascii value"));

        let episode_id = episode_id_from_metadata(&metadata).expect("valid id is extracted");

        assert_eq!(episode_id.as_str(), "ep_123");
    }

    #[test]
    fn episode_id_from_metadata_ignores_absent_and_malformed() {
        let absent = tonic::metadata::MetadataMap::new();
        assert!(
            episode_id_from_metadata(&absent).is_none(),
            "a missing coral-episode-id yields no attribution"
        );

        let mut malformed = tonic::metadata::MetadataMap::new();
        malformed.insert(
            "coral-episode-id",
            "has space".parse().expect("ascii value"),
        );
        assert!(
            episode_id_from_metadata(&malformed).is_none(),
            "a malformed id is ignored, not surfaced"
        );
    }

    /// The `TableInfo` fixture shared by the table proto-mapping tests.
    fn demo_users_table() -> TableInfo {
        TableInfo {
            schema_name: "demo".to_string(),
            table_name: "users".to_string(),
            description: "User records".to_string(),
            guide: "Filter by org_id.".to_string(),
            columns: vec![ColumnInfo {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
                nullable: false,
                is_virtual: false,
                is_required_filter: true,
                description: "User id".to_string(),
                ordinal_position: 0,
            }],
            required_filters: vec!["org_id".to_string()],
        }
    }

    #[test]
    fn table_to_proto_preserves_table_metadata() {
        let workspace_name = WorkspaceName::parse("default").expect("workspace");

        let proto = table_to_proto(&workspace_name, demo_users_table());

        assert_eq!(proto.workspace, Some(workspace_to_proto(&workspace_name)));
        assert_eq!(proto.schema_name, "demo");
        assert_eq!(proto.name, "users");
        assert_eq!(proto.description, "User records");
        assert_eq!(proto.guide, "Filter by org_id.");
        assert_eq!(proto.columns.len(), 1);
        assert_eq!(proto.columns[0].name, "id");
        assert_eq!(proto.columns[0].data_type, "Int64");
        assert!(!proto.columns[0].nullable);
        assert!(!proto.columns[0].is_virtual);
        assert!(proto.columns[0].is_required_filter);
        assert_eq!(proto.columns[0].description, "User id");
        assert_eq!(proto.columns[0].ordinal_position, 0);
        assert_eq!(proto.required_filters, vec!["org_id"]);
    }

    #[test]
    fn table_summary_to_proto_preserves_table_metadata_without_columns() {
        let workspace_name = WorkspaceName::parse("default").expect("workspace");

        let proto = table_summary_to_proto(&workspace_name, demo_users_table());

        assert_eq!(proto.workspace, Some(workspace_to_proto(&workspace_name)));
        assert_eq!(proto.schema_name, "demo");
        assert_eq!(proto.name, "users");
        assert_eq!(proto.description, "User records");
        assert_eq!(proto.guide, "Filter by org_id.");
        assert_eq!(proto.required_filters, vec!["org_id"]);
    }

    #[test]
    fn query_test_result_to_proto_preserves_result_metadata() {
        let proto = query_test_result_to_proto(&EngineQueryTestResult::failure(
            "SELECT 1",
            "failed precondition: boom",
        ));

        assert_eq!(proto.sql, "SELECT 1");
        assert!(matches!(
            proto.outcome,
            Some(query_test_result::Outcome::Failure(QueryTestFailure { error_message }))
                if error_message == "failed precondition: boom"
        ));
    }
}
