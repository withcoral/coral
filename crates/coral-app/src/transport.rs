//! Shared gRPC transport helpers for app-owned services.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use coral_api::{
    CORAL_ERROR_DOMAIN, CORAL_TASK_ID_METADATA_KEY, grpc_response_status_code,
    v1::{
        CatalogItem as ProtoCatalogItem, CatalogSearchResult as ProtoCatalogSearchResult, Column,
        ColumnSearchResult as ProtoColumnSearchResult, DescribeCatalogSurfaceResponse,
        MissingCatalogSurface, PaginationResponse, QueryTestFailure, QueryTestResult,
        QueryTestSuccess, SearchLimits, Source, Table, TableFunction, TableFunctionArgument,
        TableFunctionKind, TableFunctionResultColumn, TableSummary, ValidateSourceResponse,
        Workspace, catalog_item, describe_catalog_surface_response, query_test_result,
    },
};
use coral_spec::{SearchLimitsSpec, SourceTableFunctionKind};
use coral_telemetry::{GRPC_REQUEST_ERROR_MESSAGE, record_failure};
use opentelemetry::propagation::Extractor;
use opentelemetry::trace::Status as OtelStatus;
use tonic::codegen::{Service, http};
use tonic::metadata::MetadataMap;
use tonic::{Code, Request, Status};
use tonic_types::{ErrorDetail, StatusExt as _};
use tower::Layer;
use tracing::Instrument as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::bootstrap::{AppError, app_status, core_status, status_with_bounded_detail};
use crate::catalog::discovery::{
    CatalogItem, CatalogMetadataField, CatalogSearchResult, ColumnMetadataField,
    ColumnSearchResult, DescribeCatalogSurfaceResult,
};
use crate::identity::{PrincipalProvider, PrincipalProviderError, PrincipalProviderErrorKind};
use crate::query::manager::QueryManagerError;
use crate::request_context::RequestContext;
use crate::task::id::TaskId;
use crate::workspaces::WorkspaceName;

struct MetadataExtractor<'a>(&'a tonic::metadata::MetadataMap);

const PRINCIPAL_PROVIDER_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct GrpcServerSpan(tracing::Span);

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

/// Tower layer that installs Coral request context for gRPC route trees.
///
/// Tonic preserves `http::Request` extensions when it decodes the protobuf
/// message into a `tonic::Request`, but generated server wrappers do not insert
/// `tonic::GrpcMethod` the way generated clients do. This keeps the method
/// data and Coral request metadata at the transport boundary and lets handlers
/// read typed context from the request.
///
/// The wrapper also authenticates the inbound metadata once before dispatching
/// to a service handler, so every application gRPC route in the layered tree is
/// covered by the same principal-selection path.
#[derive(Clone)]
pub(crate) struct GrpcRequestContextLayer {
    principal_provider: Arc<dyn PrincipalProvider>,
}

impl GrpcRequestContextLayer {
    pub(crate) fn new(principal_provider: Arc<dyn PrincipalProvider>) -> Self {
        Self { principal_provider }
    }
}

impl<S> Layer<S> for GrpcRequestContextLayer {
    type Service = GrpcRequestContextService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        GrpcRequestContextService {
            inner,
            principal_provider: Arc::clone(&self.principal_provider),
        }
    }
}

#[derive(Clone)]
pub(crate) struct GrpcRequestContextService<S> {
    inner: S,
    principal_provider: Arc<dyn PrincipalProvider>,
}

impl<S, B, ResBody> Service<http::Request<B>> for GrpcRequestContextService<S>
where
    S: Service<http::Request<B>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
    B: Send + 'static,
    ResBody: Default + Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<B>) -> Self::Future {
        let task_id = annotate_request_context(&mut request);
        let method_metadata = request.extensions().get::<GrpcServerMethod>().map_or_else(
            || GrpcMethodMetadata::new("coral.v1.UnknownService", "Unknown"),
            |method| GrpcMethodMetadata::new(method.service.as_str(), method.method.as_str()),
        );
        let request_metadata = MetadataMap::from_headers(request.headers().clone());
        let principal_provider = Arc::clone(&self.principal_provider);
        let span = grpc_span_for_metadata(&method_metadata, &request_metadata);
        request
            .extensions_mut()
            .insert(GrpcServerSpan(span.clone()));

        let mut inner = self.inner.clone();
        std::mem::swap(&mut self.inner, &mut inner);

        let instrumented_span = span.clone();
        Box::pin(
            async move {
                match principal_for_request(
                    principal_provider.as_ref(),
                    &request_metadata,
                    PRINCIPAL_PROVIDER_TIMEOUT,
                )
                .await
                {
                    Ok(principal) => {
                        let task_id = match task_id {
                            Ok(task_id) => task_id,
                            Err(status) => {
                                record_grpc_status(&span, status.code(), Some(&status));
                                return Ok(status.into_http());
                            }
                        };
                        request
                            .extensions_mut()
                            .insert(RequestContext::new(principal).with_task_id(task_id));
                        inner.call(request).await
                    }
                    Err(error) => {
                        let status = principal_provider_status(&error);
                        record_grpc_status(&span, status.code(), Some(&status));
                        Ok(status.into_http())
                    }
                }
            }
            .instrument(instrumented_span),
        )
    }
}

async fn principal_for_request(
    provider: &dyn PrincipalProvider,
    metadata: &MetadataMap,
    timeout: Duration,
) -> Result<crate::identity::Principal, PrincipalProviderError> {
    tokio::time::timeout(timeout, provider.principal_for_metadata(metadata))
        .await
        .unwrap_or_else(|_| {
            Err(PrincipalProviderError::unavailable(
                "principal provider timed out",
            ))
        })
}

fn principal_provider_status(error: &PrincipalProviderError) -> Status {
    let (code, prefix) = match error.kind() {
        PrincipalProviderErrorKind::Unauthenticated => (Code::Unauthenticated, "unauthenticated"),
        PrincipalProviderErrorKind::Unavailable => (Code::Unavailable, "unavailable"),
        PrincipalProviderErrorKind::Internal => (Code::Internal, "internal"),
    };
    status_with_bounded_detail(code, format!("{prefix}: {}", error.client_message()))
}

impl<S> tonic::server::NamedService for GrpcRequestContextService<S>
where
    S: tonic::server::NamedService,
{
    const NAME: &'static str = S::NAME;
}

/// Creates a span parented to the trace context extracted from a gRPC request.
pub(crate) fn grpc_span<T>(request: &Request<T>) -> tracing::Span {
    if let Some(span) = request.extensions().get::<GrpcServerSpan>() {
        return span.0.clone();
    }
    let metadata = grpc_method(request);
    grpc_span_for_metadata(&metadata, request.metadata())
}

fn grpc_span_for_metadata(
    metadata: &GrpcMethodMetadata,
    request_metadata: &MetadataMap,
) -> tracing::Span {
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
    coral_telemetry::set_parent_from_extractor(&span, &MetadataExtractor(request_metadata));
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

fn annotate_request_context<B>(request: &mut http::Request<B>) -> Result<Option<TaskId>, Status> {
    if let Some(method) = GrpcServerMethod::from_path(request.uri().path()) {
        request.extensions_mut().insert(method);
    }
    let mut task_ids = request.headers().get_all(CORAL_TASK_ID_METADATA_KEY).iter();
    let Some(task_id) = task_ids.next() else {
        return Ok(None);
    };
    if task_ids.next().is_some() {
        return Err(Status::invalid_argument(
            "coral-task-id metadata must contain exactly one value",
        ));
    }
    task_id_from_header_value(task_id).map(Some)
}

fn task_id_from_header_value(value: &http::HeaderValue) -> Result<TaskId, Status> {
    let value = value
        .to_str()
        .map_err(|_error| Status::invalid_argument("coral-task-id metadata must be ASCII"))?;
    TaskId::parse(value).map_err(app_status)
}

pub(crate) fn request_context<T>(request: &Request<T>) -> Result<&RequestContext, Status> {
    request
        .extensions()
        .get::<RequestContext>()
        .ok_or_else(|| Status::internal("request context is unavailable"))
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

fn record_grpc_status(span: &tracing::Span, code: Code, status: Option<&Status>) {
    let response_status_code = grpc_response_status_code(code);
    span.record("grpc.status_code", code as i64);
    span.record("grpc.code", response_status_code);
    span.record("rpc.response.status_code", response_status_code);
    if code == Code::Ok {
        span.record("status", "ok");
        span.set_status(OtelStatus::Ok);
    } else {
        let error_type = status.map_or_else(|| response_status_code.to_string(), grpc_error_type);
        record_failure(span, error_type.as_str(), GRPC_REQUEST_ERROR_MESSAGE);
    }
}

fn grpc_error_type(status: &Status) -> String {
    for detail in status.get_error_details_vec() {
        if let ErrorDetail::ErrorInfo(info) = detail
            && info.domain == CORAL_ERROR_DOMAIN
        {
            return info.reason;
        }
    }

    grpc_response_status_code(status.code()).to_string()
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

pub(crate) fn table_to_proto(
    workspace_name: &WorkspaceName,
    table: coral_engine::TableInfo,
) -> Table {
    let columns = table.columns.into_iter().map(column_to_proto).collect();

    Table {
        workspace: Some(workspace_to_proto(workspace_name)),
        catalog_name: table.catalog_name.unwrap_or_default(),
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
        catalog_name: table.catalog_name.unwrap_or_default(),
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
    let schema_name = function.schema_name;
    let function_name = function.function_name;
    TableFunction {
        workspace: Some(workspace_to_proto(workspace_name)),
        schema_name,
        name: function_name,
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
        kind: table_function_kind_to_proto(function.kind) as i32,
        search_limits: function.search_limits.as_ref().map(search_limits_to_proto),
        guide: function.guide,
    }
}

fn table_function_kind_to_proto(kind: SourceTableFunctionKind) -> TableFunctionKind {
    match kind {
        SourceTableFunctionKind::Table => TableFunctionKind::Table,
        SourceTableFunctionKind::Search => TableFunctionKind::Search,
    }
}

fn search_limits_to_proto(limits: &SearchLimitsSpec) -> SearchLimits {
    SearchLimits {
        default_top_k: u32::try_from(limits.default_top_k)
            .expect("validated search limits default_top_k fits u32"),
        max_top_k: u32::try_from(limits.max_top_k)
            .expect("validated search limits max_top_k fits u32"),
        max_calls_per_query: u32::try_from(limits.max_calls_per_query)
            .expect("validated search limits max_calls_per_query fits u32"),
    }
}

pub(crate) fn describe_catalog_surface_response_to_proto(
    workspace_name: &WorkspaceName,
    result: DescribeCatalogSurfaceResult,
) -> DescribeCatalogSurfaceResponse {
    use describe_catalog_surface_response::Result;

    match result {
        DescribeCatalogSurfaceResult::Table(table) => DescribeCatalogSurfaceResponse {
            result: Some(Result::Table(table_to_proto(workspace_name, table))),
        },
        DescribeCatalogSurfaceResult::TableFunction(table_function) => {
            DescribeCatalogSurfaceResponse {
                result: Some(Result::TableFunction(table_function_to_proto(
                    workspace_name,
                    table_function,
                ))),
            }
        }
        DescribeCatalogSurfaceResult::Missing(context) => DescribeCatalogSurfaceResponse {
            result: Some(Result::Missing(MissingCatalogSurface {
                suggestions: context
                    .suggestions
                    .into_iter()
                    .map(|item| catalog_item_to_proto(workspace_name, item))
                    .collect(),
                available_schemas: context.available_schemas,
                same_schema_items: context
                    .same_schema_items
                    .into_iter()
                    .map(|item| catalog_item_to_proto(workspace_name, item))
                    .collect(),
            })),
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

    use std::collections::HashMap;

    use coral_api::{
        CORAL_ERROR_DOMAIN, CORAL_ERROR_METADATA_SUMMARY, CORAL_TASK_ID_METADATA_KEY,
        grpc_response_status_code,
        v1::{QueryTestFailure, Workspace, query_test_result},
    };
    use opentelemetry::Value;
    use opentelemetry::trace::{Status as OtelStatus, TracerProvider as _};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
    use tonic::{Code, Request, Status};
    use tonic_types::{ErrorDetail, StatusExt as _};
    use tracing_subscriber::layer::SubscriberExt as _;

    use super::{
        GRPC_REQUEST_ERROR_MESSAGE, GrpcMethodMetadata, GrpcServerMethod, annotate_request_context,
        grpc_method, grpc_span_for_metadata, instrument_grpc, principal_provider_status,
        query_status, query_test_result_to_proto, table_function_to_proto, table_summary_to_proto,
        table_to_proto, workspace_name_from_proto, workspace_to_proto,
    };
    use crate::bootstrap::AppError;
    use crate::identity::PrincipalProviderError;
    use crate::query::manager::QueryManagerError;
    use crate::workspaces::WorkspaceName;
    use coral_engine::{
        ColumnInfo, CoreError, QueryTestResult as EngineQueryTestResult, TableFunctionInfo,
        TableInfo,
    };
    use coral_spec::SourceTableFunctionKind;

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
        assert_eq!(grpc_response_status_code(Code::Ok), "OK");
        assert_eq!(
            grpc_response_status_code(Code::InvalidArgument),
            "INVALID_ARGUMENT"
        );
        assert_eq!(grpc_response_status_code(Code::Unavailable), "UNAVAILABLE");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn plain_grpc_status_message_is_not_exported() {
        let sentinel = "SENSITIVE_SERVER_GRPC_ERROR_MARKER";
        let status = Status::invalid_argument(format!("invalid input: {sentinel}"));

        let (returned_status, span) = export_grpc_status(status).await;

        assert!(returned_status.message().contains(sentinel));
        assert_eq!(
            string_attribute(&span, "error.type"),
            Some("INVALID_ARGUMENT".to_string())
        );
        assert_eq!(
            string_attribute(&span, "exception.message"),
            Some(GRPC_REQUEST_ERROR_MESSAGE.to_string())
        );
        assert_eq!(span.status, OtelStatus::error(GRPC_REQUEST_ERROR_MESSAGE));
        assert!(!format!("{span:?}").contains(sentinel));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn structured_grpc_status_keeps_only_its_categorical_reason() {
        let sentinel = "SENSITIVE_SERVER_STRUCTURED_ERROR_MARKER";
        let metadata = HashMap::from([(
            CORAL_ERROR_METADATA_SUMMARY.to_string(),
            format!("summary containing {sentinel}"),
        )]);
        let status = Status::with_error_details_vec(
            Code::InvalidArgument,
            format!("fallback containing {sentinel}"),
            vec![ErrorDetail::ErrorInfo(tonic_types::ErrorInfo::new(
                "INVALID_CATALOG_KIND",
                CORAL_ERROR_DOMAIN,
                metadata,
            ))],
        );

        let (returned_status, span) = export_grpc_status(status).await;

        assert!(returned_status.message().contains(sentinel));
        assert_eq!(
            string_attribute(&span, "error.type"),
            Some("INVALID_CATALOG_KIND".to_string())
        );
        assert_eq!(
            string_attribute(&span, "exception.message"),
            Some(GRPC_REQUEST_ERROR_MESSAGE.to_string())
        );
        assert_eq!(span.status, OtelStatus::error(GRPC_REQUEST_ERROR_MESSAGE));
        assert!(!format!("{span:?}").contains(sentinel));
    }

    async fn export_grpc_status(status: Status) -> (Status, SpanData) {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("coral-server-error-privacy-test");
        let subscriber = tracing_subscriber::Registry::default()
            .with(tracing_opentelemetry::layer().with_tracer(tracer));
        let guard = tracing::subscriber::set_default(subscriber);
        let span = grpc_span_for_metadata(
            &GrpcMethodMetadata::new("coral.v1.QueryService", "ExecuteSql"),
            &tonic::metadata::MetadataMap::new(),
        );

        let returned_status = instrument_grpc(span, async { Err::<(), Status>(status) })
            .await
            .expect_err("gRPC status should be returned to the caller");
        drop(guard);

        provider.force_flush().expect("spans should flush");
        let span = exporter
            .get_finished_spans()
            .expect("finished spans should be readable")
            .into_iter()
            .find(|span| span.name == "coral.v1.QueryService/ExecuteSql")
            .expect("server span should export");
        (returned_status, span)
    }

    fn string_attribute(span: &SpanData, name: &str) -> Option<String> {
        span.attributes.iter().find_map(|attribute| {
            if attribute.key.as_str() == name
                && let Value::String(value) = &attribute.value
            {
                Some(value.as_ref().to_string())
            } else {
                None
            }
        })
    }

    #[test]
    fn principal_provider_status_preserves_failure_class() {
        let cases = [
            (
                PrincipalProviderError::unauthenticated("bad token"),
                Code::Unauthenticated,
                "unauthenticated: bad token",
            ),
            (
                PrincipalProviderError::unavailable("key service offline"),
                Code::Unavailable,
                "unavailable: key service offline",
            ),
            (
                PrincipalProviderError::internal("invalid principal selection"),
                Code::Internal,
                "internal: invalid principal selection",
            ),
        ];
        for (error, code, message) in cases {
            let status = principal_provider_status(&error);
            assert_eq!(status.code(), code);
            assert_eq!(status.message(), message);
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
    fn annotate_request_context_parses_valid_task_id() {
        let mut request = tonic::codegen::http::Request::builder()
            .uri("/coral.v1.QueryService/ExecuteSql")
            .header(
                CORAL_TASK_ID_METADATA_KEY,
                "750e8400-e29b-41d4-a716-446655440000",
            )
            .body(())
            .expect("request");

        let task_id = annotate_request_context(&mut request)
            .expect("valid metadata")
            .expect("task id");
        assert_eq!(task_id.to_string(), "750e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn annotate_request_context_accepts_absent_and_rejects_malformed_task_id() {
        let mut absent = tonic::codegen::http::Request::builder()
            .uri("/coral.v1.QueryService/ExecuteSql")
            .body(())
            .expect("request");
        assert_eq!(
            annotate_request_context(&mut absent).expect("absent metadata"),
            None
        );

        let mut malformed = tonic::codegen::http::Request::builder()
            .uri("/coral.v1.QueryService/ExecuteSql")
            .header(CORAL_TASK_ID_METADATA_KEY, "has space")
            .body(())
            .expect("request");
        let status =
            annotate_request_context(&mut malformed).expect_err("reject malformed metadata");
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[test]
    fn annotate_request_context_rejects_repeated_task_id_metadata() {
        assert_repeated_task_id_metadata_rejected("750e8400-e29b-41d4-a716-446655440001");
    }

    #[test]
    fn annotate_request_context_rejects_valid_and_malformed_repeated_task_id_metadata() {
        assert_repeated_task_id_metadata_rejected("not-a-task-id");
    }

    fn assert_repeated_task_id_metadata_rejected(second_value: &str) {
        let mut request = tonic::codegen::http::Request::builder()
            .uri("/coral.v1.QueryService/ExecuteSql")
            .header(
                CORAL_TASK_ID_METADATA_KEY,
                "750e8400-e29b-41d4-a716-446655440000",
            )
            .header(CORAL_TASK_ID_METADATA_KEY, second_value)
            .body(())
            .expect("request");

        let status = annotate_request_context(&mut request).expect_err("reject repeated metadata");
        assert_eq!(status.code(), Code::InvalidArgument);
        assert_eq!(
            status.message(),
            "coral-task-id metadata must contain exactly one value"
        );
    }

    #[test]
    fn table_to_proto_preserves_table_metadata() {
        let workspace_name = WorkspaceName::parse("default").expect("workspace");
        let table = TableInfo {
            catalog_name: None,
            schema_name: "demo".to_string(),
            table_name: "users".to_string(),
            description: "User records".to_string(),
            guide: "Filter by org_id.".to_string(),
            require_guide_read: true,
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
        };

        let proto = table_to_proto(&workspace_name, table);

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
        let table = TableInfo {
            catalog_name: None,
            schema_name: "demo".to_string(),
            table_name: "users".to_string(),
            description: "User records".to_string(),
            guide: "Filter by org_id.".to_string(),
            require_guide_read: true,
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
        };

        let proto = table_summary_to_proto(&workspace_name, table);

        assert_eq!(proto.workspace, Some(workspace_to_proto(&workspace_name)));
        assert_eq!(proto.schema_name, "demo");
        assert_eq!(proto.name, "users");
        assert_eq!(proto.description, "User records");
        assert_eq!(proto.guide, "Filter by org_id.");
        assert_eq!(proto.required_filters, vec!["org_id"]);
    }

    #[test]
    fn table_function_to_proto_preserves_argument_metadata() {
        let workspace_name = WorkspaceName::parse("default").expect("workspace");
        let function = TableFunctionInfo {
            schema_name: "demo".to_string(),
            function_name: "search".to_string(),
            description: "Search demo records".to_string(),
            guide: "Prefer search for record lookup.".to_string(),
            require_guide_read: true,
            arguments: vec![coral_engine::TableFunctionArgumentInfo {
                name: "payload".to_string(),
                required: true,
                values: Vec::new(),
            }],
            result_columns: Vec::new(),
            kind: SourceTableFunctionKind::Search,
            search_limits: None,
        };

        let proto = table_function_to_proto(&workspace_name, function);

        assert_eq!(proto.workspace, Some(workspace_to_proto(&workspace_name)));
        assert_eq!(proto.schema_name, "demo");
        assert_eq!(proto.name, "search");
        assert_eq!(proto.description, "Search demo records");
        assert_eq!(proto.guide, "Prefer search for record lookup.");
        assert_eq!(proto.arguments.len(), 1);
        assert_eq!(proto.arguments[0].name, "payload");
        assert!(proto.arguments[0].required);
        assert!(proto.arguments[0].values.is_empty());
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
