//! Shared gRPC transport helpers for app-owned services.

use coral_api::v1::{
    Column, QueryTestFailure, QueryTestResult, QueryTestSuccess, Source, Table,
    ValidateSourceResponse, Workspace, query_test_result,
};
use opentelemetry::propagation::Extractor;
use tonic::Status;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::bootstrap::{AppError, app_status, core_status};
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

/// Creates a span parented to the trace context extracted from `metadata`.
pub(crate) fn grpc_span(
    metadata: &tonic::metadata::MetadataMap,
    name: &'static str,
) -> tracing::Span {
    let parent_cx = extract_trace_context(metadata);
    let span = tracing::info_span!("grpc", grpc.method = name);
    let _ = span.set_parent(parent_cx);
    span
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "used directly as a map_err adapter across tonic service handlers"
)]
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
    Table {
        workspace: Some(workspace_to_proto(workspace_name)),
        schema_name: table.schema_name,
        name: table.table_name,
        description: table.description,
        columns: table
            .columns
            .into_iter()
            .map(|column| Column {
                name: column.name,
                data_type: column.data_type,
                nullable: column.nullable,
            })
            .collect(),
        required_filters: table.required_filters,
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
        query_tests,
    } = report;
    ValidateSourceResponse {
        source: Some(source),
        tables: tables
            .into_iter()
            .map(|table| table_to_proto(workspace_name, table))
            .collect(),
        query_tests: query_tests.iter().map(query_test_result_to_proto).collect(),
    }
}

#[cfg(test)]
mod tests {
    use coral_api::v1::{QueryTestFailure, Workspace, query_test_result};
    use tonic::Code;

    use super::{
        query_status, query_test_result_to_proto, table_to_proto, workspace_name_from_proto,
        workspace_to_proto,
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
    fn table_to_proto_preserves_table_metadata() {
        let workspace_name = WorkspaceName::parse("default").expect("workspace");
        let table = TableInfo {
            schema_name: "demo".to_string(),
            table_name: "users".to_string(),
            description: "User records".to_string(),
            columns: vec![ColumnInfo {
                name: "id".to_string(),
                data_type: "Int64".to_string(),
                nullable: false,
            }],
            required_filters: vec!["org_id".to_string()],
        };

        let proto = table_to_proto(&workspace_name, table);

        assert_eq!(proto.workspace, Some(workspace_to_proto(&workspace_name)));
        assert_eq!(proto.schema_name, "demo");
        assert_eq!(proto.name, "users");
        assert_eq!(proto.description, "User records");
        assert_eq!(proto.columns.len(), 1);
        assert_eq!(proto.columns[0].name, "id");
        assert_eq!(proto.columns[0].data_type, "Int64");
        assert!(!proto.columns[0].nullable);
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
