//! Shared gRPC transport helpers for app-owned services.

use coral_api::v1::{Column, QueryTestResult, Table, Workspace};
use tonic::Status;

use crate::bootstrap::{app_status, core_status};
use crate::query::manager::QueryManagerError;
use crate::workspaces::WorkspaceName;

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
    QueryTestResult {
        sql: result.sql().to_string(),
        passed: result.passed(),
        row_count: result.row_count(),
        error_message: result.error_message().map(ToString::to_string),
    }
}

#[cfg(test)]
mod tests {
    use tonic::Code;

    use super::{query_status, query_test_result_to_proto, table_to_proto, workspace_to_proto};
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
        let proto = query_test_result_to_proto(&EngineQueryTestResult::new(
            "SELECT 1",
            false,
            None,
            Some("failed precondition: boom".to_string()),
        ));

        assert_eq!(proto.sql, "SELECT 1");
        assert!(!proto.passed);
        assert_eq!(proto.row_count, None);
        assert_eq!(
            proto.error_message.as_deref(),
            Some("failed precondition: boom")
        );
    }
}
