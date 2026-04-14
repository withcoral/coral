//! Shared gRPC transport helpers for app-owned services.

use coral_api::v1::{Column, Table, Workspace};
use tonic::Status;

use crate::bootstrap::{app_status, core_status};
use crate::query::manager::QueryManagerError;

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

pub(crate) fn table_to_proto(workspace: &Workspace, table: coral_engine::TableInfo) -> Table {
    Table {
        workspace: Some(workspace.clone()),
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

#[cfg(test)]
mod tests {
    use tonic::Code;

    use super::{query_status, table_to_proto};
    use crate::bootstrap::AppError;
    use crate::query::manager::QueryManagerError;
    use coral_api::v1::Workspace;
    use coral_engine::{ColumnInfo, CoreError, TableInfo};

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
        let workspace = Workspace {
            name: "default".to_string(),
        };
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

        let proto = table_to_proto(&workspace, table);

        assert_eq!(proto.workspace, Some(workspace));
        assert_eq!(proto.schema_name, "demo");
        assert_eq!(proto.name, "users");
        assert_eq!(proto.description, "User records");
        assert_eq!(proto.columns.len(), 1);
        assert_eq!(proto.columns[0].name, "id");
        assert_eq!(proto.columns[0].data_type, "Int64");
        assert!(!proto.columns[0].nullable);
        assert_eq!(proto.required_filters, vec!["org_id"]);
    }
}
