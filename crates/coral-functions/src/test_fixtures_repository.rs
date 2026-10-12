//! Test-only faults in persisted Saved Functions, for crates that read them.
//!
//! The `test-fixtures` feature enables this module for dev-dependencies; production
//! builds never do. Each fault changes exactly one Workspace-qualified Saved Function
//! and panics unless it changed exactly one row.
use coral_db::Database;
use coral_workspace::WorkspaceId;
use sea_query::{Alias, Expr, ExprTrait as _, Query};

use crate::{SAVED_FUNCTION_SOURCE_MAX_BYTES, SavedFunctionId};

/// Stores declaration fields whose path the projection decoder rejects.
///
/// # Panics
///
/// Panics when the write fails or does not change exactly one Saved Function.
pub async fn corrupt_declaration_fields(
    database: &Database,
    workspace: &WorkspaceId,
    function: &SavedFunctionId,
) {
    corrupt(
        database,
        workspace,
        function,
        "declaration_fields_json",
        r#"[{"path":"bogus","text":"value"}]"#.to_owned(),
    )
    .await;
}

/// Stores source one byte over the authoring bound, so loading it fails.
///
/// # Panics
///
/// Panics when the write fails or does not change exactly one Saved Function.
pub async fn corrupt_source(
    database: &Database,
    workspace: &WorkspaceId,
    function: &SavedFunctionId,
) {
    corrupt(
        database,
        workspace,
        function,
        "source",
        "x".repeat(SAVED_FUNCTION_SOURCE_MAX_BYTES + 1),
    )
    .await;
}

/// Stores a blank description, so every read of the Saved Function's metadata fails.
///
/// # Panics
///
/// Panics when the write fails or does not change exactly one Saved Function.
pub async fn corrupt_description(
    database: &Database,
    workspace: &WorkspaceId,
    function: &SavedFunctionId,
) {
    corrupt(database, workspace, function, "description", " ".to_owned()).await;
}

async fn corrupt(
    database: &Database,
    workspace: &WorkspaceId,
    function: &SavedFunctionId,
    column: &'static str,
    value: String,
) {
    let changed = database
        .execute(
            Query::update()
                .table(Alias::new("saved_function_records"))
                .value(Alias::new(column), value)
                .and_where(Expr::col(Alias::new("workspace_id")).eq(workspace.to_string()))
                .and_where(Expr::col(Alias::new("id")).eq(function.to_string()))
                .to_owned(),
        )
        .await
        .expect("inject Saved Function fault");
    assert_eq!(changed, 1, "exactly one Saved Function");
}
