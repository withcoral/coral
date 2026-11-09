//! Test-only faults in persisted Private Tools, for crates that read them.
//!
//! The `test-fixtures` feature enables this module for dev-dependencies; production
//! builds never do. Each fault changes exactly one Workspace-qualified Private Tool
//! and panics unless it changed exactly one row.
use coral_db::Database;
use coral_workspace::WorkspaceId;
use sea_query::{Alias, Expr, ExprTrait as _, Query};

use crate::{AUTHORED_TOOL_SOURCE_MAX_BYTES, PrivateToolId};

/// Stores declaration fields whose path the projection decoder rejects.
///
/// # Panics
///
/// Panics when the write fails or does not change exactly one Private Tool.
pub async fn corrupt_declaration_fields(
    database: &Database,
    workspace: &WorkspaceId,
    tool: &PrivateToolId,
) {
    corrupt(
        database,
        workspace,
        tool,
        "declaration_fields_json",
        r#"[{"path":"bogus","text":"value"}]"#.to_owned(),
    )
    .await;
}

/// Stores source one byte over the authoring bound, so loading it fails.
///
/// # Panics
///
/// Panics when the write fails or does not change exactly one Private Tool.
pub async fn corrupt_source(database: &Database, workspace: &WorkspaceId, tool: &PrivateToolId) {
    corrupt(
        database,
        workspace,
        tool,
        "source",
        "x".repeat(AUTHORED_TOOL_SOURCE_MAX_BYTES + 1),
    )
    .await;
}

/// Stores a blank description, so every read of the Private Tool's metadata fails.
///
/// # Panics
///
/// Panics when the write fails or does not change exactly one Private Tool.
pub async fn corrupt_description(
    database: &Database,
    workspace: &WorkspaceId,
    tool: &PrivateToolId,
) {
    corrupt(database, workspace, tool, "description", " ".to_owned()).await;
}

async fn corrupt(
    database: &Database,
    workspace: &WorkspaceId,
    tool: &PrivateToolId,
    column: &'static str,
    value: String,
) {
    let changed = database
        .execute(
            Query::update()
                .table(Alias::new("private_tool_records"))
                .value(Alias::new(column), value)
                .and_where(Expr::col(Alias::new("workspace_id")).eq(workspace.to_string()))
                .and_where(Expr::col(Alias::new("id")).eq(tool.to_string()))
                .to_owned(),
        )
        .await
        .expect("inject Private Tool fault");
    assert_eq!(changed, 1, "exactly one Private Tool");
}
