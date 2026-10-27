//! Test-only faults in persisted Provider material, for crates that read it.
//!
//! The `test-fixtures` feature enables this module for dev-dependencies; production
//! builds never do. Each fault changes exactly one Workspace-qualified Provider's
//! current material and panics unless it changed exactly one row.
use coral_db::Database;
use coral_workspace::WorkspaceId;
use sea_query::{Alias, Expr, ExprTrait as _, Query, SelectStatement, SimpleExpr};

use crate::ProviderId;

/// Makes the Provider's active credential contract undecodable.
///
/// # Panics
///
/// Panics when the write fails or does not change exactly one Generation.
pub async fn corrupt_credential_contract(
    database: &Database,
    workspace: &WorkspaceId,
    provider: &ProviderId,
) {
    update_active_generation(
        database,
        workspace,
        provider,
        "credential_contract",
        "not-a-contract",
    )
    .await;
}

/// Makes one active Tool's authoritative tags undecodable, so its complete
/// description cannot load while its search index entry stays healthy.
///
/// # Panics
///
/// Panics when the write fails or does not change exactly one Tool.
pub async fn corrupt_tool_tags(
    database: &Database,
    workspace: &WorkspaceId,
    provider: &ProviderId,
    tool_key: &str,
) {
    let changed = database
        .execute(
            Query::update()
                .table(Alias::new("materialized_tools"))
                .value(Alias::new("tags_json"), "not-json")
                .and_where(
                    Expr::col(Alias::new("provider_generation_id"))
                        .in_subquery(active_generation(workspace, provider)),
                )
                .and_where(Expr::col(Alias::new("tool_key")).eq(tool_key))
                .to_owned(),
        )
        .await
        .expect("inject Tool tags fault");
    assert_eq!(changed, 1, "exactly one active Tool");
}

/// Stores a namespace the Provider decoder rejects, so every read of this Provider fails.
///
/// # Panics
///
/// Panics when the write fails or does not change exactly one Provider.
pub async fn make_provider_unreadable(
    database: &Database,
    workspace: &WorkspaceId,
    provider: &ProviderId,
) {
    let changed = database
        .execute(
            Query::update()
                .table(Alias::new("provider_records"))
                .value(Alias::new("namespace"), "constructor")
                .and_where(provider_match(workspace, provider))
                .to_owned(),
        )
        .await
        .expect("inject invalid Provider namespace");
    assert_eq!(changed, 1, "exactly one Provider");
}

async fn update_active_generation(
    database: &Database,
    workspace: &WorkspaceId,
    provider: &ProviderId,
    column: &'static str,
    value: &str,
) {
    let changed = database
        .execute(
            Query::update()
                .table(Alias::new("provider_generations"))
                .value(Alias::new(column), value)
                .and_where(
                    Expr::col(Alias::new("id")).in_subquery(active_generation(workspace, provider)),
                )
                .to_owned(),
        )
        .await
        .expect("inject Provider Generation fault");
    assert_eq!(changed, 1, "exactly one active Provider Generation");
}

fn active_generation(workspace: &WorkspaceId, provider: &ProviderId) -> SelectStatement {
    Query::select()
        .column(Alias::new("active_generation_id"))
        .from(Alias::new("provider_records"))
        .and_where(provider_match(workspace, provider))
        .to_owned()
}

fn provider_match(workspace: &WorkspaceId, provider: &ProviderId) -> SimpleExpr {
    Expr::col(Alias::new("workspace_id"))
        .eq(workspace.to_string())
        .and(Expr::col(Alias::new("id")).eq(provider.to_string()))
}
