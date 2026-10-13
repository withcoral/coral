//! Persistence of a Built-in Provider catalog update.
use coral_catalog::ProviderGenerationId;
use coral_db::Transaction;
use coral_workspace::WorkspaceId;
use sea_query::{Expr, ExprTrait as _, Iden, Query};

use crate::{Error, ProviderCallRoot, ProviderId};

#[derive(Iden)]
enum ProviderRecords {
    Table,
    Id,
    WorkspaceId,
    /// Holds the complete Provider Call Root; the column predates Call Roots.
    Namespace,
    ActiveGenerationId,
    ResourceVersion,
}

/// Activates `replacement` when the Workspace's Built-in Provider still has
/// `expected` active, whatever the Provider's state.
pub(super) async fn activate_builtin_generation(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    provider: &ProviderId,
    expected: &ProviderGenerationId,
    replacement: &ProviderGenerationId,
) -> Result<bool, Error> {
    let statement = Query::update()
        .table(ProviderRecords::Table)
        .values([
            (
                ProviderRecords::ActiveGenerationId,
                Expr::val(replacement.to_string()),
            ),
            (
                ProviderRecords::ResourceVersion,
                Expr::col(ProviderRecords::ResourceVersion).add(1_i64),
            ),
        ])
        .and_where(Expr::col(ProviderRecords::WorkspaceId).eq(workspace.to_string()))
        .and_where(Expr::col(ProviderRecords::Id).eq(provider.to_string()))
        .and_where(Expr::col(ProviderRecords::Namespace).eq(ProviderCallRoot::Builtin.as_str()))
        .and_where(Expr::col(ProviderRecords::ActiveGenerationId).eq(expected.to_string()))
        .to_owned();
    Ok(transaction.execute(statement).await? == 1)
}
