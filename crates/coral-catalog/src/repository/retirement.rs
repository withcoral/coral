//! Retirement marks and the deletion of retired Generations.
use coral_db::Transaction;
use sea_query::{
    ColumnRef, Expr, ExprTrait as _, IntoColumnRef as _, OnConflict, Order, Query, SimpleExpr,
};

use super::{MaterializedOperations, ProviderGenerationTypes, ProviderGenerations};
use crate::{Error, ProviderGenerationId};

#[derive(sea_query::Iden)]
enum ProviderGenerationRetirements {
    Table,
    ProviderGenerationId,
    RetiredAtUnixNanos,
}

fn retirement_generation() -> ColumnRef {
    (
        ProviderGenerationRetirements::Table,
        ProviderGenerationRetirements::ProviderGenerationId,
    )
        .into_column_ref()
}

/// Marks every unmarked Generation that `in_use` does not hold as retired at
/// `now`. A mark another pass wrote first stands.
pub(crate) async fn retire_unused(
    transaction: &mut Transaction<'_>,
    in_use: &(dyn Fn(ColumnRef) -> SimpleExpr + Sync),
    now_unix_nanos: i64,
) -> Result<u64, Error> {
    let generation = (ProviderGenerations::Table, ProviderGenerations::Id).into_column_ref();
    let unused = Query::select()
        .column(generation.clone())
        .expr(Expr::val(now_unix_nanos))
        .from(ProviderGenerations::Table)
        .and_where(
            Expr::col(generation.clone()).not_in_subquery(
                Query::select()
                    .column(retirement_generation())
                    .from(ProviderGenerationRetirements::Table)
                    .to_owned(),
            ),
        )
        .and_where(in_use(generation).not())
        .to_owned();
    Ok(transaction
        .execute(
            Query::insert()
                .into_table(ProviderGenerationRetirements::Table)
                .columns([
                    ProviderGenerationRetirements::ProviderGenerationId,
                    ProviderGenerationRetirements::RetiredAtUnixNanos,
                ])
                .select_from(unused)
                .map_err(|error| Error::InvalidInput(error.to_string()))?
                .on_conflict(
                    OnConflict::column(ProviderGenerationRetirements::ProviderGenerationId)
                        .do_nothing()
                        .to_owned(),
                )
                .to_owned(),
        )
        .await?)
}

/// Up to `limit` Generations retired at or before `retired_by` that `in_use`
/// does not hold, earliest first.
pub(crate) async fn due_retirements(
    transaction: &mut Transaction<'_>,
    in_use: &(dyn Fn(ColumnRef) -> SimpleExpr + Sync),
    retired_by_unix_nanos: i64,
    limit: u64,
) -> Result<Vec<ProviderGenerationId>, Error> {
    let rows: Vec<(String,)> = transaction
        .fetch_all(
            Query::select()
                .column(retirement_generation())
                .from(ProviderGenerationRetirements::Table)
                .and_where(due(in_use, retired_by_unix_nanos))
                .order_by(
                    ProviderGenerationRetirements::RetiredAtUnixNanos,
                    Order::Asc,
                )
                .order_by(
                    ProviderGenerationRetirements::ProviderGenerationId,
                    Order::Asc,
                )
                .limit(limit)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(id,)| {
            ProviderGenerationId::parse(&id)
                .map_err(|_error| Error::CorruptData("retired Generation id".into()))
        })
        .collect()
}

/// Deletes one Generation, its Operations and shared types when it is still
/// retired at or before `retired_by` and `in_use` still does not hold it.
/// Its search rows cascade. Reports whether it deleted the Generation.
pub(crate) async fn delete_retired(
    transaction: &mut Transaction<'_>,
    generation: &ProviderGenerationId,
    in_use: &(dyn Fn(ColumnRef) -> SimpleExpr + Sync),
    retired_by_unix_nanos: i64,
) -> Result<bool, Error> {
    // Deleting the mark first takes its row, so two passes never both
    // delete the Generation, and rechecks that nothing holds it now.
    let claimed = transaction
        .execute(
            Query::delete()
                .from_table(ProviderGenerationRetirements::Table)
                .and_where(
                    Expr::col(ProviderGenerationRetirements::ProviderGenerationId)
                        .eq(generation.to_string()),
                )
                .and_where(due(in_use, retired_by_unix_nanos))
                .to_owned(),
        )
        .await?;
    if claimed == 0 {
        return Ok(false);
    }
    transaction
        .execute(
            Query::delete()
                .from_table(ProviderGenerationTypes::Table)
                .and_where(
                    Expr::col(ProviderGenerationTypes::ProviderGenerationId)
                        .eq(generation.to_string()),
                )
                .to_owned(),
        )
        .await?;
    transaction
        .execute(
            Query::delete()
                .from_table(MaterializedOperations::Table)
                .and_where(
                    Expr::col(MaterializedOperations::ProviderGenerationId)
                        .eq(generation.to_string()),
                )
                .to_owned(),
        )
        .await?;
    transaction
        .execute(
            Query::delete()
                .from_table(ProviderGenerations::Table)
                .and_where(Expr::col(ProviderGenerations::Id).eq(generation.to_string()))
                .to_owned(),
        )
        .await?;
    Ok(true)
}

fn due(
    in_use: &(dyn Fn(ColumnRef) -> SimpleExpr + Sync),
    retired_by_unix_nanos: i64,
) -> SimpleExpr {
    Expr::col((
        ProviderGenerationRetirements::Table,
        ProviderGenerationRetirements::RetiredAtUnixNanos,
    ))
    .lte(retired_by_unix_nanos)
    .and(in_use(retirement_generation()).not())
}
