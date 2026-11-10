//! The Run Executor lease rows: `run_executors(id, kind, lease_until)`.

use coral_db::Database;
use sea_query::{Alias, Expr, ExprTrait as _, Iden, OnConflict, Query};

use crate::pull::Kind;

#[derive(Iden)]
enum RunExecutors {
    Table,
    Id,
    Kind,
    LeaseUntil,
}

/// Writes Run Executor `id`'s row with its lease until `lease_until`, in Unix
/// nanoseconds. A lease never moves back: two pull streams that claim one id
/// both renew it.
pub(crate) async fn renew(
    database: &Database,
    id: &str,
    kind: Kind,
    lease_until: i64,
) -> Result<(), coral_db::Error> {
    let upsert = Query::insert()
        .into_table(RunExecutors::Table)
        .columns([
            RunExecutors::Id,
            RunExecutors::Kind,
            RunExecutors::LeaseUntil,
        ])
        .values_panic([
            Expr::val(id),
            Expr::val(match kind {
                Kind::Local => "local",
                Kind::Dialed => "dialed",
            }),
            Expr::val(lease_until),
        ])
        .on_conflict(
            OnConflict::column(RunExecutors::Id)
                .update_columns([RunExecutors::Kind, RunExecutors::LeaseUntil])
                .action_and_where(
                    Expr::col((Alias::new("excluded"), RunExecutors::LeaseUntil))
                        .gt(Expr::col((RunExecutors::Table, RunExecutors::LeaseUntil))),
                )
                .to_owned(),
        )
        .to_owned();
    // A lease write, which wakes no observer.
    database.execute(upsert).await.map(drop)
}
