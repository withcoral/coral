//! The install row and the stored reports.

use coral_db::{Database, Transaction};
use sea_query::{Expr, ExprTrait as _, Func, Iden, OnConflict, Order, Query};

use crate::Error;

#[derive(Iden)]
enum UsageInstalls {
    Table,
    InstallId,
    Singleton,
    Sharing,
    NextReportDay,
    FirstSuccessfulRunDay,
    FirstSavedFunctionDay,
    FunctionReusedDay,
    ChangedAtUnixNanos,
}

#[derive(Iden)]
enum UsageReports {
    Table,
    ReportDay,
    Body,
    SentAtUnixNanos,
}

/// The deployment's install row.
#[derive(sqlx::FromRow)]
pub(crate) struct InstallRow {
    pub install_id: String,
    pub sharing: i64,
    pub next_report_day: String,
    pub first_successful_run_day: Option<String>,
    pub first_saved_function_day: Option<String>,
    pub function_reused_day: Option<String>,
}

/// A stored report.
#[derive(sqlx::FromRow)]
pub(crate) struct ReportRow {
    pub report_day: String,
    pub body: String,
    pub sent_at_unix_nanos: Option<i64>,
}

/// Inserts the install row unless one exists. Concurrent first starts keep
/// the first row.
pub(crate) async fn insert_install(
    database: &Database,
    install_id: &str,
    first_day: &str,
    now: i64,
) -> Result<(), Error> {
    let mut tx = database.begin().await?;
    tx.execute_change(
        Query::insert()
            .into_table(UsageInstalls::Table)
            .columns([
                UsageInstalls::InstallId,
                UsageInstalls::Singleton,
                UsageInstalls::Sharing,
                UsageInstalls::NextReportDay,
                UsageInstalls::ChangedAtUnixNanos,
            ])
            .values_panic([
                install_id.into(),
                1_i64.into(),
                1_i64.into(),
                first_day.into(),
                now.into(),
            ])
            .on_conflict(
                OnConflict::column(UsageInstalls::Singleton)
                    .do_nothing()
                    .to_owned(),
            )
            .to_owned(),
    )
    .await?;
    tx.commit().await?;
    Ok(())
}

fn select_install() -> sea_query::SelectStatement {
    Query::select()
        .columns([
            UsageInstalls::InstallId,
            UsageInstalls::Sharing,
            UsageInstalls::NextReportDay,
            UsageInstalls::FirstSuccessfulRunDay,
            UsageInstalls::FirstSavedFunctionDay,
            UsageInstalls::FunctionReusedDay,
        ])
        .from(UsageInstalls::Table)
        .to_owned()
}

/// The install row, read in the caller's transaction.
pub(crate) async fn install(tx: &mut Transaction<'_>) -> Result<Option<InstallRow>, Error> {
    Ok(tx.fetch_optional(select_install()).await?)
}

/// Writes the sharing setting. Turning it on starts reports at `from_day`;
/// turning it off drops every report not yet sent. Returns whether a row
/// changed.
pub(crate) async fn set_sharing(
    tx: &mut Transaction<'_>,
    sharing: bool,
    from_day: &str,
    now: i64,
) -> Result<bool, Error> {
    let mut update = Query::update();
    update
        .table(UsageInstalls::Table)
        .value(UsageInstalls::Sharing, i64::from(sharing))
        .value(UsageInstalls::ChangedAtUnixNanos, now)
        .and_where(Expr::col(UsageInstalls::Sharing).ne(i64::from(sharing)));
    if sharing {
        update.value(UsageInstalls::NextReportDay, from_day);
    }
    let changed = tx.execute_change(update).await? > 0;
    if changed && !sharing {
        tx.execute_change(
            Query::delete()
                .from_table(UsageReports::Table)
                .and_where(Expr::col(UsageReports::SentAtUnixNanos).is_null())
                .to_owned(),
        )
        .await?;
    }
    Ok(changed)
}

/// Moves the first unmade day from `from` to `to` while sharing is on,
/// unless another writer moved it first. Returns whether it moved.
pub(crate) async fn advance_day(
    tx: &mut Transaction<'_>,
    from: &str,
    to: &str,
) -> Result<bool, Error> {
    Ok(tx
        .execute(
            Query::update()
                .table(UsageInstalls::Table)
                .value(UsageInstalls::NextReportDay, to)
                .and_where(Expr::col(UsageInstalls::NextReportDay).eq(from))
                .to_owned(),
        )
        .await?
        > 0)
}

/// The first day of each milestone, as one report found them.
pub(crate) struct MilestoneDays<'a> {
    pub first_successful_run: Option<&'a str>,
    pub first_saved_function: Option<&'a str>,
    pub function_reused: Option<&'a str>,
}

/// Keeps each milestone's first day once found. A kept day never changes.
pub(crate) async fn keep_milestones(
    tx: &mut Transaction<'_>,
    days: &MilestoneDays<'_>,
) -> Result<(), Error> {
    let keep = |column: UsageInstalls, day: Option<&str>| {
        Func::coalesce([Expr::col(column), Expr::val(day.map(str::to_owned))])
    };
    tx.execute(
        Query::update()
            .table(UsageInstalls::Table)
            .value(
                UsageInstalls::FirstSuccessfulRunDay,
                keep(
                    UsageInstalls::FirstSuccessfulRunDay,
                    days.first_successful_run,
                ),
            )
            .value(
                UsageInstalls::FirstSavedFunctionDay,
                keep(
                    UsageInstalls::FirstSavedFunctionDay,
                    days.first_saved_function,
                ),
            )
            .value(
                UsageInstalls::FunctionReusedDay,
                keep(UsageInstalls::FunctionReusedDay, days.function_reused),
            )
            .to_owned(),
    )
    .await?;
    Ok(())
}

/// Stores one day's report to send, unless that day has one.
pub(crate) async fn insert_report(
    tx: &mut Transaction<'_>,
    day: &str,
    body: &str,
) -> Result<(), Error> {
    tx.execute_change(
        Query::insert()
            .into_table(UsageReports::Table)
            .columns([UsageReports::ReportDay, UsageReports::Body])
            .values_panic([day.into(), body.into()])
            .on_conflict(
                OnConflict::column(UsageReports::ReportDay)
                    .do_nothing()
                    .to_owned(),
            )
            .to_owned(),
    )
    .await?;
    Ok(())
}

fn select_reports() -> sea_query::SelectStatement {
    Query::select()
        .columns([
            UsageReports::ReportDay,
            UsageReports::Body,
            UsageReports::SentAtUnixNanos,
        ])
        .from(UsageReports::Table)
        .to_owned()
}

/// The reports waiting to be sent, oldest day first.
pub(crate) async fn unsent_reports(tx: &mut Transaction<'_>) -> Result<Vec<ReportRow>, Error> {
    Ok(tx
        .fetch_all(
            select_reports()
                .and_where(Expr::col(UsageReports::SentAtUnixNanos).is_null())
                .order_by(UsageReports::ReportDay, Order::Asc)
                .to_owned(),
        )
        .await?)
}

/// The last report the collector stored.
pub(crate) async fn last_sent(tx: &mut Transaction<'_>) -> Result<Option<ReportRow>, Error> {
    Ok(tx
        .fetch_optional(
            select_reports()
                .and_where(Expr::col(UsageReports::SentAtUnixNanos).is_not_null())
                .order_by(UsageReports::SentAtUnixNanos, Order::Desc)
                .limit(1)
                .to_owned(),
        )
        .await?)
}

/// Records that the collector stored `day`'s report, and drops every other
/// sent report: only the last one is kept.
pub(crate) async fn mark_sent(tx: &mut Transaction<'_>, day: &str, now: i64) -> Result<(), Error> {
    let marked = tx
        .execute_change(
            Query::update()
                .table(UsageReports::Table)
                .value(UsageReports::SentAtUnixNanos, now)
                .and_where(Expr::col(UsageReports::ReportDay).eq(day))
                .and_where(Expr::col(UsageReports::SentAtUnixNanos).is_null())
                .to_owned(),
        )
        .await?;
    if marked > 0 {
        tx.execute_change(
            Query::delete()
                .from_table(UsageReports::Table)
                .and_where(Expr::col(UsageReports::ReportDay).ne(day))
                .and_where(Expr::col(UsageReports::SentAtUnixNanos).is_not_null())
                .to_owned(),
        )
        .await?;
    }
    Ok(())
}

/// Drops a report the collector will never accept.
pub(crate) async fn delete_unsent(tx: &mut Transaction<'_>, day: &str) -> Result<(), Error> {
    tx.execute_change(
        Query::delete()
            .from_table(UsageReports::Table)
            .and_where(Expr::col(UsageReports::ReportDay).eq(day))
            .and_where(Expr::col(UsageReports::SentAtUnixNanos).is_null())
            .to_owned(),
    )
    .await?;
    Ok(())
}

#[derive(sqlx::FromRow)]
struct FirstAt {
    value: Option<i64>,
}

/// When the first Workspace still kept was created.
pub(crate) async fn first_workspace_created(database: &Database) -> Result<Option<i64>, Error> {
    let mut tx = database.begin_read_snapshot().await?;
    let row: Option<FirstAt> = tx
        .fetch_optional(
            Query::select()
                .expr_as(
                    Func::min(Expr::col(sea_query::Alias::new("created_at_unix_nanos"))),
                    sea_query::Alias::new("value"),
                )
                .from(sea_query::Alias::new("workspace_records"))
                .to_owned(),
        )
        .await?;
    tx.commit().await?;
    Ok(row.and_then(|row| row.value))
}
