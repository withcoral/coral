//! The install row and the stored reports.

use chrono::NaiveDate;
use coral_db::Transaction;
use sea_query::{Expr, ExprTrait as _, Func, Iden, OnConflict, Order, Query};

use crate::Error;
use crate::SentReport;
use crate::day::{day_string, parse_day};

#[derive(Iden)]
enum UsageInstalls {
    Table,
    InstallId,
    Singleton,
    Sharing,
    NextReportDay,
    LastCheckDay,
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
pub(crate) struct Install {
    /// The random install ID every report carries.
    pub id: String,
    pub sharing: bool,
    /// The first day whose report is not made yet.
    pub next_report_day: NaiveDate,
    /// The last day a process that may send checked for due reports.
    pub last_check_day: Option<NaiveDate>,
    /// When the setting last changed, or the row was made. While sharing is
    /// on, a day's counts start then. Milestones do not: they are the
    /// earliest date in the history Coral keeps.
    pub sharing_since: i64,
    pub milestones: MilestoneDays,
}

/// The first day of each milestone.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MilestoneDays {
    pub first_successful_run: Option<NaiveDate>,
    pub first_saved_function: Option<NaiveDate>,
    pub function_reused: Option<NaiveDate>,
}

/// A report waiting to be sent.
pub(crate) struct Unsent {
    pub day: NaiveDate,
    pub json: String,
}

#[derive(sqlx::FromRow)]
struct InstallRow {
    install_id: String,
    sharing: i64,
    next_report_day: String,
    last_check_day: Option<String>,
    first_successful_run_day: Option<String>,
    first_saved_function_day: Option<String>,
    function_reused_day: Option<String>,
    changed_at_unix_nanos: i64,
}

impl TryFrom<InstallRow> for Install {
    type Error = Error;

    fn try_from(row: InstallRow) -> Result<Self, Error> {
        let day = |day: Option<String>| day.as_deref().map(parse_day).transpose();
        Ok(Self {
            id: row.install_id,
            sharing: row.sharing != 0,
            next_report_day: parse_day(&row.next_report_day)?,
            last_check_day: day(row.last_check_day)?,
            sharing_since: row.changed_at_unix_nanos,
            milestones: MilestoneDays {
                first_successful_run: day(row.first_successful_run_day)?,
                first_saved_function: day(row.first_saved_function_day)?,
                function_reused: day(row.function_reused_day)?,
            },
        })
    }
}

#[derive(sqlx::FromRow)]
struct ReportRow {
    report_day: String,
    body: String,
    sent_at_unix_nanos: Option<i64>,
}

/// Inserts the install row unless one exists, with reports from `first_day`.
/// Concurrent first starts keep the first row.
pub(crate) async fn insert_install(
    tx: &mut Transaction<'_>,
    install_id: &str,
    first_day: NaiveDate,
    now: i64,
) -> Result<(), Error> {
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
                day_string(first_day).into(),
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
    Ok(())
}

/// The install row, read in the caller's transaction.
pub(crate) async fn install(tx: &mut Transaction<'_>) -> Result<Option<Install>, Error> {
    let row: Option<InstallRow> = tx
        .fetch_optional(
            Query::select()
                .columns([
                    UsageInstalls::InstallId,
                    UsageInstalls::Sharing,
                    UsageInstalls::NextReportDay,
                    UsageInstalls::LastCheckDay,
                    UsageInstalls::FirstSuccessfulRunDay,
                    UsageInstalls::FirstSavedFunctionDay,
                    UsageInstalls::FunctionReusedDay,
                    UsageInstalls::ChangedAtUnixNanos,
                ])
                .from(UsageInstalls::Table)
                .to_owned(),
        )
        .await?;
    row.map(Install::try_from).transpose()
}

/// Writes the sharing setting. Turning it on starts reports at `from_day`;
/// turning it off drops every report not yet sent. Returns whether a row
/// changed.
pub(crate) async fn set_sharing(
    tx: &mut Transaction<'_>,
    sharing: bool,
    from_day: NaiveDate,
    now: i64,
) -> Result<bool, Error> {
    let mut update = Query::update();
    update
        .table(UsageInstalls::Table)
        .value(UsageInstalls::Sharing, i64::from(sharing))
        .value(UsageInstalls::ChangedAtUnixNanos, now)
        .and_where(Expr::col(UsageInstalls::Sharing).ne(i64::from(sharing)));
    if sharing {
        update.value(UsageInstalls::NextReportDay, day_string(from_day));
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

/// Moves the first unmade day from `from` to `to`, unless another writer
/// moved it first. Returns whether it moved.
pub(crate) async fn advance_day(
    tx: &mut Transaction<'_>,
    from: NaiveDate,
    to: NaiveDate,
) -> Result<bool, Error> {
    Ok(tx.execute_change(move_day(from, to)).await? > 0)
}

/// Claims `day`'s report for this writer: moves the first unmade day to
/// `next` while sharing is on, unless another writer moved it first. A report
/// made before sharing turned off then never lands after the off dropped
/// the others. Returns whether this writer may store the report.
pub(crate) async fn claim_day(
    tx: &mut Transaction<'_>,
    day: NaiveDate,
    next: NaiveDate,
) -> Result<bool, Error> {
    Ok(tx
        .execute_change(
            move_day(day, next)
                .and_where(Expr::col(UsageInstalls::Sharing).eq(1_i64))
                .to_owned(),
        )
        .await?
        > 0)
}

fn move_day(from: NaiveDate, to: NaiveDate) -> sea_query::UpdateStatement {
    Query::update()
        .table(UsageInstalls::Table)
        .value(UsageInstalls::NextReportDay, day_string(to))
        .and_where(Expr::col(UsageInstalls::NextReportDay).eq(day_string(from)))
        .to_owned()
}

/// Records that a process that may send checked on `day`. The day only
/// moves forward.
pub(crate) async fn mark_checked(tx: &mut Transaction<'_>, day: NaiveDate) -> Result<(), Error> {
    let day = day_string(day);
    tx.execute_change(
        Query::update()
            .table(UsageInstalls::Table)
            .value(UsageInstalls::LastCheckDay, day.clone())
            .and_where(
                Expr::col(UsageInstalls::LastCheckDay)
                    .is_null()
                    .or(Expr::col(UsageInstalls::LastCheckDay).lt(day)),
            )
            .to_owned(),
    )
    .await?;
    Ok(())
}

/// Keeps each milestone's first day once found. A kept day never changes.
pub(crate) async fn keep_milestones(
    tx: &mut Transaction<'_>,
    days: MilestoneDays,
) -> Result<(), Error> {
    let keep = |column: UsageInstalls, day: Option<NaiveDate>| {
        Func::coalesce([Expr::col(column), Expr::val(day.map(day_string))])
    };
    tx.execute_change(
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
    day: NaiveDate,
    body: &str,
) -> Result<(), Error> {
    tx.execute_change(
        Query::insert()
            .into_table(UsageReports::Table)
            .columns([UsageReports::ReportDay, UsageReports::Body])
            .values_panic([day_string(day).into(), body.into()])
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

/// The oldest report waiting to be sent.
pub(crate) async fn oldest_unsent(tx: &mut Transaction<'_>) -> Result<Option<Unsent>, Error> {
    let row: Option<ReportRow> = tx
        .fetch_optional(
            select_reports()
                .and_where(Expr::col(UsageReports::SentAtUnixNanos).is_null())
                .order_by(UsageReports::ReportDay, Order::Asc)
                .limit(1)
                .to_owned(),
        )
        .await?;
    row.map(|row| {
        Ok(Unsent {
            day: parse_day(&row.report_day)?,
            json: row.body,
        })
    })
    .transpose()
}

/// The last report the collector stored.
pub(crate) async fn last_sent(tx: &mut Transaction<'_>) -> Result<Option<SentReport>, Error> {
    let row: Option<ReportRow> = tx
        .fetch_optional(
            select_reports()
                .and_where(Expr::col(UsageReports::SentAtUnixNanos).is_not_null())
                .order_by(UsageReports::SentAtUnixNanos, Order::Desc)
                .limit(1)
                .to_owned(),
        )
        .await?;
    Ok(row.and_then(|row| {
        row.sent_at_unix_nanos.map(|sent_at_unix_nanos| SentReport {
            json: row.body,
            sent_at_unix_nanos,
        })
    }))
}

/// Records that the collector stored `day`'s report, and drops every other
/// sent report: only the last one is kept.
pub(crate) async fn mark_sent(
    tx: &mut Transaction<'_>,
    day: NaiveDate,
    now: i64,
) -> Result<(), Error> {
    let day = day_string(day);
    let marked = tx
        .execute_change(
            Query::update()
                .table(UsageReports::Table)
                .value(UsageReports::SentAtUnixNanos, now)
                .and_where(Expr::col(UsageReports::ReportDay).eq(day.clone()))
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
pub(crate) async fn delete_unsent(tx: &mut Transaction<'_>, day: NaiveDate) -> Result<(), Error> {
    tx.execute_change(
        Query::delete()
            .from_table(UsageReports::Table)
            .and_where(Expr::col(UsageReports::ReportDay).eq(day_string(day)))
            .and_where(Expr::col(UsageReports::SentAtUnixNanos).is_null())
            .to_owned(),
    )
    .await?;
    Ok(())
}
