use sea_query::{Alias, Expr, ExprTrait, OnConflict, Order, Query};

use crate::state::db::schema::TraceSummaries;
use crate::state::db::{DbError, DbSession, DbWriteSession};
use crate::telemetry::{StoredTraceStatus, TraceSummaryRecord};
use crate::workspaces::WorkspaceName;

#[derive(Debug, sqlx::FromRow)]
struct TraceSummaryRow {
    trace_id: String,
    workspace_id: String,
    root_span_id: String,
    name: String,
    query: String,
    status: String,
    start_time_unix_nanos: i64,
    end_time_unix_nanos: i64,
    duration_nanos: i64,
    span_count: i64,
    row_count: Option<i64>,
}

impl TryFrom<TraceSummaryRow> for TraceSummaryRecord {
    type Error = DbError;

    fn try_from(value: TraceSummaryRow) -> Result<Self, Self::Error> {
        validate_trace_identity(&value)?;
        let span_count = u32::try_from(value.span_count).map_err(|_error| {
            DbError::InvalidData(format!(
                "trace summary '{}' has invalid span_count {}",
                value.trace_id, value.span_count
            ))
        })?;
        let row_count = value
            .row_count
            .map(u64::try_from)
            .transpose()
            .map_err(|_error| {
                DbError::InvalidData(format!(
                    "trace summary '{}' has invalid row_count",
                    value.trace_id
                ))
            })?;
        if value.start_time_unix_nanos < 0
            || value.end_time_unix_nanos < 0
            || value.duration_nanos < 0
        {
            return Err(DbError::InvalidData(format!(
                "trace summary '{}' has negative timestamp or duration",
                value.trace_id
            )));
        }

        Ok(Self {
            trace_id: value.trace_id,
            workspace_id: Some(value.workspace_id),
            root_span_id: value.root_span_id,
            name: value.name,
            query: value.query,
            status: status_from_db(&value.status)?,
            start_time_unix_nanos: value.start_time_unix_nanos,
            end_time_unix_nanos: value.end_time_unix_nanos,
            duration_nanos: value.duration_nanos,
            span_count,
            row_count: row_count.unwrap_or_default(),
            row_count_recorded: row_count.is_some(),
        })
    }
}

pub(crate) struct TraceSummariesRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> TraceSummariesRepo<'a, S>
where
    S: DbSession,
{
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }

    pub(crate) async fn get(
        &mut self,
        trace_id: &str,
    ) -> Result<Option<TraceSummaryRecord>, DbError> {
        let statement = Query::select()
            .columns(record_columns())
            .from(TraceSummaries::Table)
            .and_where(Expr::col(TraceSummaries::TraceId).eq(trace_id))
            .to_owned();
        self.session
            .fetch_optional::<TraceSummaryRow>(statement)
            .await?
            .map(TryInto::try_into)
            .transpose()
    }

    pub(crate) async fn list(
        &mut self,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<TraceSummaryRecord>, DbError> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let statement = Query::select()
            .columns(record_columns())
            .from(TraceSummaries::Table)
            .order_by(TraceSummaries::EndTimeUnixNanos, Order::Desc)
            .order_by(TraceSummaries::TraceId, Order::Asc)
            .limit(limit_to_u64(limit)?)
            .offset(limit_to_u64(offset)?)
            .to_owned();
        let rows: Vec<TraceSummaryRow> = self.session.fetch_all(statement).await?;
        rows.into_iter().map(TryInto::try_into).collect()
    }
}

impl<S> TraceSummariesRepo<'_, S>
where
    S: DbWriteSession,
{
    pub(crate) async fn upsert(&mut self, summary: &TraceSummaryRecord) -> Result<(), DbError> {
        let row_count = row_count_to_db(summary)?;
        let workspace_id = summary_workspace_id(summary)?;
        let span_count = i64::from(summary.span_count);
        let statement = Query::insert()
            .into_table(TraceSummaries::Table)
            .columns([
                TraceSummaries::TraceId,
                TraceSummaries::WorkspaceId,
                TraceSummaries::RootSpanId,
                TraceSummaries::Name,
                TraceSummaries::Query,
                TraceSummaries::Status,
                TraceSummaries::StartTimeUnixNanos,
                TraceSummaries::EndTimeUnixNanos,
                TraceSummaries::DurationNanos,
                TraceSummaries::SpanCount,
                TraceSummaries::RowCount,
            ])
            .values_panic([
                Expr::val(summary.trace_id.clone()),
                Expr::val(workspace_id),
                Expr::val(summary.root_span_id.clone()),
                Expr::val(summary.name.clone()),
                Expr::val(summary.query.clone()),
                Expr::val(status_to_db(summary.status)),
                Expr::val(summary.start_time_unix_nanos),
                Expr::val(summary.end_time_unix_nanos),
                Expr::val(summary.duration_nanos),
                Expr::val(span_count),
                Expr::val(row_count),
            ])
            .on_conflict(
                OnConflict::columns([TraceSummaries::WorkspaceId, TraceSummaries::TraceId])
                    .update_columns([
                        TraceSummaries::RootSpanId,
                        TraceSummaries::Name,
                        TraceSummaries::Query,
                        TraceSummaries::Status,
                        TraceSummaries::StartTimeUnixNanos,
                        TraceSummaries::EndTimeUnixNanos,
                        TraceSummaries::DurationNanos,
                        TraceSummaries::SpanCount,
                        TraceSummaries::RowCount,
                    ])
                    .action_and_where(
                        Expr::col((TraceSummaries::Table, TraceSummaries::EndTimeUnixNanos)).lte(
                            Expr::col((Alias::new("excluded"), TraceSummaries::EndTimeUnixNanos)),
                        ),
                    )
                    .to_owned(),
            )
            .to_owned();
        self.session.execute(statement).await
    }

    pub(crate) async fn delete(
        &mut self,
        workspace_id: &str,
        trace_id: &str,
    ) -> Result<(), DbError> {
        let statement = Query::delete()
            .from_table(TraceSummaries::Table)
            .and_where(Expr::col(TraceSummaries::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(TraceSummaries::TraceId).eq(trace_id))
            .to_owned();
        self.session.execute_delete(statement).await
    }
}

fn record_columns() -> [TraceSummaries; 11] {
    [
        TraceSummaries::TraceId,
        TraceSummaries::WorkspaceId,
        TraceSummaries::RootSpanId,
        TraceSummaries::Name,
        TraceSummaries::Query,
        TraceSummaries::Status,
        TraceSummaries::StartTimeUnixNanos,
        TraceSummaries::EndTimeUnixNanos,
        TraceSummaries::DurationNanos,
        TraceSummaries::SpanCount,
        TraceSummaries::RowCount,
    ]
}

fn validate_trace_identity(row: &TraceSummaryRow) -> Result<(), DbError> {
    if row.trace_id.trim().is_empty() || row.root_span_id.trim().is_empty() {
        return Err(DbError::InvalidData(
            "trace summary has empty trace identity".to_string(),
        ));
    }
    WorkspaceName::parse(&row.workspace_id).map_err(|error| {
        DbError::InvalidData(format!(
            "trace summary '{}' has invalid workspace_id: {error}",
            row.trace_id
        ))
    })?;
    Ok(())
}

fn summary_workspace_id(summary: &TraceSummaryRecord) -> Result<String, DbError> {
    if summary.trace_id.trim().is_empty() || summary.root_span_id.trim().is_empty() {
        return Err(DbError::InvalidData(
            "trace summary has empty trace identity".to_string(),
        ));
    }
    let workspace_id = summary.workspace_id.as_deref().ok_or_else(|| {
        DbError::InvalidData(format!(
            "trace summary '{}' is missing workspace_id",
            summary.trace_id
        ))
    })?;
    WorkspaceName::parse(workspace_id).map_err(|error| {
        DbError::InvalidData(format!(
            "trace summary '{}' has invalid workspace_id: {error}",
            summary.trace_id
        ))
    })?;
    Ok(workspace_id.to_string())
}

fn row_count_to_db(summary: &TraceSummaryRecord) -> Result<Option<i64>, DbError> {
    if summary.row_count_recorded {
        return i64::try_from(summary.row_count)
            .map(Some)
            .map_err(|_error| {
                DbError::InvalidData(format!(
                    "trace summary '{}' row_count exceeds database range",
                    summary.trace_id
                ))
            });
    }
    Ok(None)
}

fn status_to_db(status: StoredTraceStatus) -> String {
    match status {
        StoredTraceStatus::Unspecified => "unspecified",
        StoredTraceStatus::Ok => "ok",
        StoredTraceStatus::Error => "error",
    }
    .to_string()
}

fn status_from_db(status: &str) -> Result<StoredTraceStatus, DbError> {
    match status {
        "unspecified" => Ok(StoredTraceStatus::Unspecified),
        "ok" => Ok(StoredTraceStatus::Ok),
        "error" => Ok(StoredTraceStatus::Error),
        other => Err(DbError::InvalidData(format!(
            "database contains invalid trace status '{other}'"
        ))),
    }
}

fn limit_to_u64(value: usize) -> Result<u64, DbError> {
    u64::try_from(value).map_err(|_error| {
        DbError::InvalidData(format!(
            "trace summary page value {value} exceeds database range"
        ))
    })
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::bootstrap;
    use crate::state::AppStateLayout;
    use crate::state::db::session::DbRepos;
    use crate::state::db::{CoralDb, DatabaseConfig, DbError, ResolvedDatabaseConfig};
    use crate::telemetry::{StoredTraceStatus, TraceSummaryRecord};

    #[tokio::test]
    async fn trace_summary_repository_round_trips_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let db = open_sqlite(&layout).await;

        assert_trace_summary_repository_round_trip(&db).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared repository harness against Postgres"]
    async fn trace_summary_repository_round_trips_against_postgres() {
        let Some(url) = bootstrap::env_var("CORAL_TEST_POSTGRES_URL") else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");

        assert_trace_summary_repository_round_trip(&db).await;
    }

    async fn open_sqlite(layout: &AppStateLayout) -> CoralDb {
        let config = DatabaseConfig::load(layout).expect("db config");
        let DatabaseConfig::Sqlite { path } = config else {
            panic!("default test config should be sqlite");
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");
        db
    }

    async fn assert_trace_summary_repository_round_trip(db: &CoralDb) {
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let first_workspace = format!("trace_workspace_{suffix}");
        let second_workspace = format!("trace_other_{suffix}");
        let first = summary(
            &format!("trace_a_{suffix}"),
            &first_workspace,
            20,
            StoredTraceStatus::Ok,
            true,
        );
        let second = summary(
            &format!("trace_b_{suffix}"),
            &second_workspace,
            40,
            StoredTraceStatus::Error,
            false,
        );

        insert_initial_summaries(db, &first, &second).await;
        assert_initial_summaries(db, &first, &second).await;

        let updated = update_first_summary(first);
        assert_update_and_failed_replacement(db, &updated).await;
        cleanup_workspace(
            db,
            updated.workspace_id.as_deref().expect("first workspace"),
        )
        .await;
        cleanup_workspace(
            db,
            second.workspace_id.as_deref().expect("second workspace"),
        )
        .await;
    }

    fn summary(
        trace_id: &str,
        workspace_id: &str,
        end_time_unix_nanos: i64,
        status: StoredTraceStatus,
        row_count_recorded: bool,
    ) -> TraceSummaryRecord {
        TraceSummaryRecord {
            trace_id: trace_id.to_string(),
            workspace_id: Some(workspace_id.to_string()),
            root_span_id: "root".to_string(),
            name: "coral.query".to_string(),
            query: "SELECT 1".to_string(),
            status,
            start_time_unix_nanos: end_time_unix_nanos - 10,
            end_time_unix_nanos,
            duration_nanos: 10,
            span_count: 1,
            row_count: 0,
            row_count_recorded,
        }
    }

    fn update_first_summary(first: TraceSummaryRecord) -> TraceSummaryRecord {
        TraceSummaryRecord {
            query: "SELECT updated".to_string(),
            row_count: 10,
            ..first
        }
    }

    async fn insert_initial_summaries(
        db: &CoralDb,
        first: &TraceSummaryRecord,
        second: &TraceSummaryRecord,
    ) {
        let mut tx = db.begin().await.expect("begin tx");
        ensure_summary_workspace(&mut tx, first).await;
        ensure_summary_workspace(&mut tx, second).await;
        tx.trace_summaries()
            .upsert(first)
            .await
            .expect("upsert first");
        tx.trace_summaries()
            .upsert(second)
            .await
            .expect("upsert second");
        tx.commit().await.expect("commit trace summaries");
    }

    async fn assert_initial_summaries(
        db: &CoralDb,
        first: &TraceSummaryRecord,
        second: &TraceSummaryRecord,
    ) {
        let mut session = db;
        assert_eq!(
            session
                .trace_summaries()
                .get(&first.trace_id)
                .await
                .expect("get first"),
            Some(first.clone())
        );
        let summaries = fixture_summaries(&mut session, first, second).await;
        assert_eq!(summaries, vec![second.clone(), first.clone()]);
    }

    async fn assert_update_and_failed_replacement(db: &CoralDb, updated: &TraceSummaryRecord) {
        let mut tx = db.begin().await.expect("begin update tx");
        tx.trace_summaries()
            .upsert(updated)
            .await
            .expect("upsert updated");
        let invalid = TraceSummaryRecord {
            row_count: i64::MAX as u64 + 1,
            ..updated.clone()
        };
        assert!(
            matches!(
                tx.trace_summaries().upsert(&invalid).await,
                Err(DbError::InvalidData(_))
            ),
            "invalid replacement should fail before deleting the existing summary"
        );
        tx.commit().await.expect("commit trace summary update");

        let mut session = db;
        assert_eq!(
            session
                .trace_summaries()
                .get(&updated.trace_id)
                .await
                .expect("get updated summary"),
            Some(updated.clone())
        );
    }

    async fn ensure_summary_workspace<S>(session: &mut S, summary: &TraceSummaryRecord)
    where
        S: crate::state::db::DbSession,
    {
        session
            .workspaces()
            .ensure(
                summary.workspace_id.as_deref().expect("summary workspace"),
                1,
            )
            .await
            .expect("ensure summary workspace");
    }

    async fn fixture_summaries<S>(
        session: &mut S,
        first: &TraceSummaryRecord,
        second: &TraceSummaryRecord,
    ) -> Vec<TraceSummaryRecord>
    where
        S: crate::state::db::DbSession,
    {
        session
            .trace_summaries()
            .list(1_000, 0)
            .await
            .expect("list summaries")
            .into_iter()
            .filter(|summary| is_fixture_summary(summary, first, second))
            .collect()
    }

    fn is_fixture_summary(
        summary: &TraceSummaryRecord,
        first: &TraceSummaryRecord,
        second: &TraceSummaryRecord,
    ) -> bool {
        summary.workspace_id == first.workspace_id || summary.workspace_id == second.workspace_id
    }

    async fn cleanup_workspace(db: &CoralDb, workspace_id: &str) {
        let mut tx = db.begin().await.expect("begin cleanup tx");
        tx.workspaces()
            .remove(workspace_id)
            .await
            .expect("delete test workspace");
        tx.commit().await.expect("commit cleanup tx");
    }
}
