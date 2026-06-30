#![cfg_attr(
    not(test),
    allow(
        dead_code,
        reason = "feedback repository lands before runtime wiring in the stacked PR sequence"
    )
)]

use sea_query::{Expr, ExprTrait, Order, Query};

use crate::state::db::schema::FeedbackReports;
use crate::state::db::{DbError, DbSession};
use crate::workspaces::WorkspaceName;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FeedbackReportRecord {
    pub(crate) id: String,
    pub(crate) created_at_unix_nanos: i64,
    pub(crate) trying_to_do: String,
    pub(crate) tried: String,
    pub(crate) stuck: String,
    pub(crate) publish_status: Option<String>,
    pub(crate) publish_error: Option<String>,
    pub(crate) published_at_unix_nanos: Option<i64>,
}

#[derive(Debug, sqlx::FromRow)]
struct FeedbackReportRow {
    id: String,
    created_at_unix_nanos: i64,
    trying_to_do: String,
    tried: String,
    stuck: String,
    publish_status: Option<String>,
    publish_error: Option<String>,
    published_at_unix_nanos: Option<i64>,
}

impl From<FeedbackReportRow> for FeedbackReportRecord {
    fn from(value: FeedbackReportRow) -> Self {
        Self {
            id: value.id,
            created_at_unix_nanos: value.created_at_unix_nanos,
            trying_to_do: value.trying_to_do,
            tried: value.tried,
            stuck: value.stuck,
            publish_status: value.publish_status,
            publish_error: value.publish_error,
            published_at_unix_nanos: value.published_at_unix_nanos,
        }
    }
}

pub(crate) struct FeedbackReportsRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> FeedbackReportsRepo<'a, S>
where
    S: DbSession,
{
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }

    pub(crate) async fn get(
        &mut self,
        workspace_name: &WorkspaceName,
        id: &str,
    ) -> Result<Option<FeedbackReportRecord>, DbError> {
        let statement = Query::select()
            .columns(record_columns())
            .from(FeedbackReports::Table)
            .and_where(Expr::col(FeedbackReports::WorkspaceId).eq(workspace_name.as_str()))
            .and_where(Expr::col(FeedbackReports::Id).eq(id))
            .to_owned();
        let row: Option<FeedbackReportRow> = self.session.fetch_optional(statement).await?;
        Ok(row.map(Into::into))
    }

    pub(crate) async fn list_workspace_reports(
        &mut self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<FeedbackReportRecord>, DbError> {
        let statement = Query::select()
            .columns(record_columns())
            .from(FeedbackReports::Table)
            .and_where(Expr::col(FeedbackReports::WorkspaceId).eq(workspace_name.as_str()))
            .order_by(FeedbackReports::CreatedAtUnixNanos, Order::Asc)
            .order_by(FeedbackReports::Id, Order::Asc)
            .to_owned();
        let rows: Vec<FeedbackReportRow> = self.session.fetch_all(statement).await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }
}

impl<S> FeedbackReportsRepo<'_, S>
where
    S: DbSession,
{
    pub(crate) async fn append(
        &mut self,
        workspace_name: &WorkspaceName,
        report: &FeedbackReportRecord,
    ) -> Result<(), DbError> {
        let statement = Query::insert()
            .into_table(FeedbackReports::Table)
            .columns([
                FeedbackReports::Id,
                FeedbackReports::WorkspaceId,
                FeedbackReports::CreatedAtUnixNanos,
                FeedbackReports::TryingToDo,
                FeedbackReports::Tried,
                FeedbackReports::Stuck,
                FeedbackReports::PublishStatus,
                FeedbackReports::PublishError,
                FeedbackReports::PublishedAtUnixNanos,
            ])
            .values_panic([
                Expr::val(report.id.clone()),
                Expr::val(workspace_name.as_str().to_string()),
                Expr::val(report.created_at_unix_nanos),
                Expr::val(report.trying_to_do.clone()),
                Expr::val(report.tried.clone()),
                Expr::val(report.stuck.clone()),
                Expr::val(report.publish_status.clone()),
                Expr::val(report.publish_error.clone()),
                Expr::val(report.published_at_unix_nanos),
            ])
            .to_owned();
        self.session.execute(statement).await
    }
}

fn record_columns() -> [FeedbackReports; 8] {
    [
        FeedbackReports::Id,
        FeedbackReports::CreatedAtUnixNanos,
        FeedbackReports::TryingToDo,
        FeedbackReports::Tried,
        FeedbackReports::Stuck,
        FeedbackReports::PublishStatus,
        FeedbackReports::PublishError,
        FeedbackReports::PublishedAtUnixNanos,
    ]
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::FeedbackReportRecord;
    use crate::state::AppStateLayout;
    use crate::state::db::session::DbRepos;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};
    use crate::workspaces::WorkspaceName;

    #[tokio::test]
    async fn feedback_report_repository_round_trips_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let db = open_sqlite(&layout).await;

        assert_feedback_report_repository_round_trip(&db).await;
        assert_feedback_report_repository_round_trip(&db).await;
    }

    #[tokio::test]
    async fn feedback_report_repository_round_trips_against_postgres() {
        let Some(url) = postgres_test_url() else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");

        assert_feedback_report_repository_round_trip(&db).await;
        assert_feedback_report_repository_round_trip(&db).await;
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

    async fn assert_feedback_report_repository_round_trip(db: &CoralDb) {
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let workspace = WorkspaceName::parse(&format!("feedback{suffix}")).expect("workspace");
        let other_workspace =
            WorkspaceName::parse(&format!("feedbackalt{suffix}")).expect("workspace");
        let report = FeedbackReportRecord {
            id: "feedback-1".to_string(),
            created_at_unix_nanos: 42,
            trying_to_do: "find stale state".to_string(),
            tried: "checked logs".to_string(),
            stuck: "no durable feedback row".to_string(),
            publish_status: Some("accepted".to_string()),
            publish_error: None,
            published_at_unix_nanos: Some(99),
        };
        let other = FeedbackReportRecord {
            id: "feedback-2".to_string(),
            created_at_unix_nanos: 43,
            publish_status: Some("failed".to_string()),
            publish_error: Some("network".to_string()),
            published_at_unix_nanos: None,
            ..report.clone()
        };
        let alternate_workspace_report = FeedbackReportRecord {
            trying_to_do: "same report id elsewhere".to_string(),
            ..report.clone()
        };

        let mut tx = db.begin().await.expect("begin tx");
        tx.workspaces()
            .ensure(workspace.as_str(), 1)
            .await
            .expect("ensure workspace");
        tx.workspaces()
            .ensure(other_workspace.as_str(), 2)
            .await
            .expect("ensure alternate workspace");
        tx.feedback_reports()
            .append(&workspace, &report)
            .await
            .expect("append report");
        tx.feedback_reports()
            .append(&workspace, &other)
            .await
            .expect("append second report");
        tx.feedback_reports()
            .append(&other_workspace, &alternate_workspace_report)
            .await
            .expect("same report id should be valid in another workspace");
        tx.commit().await.expect("commit feedback reports");

        let mut session = db;
        assert_eq!(
            session
                .feedback_reports()
                .get(&workspace, "feedback-1")
                .await
                .expect("get report"),
            Some(report.clone())
        );
        assert_eq!(
            session
                .feedback_reports()
                .list_workspace_reports(&workspace)
                .await
                .expect("list reports"),
            vec![report.clone(), other]
        );
        assert_eq!(
            session
                .feedback_reports()
                .get(&other_workspace, "feedback-1")
                .await
                .expect("get alternate workspace report"),
            Some(alternate_workspace_report)
        );
        assert_feedback_report_rejects_missing_workspace(db).await;
        assert_feedback_report_cascades_with_workspace(db, &workspace, &report).await;
    }

    async fn assert_feedback_report_rejects_missing_workspace(db: &CoralDb) {
        let workspace = WorkspaceName::parse(&format!("missing{}", uuid::Uuid::new_v4().simple()))
            .expect("workspace");
        let report = feedback_report("orphan");
        let mut tx = db.begin().await.expect("begin orphan tx");

        let error = tx
            .feedback_reports()
            .append(&workspace, &report)
            .await
            .expect_err("feedback reports must require an existing workspace");

        assert!(
            error.to_string().to_lowercase().contains("foreign key"),
            "unexpected error: {error}"
        );
        tx.rollback().await.expect("rollback orphan tx");
    }

    async fn assert_feedback_report_cascades_with_workspace(
        db: &CoralDb,
        workspace: &WorkspaceName,
        report: &FeedbackReportRecord,
    ) {
        let mut tx = db.begin().await.expect("begin cascade tx");
        tx.workspaces()
            .delete(workspace.as_str())
            .await
            .expect("remove workspace");
        tx.commit().await.expect("commit cascade tx");

        let mut session = db;
        assert_eq!(
            session
                .feedback_reports()
                .get(workspace, &report.id)
                .await
                .expect("get cascaded report"),
            None
        );
        assert!(
            session
                .feedback_reports()
                .list_workspace_reports(workspace)
                .await
                .expect("list cascaded reports")
                .is_empty()
        );
    }

    fn feedback_report(id: &str) -> FeedbackReportRecord {
        FeedbackReportRecord {
            id: id.to_string(),
            created_at_unix_nanos: 42,
            trying_to_do: "find stale state".to_string(),
            tried: "checked logs".to_string(),
            stuck: "no durable feedback row".to_string(),
            publish_status: Some("accepted".to_string()),
            publish_error: None,
            published_at_unix_nanos: Some(99),
        }
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "The Postgres repository harness is explicitly gated by this CI/test-only variable."
    )]
    fn postgres_test_url() -> Option<String> {
        std::env::var("CORAL_TEST_POSTGRES_URL")
            .ok()
            .filter(|value| !value.is_empty())
    }
}
