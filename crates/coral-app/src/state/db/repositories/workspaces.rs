use sea_query::{Expr, ExprTrait, OnConflict, Query};

use crate::state::db::schema::Workspaces;
use crate::state::db::{DbError, DbSession};

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(crate) struct WorkspaceRecord {
    pub(crate) id: String,
    pub(crate) created_at_unix_nanos: i64,
}

pub(crate) struct WorkspacesRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> WorkspacesRepo<'a, S>
where
    S: DbSession,
{
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }

    pub(crate) async fn ensure(
        &mut self,
        id: &str,
        created_at_unix_nanos: i64,
    ) -> Result<(), DbError> {
        let statement = Query::insert()
            .into_table(Workspaces::Table)
            .columns([Workspaces::Id, Workspaces::CreatedAtUnixNanos])
            .values_panic([Expr::val(id.to_string()), Expr::val(created_at_unix_nanos)])
            .on_conflict(OnConflict::column(Workspaces::Id).do_nothing().to_owned())
            .to_owned();
        self.session.execute(statement).await
    }

    pub(crate) async fn get(&mut self, id: &str) -> Result<Option<WorkspaceRecord>, DbError> {
        let statement = Query::select()
            .columns([Workspaces::Id, Workspaces::CreatedAtUnixNanos])
            .from(Workspaces::Table)
            .and_where(Expr::col(Workspaces::Id).eq(id))
            .to_owned();
        self.session.fetch_optional(statement).await
    }

    pub(crate) async fn list(&mut self) -> Result<Vec<WorkspaceRecord>, DbError> {
        let statement = Query::select()
            .columns([Workspaces::Id, Workspaces::CreatedAtUnixNanos])
            .from(Workspaces::Table)
            .order_by(Workspaces::Id, sea_query::Order::Asc)
            .to_owned();
        self.session.fetch_all(statement).await
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use tempfile::tempdir;

    use super::WorkspaceRecord;
    use crate::state::AppStateLayout;
    use crate::state::db::session::DbRepos;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};

    #[tokio::test]
    async fn workspace_repository_round_trips_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let db = open_sqlite(&layout).await;

        assert_workspace_repository_round_trip(&db).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared repository harness against Postgres"]
    async fn workspace_repository_round_trips_against_postgres() {
        let Some(url) = postgres_test_url() else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");

        assert_workspace_repository_round_trip(&db).await;
    }

    async fn open_sqlite(layout: &AppStateLayout) -> CoralDb {
        let config = DatabaseConfig::load(layout).expect("db config");
        let DatabaseConfig::Sqlite { path } = config else {
            panic!("default test config should be sqlite");
        };
        assert_eq!(path, layout.database_file());
        assert!(!path.exists());

        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
            .await
            .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");
        db
    }

    async fn assert_workspace_repository_round_trip(db: &CoralDb) {
        db.ping().await.expect("ping database");

        let workspace_id = unique_workspace_id();
        let missing_workspace_id = format!("{workspace_id}_missing");
        let rolled_back_workspace_id = format!("{workspace_id}_rolled_back");
        let mut tx = db.begin().await.expect("begin tx");
        tx.workspaces()
            .ensure(&workspace_id, 42)
            .await
            .expect("ensure workspace");
        tx.workspaces()
            .ensure(&workspace_id, 99)
            .await
            .expect("ensure existing workspace");
        tx.commit().await.expect("commit tx");

        let expected = WorkspaceRecord {
            id: workspace_id.clone(),
            created_at_unix_nanos: 42,
        };
        let mut session = db;
        assert_eq!(
            session
                .workspaces()
                .get(&workspace_id)
                .await
                .expect("get workspace"),
            Some(expected.clone())
        );
        assert_eq!(
            session
                .workspaces()
                .get(&missing_workspace_id)
                .await
                .expect("get missing workspace"),
            None
        );

        let workspaces = session.workspaces().list().await.expect("list workspaces");
        assert!(
            workspaces.contains(&expected),
            "workspace list did not contain expected record: {workspaces:?}"
        );

        let mut tx = db.begin().await.expect("begin rollback tx");
        tx.workspaces()
            .ensure(&rolled_back_workspace_id, 77)
            .await
            .expect("ensure rolled-back workspace");
        tx.rollback().await.expect("rollback tx");
        assert_eq!(
            session
                .workspaces()
                .get(&rolled_back_workspace_id)
                .await
                .expect("get rolled-back workspace"),
            None
        );
    }

    fn unique_workspace_id() -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos();
        format!("workspace_{}_{}", std::process::id(), nanos)
    }

    #[expect(
        clippy::disallowed_methods,
        reason = "The ignored Postgres repository harness is explicitly gated by this CI/test-only variable."
    )]
    fn postgres_test_url() -> Option<String> {
        std::env::var("CORAL_TEST_POSTGRES_URL")
            .ok()
            .filter(|value| !value.is_empty())
    }
}
