use sea_query::{Expr, ExprTrait, Query};

use crate::state::db::schema::Workspaces;
use crate::state::db::{DbError, DbSession};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WorkspaceRecord {
    pub(crate) id: String,
    pub(crate) created_at_unix_nanos: i64,
}

#[derive(Debug, sqlx::FromRow)]
struct WorkspaceRow {
    id: String,
    created_at_unix_nanos: i64,
}

impl From<WorkspaceRow> for WorkspaceRecord {
    fn from(value: WorkspaceRow) -> Self {
        Self {
            id: value.id,
            created_at_unix_nanos: value.created_at_unix_nanos,
        }
    }
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
        if self.get(id).await?.is_some() {
            return Ok(());
        }

        let statement = Query::insert()
            .into_table(Workspaces::Table)
            .columns([Workspaces::Id, Workspaces::CreatedAtUnixNanos])
            .values_panic([Expr::val(id.to_string()), Expr::val(created_at_unix_nanos)])
            .to_owned();
        self.session.execute(statement).await
    }

    pub(crate) async fn get(&mut self, id: &str) -> Result<Option<WorkspaceRecord>, DbError> {
        let statement = Query::select()
            .columns([Workspaces::Id, Workspaces::CreatedAtUnixNanos])
            .from(Workspaces::Table)
            .and_where(Expr::col(Workspaces::Id).eq(id))
            .to_owned();
        let row: Option<WorkspaceRow> = self.session.fetch_optional(statement).await?;
        Ok(row.map(Into::into))
    }

    pub(crate) async fn list(&mut self) -> Result<Vec<WorkspaceRecord>, DbError> {
        let statement = Query::select()
            .columns([Workspaces::Id, Workspaces::CreatedAtUnixNanos])
            .from(Workspaces::Table)
            .order_by(Workspaces::Id, sea_query::Order::Asc)
            .to_owned();
        let rows: Vec<WorkspaceRow> = self.session.fetch_all(statement).await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::WorkspaceRecord;
    use crate::bootstrap;
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
        let Some(url) = bootstrap::env_var("CORAL_TEST_POSTGRES_URL") else {
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

        let mut tx = db.begin().await.expect("begin tx");
        tx.workspaces()
            .ensure("default", 42)
            .await
            .expect("ensure workspace");
        tx.commit().await.expect("commit tx");

        let mut session = db;
        let workspaces = session.workspaces().list().await.expect("list workspaces");

        assert_eq!(
            workspaces,
            vec![WorkspaceRecord {
                id: "default".to_string(),
                created_at_unix_nanos: 42,
            }]
        );
    }
}
