use sea_query::{Expr, ExprTrait, OnConflict, Order, Query, SelectStatement};

use super::workspace_members::{non_local_owner_bearing_workspaces, owner_bearing_workspaces};
use crate::state::db::schema::Workspaces;
use crate::state::db::session::DbSession;
use crate::state::db::{CoralTx, DbError};

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(crate) struct WorkspaceRecord {
    pub(crate) id: String,
    pub(crate) created_at_unix_nanos: i64,
}

/// The workspaces no authenticated caller can *own*, split by why.
///
/// The two categories are different operator situations — one needs an owner
/// appointed, the other needs ownership moved off the local principal — so
/// they are reported apart rather than merged into one list.
///
/// Deliberately about owners, not reachability. The classifying query looks
/// only at owner rows, so a workspace can appear here while a Member still
/// reads and queries it — a local host can grant membership to a directory
/// user before the install is switched to shared mode. What both categories
/// prove is that nobody left can *manage* the workspace, which is what makes
/// operator action necessary.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct InaccessibleWorkspaces {
    /// Ids with no owner at all. These are concealed from every caller,
    /// members included, because concealment treats ownerless exactly as it
    /// treats nonexistent.
    pub(crate) without_owner: Vec<String>,
    /// Ids whose only owner is the synthetic local principal. Storage-wise
    /// these are validly owned and preserve the owner floor, but no
    /// authenticated caller can manage them until ownership is transferred.
    /// Any member they already have keeps its read access.
    pub(crate) local_owner_only: Vec<String>,
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

    pub(crate) async fn create(
        &mut self,
        id: &str,
        created_at_unix_nanos: i64,
    ) -> Result<(), DbError> {
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

    pub(crate) async fn delete(&mut self, id: &str) -> Result<bool, DbError> {
        let statement = Query::delete()
            .from_table(Workspaces::Table)
            .and_where(Expr::col(Workspaces::Id).eq(id))
            .to_owned();
        Ok(self.session.execute_rows_affected(statement).await? == 1)
    }

    pub(crate) async fn delete_all(&mut self) -> Result<(), DbError> {
        let statement = Query::delete().from_table(Workspaces::Table).to_owned();
        self.session.execute(statement).await
    }

    /// Reports the workspaces no authenticated caller can manage.
    ///
    /// Neither half has a reachable owner, which is what management needs:
    /// [`InaccessibleWorkspaces::without_owner`] has no owner at all and is
    /// concealed from every caller, while
    /// [`InaccessibleWorkspaces::local_owner_only`] is owned solely by the
    /// synthetic local principal and stays unmanageable until ownership is
    /// transferred — the members it already has keep their read access.
    pub(crate) async fn inaccessible(&mut self) -> Result<InaccessibleWorkspaces, DbError> {
        let without_owner = self
            .ids(
                select_workspace_ids()
                    .and_where(
                        Expr::col(Workspaces::Id).not_in_subquery(owner_bearing_workspaces()),
                    )
                    .to_owned(),
            )
            .await?;
        let local_owner_only = self
            .ids(
                select_workspace_ids()
                    .and_where(Expr::col(Workspaces::Id).in_subquery(owner_bearing_workspaces()))
                    .and_where(
                        Expr::col(Workspaces::Id)
                            .not_in_subquery(non_local_owner_bearing_workspaces()),
                    )
                    .to_owned(),
            )
            .await?;
        Ok(InaccessibleWorkspaces {
            without_owner,
            local_owner_only,
        })
    }

    async fn ids(&mut self, statement: SelectStatement) -> Result<Vec<String>, DbError> {
        let rows: Vec<(String,)> = self.session.fetch_all(statement).await?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
    }
}

fn select_workspace_ids() -> SelectStatement {
    Query::select()
        .column(Workspaces::Id)
        .from(Workspaces::Table)
        .order_by(Workspaces::Id, Order::Asc)
        .to_owned()
}

impl WorkspacesRepo<'_, CoralTx<'_>> {
    /// Holds an existing workspace parent for a child-table mutation.
    ///
    /// The no-op update is portable across `SQLite` and Postgres and establishes
    /// one parent-before-child serialization point for workspace-scoped writes.
    pub(crate) async fn hold_for_child_mutation(&mut self, id: &str) -> Result<bool, DbError> {
        let statement = Query::update()
            .table(Workspaces::Table)
            .value(
                Workspaces::CreatedAtUnixNanos,
                Expr::col(Workspaces::CreatedAtUnixNanos),
            )
            .and_where(Expr::col(Workspaces::Id).eq(id))
            .to_owned();
        Ok(DbSession::execute_rows_affected(self.session, statement).await? == 1)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

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
    async fn workspace_repository_contract_on_postgres() {
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

        let mut tx = db.begin().await.expect("begin hold tx");
        assert!(
            tx.workspaces()
                .hold_for_child_mutation(&workspace_id)
                .await
                .expect("hold existing workspace")
        );
        assert!(
            !tx.workspaces()
                .hold_for_child_mutation(&missing_workspace_id)
                .await
                .expect("hold missing workspace")
        );
        tx.rollback().await.expect("roll back hold tx");

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

        let mut tx = db.begin().await.expect("begin delete tx");
        assert!(
            tx.workspaces()
                .delete(&workspace_id)
                .await
                .expect("delete workspace")
        );
        tx.commit().await.expect("commit delete tx");
        assert_eq!(
            session
                .workspaces()
                .get(&workspace_id)
                .await
                .expect("get deleted workspace"),
            None
        );

        let mut tx = db.begin().await.expect("begin missing delete tx");
        assert!(
            !tx.workspaces()
                .delete(&missing_workspace_id)
                .await
                .expect("delete missing workspace")
        );
        tx.rollback().await.expect("roll back missing delete tx");
    }

    fn unique_workspace_id() -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before Unix epoch")
            .as_nanos();
        format!("workspace_{}_{}", std::process::id(), nanos)
    }

    fn postgres_test_url() -> Option<String> {
        bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
            .expect("read CORAL_TEST_POSTGRES_URL")
            .filter(|value| !value.is_empty())
    }
}
