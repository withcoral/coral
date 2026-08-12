use sea_query::{Expr, ExprTrait, OnConflict, Query};

use super::users::UserRecord;
use crate::state::db::schema::Workspaces;
use crate::state::db::session::DbSession;
use crate::state::db::workspace_member_state::{
    AddMemberOutcome, RemoveMemberOutcome, WorkspaceMemberView,
};
use crate::state::db::workspace_state::WorkspaceCreationOutcome;
use crate::state::db::{CoralDb, CoralTx, DbError, DbRepos};
use crate::workspaces::MemberRole;

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

    #[cfg_attr(not(test), expect(dead_code, reason = "used higher in the PR stack"))]
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
}

impl WorkspacesRepo<'_, &CoralDb> {
    pub(crate) async fn create_with_owner(
        &mut self,
        workspace_id: &str,
        creator_user_id: &str,
        created_at_unix_nanos: i64,
    ) -> Result<WorkspaceCreationOutcome, DbError> {
        let db = *self.session;
        let mut tx = db.begin().await?;
        if !tx
            .users()
            .hold_for_workspace_creation(creator_user_id)
            .await?
        {
            tx.rollback().await?;
            return Ok(WorkspaceCreationOutcome::UserNotFound);
        }
        if !tx
            .workspaces()
            .try_create_with_owner(workspace_id, creator_user_id, created_at_unix_nanos)
            .await?
        {
            tx.rollback().await?;
            return Ok(WorkspaceCreationOutcome::AlreadyExists);
        }
        tx.commit().await?;
        Ok(WorkspaceCreationOutcome::Created)
    }

    pub(crate) async fn create_with_local_owner(
        &mut self,
        workspace_id: &str,
        created_at_unix_nanos: i64,
    ) -> Result<WorkspaceCreationOutcome, DbError> {
        let db = *self.session;
        let mut tx = db.begin().await?;
        tx.users().ensure_local_user(created_at_unix_nanos).await?;
        if !tx
            .workspaces()
            .try_create_with_owner(
                workspace_id,
                crate::identity::LOCAL_PRINCIPAL_ID,
                created_at_unix_nanos,
            )
            .await?
        {
            tx.rollback().await?;
            return Ok(WorkspaceCreationOutcome::AlreadyExists);
        }
        tx.commit().await?;
        Ok(WorkspaceCreationOutcome::Created)
    }

    pub(crate) async fn add_member(
        &mut self,
        workspace_id: &str,
        user_id: &str,
        role: MemberRole,
        created_at_unix_nanos: i64,
    ) -> Result<AddMemberOutcome, DbError> {
        let db = *self.session;
        let mut tx = db.begin().await?;
        if !tx
            .workspaces()
            .hold_for_child_mutation(workspace_id)
            .await?
        {
            tx.rollback().await?;
            return Ok(AddMemberOutcome::WorkspaceNotFound);
        }
        let Some(user) = tx.users().get_by_user_id(user_id).await? else {
            tx.rollback().await?;
            return Ok(AddMemberOutcome::UserNotFound);
        };
        if let Some(existing_role) = tx
            .workspace_members()
            .role_for_user_id(workspace_id, user_id)
            .await?
        {
            tx.rollback().await?;
            return Ok(existing_add_outcome(user, existing_role, role));
        }
        match tx
            .workspace_members()
            .insert(workspace_id, user_id, role, created_at_unix_nanos)
            .await
        {
            Ok(()) => {
                tx.commit().await?;
                Ok(AddMemberOutcome::Added(member_view(user, role)))
            }
            Err(error) if error.is_unique_violation() => {
                tx.rollback().await?;
                let mut session = db;
                let Some(existing_role) = session
                    .workspace_members()
                    .role_for_user_id(workspace_id, user_id)
                    .await?
                else {
                    return Err(error);
                };
                Ok(existing_add_outcome(user, existing_role, role))
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn remove_member(
        &mut self,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<RemoveMemberOutcome, DbError> {
        let db = *self.session;
        let mut tx = db.begin().await?;
        if !tx
            .workspaces()
            .hold_for_child_mutation(workspace_id)
            .await?
        {
            tx.rollback().await?;
            return Ok(RemoveMemberOutcome::WorkspaceNotFound);
        }
        let Some(role) = tx
            .workspace_members()
            .role_for_user_id(workspace_id, user_id)
            .await?
        else {
            tx.rollback().await?;
            return Ok(RemoveMemberOutcome::MemberNotFound);
        };
        if role == MemberRole::Owner && tx.workspace_members().owner_count(workspace_id).await? <= 1
        {
            tx.rollback().await?;
            return Ok(RemoveMemberOutcome::LastOwnerProtected);
        }
        let removed = tx.workspace_members().delete(workspace_id, user_id).await?;
        if removed {
            tx.commit().await?;
            Ok(RemoveMemberOutcome::Removed)
        } else {
            tx.rollback().await?;
            Ok(RemoveMemberOutcome::MemberNotFound)
        }
    }
}

impl WorkspacesRepo<'_, CoralTx<'_>> {
    pub(crate) async fn try_create_with_owner(
        &mut self,
        workspace_id: &str,
        creator_user_id: &str,
        created_at_unix_nanos: i64,
    ) -> Result<bool, DbError> {
        let statement = Query::insert()
            .into_table(Workspaces::Table)
            .columns([Workspaces::Id, Workspaces::CreatedAtUnixNanos])
            .values_panic([
                Expr::val(workspace_id.to_string()),
                Expr::val(created_at_unix_nanos),
            ])
            .on_conflict(OnConflict::column(Workspaces::Id).do_nothing().to_owned())
            .to_owned();
        if self.session.execute_rows_affected(statement).await? == 0 {
            return Ok(false);
        }
        self.session
            .workspace_members()
            .insert(
                workspace_id,
                creator_user_id,
                MemberRole::Owner,
                created_at_unix_nanos,
            )
            .await?;
        Ok(true)
    }

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

fn existing_add_outcome(
    user: UserRecord,
    existing_role: MemberRole,
    requested_role: MemberRole,
) -> AddMemberOutcome {
    if existing_role == requested_role {
        AddMemberOutcome::ExistingSameRole(member_view(user, existing_role))
    } else {
        AddMemberOutcome::RoleConflict
    }
}

fn member_view(user: UserRecord, role: MemberRole) -> WorkspaceMemberView {
    WorkspaceMemberView {
        user_id: user.user_id,
        role,
        display_name: user.display_name,
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
