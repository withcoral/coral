use sea_query::{Expr, ExprTrait, Func, Order, Query};

use crate::identity::LOCAL_PRINCIPAL_ID;
use crate::state::db::schema::WorkspaceMembers;
use crate::state::db::{CoralTx, DbError, DbSession};
use crate::workspaces::MemberRole;

pub(crate) struct WorkspaceMembersRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> WorkspaceMembersRepo<'a, S>
where
    S: DbSession,
{
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }

    pub(crate) async fn role_for_user_id(
        &mut self,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<Option<MemberRole>, DbError> {
        let statement = Query::select()
            .column(WorkspaceMembers::Role)
            .from(WorkspaceMembers::Table)
            .and_where(Expr::col(WorkspaceMembers::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(WorkspaceMembers::UserId).eq(user_id))
            .to_owned();
        let row: Option<(String,)> = self.session.fetch_optional(statement).await?;
        row.map(|(role,)| parse_role(&role)).transpose()
    }

    pub(crate) async fn workspaces_for_user_id(
        &mut self,
        user_id: &str,
    ) -> Result<Vec<(String, MemberRole)>, DbError> {
        let statement = Query::select()
            .columns([WorkspaceMembers::WorkspaceId, WorkspaceMembers::Role])
            .from(WorkspaceMembers::Table)
            .and_where(Expr::col(WorkspaceMembers::UserId).eq(user_id))
            .order_by(WorkspaceMembers::WorkspaceId, Order::Asc)
            .to_owned();
        let rows: Vec<(String, String)> = self.session.fetch_all(statement).await?;
        rows.into_iter()
            .map(|(workspace_id, role)| Ok((workspace_id, parse_role(&role)?)))
            .collect()
    }

    pub(crate) async fn owned_workspaces_for_user_id(
        &mut self,
        user_id: &str,
        after_workspace_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<String>, DbError> {
        let mut statement = Query::select();
        statement
            .column(WorkspaceMembers::WorkspaceId)
            .from(WorkspaceMembers::Table)
            .and_where(Expr::col(WorkspaceMembers::UserId).eq(user_id))
            .and_where(Expr::col(WorkspaceMembers::Role).eq(MemberRole::Owner.as_str()))
            .order_by(WorkspaceMembers::WorkspaceId, Order::Asc)
            .limit(u64::try_from(limit).unwrap_or(u64::MAX));
        if let Some(after_workspace_id) = after_workspace_id {
            statement.and_where(Expr::col(WorkspaceMembers::WorkspaceId).gt(after_workspace_id));
        }

        let rows: Vec<(String,)> = self.session.fetch_all(statement.clone()).await?;
        Ok(rows
            .into_iter()
            .map(|(workspace_id,)| workspace_id)
            .collect())
    }

    pub(crate) async fn workspaces_for_user_id_with_non_local_owner(
        &mut self,
        user_id: &str,
    ) -> Result<Vec<(String, MemberRole)>, DbError> {
        let non_local_owned_workspaces = Query::select()
            .column(WorkspaceMembers::WorkspaceId)
            .from(WorkspaceMembers::Table)
            .and_where(Expr::col(WorkspaceMembers::Role).eq(MemberRole::Owner.as_str()))
            .and_where(Expr::col(WorkspaceMembers::UserId).ne(LOCAL_PRINCIPAL_ID))
            .to_owned();
        let statement = Query::select()
            .columns([WorkspaceMembers::WorkspaceId, WorkspaceMembers::Role])
            .from(WorkspaceMembers::Table)
            .and_where(Expr::col(WorkspaceMembers::UserId).eq(user_id))
            .and_where(
                Expr::col(WorkspaceMembers::WorkspaceId).in_subquery(non_local_owned_workspaces),
            )
            .order_by(WorkspaceMembers::WorkspaceId, Order::Asc)
            .to_owned();
        let rows: Vec<(String, String)> = self.session.fetch_all(statement).await?;
        rows.into_iter()
            .map(|(workspace_id, role)| Ok((workspace_id, parse_role(&role)?)))
            .collect()
    }

    pub(crate) async fn role_for_user_id_with_non_local_owner(
        &mut self,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<Option<MemberRole>, DbError> {
        let non_local_owner = Query::select()
            .column(WorkspaceMembers::UserId)
            .from(WorkspaceMembers::Table)
            .and_where(Expr::col(WorkspaceMembers::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(WorkspaceMembers::Role).eq(MemberRole::Owner.as_str()))
            .and_where(Expr::col(WorkspaceMembers::UserId).ne(LOCAL_PRINCIPAL_ID))
            .to_owned();
        let statement = Query::select()
            .column(WorkspaceMembers::Role)
            .from(WorkspaceMembers::Table)
            .and_where(Expr::col(WorkspaceMembers::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(WorkspaceMembers::UserId).eq(user_id))
            .and_where(Expr::exists(non_local_owner))
            .to_owned();
        let row: Option<(String,)> = self.session.fetch_optional(statement).await?;
        row.map(|(role,)| parse_role(&role)).transpose()
    }
}

impl WorkspaceMembersRepo<'_, CoralTx<'_>> {
    pub(crate) async fn owner_count(&mut self, workspace_id: &str) -> Result<u64, DbError> {
        let statement = Query::select()
            .expr(Func::count(Expr::col(WorkspaceMembers::UserId)))
            .from(WorkspaceMembers::Table)
            .and_where(Expr::col(WorkspaceMembers::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(WorkspaceMembers::Role).eq(MemberRole::Owner.as_str()))
            .to_owned();
        let (count,): (i64,) = self
            .session
            .fetch_optional(statement)
            .await?
            .unwrap_or_default();
        Ok(u64::try_from(count).unwrap_or(0))
    }

    pub(crate) async fn insert(
        &mut self,
        workspace_id: &str,
        user_id: &str,
        role: MemberRole,
        created_at_unix_nanos: i64,
    ) -> Result<(), DbError> {
        let statement = Query::insert()
            .into_table(WorkspaceMembers::Table)
            .columns([
                WorkspaceMembers::WorkspaceId,
                WorkspaceMembers::UserId,
                WorkspaceMembers::Role,
                WorkspaceMembers::CreatedAtUnixNanos,
            ])
            .values_panic([
                Expr::val(workspace_id.to_string()),
                Expr::val(user_id.to_string()),
                Expr::val(role.as_str()),
                Expr::val(created_at_unix_nanos),
            ])
            .to_owned();
        self.session.execute(statement).await
    }

    pub(crate) async fn delete(
        &mut self,
        workspace_id: &str,
        user_id: &str,
    ) -> Result<bool, DbError> {
        let statement = Query::delete()
            .from_table(WorkspaceMembers::Table)
            .and_where(Expr::col(WorkspaceMembers::WorkspaceId).eq(workspace_id))
            .and_where(Expr::col(WorkspaceMembers::UserId).eq(user_id))
            .to_owned();
        Ok(self.session.execute_rows_affected(statement).await? == 1)
    }
}

fn parse_role(value: &str) -> Result<MemberRole, DbError> {
    MemberRole::parse(value)
        .ok_or_else(|| DbError::CorruptData(format!("invalid workspace member role '{value}'")))
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::MemberRole;
    use crate::bootstrap;
    use crate::state::AppStateLayout;
    use crate::state::db::repositories::users::UpsertLoginOutcome;
    use crate::state::db::session::DbRepos;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};

    #[tokio::test]
    async fn owned_workspaces_for_user_id_and_member_repository_round_trips_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let db = open_sqlite(&layout).await;

        assert_workspace_member_repository_round_trip(&db, &uuid::Uuid::new_v4().to_string()).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared repository harness against Postgres"]
    async fn owned_workspaces_for_user_id_and_member_repository_round_trips_against_postgres() {
        let Some(url) = postgres_test_url() else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");

        assert_workspace_member_repository_round_trip(&db, &uuid::Uuid::new_v4().to_string()).await;
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

    #[expect(
        clippy::too_many_lines,
        reason = "the shared backend harness verifies one ordered repository lifecycle"
    )]
    async fn assert_workspace_member_repository_round_trip(db: &CoralDb, suffix: &str) {
        let workspace_a = format!("a-{suffix}");
        let workspace_b = format!("b-{suffix}");
        let workspace_c = format!("c-{suffix}");
        let missing_workspace = format!("missing-{suffix}");
        let user_id = create_user(db, suffix).await;
        let other_user_id = create_user(db, &format!("other-{suffix}")).await;

        let mut tx = db.begin().await.expect("begin insert tx");
        tx.workspaces()
            .create(&workspace_b, 10)
            .await
            .expect("create workspace b");
        tx.workspaces()
            .create(&workspace_a, 11)
            .await
            .expect("create workspace a");
        tx.workspaces()
            .create(&workspace_c, 12)
            .await
            .expect("create workspace c");
        assert!(
            tx.workspaces()
                .hold_for_child_mutation(&workspace_b)
                .await
                .expect("hold workspace b")
        );
        tx.workspace_members()
            .insert(&workspace_b, &user_id, MemberRole::Member, 20)
            .await
            .expect("insert member role");
        assert!(
            tx.workspaces()
                .hold_for_child_mutation(&workspace_a)
                .await
                .expect("hold workspace a")
        );
        tx.workspace_members()
            .insert(&workspace_a, &user_id, MemberRole::Owner, 21)
            .await
            .expect("insert owner role");
        tx.workspace_members()
            .insert(&workspace_a, &other_user_id, MemberRole::Owner, 22)
            .await
            .expect("insert second owner");
        assert!(
            tx.workspaces()
                .hold_for_child_mutation(&workspace_c)
                .await
                .expect("hold workspace c")
        );
        tx.workspace_members()
            .insert(&workspace_c, &user_id, MemberRole::Owner, 23)
            .await
            .expect("insert second owned workspace");
        assert_eq!(
            tx.workspace_members()
                .owner_count(&workspace_a)
                .await
                .expect("count owners"),
            2
        );
        assert_eq!(
            tx.workspace_members()
                .owner_count(&missing_workspace)
                .await
                .expect("count owners in missing workspace"),
            0
        );
        tx.commit().await.expect("commit insert tx");

        let mut session = db;
        assert_eq!(
            session
                .workspace_members()
                .role_for_user_id(&workspace_a, &user_id)
                .await
                .expect("lookup owner role"),
            Some(MemberRole::Owner)
        );
        assert_eq!(
            session
                .workspace_members()
                .role_for_user_id(&workspace_b, &user_id)
                .await
                .expect("lookup member role"),
            Some(MemberRole::Member)
        );
        assert_eq!(
            session
                .workspace_members()
                .role_for_user_id(&missing_workspace, &user_id)
                .await
                .expect("lookup missing role"),
            None
        );
        assert_eq!(
            session
                .workspace_members()
                .workspaces_for_user_id(&user_id)
                .await
                .expect("list visible workspaces"),
            vec![
                (workspace_a.clone(), MemberRole::Owner),
                (workspace_b.clone(), MemberRole::Member),
                (workspace_c.clone(), MemberRole::Owner),
            ]
        );
        assert_eq!(
            session
                .workspace_members()
                .owned_workspaces_for_user_id("missing-user", None, 10)
                .await
                .expect("list no owned workspaces"),
            Vec::<String>::new()
        );
        assert_eq!(
            session
                .workspace_members()
                .owned_workspaces_for_user_id(&user_id, None, 1)
                .await
                .expect("list first owned workspace"),
            vec![workspace_a.clone()]
        );
        assert_eq!(
            session
                .workspace_members()
                .owned_workspaces_for_user_id(&user_id, Some(&workspace_a), 10)
                .await
                .expect("list owned workspaces after cursor"),
            vec![workspace_c.clone()]
        );
        assert_eq!(
            session
                .workspace_members()
                .owned_workspaces_for_user_id(&user_id, None, 10)
                .await
                .expect("list all owned workspaces"),
            vec![workspace_a.clone(), workspace_c.clone()]
        );

        let mut tx = db.begin().await.expect("begin delete tx");
        assert!(
            tx.workspaces()
                .hold_for_child_mutation(&workspace_b)
                .await
                .expect("hold workspace for member delete")
        );
        assert!(
            tx.workspace_members()
                .delete(&workspace_b, &user_id)
                .await
                .expect("delete membership")
        );
        assert!(
            !tx.workspace_members()
                .delete(&workspace_b, &user_id)
                .await
                .expect("delete missing membership")
        );
        tx.rollback().await.expect("rollback member delete");
        assert_eq!(
            session
                .workspace_members()
                .role_for_user_id(&workspace_b, &user_id)
                .await
                .expect("lookup rolled-back member"),
            Some(MemberRole::Member)
        );

        let mut tx = db.begin().await.expect("begin cascade tx");
        assert!(
            tx.workspaces()
                .delete(&workspace_a)
                .await
                .expect("delete workspace")
        );
        tx.commit().await.expect("commit workspace delete");
        assert_eq!(
            session
                .workspace_members()
                .role_for_user_id(&workspace_a, &user_id)
                .await
                .expect("lookup cascaded membership"),
            None
        );
        assert_eq!(
            session
                .workspace_members()
                .workspaces_for_user_id(&user_id)
                .await
                .expect("list workspaces after cascade"),
            vec![
                (workspace_b, MemberRole::Member),
                (workspace_c, MemberRole::Owner),
            ]
        );
    }

    async fn create_user(db: &CoralDb, suffix: &str) -> String {
        let mut session = db;
        let UpsertLoginOutcome::Upserted(user) = session
            .users()
            .upsert_login(
                &format!("issuer-{suffix}"),
                &format!("subject-{suffix}"),
                None,
                1,
            )
            .await
            .expect("create user")
        else {
            panic!("unique subject should create user");
        };
        user.user_id
    }

    fn postgres_test_url() -> Option<String> {
        bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
            .expect("read CORAL_TEST_POSTGRES_URL")
            .filter(|value| !value.is_empty())
    }
}
