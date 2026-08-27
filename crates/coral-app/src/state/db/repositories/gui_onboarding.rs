use sea_query::{Expr, ExprTrait, OnConflict, Query};

use crate::state::db::schema::GuiOnboardingCompletions;
use crate::state::db::{DbError, DbSession};

pub(crate) struct GuiOnboardingRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> GuiOnboardingRepo<'a, S>
where
    S: DbSession,
{
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }

    pub(crate) async fn is_completed(&mut self, principal_id: &str) -> Result<bool, DbError> {
        let statement = Query::select()
            .column(GuiOnboardingCompletions::PrincipalId)
            .from(GuiOnboardingCompletions::Table)
            .and_where(Expr::col(GuiOnboardingCompletions::PrincipalId).eq(principal_id))
            .to_owned();
        let completion: Option<(String,)> = self.session.fetch_optional(statement).await?;
        Ok(completion.is_some())
    }

    pub(crate) async fn complete(
        &mut self,
        principal_id: &str,
        completed_at_unix_nanos: i64,
    ) -> Result<(), DbError> {
        let statement = Query::insert()
            .into_table(GuiOnboardingCompletions::Table)
            .columns([
                GuiOnboardingCompletions::PrincipalId,
                GuiOnboardingCompletions::CompletedAtUnixNanos,
            ])
            .values_panic([
                Expr::val(principal_id.to_string()),
                Expr::val(completed_at_unix_nanos),
            ])
            .on_conflict(
                OnConflict::column(GuiOnboardingCompletions::PrincipalId)
                    .do_nothing()
                    .to_owned(),
            )
            .to_owned();
        self.session.execute(statement).await
    }

    #[cfg(test)]
    async fn completed_at_unix_nanos(
        &mut self,
        principal_id: &str,
    ) -> Result<Option<i64>, DbError> {
        let statement = Query::select()
            .column(GuiOnboardingCompletions::CompletedAtUnixNanos)
            .from(GuiOnboardingCompletions::Table)
            .and_where(Expr::col(GuiOnboardingCompletions::PrincipalId).eq(principal_id))
            .to_owned();
        let completion: Option<(i64,)> = self.session.fetch_optional(statement).await?;
        Ok(completion.map(|(completed_at_unix_nanos,)| completed_at_unix_nanos))
    }

    #[cfg(test)]
    async fn delete_completion(&mut self, principal_id: &str) -> Result<(), DbError> {
        let statement = Query::delete()
            .from_table(GuiOnboardingCompletions::Table)
            .and_where(Expr::col(GuiOnboardingCompletions::PrincipalId).eq(principal_id))
            .to_owned();
        self.session.execute(statement).await
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use crate::bootstrap;
    use crate::state::AppStateLayout;
    use crate::state::db::session::DbRepos;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};

    #[tokio::test]
    async fn gui_onboarding_repository_round_trips_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("layout dirs");
        let db = open_sqlite(&layout).await;

        let mut fresh_session = &db;
        assert!(
            !fresh_session
                .gui_onboarding()
                .is_completed("fresh_sqlite_principal")
                .await
                .expect("get completion from freshly migrated SQLite database"),
            "the migration must create an empty completion table"
        );
        assert_gui_onboarding_repository_round_trip(&db).await;
    }

    #[tokio::test]
    async fn gui_onboarding_repository_round_trips_against_postgres() {
        let Some(url) = postgres_test_url() else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");

        assert_gui_onboarding_repository_round_trip(&db).await;
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

    async fn assert_gui_onboarding_repository_round_trip(db: &CoralDb) {
        db.ping().await.expect("ping database");

        let test_id = uuid::Uuid::new_v4();
        let alice_principal_id = format!("gui_onboarding_alice_{test_id}");
        let bob_principal_id = format!("gui_onboarding_bob_{test_id}");
        {
            let mut session = db;
            assert!(
                !session
                    .gui_onboarding()
                    .is_completed(&alice_principal_id)
                    .await
                    .expect("get missing Alice completion")
            );
            assert!(
                !session
                    .gui_onboarding()
                    .is_completed(&bob_principal_id)
                    .await
                    .expect("get missing Bob completion")
            );

            session
                .gui_onboarding()
                .complete(&alice_principal_id, 42)
                .await
                .expect("complete Alice onboarding");
            session
                .gui_onboarding()
                .complete(&alice_principal_id, 99)
                .await
                .expect("complete Alice onboarding again");
        }

        let mut session = db;
        assert!(
            session
                .gui_onboarding()
                .is_completed(&alice_principal_id)
                .await
                .expect("get committed Alice completion")
        );
        assert!(
            !session
                .gui_onboarding()
                .is_completed(&bob_principal_id)
                .await
                .expect("get missing Bob completion after commit")
        );
        assert_eq!(
            session
                .gui_onboarding()
                .completed_at_unix_nanos(&alice_principal_id)
                .await
                .expect("get Alice completion timestamp"),
            Some(42),
            "idempotent completion must preserve the first timestamp"
        );

        let mut cleanup_tx = db.begin().await.expect("begin cleanup tx");
        cleanup_tx
            .gui_onboarding()
            .delete_completion(&alice_principal_id)
            .await
            .expect("clean up Alice completion");
        cleanup_tx
            .gui_onboarding()
            .delete_completion(&bob_principal_id)
            .await
            .expect("clean up Bob completion");
        cleanup_tx.commit().await.expect("commit cleanup tx");
        assert!(
            !session
                .gui_onboarding()
                .is_completed(&alice_principal_id)
                .await
                .expect("get cleaned-up Alice completion")
        );
    }

    fn postgres_test_url() -> Option<String> {
        bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
            .expect("read CORAL_TEST_POSTGRES_URL")
            .filter(|value| !value.is_empty())
    }
}
