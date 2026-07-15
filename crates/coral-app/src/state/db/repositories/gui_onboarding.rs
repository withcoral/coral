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

    pub(crate) async fn complete(&mut self, principal_id: &str) -> Result<(), DbError> {
        let statement = Query::insert()
            .into_table(GuiOnboardingCompletions::Table)
            .columns([GuiOnboardingCompletions::PrincipalId])
            .values_panic([Expr::val(principal_id.to_string())])
            .on_conflict(
                OnConflict::column(GuiOnboardingCompletions::PrincipalId)
                    .do_nothing()
                    .to_owned(),
            )
            .to_owned();
        self.session.execute(statement).await
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

    const ALICE_PRINCIPAL_ID: &str = "alice";
    const BOB_PRINCIPAL_ID: &str = "bob";

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
                .is_completed(ALICE_PRINCIPAL_ID)
                .await
                .expect("get completion from freshly migrated SQLite database"),
            "the migration must create an empty completion table"
        );
        assert_gui_onboarding_repository_round_trip(&db).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared repository harness against Postgres"]
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

        let mut tx = db.begin().await.expect("begin state transition tx");
        tx.gui_onboarding()
            .delete_completion(ALICE_PRINCIPAL_ID)
            .await
            .expect("clear Alice completion in test transaction");
        tx.gui_onboarding()
            .delete_completion(BOB_PRINCIPAL_ID)
            .await
            .expect("clear Bob completion in test transaction");
        assert!(
            !tx.gui_onboarding()
                .is_completed(ALICE_PRINCIPAL_ID)
                .await
                .expect("get missing Alice completion")
        );
        assert!(
            !tx.gui_onboarding()
                .is_completed(BOB_PRINCIPAL_ID)
                .await
                .expect("get missing Bob completion")
        );

        tx.gui_onboarding()
            .complete(ALICE_PRINCIPAL_ID)
            .await
            .expect("complete Alice onboarding");
        tx.gui_onboarding()
            .complete(ALICE_PRINCIPAL_ID)
            .await
            .expect("complete Alice onboarding again");
        assert!(
            tx.gui_onboarding()
                .is_completed(ALICE_PRINCIPAL_ID)
                .await
                .expect("get Alice completion")
        );
        assert!(
            !tx.gui_onboarding()
                .is_completed(BOB_PRINCIPAL_ID)
                .await
                .expect("get Bob completion")
        );
        tx.rollback().await.expect("roll back state transition tx");
    }

    fn postgres_test_url() -> Option<String> {
        bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
            .expect("read CORAL_TEST_POSTGRES_URL")
            .filter(|value| !value.is_empty())
    }
}
