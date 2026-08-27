#[cfg(test)]
use sea_query::ExprTrait;
use sea_query::{Expr, OnConflict, Query};

use crate::state::db::schema::AppStateMigrations;
use crate::state::db::{CoralTx, DbError, DbSession};

/// Identifies the one-time migration that gives legacy ownerless workspaces
/// their local owner.
///
/// The id is part of the on-disk contract: it is what an already-upgraded
/// state directory matches against, so it must never change.
pub(crate) const LOCAL_WORKSPACE_OWNERSHIP_MIGRATION_ID: &str = "local_workspace_ownership_v1";

#[cfg(test)]
#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(crate) struct StateMigrationRecord {
    pub(crate) id: String,
    pub(crate) completed_at_unix_nanos: i64,
}

pub(crate) struct StateMigrationsRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> StateMigrationsRepo<'a, S>
where
    S: DbSession,
{
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }

    #[cfg(test)]
    pub(crate) async fn has_completed(&mut self, id: &str) -> Result<bool, DbError> {
        Ok(self.get(id).await?.is_some())
    }

    #[cfg(test)]
    async fn get(&mut self, id: &str) -> Result<Option<StateMigrationRecord>, DbError> {
        let statement = Query::select()
            .columns([
                AppStateMigrations::Id,
                AppStateMigrations::CompletedAtUnixNanos,
            ])
            .from(AppStateMigrations::Table)
            .and_where(Expr::col(AppStateMigrations::Id).eq(id))
            .to_owned();
        self.session.fetch_optional(statement).await
    }
}

impl StateMigrationsRepo<'_, CoralTx<'_>> {
    /// Claims a one-time migration for the lifetime of the current transaction.
    ///
    /// The inserted marker becomes visible only when the transaction commits.
    /// A rollback releases the claim so another process can retry the migration.
    pub(crate) async fn try_claim(
        &mut self,
        id: &str,
        completed_at_unix_nanos: i64,
    ) -> Result<bool, DbError> {
        let statement = Query::insert()
            .into_table(AppStateMigrations::Table)
            .columns([
                AppStateMigrations::Id,
                AppStateMigrations::CompletedAtUnixNanos,
            ])
            .values_panic([
                Expr::val(id.to_string()),
                Expr::val(completed_at_unix_nanos),
            ])
            .on_conflict(
                OnConflict::column(AppStateMigrations::Id)
                    .do_nothing()
                    .to_owned(),
            )
            .to_owned();
        Ok(DbSession::execute_rows_affected(self.session, statement).await? == 1)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::StateMigrationRecord;
    use crate::bootstrap;
    use crate::state::AppStateLayout;
    use crate::state::db::session::DbRepos;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};

    #[tokio::test]
    async fn state_migration_repository_round_trips_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let db = open_sqlite(&layout).await;
        assert_claim_lifecycle(&db, "sqlite_transactional_claim").await;
    }

    #[tokio::test]
    async fn state_migration_repository_contract_on_postgres() {
        let Some(url) = postgres_test_url() else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres connection");
        db.migrate().await.expect("migrate postgres");
        let migration_id = format!("postgres_transactional_claim_{}", uuid::Uuid::new_v4());
        assert_claim_lifecycle(&db, &migration_id).await;
    }

    async fn assert_claim_lifecycle(db: &CoralDb, migration_id: &str) {
        let mut tx = db.begin().await.expect("begin rolled-back claim");
        assert!(
            tx.state_migrations()
                .try_claim(migration_id, 11)
                .await
                .expect("claim migration")
        );
        tx.rollback().await.expect("roll back claim");

        let mut session = db;
        assert!(
            !session
                .state_migrations()
                .has_completed(migration_id)
                .await
                .expect("read rolled-back claim")
        );

        let mut tx = db.begin().await.expect("begin committed claim");
        assert!(
            tx.state_migrations()
                .try_claim(migration_id, 12)
                .await
                .expect("claim migration after rollback")
        );
        tx.commit().await.expect("commit claim");

        let mut tx = db.begin().await.expect("begin duplicate claim");
        assert!(
            !tx.state_migrations()
                .try_claim(migration_id, 13)
                .await
                .expect("reject duplicate claim")
        );
        tx.rollback().await.expect("roll back duplicate claim");

        assert_eq!(
            session
                .state_migrations()
                .get(migration_id)
                .await
                .expect("read completed claim"),
            Some(StateMigrationRecord {
                id: migration_id.to_string(),
                completed_at_unix_nanos: 12,
            })
        );
    }

    #[tokio::test]
    async fn state_migration_repository_concurrency_contract_on_postgres() {
        let Some(url) = postgres_test_url() else {
            return;
        };
        let first_db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url: url.clone() })
            .await
            .expect("open first postgres connection");
        first_db.migrate().await.expect("migrate postgres");
        let second_db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open second postgres connection");
        let migration_id = format!("concurrent_claim_{}", uuid::Uuid::new_v4());

        let mut first_tx = first_db.begin().await.expect("begin first claim");
        assert!(
            first_tx
                .state_migrations()
                .try_claim(&migration_id, 21)
                .await
                .expect("claim migration first")
        );

        let (first_commit, second_claim) = tokio::join!(
            async {
                tokio::task::yield_now().await;
                first_tx.commit().await
            },
            async {
                let mut second_tx = second_db.begin().await.expect("begin second claim");
                let claimed = second_tx
                    .state_migrations()
                    .try_claim(&migration_id, 22)
                    .await
                    .expect("claim migration second");
                second_tx.rollback().await.expect("roll back second claim");
                claimed
            }
        );

        first_commit.expect("commit first claim");
        assert!(!second_claim, "the committed claimant must be unique");
    }

    async fn open_sqlite(layout: &AppStateLayout) -> CoralDb {
        layout.ensure().expect("ensure layout");
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

    fn postgres_test_url() -> Option<String> {
        bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
            .expect("read CORAL_TEST_POSTGRES_URL")
            .filter(|value| !value.is_empty())
    }
}
