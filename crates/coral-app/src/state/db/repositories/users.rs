#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "user persistence APIs are wired to production consumers in later milestones"
    )
)]

use sea_query::{Expr, ExprTrait, OnConflict, Order, Query};

use crate::state::db::schema::Users;
use crate::state::db::{DbError, DbSession};

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
pub(crate) struct UserRecord {
    pub(crate) user_id: String,
    pub(crate) issuer: String,
    pub(crate) subject: String,
    pub(crate) display_name: Option<String>,
    pub(crate) created_at_unix_nanos: i64,
    pub(crate) last_login_at_unix_nanos: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UpsertLoginOutcome {
    Upserted(UserRecord),
    IssuerMismatch { stored_issuer: String },
}

pub(crate) struct UsersRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> UsersRepo<'a, S>
where
    S: DbSession,
{
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }

    pub(crate) async fn upsert_login(
        &mut self,
        issuer: &str,
        subject: &str,
        display_name: Option<&str>,
        now_unix_nanos: i64,
    ) -> Result<UpsertLoginOutcome, DbError> {
        let proposed_user_id = uuid::Uuid::new_v4().to_string();
        let statement = Query::insert()
            .into_table(Users::Table)
            .columns(user_columns())
            .values_panic([
                Expr::val(proposed_user_id),
                Expr::val(issuer.to_string()),
                Expr::val(subject.to_string()),
                Expr::val(display_name.map(str::to_owned)),
                Expr::val(now_unix_nanos),
                Expr::val(now_unix_nanos),
            ])
            .on_conflict(
                OnConflict::column(Users::Subject)
                    .update_columns([Users::DisplayName, Users::LastLoginAtUnixNanos])
                    .action_and_where(
                        Expr::col((Users::Table, Users::Issuer)).eq(issuer.to_string()),
                    )
                    .to_owned(),
            )
            .to_owned();
        self.session.execute_rows_affected(statement).await?;

        let record = self.get_by_subject(subject).await?.ok_or_else(|| {
            DbError::CorruptData("user disappeared after login upsert".to_string())
        })?;
        if record.issuer != issuer {
            return Ok(UpsertLoginOutcome::IssuerMismatch {
                stored_issuer: record.issuer,
            });
        }
        Ok(UpsertLoginOutcome::Upserted(record))
    }

    pub(crate) async fn get_by_user_id(
        &mut self,
        user_id: &str,
    ) -> Result<Option<UserRecord>, DbError> {
        let statement = Query::select()
            .columns(user_columns())
            .from(Users::Table)
            .and_where(Expr::col(Users::UserId).eq(user_id))
            .to_owned();
        self.session.fetch_optional(statement).await
    }

    pub(crate) async fn list(&mut self) -> Result<Vec<UserRecord>, DbError> {
        let statement = Query::select()
            .columns(user_columns())
            .from(Users::Table)
            .order_by(Users::UserId, Order::Asc)
            .to_owned();
        self.session.fetch_all(statement).await
    }

    async fn get_by_subject(&mut self, subject: &str) -> Result<Option<UserRecord>, DbError> {
        let statement = Query::select()
            .columns(user_columns())
            .from(Users::Table)
            .and_where(Expr::col(Users::Subject).eq(subject))
            .to_owned();
        self.session.fetch_optional(statement).await
    }
}

fn user_columns() -> [Users; 6] {
    [
        Users::UserId,
        Users::Issuer,
        Users::Subject,
        Users::DisplayName,
        Users::CreatedAtUnixNanos,
        Users::LastLoginAtUnixNanos,
    ]
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::UpsertLoginOutcome;
    use crate::bootstrap;
    use crate::state::AppStateLayout;
    use crate::state::db::session::DbRepos;
    use crate::state::db::{CoralDb, DatabaseConfig, ResolvedDatabaseConfig};

    #[tokio::test]
    async fn user_repository_round_trips_against_sqlite() {
        let temp = tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        let db = open_sqlite(&layout).await;

        assert_user_repository_round_trip(&db, &uuid::Uuid::new_v4().simple().to_string()).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared repository harness against Postgres"]
    async fn user_repository_round_trips_against_postgres() {
        let Some(url) = postgres_test_url() else {
            return;
        };
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");

        assert_user_repository_round_trip(&db, &uuid::Uuid::new_v4().simple().to_string()).await;
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

    async fn assert_user_repository_round_trip(db: &CoralDb, suffix: &str) {
        let issuer = format!("issuer-{suffix}");
        let subject = format!("subject-{suffix}");
        let missing_user_id = uuid::Uuid::new_v4().to_string();
        let mut session = db;
        assert_eq!(
            session
                .users()
                .get_by_user_id(&missing_user_id)
                .await
                .expect("get missing user"),
            None
        );

        let UpsertLoginOutcome::Upserted(created) = session
            .users()
            .upsert_login(&issuer, &subject, Some("First Name"), 10)
            .await
            .expect("create user")
        else {
            panic!("new subject must create a user");
        };
        let parsed_user_id = uuid::Uuid::parse_str(&created.user_id).expect("UUID user id");
        assert_eq!(
            parsed_user_id.get_version_num(),
            4,
            "user ID must be UUID-v4"
        );
        assert_eq!(created.issuer, issuer);
        assert_eq!(created.subject, subject);
        assert_eq!(created.display_name.as_deref(), Some("First Name"));
        assert_eq!(created.created_at_unix_nanos, 10);
        assert_eq!(created.last_login_at_unix_nanos, 10);

        let UpsertLoginOutcome::Upserted(refreshed) = session
            .users()
            .upsert_login(&issuer, &subject, Some("Refreshed Name"), 20)
            .await
            .expect("refresh user")
        else {
            panic!("same issuer must refresh the user");
        };
        assert_eq!(refreshed.user_id, created.user_id);
        assert_eq!(refreshed.created_at_unix_nanos, 10);
        assert_eq!(refreshed.last_login_at_unix_nanos, 20);
        assert_eq!(refreshed.display_name.as_deref(), Some("Refreshed Name"));

        assert_eq!(
            session
                .users()
                .upsert_login("different-issuer", &subject, Some("Rejected Name"), 30)
                .await
                .expect("detect issuer mismatch"),
            UpsertLoginOutcome::IssuerMismatch {
                stored_issuer: issuer,
            }
        );
        assert_eq!(
            session
                .users()
                .get_by_user_id(&created.user_id)
                .await
                .expect("get user after rejected login"),
            Some(refreshed.clone()),
            "issuer mismatch must not modify the user"
        );

        let users = session.users().list().await.expect("list users");
        assert!(users.contains(&refreshed));
        assert!(
            users
                .windows(2)
                .all(|pair| matches!(pair, [first, second] if first.user_id <= second.user_id)),
            "users must be listed deterministically: {users:?}"
        );
    }

    fn postgres_test_url() -> Option<String> {
        bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
            .expect("read CORAL_TEST_POSTGRES_URL")
            .filter(|value| !value.is_empty())
    }
}
