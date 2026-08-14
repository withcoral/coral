#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the login directory API is not yet wired to production consumers"
    )
)]

use sea_query::{Expr, ExprTrait, OnConflict, Order, Query, SelectStatement};
use uuid::Uuid;

use crate::state::db::DbError;
use crate::state::db::schema::Users;
use crate::state::db::session::DbSession;

/// One directory row for an authenticated upstream identity.
///
/// `issuer` and `subject` stay inside this module's callers: they are upstream
/// identifiers, not part of any client-facing surface.
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

    /// Provisions the directory row for a verified login.
    ///
    /// The insert mints a fresh UUID v4 that a conflicting row discards, so a
    /// first login mints the internal id, a later same-issuer login preserves
    /// it, and concurrent first logins converge on whichever insert won. The
    /// conditional update leaves a subject that is already bound to another
    /// issuer untouched, which the follow-up read reports as a mismatch instead
    /// of an error, so this stays safe to call inside a caller's transaction.
    pub(crate) async fn upsert_login(
        &mut self,
        issuer: &str,
        subject: &str,
        display_name: Option<&str>,
        now_unix_nanos: i64,
    ) -> Result<UpsertLoginOutcome, DbError> {
        let statement = Query::insert()
            .into_table(Users::Table)
            .columns(user_columns())
            .values_panic([
                Expr::val(Uuid::new_v4().to_string()),
                Expr::val(issuer.to_string()),
                Expr::val(subject.to_string()),
                Expr::val(display_name.map(ToString::to_string)),
                Expr::val(now_unix_nanos),
                Expr::val(now_unix_nanos),
            ])
            .on_conflict(
                OnConflict::column(Users::Subject)
                    .update_columns([Users::DisplayName, Users::LastLoginAtUnixNanos])
                    .action_and_where(Expr::col((Users::Table, Users::Issuer)).eq(issuer))
                    .to_owned(),
            )
            .to_owned();
        self.session.execute(statement).await?;

        let record = self.get_by_subject(subject).await?.ok_or_else(|| {
            DbError::CorruptData(
                "upserted login row is missing from the user directory".to_string(),
            )
        })?;
        if record.issuer == issuer {
            Ok(UpsertLoginOutcome::Upserted(record))
        } else {
            Ok(UpsertLoginOutcome::IssuerMismatch {
                stored_issuer: record.issuer,
            })
        }
    }

    pub(crate) async fn get_by_user_id(
        &mut self,
        user_id: &str,
    ) -> Result<Option<UserRecord>, DbError> {
        let statement = select_users()
            .and_where(Expr::col(Users::UserId).eq(user_id))
            .to_owned();
        self.session.fetch_optional(statement).await
    }

    pub(crate) async fn list(&mut self) -> Result<Vec<UserRecord>, DbError> {
        let statement = select_users()
            .order_by(Users::CreatedAtUnixNanos, Order::Asc)
            .order_by(Users::UserId, Order::Asc)
            .to_owned();
        self.session.fetch_all(statement).await
    }

    async fn get_by_subject(&mut self, subject: &str) -> Result<Option<UserRecord>, DbError> {
        let statement = select_users()
            .and_where(Expr::col(Users::Subject).eq(subject))
            .to_owned();
        self.session.fetch_optional(statement).await
    }
}

fn select_users() -> SelectStatement {
    Query::select()
        .columns(user_columns())
        .from(Users::Table)
        .to_owned()
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
    use tempfile::{TempDir, tempdir};

    use super::{UpsertLoginOutcome, UserRecord};
    use crate::bootstrap;
    use crate::state::db::{CoralDb, DbRepos, ResolvedDatabaseConfig};

    #[tokio::test]
    async fn users_repository_contract_holds_against_sqlite() {
        let (db, _temp) = open_sqlite().await;
        assert_users_repository_contract(&db).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared contract against Postgres"]
    async fn users_repository_contract_on_postgres() {
        let Some(db) = open_postgres().await else {
            return;
        };
        assert_users_repository_contract(&db).await;
    }

    #[tokio::test]
    async fn users_repository_first_login_race_contract_holds_against_sqlite() {
        let (db, _temp) = open_sqlite().await;
        assert_first_login_race_converges(&db).await;
    }

    #[tokio::test]
    #[ignore = "set CORAL_TEST_POSTGRES_URL to run the shared contract against Postgres"]
    async fn users_repository_first_login_race_contract_on_postgres() {
        let Some(db) = open_postgres().await else {
            return;
        };
        assert_first_login_race_converges(&db).await;
    }

    async fn assert_users_repository_contract(db: &CoralDb) {
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let issuer = format!("https://issuer.test/{suffix}");
        let subject = format!("subject_{suffix}");

        let first = assert_first_login_mints_user(db, &issuer, &subject).await;
        let refreshed = assert_same_issuer_login_refreshes(db, &issuer, &subject, &first).await;
        assert_issuer_mismatch_preserves_row(db, &suffix, &subject, &refreshed).await;
        assert_list_orders_by_creation(db, &issuer, &subject, &refreshed).await;
    }

    async fn assert_first_login_mints_user(
        db: &CoralDb,
        issuer: &str,
        subject: &str,
    ) -> UserRecord {
        let mut session = db;
        let first = expect_upserted(
            session
                .users()
                .upsert_login(issuer, subject, Some("First Name"), 100)
                .await
                .expect("first login"),
        );
        assert_eq!(
            uuid::Uuid::parse_str(&first.user_id)
                .expect("user id is a UUID")
                .get_version(),
            Some(uuid::Version::Random)
        );
        assert_eq!(
            first,
            UserRecord {
                user_id: first.user_id.clone(),
                issuer: issuer.to_string(),
                subject: subject.to_string(),
                display_name: Some("First Name".to_string()),
                created_at_unix_nanos: 100,
                last_login_at_unix_nanos: 100,
            }
        );
        first
    }

    async fn assert_same_issuer_login_refreshes(
        db: &CoralDb,
        issuer: &str,
        subject: &str,
        first: &UserRecord,
    ) -> UserRecord {
        let mut session = db;
        // A same-issuer login keeps the internal id and creation time while the
        // design's literal contract refreshes the provider-supplied name, so an
        // omitted claim clears it.
        let refreshed = expect_upserted(
            session
                .users()
                .upsert_login(issuer, subject, None, 200)
                .await
                .expect("same-issuer login"),
        );
        assert_eq!(
            refreshed,
            UserRecord {
                display_name: None,
                last_login_at_unix_nanos: 200,
                ..first.clone()
            }
        );

        assert_eq!(
            session
                .users()
                .get_by_user_id(&first.user_id)
                .await
                .expect("look up user"),
            Some(refreshed.clone())
        );
        assert_eq!(
            session
                .users()
                .get_by_user_id(&format!("{}_missing", first.user_id))
                .await
                .expect("look up missing user"),
            None
        );
        refreshed
    }

    async fn assert_issuer_mismatch_preserves_row(
        db: &CoralDb,
        suffix: &str,
        subject: &str,
        stored: &UserRecord,
    ) {
        let mut session = db;
        let mismatch = session
            .users()
            .upsert_login(
                &format!("https://other.test/{suffix}"),
                subject,
                Some("Rebound"),
                300,
            )
            .await
            .expect("reused subject under another issuer");
        assert_eq!(
            mismatch,
            UpsertLoginOutcome::IssuerMismatch {
                stored_issuer: stored.issuer.clone(),
            }
        );
        assert_eq!(
            session
                .users()
                .get_by_user_id(&stored.user_id)
                .await
                .expect("look up user after mismatch"),
            Some(stored.clone()),
            "a mismatched login must leave the stored row untouched"
        );
    }

    async fn assert_list_orders_by_creation(
        db: &CoralDb,
        issuer: &str,
        subject: &str,
        stored: &UserRecord,
    ) {
        let mut session = db;
        let earlier = expect_upserted(
            session
                .users()
                .upsert_login(issuer, &format!("{subject}_earlier"), None, 50)
                .await
                .expect("earlier login"),
        );
        // Shares the stored row's creation time, so only the user-id tiebreak
        // makes the listing order deterministic.
        let tied = expect_upserted(
            session
                .users()
                .upsert_login(issuer, &format!("{subject}_tied"), None, 200)
                .await
                .expect("tied login"),
        );

        let mut expected = vec![earlier, stored.clone(), tied];
        expected.sort_by(|left, right| {
            (left.created_at_unix_nanos, &left.user_id)
                .cmp(&(right.created_at_unix_nanos, &right.user_id))
        });
        let listed: Vec<UserRecord> = session
            .users()
            .list()
            .await
            .expect("list users")
            .into_iter()
            .filter(|record| expected.iter().any(|user| user.user_id == record.user_id))
            .collect();
        assert_eq!(listed, expected);
    }

    async fn assert_first_login_race_converges(db: &CoralDb) {
        let suffix = uuid::Uuid::new_v4().simple().to_string();
        let issuer = format!("https://issuer.test/{suffix}");
        let subject = format!("subject_{suffix}");
        let (mut first, mut second, mut third) = (db, db, db);
        let (mut first, mut second, mut third) = (first.users(), second.users(), third.users());

        let (left, middle, right) = tokio::join!(
            first.upsert_login(&issuer, &subject, Some("A"), 10),
            second.upsert_login(&issuer, &subject, Some("B"), 20),
            third.upsert_login(&issuer, &subject, Some("C"), 30),
        );

        let winner = expect_upserted(left.expect("first concurrent login"));
        for outcome in [
            expect_upserted(middle.expect("second concurrent login")),
            expect_upserted(right.expect("third concurrent login")),
        ] {
            assert_eq!(outcome.user_id, winner.user_id);
            assert_eq!(outcome.created_at_unix_nanos, winner.created_at_unix_nanos);
        }

        let mut session = db;
        let stored = session
            .users()
            .get_by_user_id(&winner.user_id)
            .await
            .expect("look up converged user")
            .expect("converged user exists");
        assert_eq!(stored.subject, subject);
    }

    fn expect_upserted(outcome: UpsertLoginOutcome) -> UserRecord {
        match outcome {
            UpsertLoginOutcome::Upserted(record) => record,
            UpsertLoginOutcome::IssuerMismatch { stored_issuer } => {
                panic!("expected an upserted login, got a mismatch with issuer {stored_issuer}")
            }
        }
    }

    async fn open_sqlite() -> (CoralDb, TempDir) {
        let temp = tempdir().expect("temp dir");
        let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite {
            path: temp.path().join("coral.sqlite"),
        })
        .await
        .expect("open sqlite");
        db.migrate().await.expect("migrate sqlite");
        (db, temp)
    }

    async fn open_postgres() -> Option<CoralDb> {
        let url = bootstrap::env_var("CORAL_TEST_POSTGRES_URL")
            .expect("read CORAL_TEST_POSTGRES_URL")
            .filter(|value| !value.is_empty())?;
        let db = CoralDb::open(ResolvedDatabaseConfig::Postgres { url })
            .await
            .expect("open postgres");
        db.migrate().await.expect("migrate postgres");
        Some(db)
    }
}
