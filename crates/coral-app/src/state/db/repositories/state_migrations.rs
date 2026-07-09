use sea_query::{Expr, ExprTrait, Query};

use crate::state::db::schema::AppStateMigrations;
use crate::state::db::{DbError, DbSession};

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

    pub(crate) async fn has_completed(&mut self, id: &str) -> Result<bool, DbError> {
        Ok(self.get(id).await?.is_some())
    }

    pub(crate) async fn mark_completed(
        &mut self,
        id: &str,
        completed_at_unix_nanos: i64,
    ) -> Result<(), DbError> {
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
            .to_owned();
        self.session.execute(statement).await
    }

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
