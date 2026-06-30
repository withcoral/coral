use sea_query::{Expr, ExprTrait, Query};

use crate::state::db::schema::AppStateMarkers;
use crate::state::db::{DbError, DbSession};

#[derive(Debug, sqlx::FromRow)]
struct AppStateMarkerRow {
    key: String,
}

pub(crate) struct AppStateMarkersRepo<'a, S> {
    session: &'a mut S,
}

impl<'a, S> AppStateMarkersRepo<'a, S>
where
    S: DbSession,
{
    pub(crate) fn new(session: &'a mut S) -> Self {
        Self { session }
    }

    pub(crate) async fn contains(&mut self, key: &str) -> Result<bool, DbError> {
        let statement = Query::select()
            .column(AppStateMarkers::Key)
            .from(AppStateMarkers::Table)
            .and_where(Expr::col(AppStateMarkers::Key).eq(key))
            .to_owned();
        let row: Option<AppStateMarkerRow> = self.session.fetch_optional(statement).await?;
        Ok(row.is_some_and(|row| row.key == key))
    }

    pub(crate) async fn insert(
        &mut self,
        key: &str,
        created_at_unix_nanos: i64,
    ) -> Result<(), DbError> {
        let statement = Query::insert()
            .into_table(AppStateMarkers::Table)
            .columns([AppStateMarkers::Key, AppStateMarkers::CreatedAtUnixNanos])
            .values_panic([key.into(), created_at_unix_nanos.into()])
            .to_owned();
        self.session.execute(statement).await
    }
}
