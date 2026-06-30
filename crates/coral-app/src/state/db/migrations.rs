use super::backend::CoralDbBackend;
use super::{CoralDb, DbError};

impl CoralDb {
    pub(crate) async fn migrate(&self) -> Result<(), DbError> {
        match &self.backend {
            CoralDbBackend::Sqlite(db) => {
                sqlx::migrate!("./migrations").run(&db.pool).await?;
            }
            CoralDbBackend::Postgres(db) => {
                sqlx::migrate!("./migrations").run(&db.pool).await?;
            }
        }
        Ok(())
    }
}
