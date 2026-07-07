//! RDBMS-backed durable app-state infrastructure.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Phase 1 lands the DB boundary and repository harness before later PRs wire managers to these repositories."
    )
)]

mod backend;
mod config;
mod coral_db;
mod error;
mod import;
mod migrations;
mod repositories;
mod schema;
mod session;
mod transaction;

use std::time::{SystemTime, UNIX_EPOCH};

use crate::bootstrap::AppError;

pub(crate) use config::{DatabaseConfig, ResolvedDatabaseConfig};
pub(crate) use coral_db::CoralDb;
pub(crate) use error::DbError;
pub(crate) use import::import_legacy_config;
pub(crate) use session::{DbRepos, DbSession};
pub(crate) use transaction::CoralTx;

pub(crate) fn now_unix_nanos_i64() -> Result<i64, AppError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| AppError::FailedPrecondition(format!("system clock error: {error}")))?
        .as_nanos();
    i64::try_from(nanos).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "system clock timestamp exceeds i64 nanoseconds: {error}"
        ))
    })
}

#[cfg(test)]
pub(crate) fn open_test_sqlite(layout: &super::AppStateLayout) -> CoralDb {
    fn open_on_runtime(layout: super::AppStateLayout) -> CoralDb {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test sqlite runtime");
        runtime.block_on(async move {
            let DatabaseConfig::Sqlite { path } =
                DatabaseConfig::load(&layout).expect("test db config")
            else {
                panic!("default test config should be sqlite");
            };
            let db = CoralDb::open(ResolvedDatabaseConfig::Sqlite { path })
                .await
                .expect("open test sqlite");
            db.migrate().await.expect("migrate test sqlite");
            db
        })
    }

    let layout = layout.clone();
    if tokio::runtime::Handle::try_current().is_ok() {
        return std::thread::spawn(move || open_on_runtime(layout))
            .join()
            .expect("test sqlite runtime thread");
    }
    open_on_runtime(layout)
}

#[cfg(test)]
pub(crate) fn open_test_sqlite_with_legacy_config(
    layout: &super::AppStateLayout,
    config_store: &super::ConfigStore,
) -> CoralDb {
    let db = open_test_sqlite(layout);
    import_test_legacy_config(&db, config_store);
    db
}

#[cfg(test)]
pub(crate) fn import_test_legacy_config(db: &CoralDb, config_store: &super::ConfigStore) {
    async fn import(db: &CoralDb, config_store: &super::ConfigStore) -> Result<(), AppError> {
        import_legacy_config(db, config_store).await?;
        Ok(())
    }

    fn import_on_runtime(db: &CoralDb, config_store: &super::ConfigStore) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test sqlite runtime");
        runtime
            .block_on(import(db, config_store))
            .expect("import test legacy config");
    }

    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::scope(|scope| {
            scope
                .spawn(|| import_on_runtime(db, config_store))
                .join()
                .expect("import test legacy config thread");
        });
        return;
    }
    import_on_runtime(db, config_store);
}

#[cfg(test)]
pub(crate) fn upsert_test_source(
    db: &CoralDb,
    workspace_name: &crate::workspaces::WorkspaceName,
    source: &crate::sources::model::InstalledSource,
) {
    async fn upsert(
        db: &CoralDb,
        workspace_name: &crate::workspaces::WorkspaceName,
        source: &crate::sources::model::InstalledSource,
    ) -> Result<(), DbError> {
        let mut tx = db.begin().await?;
        tx.workspaces().ensure(workspace_name.as_str(), 1).await?;
        tx.sources()
            .upsert_source(workspace_name, source, 1)
            .await?;
        tx.commit().await
    }

    fn upsert_on_runtime(
        db: &CoralDb,
        workspace_name: &crate::workspaces::WorkspaceName,
        source: &crate::sources::model::InstalledSource,
    ) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test sqlite runtime");
        runtime
            .block_on(upsert(db, workspace_name, source))
            .expect("upsert test source");
    }

    if tokio::runtime::Handle::try_current().is_ok() {
        std::thread::scope(|scope| {
            scope
                .spawn(|| upsert_on_runtime(db, workspace_name, source))
                .join()
                .expect("upsert test source thread");
        });
        return;
    }
    upsert_on_runtime(db, workspace_name, source);
}
