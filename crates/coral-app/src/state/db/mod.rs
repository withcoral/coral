//! RDBMS-backed durable app-state infrastructure.

use crate::bootstrap::AppError;

mod backend;
mod config;
mod coral_db;
mod error;
mod import;
mod migrations;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Repository harness stays test-only until manager wiring lands in later stack PRs."
    )
)]
mod repositories;
mod schema;
mod session;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Database transactions stay test-only until repositories are wired into managers."
    )
)]
mod transaction;

pub(crate) use config::{DatabaseConfig, ResolvedDatabaseConfig};
pub(crate) use coral_db::CoralDb;
pub(crate) use error::DbError;
pub(crate) use import::import_legacy_config;
pub(crate) use session::{DbRepos, DbSession};
pub(crate) use transaction::CoralTx;

pub(crate) fn now_unix_nanos_i64() -> Result<i64, AppError> {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|error| {
            AppError::FailedPrecondition(format!("system clock before Unix epoch: {error}"))
        })?
        .as_nanos();
    i64::try_from(nanos).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "system clock timestamp exceeds i64 nanoseconds: {error}"
        ))
    })
}
