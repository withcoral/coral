//! RDBMS-backed durable app-state infrastructure.

mod backend;
mod clock;
mod config;
mod coral_db;
mod error;
mod import;
mod migrations;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Some repository helpers stay test-only until later catalog surfaces use them."
    )
)]
mod repositories;
mod schema;
mod session;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Rollback support stays test-only until later write paths need explicit rollback."
    )
)]
mod transaction;

pub(crate) use clock::now_unix_nanos_i64;
pub(crate) use config::{DatabaseConfig, ResolvedDatabaseConfig};
pub(crate) use coral_db::CoralDb;
pub(crate) use error::DbError;
pub(crate) use import::import_legacy_config;
pub(crate) use transaction::CoralTx;
