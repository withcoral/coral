//! RDBMS-backed durable app-state infrastructure.

mod backend;
mod config;
mod coral_db;
mod error;
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
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "Database sessions stay test-only until repositories are wired into managers."
    )
)]
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
pub(crate) use session::DbSession;
pub(crate) use transaction::CoralTx;
