//! RDBMS-backed durable app-state infrastructure.

mod backend;
mod clock;
mod config;
mod coral_db;
mod error;
mod import;
mod migrations;
mod repositories;
mod schema;
mod session;
mod transaction;

pub(crate) use clock::now_unix_nanos_i64;
pub(crate) use config::{DatabaseConfig, ResolvedDatabaseConfig};
pub(crate) use coral_db::CoralDb;
pub(crate) use error::DbError;
pub(crate) use import::run_state_migrations;
#[cfg_attr(
    not(test),
    expect(unused_imports, reason = "B2c wires identity document writes")
)]
pub(crate) use repositories::identity_specs::{
    IdentitySpecDocumentRecord, IdentitySpecDocumentWrite, IdentitySpecKey, IdentitySpecRecord,
    IdentitySpecScope, IdentitySpecWrite,
};
pub(crate) use session::{DbRepos, DbSession};
pub(crate) use transaction::CoralTx;
