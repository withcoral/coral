//! RDBMS-backed durable app-state infrastructure.

#![expect(
    dead_code,
    reason = "Phase 1 lands the DB boundary and repository harness before later PRs wire managers to these repositories."
)]

mod backend;
mod config;
mod coral_db;
mod error;
mod migrations;

pub(crate) use config::{DatabaseConfig, ResolvedDatabaseConfig};
pub(crate) use coral_db::CoralDb;
pub(crate) use error::DbError;
