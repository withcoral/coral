//! RDBMS-backed durable app-state infrastructure.

mod backend;
mod config;
mod coral_db;
mod error;
mod migrations;

pub(crate) use config::{DatabaseConfig, ResolvedDatabaseConfig};
pub(crate) use coral_db::CoralDb;
pub(crate) use error::DbError;
