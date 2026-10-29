//! `SQLite` and Postgres persistence mechanics for Coral.
//!
//! This private foundation owns backend selection, connection pools, short
//! transactions, migration execution, and bounded `SQLite` contention handling.
//! It holds every Module's schema migrations in one plan, with a half for each
//! database where the two differ. It has no generic CRUD repository.
//!
//! # Interface
//!
//! - [`Database::connect`] selects and opens one Adapter from [`DatabaseOptions`].
//! - [`migration_plan`] is the one migration plan for every Module's tables.
//! - [`Database::migrate`] validates and applies the selected database's half
//!   of a [`MigrationPlan`].
//! - [`Database::begin`] and [`Database::begin_read_snapshot`] start short
//!   transactions.
//! - [`Transaction::execute_change`] marks observable mutations; only their commits
//!   wake [`Database::subscribe_changes`] readers. `SQLite` uses the shared application
//!   handle; Postgres propagates invalidations across workers. Notifications are
//!   coalescing hints, not retained Events, payloads, or authority.
//! - [`Database::execute`], [`Database::fetch_optional`], and
//!   [`Database::fetch_all`] execute domain-owned `SeaQuery` statements.
//! - [`QueryExecutor`] lets one domain read run directly against a [`Database`]
//!   or inside a caller-owned [`Transaction`] without duplicating repository
//!   behavior.
//! - [`DatabaseSystem`] lets a domain repository select a backend-specific query
//!   or locking strategy without access to a connection pool.
//! - [`Transaction::fetch_optional_locked`] reads a fact row under a
//!   [`RowLock`]. On Postgres a shared hold lets concurrent decisions read
//!   side by side while a writer of that row waits for them; on SQLite it
//!   adds nothing, since the write transaction already excludes writers.
//! - [`Transaction::execute_sql`] and [`Transaction::execute_script`] run SQL a
//!   domain repository wrote for the selected backend, for features the
//!   portable statements cannot express.
//! - [`ReadOnlyDatabase`] permits pre-migration inspection without write
//!   authority or access to a concrete connection.
//!
//! # Invariants
//!
//! Process bootstrap resolves environment variables before it creates
//! [`DatabaseOptions`]. Domain Modules own schemas, statements, transaction
//! choreography, and error meaning. A migration plan must use direct DDL and
//! must fail on unexpected objects instead of hiding drift with
//! `IF NOT EXISTS`. No remote work belongs inside a database transaction.
//!
//! The `SQLite` Adapter uses WAL mode, foreign keys, and a five-second busy
//! timeout for the supported local single-process topology. The Postgres Adapter
//! uses a pool for remote multi-worker topology and requires `verify-full` TLS
//! for remote TCP connections.

// The metric tests record under a tracing subscriber.
#[cfg(test)]
use tracing as _;

mod backend;
mod changes;
mod database;
mod error;
mod executor;
mod migration;
mod options;
mod query;
mod transaction;

pub use backend::DatabaseSystem;
pub use changes::ChangeSubscription;
pub use database::{Database, ReadOnlyDatabase, validate_postgres_url};
pub use error::{Error, sqlx_error_is_serialization_conflict, sqlx_error_is_unique_violation};
pub use executor::QueryExecutor;
pub use migration::{MigrationPlan, migration_plan};
pub use options::DatabaseOptions;
pub use query::{col, column};
pub use transaction::{RowLock, Transaction, TransactionToken};
