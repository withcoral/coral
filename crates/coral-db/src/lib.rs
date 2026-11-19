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
//! - [`Transaction::fetch_optional_locked`] reads a row under a [`RowLock`].
//!   On Postgres a shared hold lets concurrent decisions read side by side
//!   while a writer of that row waits for them, and an exclusive hold makes
//!   a shared hold, another exclusive hold and a write of that row wait; on
//!   SQLite a hold adds nothing, since the write transaction already excludes
//!   writers.
//! - [`PageRequest`] and [`Page`] are one forward page of a list that its
//!   owner reads by key; [`PageKey`] is the key's text form on the wire, and
//!   [`text_key_order`] orders a text key alike on both databases.
//! - [`Transaction::execute_sql`] and [`Transaction::execute_script`] run SQL a
//!   domain repository wrote for the selected backend, for features the
//!   portable statements cannot express.
//! - [`ReadOnlyDatabase`] permits pre-migration inspection without write
//!   authority or access to a concrete connection.
//! - [`UnixNanos`] is an instant as the tables store it: a statement binds
//!   one and a row decodes one as the integer its column holds.
//!   [`SystemClock`] is the one reader of the host's clock, behind [`Clock`].
//! - [`Version`] is the count of accepted changes that a row keeps of itself,
//!   and owns the one text form of that count.
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

// `tests/persistence_foundation.rs` uses `tracing`; the unit tests do not.
#[cfg(test)]
use tracing as _;

mod backend;
mod changes;
mod database;
mod error;
mod executor;
mod migration;
mod options;
mod page;
mod query;
mod statements;
mod time;
mod transaction;
mod upgrade;
mod version;

pub use backend::DatabaseSystem;
pub use changes::ChangeSubscription;
pub use database::{Database, ReadOnlyDatabase, validate_postgres_url};
pub use error::{Error, sqlx_error_is_serialization_conflict, sqlx_error_is_unique_violation};
pub use executor::QueryExecutor;
pub use migration::{MigrationPlan, MigrationProgress, MigrationReport, migration_plan};
pub use options::DatabaseOptions;
pub use page::{Page, PageKey, PageRequest, PageSize, text_key_order};
pub use query::{col, column};
pub use time::{Clock, SystemClock, UnixNanos};
pub use transaction::{RowLock, Transaction, TransactionToken};
pub use version::{InvalidVersion, Version};
