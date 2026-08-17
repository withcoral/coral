//! Repository-side recovery for shared deployments no human can administer.
//!
//! Possession of the state database is the entire authority here. This module
//! authenticates and authorizes nobody, so recovering a `SQLite` deployment
//! requires filesystem access to the state directory, and recovering a
//! Postgres deployment requires the configured connection URL. No locality of
//! the host is claimed or checked.
//!
//! The tool is deliberately **non-migrating**. It reads and repairs an
//! existing repository database exactly as the server left it and never
//! applies a schema or state migration; bringing a database up to the current
//! schema is the server's job, not recovery's.
//!
//! The module compiles only under `xtask`'s off-by-default `admin` feature, so
//! the default `xtask` build compiles none of this code and pulls in none of
//! the dependencies it needs. No shipped Coral binary depends on `xtask` at
//! all, so none of this reaches a released artifact.

use std::path::PathBuf;

// The recovery commands land in follow-up changes. Until then these imports
// link the admin-only dependencies those commands need, so `--features admin`
// compiles the feature's real dependency set. Drop each line as the matching
// dependency gains a genuine use.
use etcetera as _;
use sqlx as _;
use tokio as _;
use toml as _;

/// The existing state database one recovery command operates on.
///
/// Configuration resolution follows the server's own rules and yields one of
/// these variants; every command then works against the already-migrated
/// database it names.
#[derive(Debug, Clone, PartialEq, Eq)]
#[expect(
    dead_code,
    reason = "the commands that resolve and open this database land in a follow-up change"
)]
pub(crate) enum RecoveryDatabase {
    /// A `SQLite` state file inside the resolved state directory.
    Sqlite {
        /// Path to the existing state database file.
        path: PathBuf,
    },
    /// A Postgres database reached through the resolved connection URL.
    Postgres {
        /// Connection URL taken from the configured environment variable.
        url: String,
    },
}

/// The built-in local principal, which recovery refuses to appoint or rebind.
///
/// It is synthetic: it identifies the single-user local process rather than a
/// person who can authenticate against a shared deployment. Making it an owner
/// would leave the workspace exactly as unreachable as before.
#[expect(
    dead_code,
    reason = "the commands that refuse this identity land in a follow-up change"
)]
pub(crate) const LOCAL_PRINCIPAL_ID: &str = "coral:local";
