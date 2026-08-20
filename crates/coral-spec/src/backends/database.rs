#![allow(
    missing_docs,
    reason = "This module defines field-heavy database source manifest types."
)]

//! Backend-owned manifest model for relational database sources.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{ManifestInputSpec, ParsedTemplate, SourceManifestCommon};

/// Provider selected by an authored database surface.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseProvider {
    Postgres,
    #[serde(rename = "mysql")]
    MySql,
    Sqlite,
}

impl DatabaseProvider {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Postgres => "postgres",
            Self::MySql => "mysql",
            Self::Sqlite => "sqlite",
        }
    }
}

/// Validated database source manifest consumed by the query engine.
#[derive(Debug, Clone, Serialize)]
pub struct DatabaseSourceManifest {
    pub common: SourceManifestCommon,
    pub connection: DatabaseConnectionSpec,
    /// Skipped when serializing: declared inputs come verbatim from the
    /// authored manifest, which fingerprinting hashes separately.
    #[serde(skip)]
    pub declared_inputs: Vec<ManifestInputSpec>,
}

/// Provider-specific database connection configuration.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DatabaseConnectionSpec {
    Postgres(PostgresConnectionSpec),
    #[serde(rename = "mysql")]
    MySql(MySqlConnectionSpec),
    Sqlite(SqliteConnectionSpec),
}

/// `PostgreSQL` connection configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PostgresConnectionSpec {
    pub host: ParsedTemplate,
    pub port: ParsedTemplate,
    pub database: ParsedTemplate,
    pub user: ParsedTemplate,
    pub password: ParsedTemplate,
    #[serde(default)]
    pub sslmode: Option<ParsedTemplate>,
}

/// `MySQL` connection configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MySqlConnectionSpec {
    pub host: ParsedTemplate,
    pub port: ParsedTemplate,
    pub database: ParsedTemplate,
    pub user: ParsedTemplate,
    pub password: ParsedTemplate,
}

/// `SQLite` connection configuration.
#[derive(Debug, Clone, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SqliteConnectionSpec {
    pub path: ParsedTemplate,
}
