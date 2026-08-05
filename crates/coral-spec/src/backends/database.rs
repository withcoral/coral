#![allow(
    missing_docs,
    reason = "This module defines field-heavy database source manifest types."
)]

//! Backend-owned manifest model for relational database sources.

use schemars::JsonSchema;
use serde::Deserialize;

use crate::{ManifestInputSpec, ParsedTemplate, SourceManifestCommon};

/// Validated database source manifest consumed by the query engine.
#[derive(Debug, Clone)]
pub struct DatabaseSourceManifest {
    pub common: SourceManifestCommon,
    pub connection: DatabaseConnectionSpec,
    pub declared_inputs: Vec<ManifestInputSpec>,
}

/// Provider-specific database connection configuration.
#[derive(Debug, Clone)]
pub enum DatabaseConnectionSpec {
    Postgres(PostgresConnectionSpec),
    MySql(MySqlConnectionSpec),
    Sqlite(SqliteConnectionSpec),
}

/// `PostgreSQL` connection configuration.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
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
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MySqlConnectionSpec {
    pub host: ParsedTemplate,
    pub port: ParsedTemplate,
    pub database: ParsedTemplate,
    pub user: ParsedTemplate,
    pub password: ParsedTemplate,
}

/// `SQLite` connection configuration.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SqliteConnectionSpec {
    pub path: ParsedTemplate,
}
