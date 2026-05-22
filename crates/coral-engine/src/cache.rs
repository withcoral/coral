//! Deterministic query cache fingerprinting for app-side result caching.

use sha2::{Digest, Sha256};

/// Query operation categories that participate in cache fingerprinting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryCacheOperation {
    /// Materialized query execution.
    ExecuteSql,
    /// Logical and physical plan rendering.
    ExplainSql,
    /// Workspace catalog discovery.
    ListTables,
    /// Workspace catalog discovery with table functions.
    ListCatalog,
    /// Source validation and declared test queries.
    ValidateSource,
}

impl QueryCacheOperation {
    fn as_str(self) -> &'static str {
        match self {
            Self::ExecuteSql => "execute_sql",
            Self::ExplainSql => "explain_sql",
            Self::ListTables => "list_tables",
            Self::ListCatalog => "list_catalog",
            Self::ValidateSource => "validate_source",
        }
    }
}

/// Canonical inputs used to derive a query cache fingerprint.
#[derive(Debug, Clone)]
pub struct QueryCacheInput<'a> {
    /// Query or metadata operation being cached.
    pub operation: QueryCacheOperation,
    /// Workspace being queried.
    pub workspace_name: &'a str,
    /// Normalized SQL text, if the operation is SQL-backed.
    pub sql: Option<&'a str>,
    /// Stable source/runtime fingerprint supplied by the app layer.
    pub source_fingerprint: &'a str,
    /// Execution settings and request-scoped qualifiers.
    pub execution_settings: &'a [&'a str],
}

/// Normalizes SQL by trimming and collapsing whitespace.
#[must_use]
pub fn normalize_sql(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Builds a deterministic fingerprint for one cacheable query or metadata request.
#[must_use]
pub fn query_cache_fingerprint(input: QueryCacheInput<'_>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.operation.as_str().as_bytes());
    hasher.update(b"\0");
    hasher.update(input.workspace_name.as_bytes());
    hasher.update(b"\0");
    if let Some(sql) = input.sql {
        hasher.update(normalize_sql(sql).as_bytes());
    }
    hasher.update(b"\0");
    hasher.update(input.source_fingerprint.as_bytes());
    hasher.update(b"\0");
    for setting in input.execution_settings {
        hasher.update(setting.as_bytes());
        hasher.update(b"\0");
    }
    format!("{:x}", hasher.finalize())
}
