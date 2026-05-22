//! Deterministic query cache fingerprinting for app-side result caching.

use sha2::{Digest, Sha256};
use datafusion::sql::sqlparser::dialect::GenericDialect;
use datafusion::sql::sqlparser::parser::Parser;

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

/// Normalizes SQL without rewriting literal contents.
#[must_use]
pub fn normalize_sql(sql: &str) -> String {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let dialect = GenericDialect {};
    match Parser::parse_sql(&dialect, trimmed) {
        Ok(statements) => statements
            .into_iter()
            .map(|statement| statement.to_string())
            .collect::<Vec<_>>()
            .join("; "),
        Err(_) => trimmed.to_string(),
    }
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

#[cfg(test)]
mod tests {
    use super::normalize_sql;

    #[test]
    fn normalize_sql_preserves_string_literal_whitespace() {
        let with_double_space = "select 'a  b' as value";
        let with_single_space = "select 'a b' as value";

        assert_eq!(normalize_sql(with_double_space), "SELECT 'a  b' AS value");
        assert_eq!(normalize_sql(with_single_space), "SELECT 'a b' AS value");
        assert_ne!(normalize_sql(with_double_space), normalize_sql(with_single_space));
    }
}
