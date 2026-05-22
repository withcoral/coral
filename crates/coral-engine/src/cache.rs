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

/// Normalizes SQL without rewriting literal contents.
#[must_use]
pub fn normalize_sql(sql: &str) -> String {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let mut normalized = String::with_capacity(trimmed.len());
    let mut chars = trimmed.chars().peekable();
    let mut pending_space = false;
    enum Mode {
        Normal,
        SingleQuote,
        DoubleQuote,
        Backtick,
        LineComment,
        BlockComment,
    }
    let mut mode = Mode::Normal;

    while let Some(ch) = chars.next() {
        match mode {
            Mode::Normal => match ch {
                '\'' => {
                    if pending_space && !normalized.is_empty() {
                        normalized.push(' ');
                    }
                    pending_space = false;
                    normalized.push(ch);
                    mode = Mode::SingleQuote;
                }
                '"' => {
                    if pending_space && !normalized.is_empty() {
                        normalized.push(' ');
                    }
                    pending_space = false;
                    normalized.push(ch);
                    mode = Mode::DoubleQuote;
                }
                '`' => {
                    if pending_space && !normalized.is_empty() {
                        normalized.push(' ');
                    }
                    pending_space = false;
                    normalized.push(ch);
                    mode = Mode::Backtick;
                }
                '-' if chars.peek().is_some_and(|next| *next == '-') => {
                    chars.next();
                    mode = Mode::LineComment;
                }
                '/' if chars.peek().is_some_and(|next| *next == '*') => {
                    chars.next();
                    mode = Mode::BlockComment;
                }
                ch if ch.is_whitespace() => {
                    pending_space = true;
                }
                _ => {
                    if pending_space && !normalized.is_empty() {
                        normalized.push(' ');
                    }
                    pending_space = false;
                    normalized.push(ch);
                }
            },
            Mode::SingleQuote => {
                normalized.push(ch);
                if ch == '\'' {
                    if chars.peek().is_some_and(|next| *next == '\'') {
                        normalized.push(chars.next().expect("peeked next char"));
                    } else {
                        mode = Mode::Normal;
                    }
                }
            }
            Mode::DoubleQuote => {
                normalized.push(ch);
                if ch == '"' {
                    if chars.peek().is_some_and(|next| *next == '"') {
                        normalized.push(chars.next().expect("peeked next char"));
                    } else {
                        mode = Mode::Normal;
                    }
                }
            }
            Mode::Backtick => {
                normalized.push(ch);
                if ch == '`' {
                    mode = Mode::Normal;
                }
            }
            Mode::LineComment => {
                if ch == '\n' {
                    pending_space = true;
                    mode = Mode::Normal;
                }
            }
            Mode::BlockComment => {
                if ch == '*' && chars.peek().is_some_and(|next| *next == '/') {
                    chars.next();
                    pending_space = true;
                    mode = Mode::Normal;
                }
            }
        }
    }

    normalized.trim().to_string()
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

        assert_eq!(normalize_sql(with_double_space), "select 'a  b' as value");
        assert_eq!(normalize_sql(with_single_space), "select 'a b' as value");
        assert_ne!(normalize_sql(with_double_space), normalize_sql(with_single_space));
    }

    #[test]
    fn normalize_sql_collapses_formatting_outside_literals() {
        let sql = "  select   *   from   foo  where  id = 1  -- comment\n";
        assert_eq!(normalize_sql(sql), "select * from foo where id = 1");
    }
}
