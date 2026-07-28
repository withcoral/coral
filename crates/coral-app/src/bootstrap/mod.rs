//! Internal bootstrap seam for assembling the local server runtime.

use std::{cmp::Reverse, collections::HashSet, path::PathBuf};

mod consts;
mod env;
mod error;
mod health;
mod server;
mod server_config;

use crate::state::AppStateLayout;
use crate::telemetry::{
    TelemetryConfig, TraceQueryHistoryEntry, TraceQueryTableFunctionUsage, TraceQueryTableUsage,
};
use crate::workspaces::WorkspaceName;

#[cfg(test)]
pub(crate) use error::MAX_STATUS_DETAIL_BYTES;
pub(crate) use error::{app_status, core_status, status_with_bounded_detail};

pub use error::AppError;
pub use server::{RunningServer, ServerBuilder, ServerMode, StaticAsset, StaticAssetsProvider};
pub use server_config::McpHttpServeConfig;

pub(crate) fn discover_app_state_layout(
    config_dir_override: Option<PathBuf>,
) -> Result<AppStateLayout, AppError> {
    env::AppEnvironment::discover().app_state_layout(config_dir_override)
}

pub(crate) fn env_var(name: &str) -> Result<Option<String>, std::env::VarError> {
    env::AppEnvironment::env_var(name)
}

/// Reports whether `ip` addresses the local machine.
///
/// Shared by every loopback check in this crate — the `server.mcp_http.bind`
/// and `auth.http_bind_addr` bind guards, and the auth URL validator's
/// loopback-http allowance — so that tightening the rule (for instance, to
/// stop treating `::ffff:127.0.0.1` as loopback) cannot leave one call site
/// more permissive than another.
pub(crate) fn is_loopback_ip(ip: std::net::IpAddr) -> bool {
    ip.is_loopback()
        || matches!(ip, std::net::IpAddr::V6(ip) if ip.to_ipv4_mapped().is_some_and(|ip| ip.is_loopback()))
}

/// Startup context for one workspace's MCP session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceMcpStartupContext {
    workspace_name: String,
    source_names: Vec<String>,
    query_history: Vec<McpQueryHistoryEntry>,
}

impl WorkspaceMcpStartupContext {
    /// Workspace name used to load this startup context.
    #[must_use]
    pub fn workspace_name(&self) -> &str {
        &self.workspace_name
    }

    /// Installed source names supplied by the caller for the selected workspace.
    #[must_use]
    pub fn source_names(&self) -> &[String] {
        &self.source_names
    }

    /// Trace-backed successful query examples.
    #[must_use]
    pub fn query_history(&self) -> &[McpQueryHistoryEntry] {
        &self.query_history
    }
}

/// One successful query-history entry for MCP startup context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpQueryHistoryEntry {
    workspace_name: String,
    sql: String,
    sources: Vec<String>,
    tables: Vec<McpQueryTableUsage>,
    table_functions: Vec<McpQueryTableFunctionUsage>,
    row_count: u64,
}

impl McpQueryHistoryEntry {
    /// Workspace that produced this query span.
    #[must_use]
    pub fn workspace_name(&self) -> &str {
        &self.workspace_name
    }

    /// SQL text recorded on the query span.
    #[must_use]
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Installed source names used by this query.
    #[must_use]
    pub fn sources(&self) -> &[String] {
        &self.sources
    }

    /// Source-scoped tables used by this query.
    #[must_use]
    pub fn tables(&self) -> &[McpQueryTableUsage] {
        &self.tables
    }

    /// Source-scoped table functions used by this query.
    #[must_use]
    pub fn table_functions(&self) -> &[McpQueryTableFunctionUsage] {
        &self.table_functions
    }

    /// Number of rows returned by the successful query.
    #[must_use]
    pub fn row_count(&self) -> u64 {
        self.row_count
    }

    fn from_trace(entry: TraceQueryHistoryEntry) -> Self {
        Self {
            workspace_name: entry.workspace,
            sql: entry.sql,
            sources: entry.sources,
            tables: entry
                .tables
                .into_iter()
                .map(McpQueryTableUsage::from_trace)
                .collect(),
            table_functions: entry
                .table_functions
                .into_iter()
                .map(McpQueryTableFunctionUsage::from_trace)
                .collect(),
            row_count: entry.row_count,
        }
    }
}

/// Source-scoped table usage in an MCP startup query-history entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpQueryTableUsage {
    source: String,
    schema: String,
    table: String,
}

impl McpQueryTableUsage {
    /// Installed source name that owns the table.
    #[must_use]
    pub fn source_name(&self) -> &str {
        &self.source
    }

    /// SQL schema name used in the query.
    #[must_use]
    pub fn schema_name(&self) -> &str {
        &self.schema
    }

    /// SQL table name used in the query.
    #[must_use]
    pub fn table_name(&self) -> &str {
        &self.table
    }

    fn from_trace(usage: TraceQueryTableUsage) -> Self {
        Self {
            source: usage.source,
            schema: usage.schema,
            table: usage.table,
        }
    }
}

/// Source-scoped table-function usage in an MCP startup query-history entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpQueryTableFunctionUsage {
    source: String,
    schema: String,
    function: String,
}

impl McpQueryTableFunctionUsage {
    /// Installed source name that owns the table function.
    #[must_use]
    pub fn source_name(&self) -> &str {
        &self.source
    }

    /// SQL schema name used in the query.
    #[must_use]
    pub fn schema_name(&self) -> &str {
        &self.schema
    }

    /// SQL table-function name used in the query.
    #[must_use]
    pub fn function_name(&self) -> &str {
        &self.function
    }

    fn from_trace(usage: TraceQueryTableFunctionUsage) -> Self {
        Self {
            source: usage.source,
            schema: usage.schema,
            function: usage.function,
        }
    }
}

/// Builds MCP startup context for one workspace from caller-supplied source
/// names and trace-backed query history, without starting another local server.
///
/// # Errors
///
/// Returns [`AppError`] when local app state, telemetry config, or trace history
/// cannot be read. Source names must already come from the normal source
/// service path. Older trace records that do not carry workspace/query
/// provenance are ignored.
pub fn workspace_mcp_startup_context(
    workspace_name: &str,
    source_names: impl IntoIterator<Item = String>,
    query_history_limit: usize,
) -> Result<WorkspaceMcpStartupContext, AppError> {
    let workspace_name = WorkspaceName::parse(workspace_name)?;
    let source_names = source_names.into_iter().collect();
    let layout = discover_app_state_layout(None)?;
    layout.ensure()?;
    let telemetry_config = TelemetryConfig::load(&layout)?;
    let query_history = if telemetry_config.trace_history.enabled {
        let query_history = crate::telemetry::list_local_query_history(
            layout.local_trace_store_dir(),
            telemetry_config.trace_history.retention(),
        )
        .map_err(|error| {
            AppError::FailedPrecondition(format!(
                "failed to read trace-backed query history: {error}"
            ))
        })?
        .into_iter()
        .filter(|entry| entry.workspace.as_str() == workspace_name.as_str())
        .map(McpQueryHistoryEntry::from_trace)
        .collect::<Vec<_>>();
        select_mcp_query_history(query_history, query_history_limit)
    } else {
        Vec::new()
    };

    Ok(WorkspaceMcpStartupContext {
        workspace_name: workspace_name.as_str().to_string(),
        source_names,
        query_history,
    })
}

fn select_mcp_query_history(
    mut query_history: Vec<McpQueryHistoryEntry>,
    limit: usize,
) -> Vec<McpQueryHistoryEntry> {
    if limit == 0 {
        return Vec::new();
    }

    query_history.retain(|entry| entry.row_count > 0);
    query_history.sort_by_key(|entry| Reverse(query_history_sort_key(entry)));
    if query_history.len() <= limit {
        return query_history;
    }

    greedily_select_query_history(query_history, limit)
}

fn query_history_sort_key(entry: &McpQueryHistoryEntry) -> (usize, usize, usize) {
    (
        unique_source_count(&entry.sources),
        entry.tables.len(),
        entry.table_functions.len(),
    )
}

fn unique_source_count(sources: &[String]) -> usize {
    sources.iter().collect::<HashSet<_>>().len()
}

fn greedily_select_query_history(
    mut remaining: Vec<McpQueryHistoryEntry>,
    limit: usize,
) -> Vec<McpQueryHistoryEntry> {
    let mut selected = Vec::new();
    let mut selected_sources = HashSet::new();

    while selected.len() < limit && !remaining.is_empty() {
        let mut best_position = 0;
        let mut best_new_sources = 0;
        for (position, entry) in remaining.iter().enumerate() {
            let new_sources = new_source_count(&entry.sources, &selected_sources);
            if new_sources > best_new_sources {
                best_position = position;
                best_new_sources = new_sources;
            }
        }

        let entry = remaining.remove(best_position);
        for source in &entry.sources {
            selected_sources.insert(source.clone());
        }
        selected.push(entry);
    }

    selected
}

fn new_source_count(sources: &[String], selected_sources: &HashSet<String>) -> usize {
    sources
        .iter()
        .filter(|source| !selected_sources.contains(*source))
        .collect::<HashSet<_>>()
        .len()
}

#[cfg(test)]
mod tests {
    use super::{
        McpQueryHistoryEntry, McpQueryTableFunctionUsage, McpQueryTableUsage,
        select_mcp_query_history,
    };

    #[test]
    fn query_history_selection_filters_empty_results_and_sorts_by_complexity() {
        let selected = select_mcp_query_history(
            vec![
                history_entry("zero_rows", &["sentry"], 10, 10, 0),
                history_entry("one_table", &["github"], 1, 0, 1),
                history_entry("two_sources", &["slack", "linear"], 0, 0, 1),
                history_entry("two_tables", &["github"], 2, 0, 1),
                history_entry("two_tables_one_function", &["github"], 2, 1, 1),
            ],
            10,
        );

        assert_eq!(
            sqls(&selected),
            [
                "two_sources",
                "two_tables_one_function",
                "two_tables",
                "one_table",
            ]
        );
    }

    #[test]
    fn query_history_selection_keeps_top_ranked_query_even_when_not_globally_optimal() {
        let selected = select_mcp_query_history(
            vec![
                history_entry("wide_but_overlapping", &["a", "b", "c"], 3, 0, 1),
                history_entry("covers_e", &["b", "c", "e"], 1, 0, 1),
                history_entry("covers_d", &["a", "d"], 0, 0, 1),
            ],
            2,
        );

        assert_eq!(sqls(&selected), ["wide_but_overlapping", "covers_e"]);
    }

    #[test]
    fn query_history_selection_favors_new_sources_after_top_ranked_query() {
        let selected = select_mcp_query_history(
            vec![
                history_entry("top_ranked", &["a", "b"], 2, 0, 1),
                history_entry("overlap_next_ranked", &["a", "b"], 1, 0, 1),
                history_entry("uncovered_lower_ranked", &["c"], 0, 0, 1),
            ],
            2,
        );

        assert_eq!(sqls(&selected), ["top_ranked", "uncovered_lower_ranked"]);
    }

    #[test]
    fn query_history_selection_breaks_coverage_ties_by_sorted_order() {
        let selected = select_mcp_query_history(
            vec![
                history_entry("first", &["a"], 1, 0, 1),
                history_entry("second", &["b"], 0, 0, 1),
                history_entry("third", &["c"], 0, 0, 1),
            ],
            2,
        );

        assert_eq!(sqls(&selected), ["first", "second"]);
    }

    fn history_entry(
        sql: &str,
        sources: &[&str],
        table_count: usize,
        table_function_count: usize,
        row_count: u64,
    ) -> McpQueryHistoryEntry {
        let table_source = sources.first().copied().unwrap_or("source");
        McpQueryHistoryEntry {
            workspace_name: "default".to_string(),
            sql: sql.to_string(),
            sources: sources.iter().map(|source| (*source).to_string()).collect(),
            tables: (0..table_count)
                .map(|index| McpQueryTableUsage {
                    source: table_source.to_string(),
                    schema: "schema".to_string(),
                    table: format!("table_{index}"),
                })
                .collect(),
            table_functions: (0..table_function_count)
                .map(|index| McpQueryTableFunctionUsage {
                    source: table_source.to_string(),
                    schema: "schema".to_string(),
                    function: format!("function_{index}"),
                })
                .collect(),
            row_count,
        }
    }

    fn sqls(entries: &[McpQueryHistoryEntry]) -> Vec<&str> {
        entries.iter().map(McpQueryHistoryEntry::sql).collect()
    }
}
