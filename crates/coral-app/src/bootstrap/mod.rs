//! Internal bootstrap seam for assembling the local server runtime.

use std::{
    cmp::Reverse, collections::HashSet, future::Future, path::PathBuf, time::SystemTime,
    time::UNIX_EPOCH,
};

mod consts;
mod env;
mod error;
mod server;

use crate::state::db::{
    CoralDb, DatabaseConfig, DbRepos, ResolvedDatabaseConfig, import_config_source_catalog,
};
use crate::state::{AppStateLayout, ConfigStore};
use crate::telemetry::{
    TelemetryConfig, TraceQueryHistoryEntry, TraceQueryTableFunctionUsage, TraceQueryTableUsage,
};
use crate::workspaces::WorkspaceName;

#[cfg(test)]
pub(crate) use error::MAX_STATUS_DETAIL_BYTES;
pub(crate) use error::{app_status, core_status};

pub use error::AppError;
pub use server::{RunningServer, ServerBuilder, ServerMode, StaticAsset, StaticAssetsProvider};

pub(crate) fn discover_app_state_layout(
    config_dir_override: Option<PathBuf>,
) -> Result<AppStateLayout, AppError> {
    env::AppEnvironment::discover().app_state_layout(config_dir_override)
}

#[cfg(test)]
pub(crate) fn env_var(name: &str) -> Option<String> {
    env::AppEnvironment::env_var(name)
}

/// Loads installed source names for the default workspace from local durable state.
///
/// This is intentionally narrower than starting the local server and calling
/// `ListSources`: startup surfaces such as MCP initialize only need source
/// identity, not enriched source records, manifest versions, or query runtime
/// setup.
///
/// # Errors
///
/// Returns [`AppError`] when the app state layout cannot be discovered or
/// created, or when local config cannot be read or decoded.
pub fn default_workspace_source_names() -> Result<Vec<String>, AppError> {
    let layout = discover_app_state_layout(None)?;
    layout.ensure()?;
    default_workspace_source_names_for_layout(layout)
}

fn default_workspace_source_names_for_layout(
    layout: AppStateLayout,
) -> Result<Vec<String>, AppError> {
    let config_store = ConfigStore::new(layout.clone());
    run_db_bootstrap_operation(async move {
        let db = open_initialized_database(&layout, &config_store).await?;
        let mut session = &db;
        session
            .sources()
            .list_workspace_source_names(&WorkspaceName::default())
            .await
            .map_err(AppError::from)
    })
}

async fn open_initialized_database(
    layout: &AppStateLayout,
    config_store: &ConfigStore,
) -> Result<CoralDb, AppError> {
    let db = open_bootstrap_db(layout).await?;
    db.migrate().await?;
    import_config_source_catalog(&db, config_store, layout, now_unix_nanos_i64()?).await?;
    Ok(db)
}

async fn open_bootstrap_db(layout: &AppStateLayout) -> Result<CoralDb, AppError> {
    let database_config = DatabaseConfig::load(layout)?;
    let database_config = match database_config {
        DatabaseConfig::Sqlite { path } => ResolvedDatabaseConfig::Sqlite { path },
        DatabaseConfig::Postgres { url_env } => {
            let url = env::AppEnvironment::env_var(&url_env).ok_or_else(|| {
                AppError::FailedPrecondition(format!(
                    "database backend 'postgres' requires environment variable `{url_env}`"
                ))
            })?;
            ResolvedDatabaseConfig::Postgres { url }
        }
    };
    CoralDb::open(database_config).await.map_err(AppError::from)
}

fn run_db_bootstrap_operation<T, F>(operation: F) -> Result<T, AppError>
where
    T: Send + 'static,
    F: Future<Output = Result<T, AppError>> + Send + 'static,
{
    fn run_on_runtime<T, F>(operation: F) -> Result<T, AppError>
    where
        F: Future<Output = Result<T, AppError>>,
    {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|error| {
                AppError::FailedPrecondition(format!(
                    "failed to create bootstrap database runtime: {error}"
                ))
            })?;
        runtime.block_on(operation)
    }

    if tokio::runtime::Handle::try_current().is_ok() {
        return std::thread::spawn(move || run_on_runtime(operation))
            .join()
            .map_err(|_panic| {
                AppError::FailedPrecondition(
                    "bootstrap database operation thread panicked".to_string(),
                )
            })?;
    }

    run_on_runtime(operation)
}

fn now_unix_nanos_i64() -> Result<i64, AppError> {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| AppError::FailedPrecondition(format!("system clock error: {error}")))?
        .as_nanos();
    i64::try_from(nanos).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "system clock timestamp exceeds i64 nanoseconds: {error}"
        ))
    })
}

/// Startup context loaded from local state for default-workspace MCP sessions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DefaultWorkspaceMcpStartupContext {
    source_names: Vec<String>,
    query_history: Vec<McpQueryHistoryEntry>,
}

impl DefaultWorkspaceMcpStartupContext {
    /// Installed source names in the default workspace.
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
    sql: String,
    sources: Vec<String>,
    tables: Vec<McpQueryTableUsage>,
    table_functions: Vec<McpQueryTableFunctionUsage>,
    row_count: u64,
}

impl McpQueryHistoryEntry {
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

/// Loads default-workspace source names and trace-backed query history without
/// starting the local server.
///
/// # Errors
///
/// Returns [`AppError`] when local app state, config, or trace history cannot
/// be read. Older trace records that do not carry query provenance are ignored.
pub fn default_workspace_mcp_startup_context(
    query_history_limit: usize,
) -> Result<DefaultWorkspaceMcpStartupContext, AppError> {
    let layout = discover_app_state_layout(None)?;
    layout.ensure()?;
    default_workspace_mcp_startup_context_for_layout(&layout, query_history_limit)
}

fn default_workspace_mcp_startup_context_for_layout(
    layout: &AppStateLayout,
    query_history_limit: usize,
) -> Result<DefaultWorkspaceMcpStartupContext, AppError> {
    let source_names = default_workspace_source_names_for_layout(layout.clone())?;
    let telemetry_config = TelemetryConfig::load(layout)?;
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
        .map(McpQueryHistoryEntry::from_trace)
        .collect::<Vec<_>>();
        select_mcp_query_history(query_history, query_history_limit)
    } else {
        Vec::new()
    };

    Ok(DefaultWorkspaceMcpStartupContext {
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
    use std::collections::BTreeMap;

    use super::{
        DefaultWorkspaceMcpStartupContext, McpQueryHistoryEntry, McpQueryTableFunctionUsage,
        McpQueryTableUsage, default_workspace_mcp_startup_context_for_layout,
        select_mcp_query_history,
    };
    use crate::sources::SourceName;
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::workspaces::WorkspaceName;

    #[test]
    fn mcp_startup_context_imports_legacy_config_sources_into_database() {
        let temp = tempfile::tempdir().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("coral"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let workspace = WorkspaceName::default();
        let source = InstalledSource {
            name: SourceName::parse("github").expect("source name"),
            version: Some("1.2.3".to_string()),
            variables: BTreeMap::new(),
            secrets: Vec::new(),
            credential_storage: None,
            origin: SourceOrigin::Bundled,
        };
        config_store
            .upsert_source(&workspace, source)
            .expect("seed legacy config source");

        let context = default_workspace_mcp_startup_context_for_layout(&layout, 5)
            .expect("load startup context");

        assert_eq!(
            context,
            DefaultWorkspaceMcpStartupContext {
                source_names: vec!["github".to_string()],
                query_history: Vec::new(),
            }
        );
    }

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
