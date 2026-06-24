//! Workspace-scoped query history for MCP initialize examples.

use std::collections::{BTreeSet, HashSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use coral_engine::{QueryResultObserver, QueryResultObserverError, TableRef};
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::episode::store::now_unix_nanos;
use crate::state::AppStateLayout;
use crate::storage::fs::{self as storage_fs, FileLock};
use crate::workspaces::WorkspaceName;

const MAX_QUERY_HISTORY_BYTES_PER_WORKSPACE: u64 = 16 * 1024 * 1024;

/// One table referenced by a stored example query.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ExampleQueryTable {
    /// Coral source/schema name.
    pub source: String,
    /// Table name inside the source/schema.
    pub table: String,
}

impl From<&TableRef> for ExampleQueryTable {
    fn from(table: &TableRef) -> Self {
        Self {
            source: table.source.clone(),
            table: table.table.clone(),
        }
    }
}

/// Query-history record eligible for MCP initialize recaps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExampleQuery {
    /// SQL text as run locally.
    pub sql: String,
    /// Number of rows returned by the successful query.
    pub row_count: u64,
    /// Distinct referenced tables.
    pub tables: Vec<ExampleQueryTable>,
    /// Distinct referenced sources, sorted for cheap selection.
    pub sources: Vec<String>,
    /// Query completion timestamp, unix nanoseconds.
    pub created_at_unix_nanos: u128,
}

/// Errors from query-history storage.
#[derive(Debug, thiserror::Error)]
pub(crate) enum QueryHistoryError {
    /// Filesystem error reading or writing the store.
    #[error("query history io: {0}")]
    Io(#[from] std::io::Error),
    /// JSON serialization error.
    #[error("query history serialization: {0}")]
    Serde(#[from] serde_json::Error),
}

/// Best-effort query-result observer that records successful SQL metadata.
#[derive(Debug, Clone)]
pub(crate) struct QueryHistoryRecorder {
    store: QueryHistoryStore,
    workspace: WorkspaceName,
}

impl QueryHistoryRecorder {
    pub(crate) fn new(layout: AppStateLayout, workspace: WorkspaceName) -> Self {
        Self {
            store: QueryHistoryStore::new(layout),
            workspace,
        }
    }
}

impl QueryResultObserver for QueryHistoryRecorder {
    fn name(&self) -> &'static str {
        "query_history"
    }

    fn observe_result(
        &self,
        sql: &str,
        _schema: &Schema,
        batches: &[RecordBatch],
        tables: &[TableRef],
    ) -> Result<(), QueryResultObserverError> {
        if let Err(error) = self
            .store
            .record_success(&self.workspace, sql, tables, batches)
        {
            warn!(%error, "failed to record query history");
        }
        Ok(())
    }
}

/// Append-only, per-workspace JSONL query-history store.
#[derive(Debug, Clone)]
struct QueryHistoryStore {
    layout: AppStateLayout,
    max_bytes: u64,
}

impl QueryHistoryStore {
    fn new(layout: AppStateLayout) -> Self {
        Self {
            layout,
            max_bytes: MAX_QUERY_HISTORY_BYTES_PER_WORKSPACE,
        }
    }

    #[cfg(test)]
    fn with_max_bytes(mut self, max_bytes: u64) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    fn record_success(
        &self,
        workspace: &WorkspaceName,
        sql: &str,
        tables: &[TableRef],
        batches: &[RecordBatch],
    ) -> Result<(), QueryHistoryError> {
        let _lock = FileLock::exclusive(self.layout.state_lock())?;
        let path = self.layout.query_history_file(workspace);
        let existing = read_all_records(&path)?;
        let normalized_sql = normalize_sql(sql);
        if existing
            .last()
            .is_some_and(|record| normalize_sql(&record.sql) == normalized_sql)
        {
            return Ok(());
        }

        let record = ExampleQuery {
            sql: sql.to_string(),
            row_count: batches
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>()
                .try_into()
                .unwrap_or(u64::MAX),
            tables: distinct_tables(tables),
            sources: distinct_sources(tables),
            created_at_unix_nanos: now_unix_nanos(),
        };
        append_within_budget(&path, existing, record, self.max_bytes)
    }
}

pub(crate) fn select_example_queries(
    layout: &AppStateLayout,
    workspace: &WorkspaceName,
    installed_sources: &[String],
    limit: usize,
) -> Result<Vec<ExampleQuery>, QueryHistoryError> {
    if limit == 0 || installed_sources.is_empty() {
        return Ok(Vec::new());
    }

    let _lock = FileLock::shared(layout.state_lock())?;
    let records = read_all_records(&layout.query_history_file(workspace))?;
    Ok(select_example_queries_from_records(
        records,
        installed_sources,
        limit,
    ))
}

fn select_example_queries_from_records(
    records: Vec<ExampleQuery>,
    installed_sources: &[String],
    limit: usize,
) -> Vec<ExampleQuery> {
    let installed = installed_sources.iter().collect::<BTreeSet<_>>();
    let mut seen_sql = HashSet::new();
    let mut candidates = records
        .into_iter()
        .rev()
        .filter(|record| {
            !record.sources.is_empty()
                && record
                    .sources
                    .iter()
                    .all(|source| installed.contains(source))
                && seen_sql.insert(normalize_sql(&record.sql))
        })
        .collect::<Vec<_>>();
    candidates.reverse();

    let mut selected = Vec::new();
    let mut covered = BTreeSet::new();
    while selected.len() < limit && !candidates.is_empty() {
        let best_index = candidates
            .iter()
            .enumerate()
            .max_by_key(|(_, candidate)| {
                let new_sources = candidate
                    .sources
                    .iter()
                    .filter(|source| !covered.contains(*source))
                    .count();
                (
                    new_sources,
                    score_candidate(candidate),
                    candidate.created_at_unix_nanos,
                )
            })
            .map(|(index, _)| index)
            .expect("candidates is non-empty");
        let candidate = candidates.remove(best_index);
        covered.extend(candidate.sources.iter().cloned());
        selected.push(candidate);
    }

    selected
}

fn score_candidate(query: &ExampleQuery) -> u64 {
    let row_score = if query.row_count > 0 { 4 } else { 0 };
    row_score + (query.sources.len() as u64 * 2) + query.tables.len() as u64
}

fn distinct_tables(tables: &[TableRef]) -> Vec<ExampleQueryTable> {
    tables
        .iter()
        .map(ExampleQueryTable::from)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn distinct_sources(tables: &[TableRef]) -> Vec<String> {
    tables
        .iter()
        .map(|table| table.source.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn normalize_sql(sql: &str) -> String {
    sql.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn read_all_records(path: &Path) -> Result<Vec<ExampleQuery>, QueryHistoryError> {
    let contents = match std::fs::read(path) {
        Ok(contents) => contents,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    Ok(parse_records(&contents).collect())
}

fn parse_records(contents: &[u8]) -> impl Iterator<Item = ExampleQuery> + '_ {
    contents
        .split(|&byte| byte == b'\n')
        .filter_map(|raw_line| {
            let Ok(line) = std::str::from_utf8(raw_line) else {
                warn!("skipping query history record with invalid UTF-8 (likely a torn write)");
                return None;
            };
            if line.trim().is_empty() {
                return None;
            }
            match serde_json::from_str::<ExampleQuery>(line) {
                Ok(record) => Some(record),
                Err(error) => {
                    warn!(%error, "skipping unparsable query history record");
                    None
                }
            }
        })
}

fn append_within_budget(
    path: &Path,
    existing: Vec<ExampleQuery>,
    record: ExampleQuery,
    max_bytes: u64,
) -> Result<(), QueryHistoryError> {
    let line_len = serde_json::to_vec(&record)?.len() as u64 + 1;
    if line_len > max_bytes {
        warn!(
            max_bytes,
            record_bytes = line_len,
            "skipping oversized query history record"
        );
        return Ok(());
    }

    let leading = u64::from(file_needs_leading_newline(path)?);
    if file_len(path)?
        .saturating_add(line_len)
        .saturating_add(leading)
        <= max_bytes
    {
        return append_record(path, &record);
    }

    let mut kept = existing;
    kept.push(record);
    let encoded: Vec<Vec<u8>> = kept
        .iter()
        .map(serde_json::to_vec)
        .collect::<Result<_, _>>()?;
    let record_count = encoded.len();
    let mut total = 0_u64;
    let mut dropped = record_count;
    for (index, line) in encoded.iter().enumerate().rev() {
        let line_len = line.len() as u64 + 1;
        if line_len > max_bytes || total.saturating_add(line_len) > max_bytes {
            break;
        }
        total += line_len;
        dropped = index;
    }

    let mut bytes = Vec::new();
    for line in encoded.iter().skip(dropped) {
        bytes.extend_from_slice(line);
        bytes.push(b'\n');
    }
    if let Some(parent) = path.parent() {
        storage_fs::ensure_dir(parent)?;
    }
    storage_fs::write_atomic(path, &bytes)?;
    Ok(())
}

fn append_record(path: &Path, record: &ExampleQuery) -> Result<(), QueryHistoryError> {
    let encoded = serde_json::to_vec(record)?;
    let mut bytes = Vec::with_capacity(encoded.len() + 2);
    if file_needs_leading_newline(path)? {
        bytes.push(b'\n');
    }
    bytes.extend_from_slice(&encoded);
    bytes.push(b'\n');
    storage_fs::append_file_private(path, &bytes)?;
    Ok(())
}

fn file_len(path: &Path) -> Result<u64, QueryHistoryError> {
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(metadata.len()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
}

fn file_needs_leading_newline(path: &Path) -> Result<bool, QueryHistoryError> {
    let mut file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    if file.seek(SeekFrom::End(0))? == 0 {
        return Ok(false);
    }
    file.seek(SeekFrom::End(-1))?;
    let mut last = [0_u8; 1];
    file.read_exact(&mut last)?;
    Ok(last[0] != b'\n')
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::Arc;

    use super::*;
    use arrow::array::{ArrayRef, Int64Array};
    use tempfile::TempDir;

    fn example(
        sql: &str,
        row_count: u64,
        sources: &[&str],
        tables: &[(&str, &str)],
        created_at_unix_nanos: u128,
    ) -> ExampleQuery {
        ExampleQuery {
            sql: sql.to_string(),
            row_count,
            sources: sources.iter().map(|source| (*source).to_string()).collect(),
            tables: tables
                .iter()
                .map(|(source, table)| ExampleQueryTable {
                    source: (*source).to_string(),
                    table: (*table).to_string(),
                })
                .collect(),
            created_at_unix_nanos,
        }
    }

    #[test]
    fn selector_filters_uninstalled_sources_before_ranking() {
        let records = vec![
            example(
                "select * from old.users",
                100,
                &["old"],
                &[("old", "users")],
                10,
            ),
            example(
                "select * from github.pull_requests",
                1,
                &["github"],
                &[("github", "pull_requests")],
                20,
            ),
            example(
                "select * from linear.issues",
                1,
                &["linear"],
                &[("linear", "issues")],
                30,
            ),
        ];

        let selected = select_example_queries_from_records(
            records,
            &["github".to_string(), "linear".to_string()],
            2,
        );

        assert_eq!(
            selected
                .iter()
                .map(|query| query.sql.as_str())
                .collect::<Vec<_>>(),
            [
                "select * from linear.issues",
                "select * from github.pull_requests"
            ]
        );
    }

    #[test]
    fn selector_respects_limit_and_prefers_new_source_coverage() {
        let records = vec![
            example(
                "select * from github.pull_requests",
                10,
                &["github"],
                &[("github", "pull_requests")],
                10,
            ),
            example(
                "select * from github.issues",
                20,
                &["github"],
                &[("github", "issues")],
                20,
            ),
            example(
                "select * from linear.issues",
                1,
                &["linear"],
                &[("linear", "issues")],
                30,
            ),
        ];

        let selected = select_example_queries_from_records(
            records,
            &["github".to_string(), "linear".to_string()],
            2,
        );

        assert_eq!(
            selected
                .iter()
                .map(|query| query.sql.as_str())
                .collect::<Vec<_>>(),
            ["select * from linear.issues", "select * from github.issues"]
        );
    }

    #[test]
    fn selector_prefers_result_producing_and_more_tables_when_coverage_ties() {
        let records = vec![
            example(
                "select * from github.empty",
                0,
                &["github"],
                &[("github", "empty"), ("github", "other")],
                10,
            ),
            example(
                "select * from github.pull_requests",
                1,
                &["github"],
                &[("github", "pull_requests")],
                20,
            ),
            example(
                "select * from github.pull_requests join github.reviews using(id)",
                1,
                &["github"],
                &[("github", "pull_requests"), ("github", "reviews")],
                15,
            ),
        ];

        let selected = select_example_queries_from_records(records, &["github".to_string()], 1);

        let selected = selected.first().expect("selected query");
        assert_eq!(
            selected.sql,
            "select * from github.pull_requests join github.reviews using(id)"
        );
    }

    #[test]
    fn selector_deduplicates_normalized_sql_and_keeps_most_recent() {
        let records = vec![
            example(
                "select * from github.pull_requests",
                1,
                &["github"],
                &[("github", "pull_requests")],
                10,
            ),
            example(
                " select   *   from github.pull_requests ",
                2,
                &["github"],
                &[("github", "pull_requests")],
                20,
            ),
        ];

        let selected = select_example_queries_from_records(records, &["github".to_string()], 5);

        assert_eq!(selected.len(), 1);
        let selected = selected.first().expect("selected query");
        assert_eq!(selected.row_count, 2);
        assert_eq!(selected.created_at_unix_nanos, 20);
    }

    #[test]
    fn store_eviction_keeps_newest_records_within_budget() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let workspace = WorkspaceName::default();
        let store = QueryHistoryStore::new(layout.clone()).with_max_bytes(300);

        for index in 0..4 {
            store
                .record_success(
                    &workspace,
                    &format!("select {index} from github.pull_requests"),
                    &[TableRef::new("github", "pull_requests")],
                    &[],
                )
                .expect("record query");
        }

        let records =
            read_all_records(&layout.query_history_file(&workspace)).expect("read records");
        assert_eq!(records.len(), 1);
        assert!(
            records
                .first()
                .expect("newest retained record")
                .sql
                .contains("select 3")
        );
    }

    #[test]
    fn store_skips_oversized_record_without_erasing_existing_history() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let workspace = WorkspaceName::default();
        let store = QueryHistoryStore::new(layout.clone()).with_max_bytes(300);

        store
            .record_success(
                &workspace,
                "select * from github.pull_requests",
                &[TableRef::new("github", "pull_requests")],
                &[],
            )
            .expect("record first query");
        store
            .record_success(
                &workspace,
                &format!("select '{}' from github.pull_requests", "x".repeat(1_000)),
                &[TableRef::new("github", "pull_requests")],
                &[],
            )
            .expect("skip oversized query");

        let path = layout.query_history_file(&workspace);
        assert!(fs::metadata(&path).expect("history metadata").len() <= 300);
        let records = read_all_records(&path).expect("read records");
        assert_eq!(records.len(), 1);
        assert_eq!(
            records.first().expect("retained record").sql,
            "select * from github.pull_requests"
        );
    }

    #[test]
    fn store_appends_record_with_sorted_sources_tables_and_row_count() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let workspace = WorkspaceName::default();
        let store = QueryHistoryStore::new(layout.clone());
        let batch = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int64Array::from(vec![1_i64, 2])) as ArrayRef,
        )])
        .expect("record batch");

        store
            .record_success(
                &workspace,
                "select * from slack.messages join github.pull_requests on true",
                &[
                    TableRef::new("slack", "messages"),
                    TableRef::new("github", "pull_requests"),
                    TableRef::new("slack", "messages"),
                ],
                &[batch],
            )
            .expect("record query");

        let records =
            read_all_records(&layout.query_history_file(&workspace)).expect("read records");
        assert_eq!(records.len(), 1);
        let record = records.first().expect("stored query history record");
        assert_eq!(record.row_count, 2);
        assert_eq!(record.sources, ["github", "slack"]);
        assert_eq!(
            record.tables,
            [
                ExampleQueryTable {
                    source: "github".to_string(),
                    table: "pull_requests".to_string(),
                },
                ExampleQueryTable {
                    source: "slack".to_string(),
                    table: "messages".to_string(),
                },
            ]
        );
    }

    #[test]
    fn reader_tolerates_torn_final_line() {
        let complete = serde_json::to_string(&example(
            "select * from github.pull_requests",
            1,
            &["github"],
            &[("github", "pull_requests")],
            10,
        ))
        .expect("serialize");
        let contents = format!("{complete}\n{{\"sql\":").into_bytes();

        let records = parse_records(&contents).collect::<Vec<_>>();

        assert_eq!(records.len(), 1);
        assert_eq!(
            records.first().expect("intact record").sql,
            "select * from github.pull_requests"
        );
    }

    #[test]
    fn recorder_persistence_errors_are_best_effort() {
        let temp = TempDir::new().expect("temp dir");
        let config_file_path = temp.path().join("coral-config");
        fs::write(&config_file_path, "not a directory").expect("write blocking file");
        let layout = AppStateLayout::discover(Some(config_file_path)).expect("layout");
        let recorder = QueryHistoryRecorder::new(layout, WorkspaceName::default());

        recorder
            .observe_result(
                "select * from github.pull_requests",
                &Schema::empty(),
                &[],
                &[TableRef::new("github", "pull_requests")],
            )
            .expect("best-effort recorder should swallow persistence errors");
    }
}
