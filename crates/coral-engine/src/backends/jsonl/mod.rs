//! `JSONL` table provider backed by local files.

use std::any::Any;
use std::collections::HashMap;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::TableProvider;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown, TableType};
use datafusion::physical_plan::ExecutionPlan;
use serde_json::Value;

use crate::backends::shared::filter_expr::extract_filter_values;
use crate::backends::shared::json_exec::{Converter, Fetcher, JsonExec, RowFetcher};
use crate::backends::shared::mapping::convert_items;
use crate::backends::{
    BackendCompileRequest, BackendRegistration, CompiledBackendSource, RegisteredSource,
    RegisteredTable, build_registered_table, registered_columns_from_specs, required_filter_names,
    schema_from_columns,
};
use crate::{CoreError, QueryRuntimeContext};
use coral_spec::backends::file::{FileTableSpec, JsonlSourceManifest};

/// Maximum directory recursion depth to prevent stack overflow from symlink
/// cycles or pathologically deep directory trees.
const MAX_RECURSION_DEPTH: usize = 32;

/// Glob match options: `*` matches within a directory, `**` crosses boundaries.
const GLOB_MATCH_OPTIONS: glob::MatchOptions = glob::MatchOptions {
    case_sensitive: true,
    require_literal_separator: true,
    require_literal_leading_dot: false,
};

#[derive(Debug, Clone)]
pub(crate) struct CompiledJsonlTable {
    pub(crate) source_schema: String,
    pub(crate) table: FileTableSpec,
    pub(crate) location: String,
    pub(crate) base_dir: PathBuf,
    pub(crate) glob: glob::Pattern,
}

#[derive(Debug, Clone)]
struct JsonlCompiledSource {
    manifest: JsonlSourceManifest,
    tables: Vec<CompiledJsonlTable>,
}

pub(crate) fn compile_source(
    manifest: JsonlSourceManifest,
    runtime_context: &QueryRuntimeContext,
) -> Result<Box<dyn CompiledBackendSource>, CoreError> {
    let tables = manifest
        .tables
        .iter()
        .cloned()
        .map(|table| {
            compile_jsonl_table(
                &manifest.common.name,
                table,
                runtime_context.home_dir.as_deref(),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Box::new(JsonlCompiledSource { manifest, tables }))
}

pub(crate) fn compile_manifest(
    manifest: &JsonlSourceManifest,
    request: &BackendCompileRequest<'_>,
) -> Result<Box<dyn CompiledBackendSource>, CoreError> {
    compile_source(manifest.clone(), request.runtime_context)
}

/// A table provider backed by newline-delimited `JSON` (`JSONL`) files on the local
/// filesystem. Uses the same expression-based column mapping pipeline as the
/// HTTP backend (`convert_items` + `JsonExec`), but reads from local files
/// instead of API responses.
///
/// Symlinked directories are skipped to prevent infinite cycles; symlinked files
/// are followed. All matching files are read into memory on each scan.
#[derive(Debug)]
pub(crate) struct JsonlTableProvider {
    source_schema: String,
    table: Arc<FileTableSpec>,
    schema: SchemaRef,
    /// Resolved base directory for file discovery.
    base_dir: PathBuf,
    /// Compiled glob pattern for matching `JSONL` files within `base_dir`.
    pattern: glob::Pattern,
}

impl JsonlTableProvider {
    /// Build a `JSONL`-backed table provider from a source manifest.
    pub(crate) fn try_new(compiled_table: CompiledJsonlTable) -> Result<Self> {
        let CompiledJsonlTable {
            source_schema,
            table,
            location,
            base_dir,
            glob,
        } = compiled_table;
        if table.columns().is_empty() {
            return Err(DataFusionError::Plan(format!(
                "{source_schema}.{} uses backend=jsonl and must define columns",
                table.name()
            )));
        }

        let schema = schema_from_columns(table.columns(), &source_schema, table.name())?;
        if !base_dir.is_dir() {
            return Err(DataFusionError::Plan(format!(
                "{source_schema}.{} source.location '{}' is not a directory",
                table.name(),
                location
            )));
        }

        Ok(Self {
            source_schema,
            table: Arc::new(table),
            schema,
            base_dir,
            pattern: glob,
        })
    }
}

/// Discover and parse all JSONL files under `base_dir` matching `pattern`.
/// Uses `BufReader` for line-by-line reading rather than loading each file as
/// a single string, though all parsed records are collected into memory.
fn read_jsonl_files(base_dir: &Path, pattern: &glob::Pattern) -> Result<Vec<Value>> {
    let files = discover_matching_files(base_dir, pattern)?;

    let mut items = Vec::new();
    for file_path in &files {
        parse_jsonl_file(file_path, &mut items)?;
    }
    Ok(items)
}

/// Recursively find files under `base_dir` whose relative path matches
/// `pattern`, returned in sorted order for deterministic results. Skips
/// symlinked directories (to prevent cycles) but follows symlinked files.
fn discover_matching_files(base_dir: &Path, pattern: &glob::Pattern) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_matching_files(base_dir, base_dir, pattern, &mut files, 0)?;
    files.sort();
    Ok(files)
}

/// Parse a single JSONL file, appending each non-empty line as a parsed JSON
/// value to `out`. Malformed lines are skipped with a warning rather than
/// erroring, since files may be actively written to by other processes.
fn parse_jsonl_file(path: &Path, out: &mut Vec<Value>) -> Result<()> {
    let file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "skipping unreadable file");
            return Ok(());
        }
    };
    for (i, line) in BufReader::new(file).lines().enumerate() {
        let line =
            line.map_err(|e| exec_err(format!("{}:{}: read error: {e}", path.display(), i + 1)))?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(&line) {
            Ok(value) => out.push(value),
            Err(e) => {
                tracing::warn!(
                    file = %path.display(),
                    line = i + 1,
                    error = %e,
                    "skipping malformed JSONL line"
                );
            }
        }
    }
    Ok(())
}

/// Recursively collect files under `dir` whose path relative to `base`
/// matches `pattern`. Skips symlinked directories (to prevent cycles) but
/// follows symlinked files. Limits recursion depth to [`MAX_RECURSION_DEPTH`].
fn collect_matching_files(
    base: &Path,
    dir: &Path,
    pattern: &glob::Pattern,
    out: &mut Vec<PathBuf>,
    depth: usize,
) -> Result<()> {
    if depth > MAX_RECURSION_DEPTH {
        tracing::warn!(
            dir = %dir.display(),
            "JSONL file discovery hit max recursion depth ({MAX_RECURSION_DEPTH}); \
             some files may be missing"
        );
        return Ok(());
    }
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) => {
            tracing::warn!(dir = %dir.display(), error = %e, "skipping unreadable directory");
            return Ok(());
        }
    };
    for entry in entries {
        let Ok(entry) = entry else {
            tracing::warn!(dir = %dir.display(), "skipping unreadable directory entry");
            continue;
        };
        // entry.file_type() does NOT follow symlinks, preventing infinite
        // loops from directory symlink cycles.
        let Ok(file_type) = entry.file_type() else {
            tracing::warn!(path = %entry.path().display(), "skipping entry with unreadable file type");
            continue;
        };
        let path = entry.path();
        if file_type.is_dir() {
            collect_matching_files(base, &path, pattern, out, depth + 1)?;
        } else if file_type.is_symlink() {
            // Resolve symlink target to distinguish files from directories.
            // Symlinked files are safe to follow; symlinked directories are
            // skipped to prevent infinite cycles.
            let Ok(meta) = std::fs::metadata(&path) else {
                continue; // dangling symlink
            };
            if meta.is_file() {
                push_if_matches(path, base, pattern, out);
            }
        } else if file_type.is_file() {
            push_if_matches(path, base, pattern, out);
        }
    }
    Ok(())
}

/// Push `path` into `out` if its relative path from `base` matches `pattern`.
fn push_if_matches(path: PathBuf, base: &Path, pattern: &glob::Pattern, out: &mut Vec<PathBuf>) {
    if let Ok(rel) = path.strip_prefix(base)
        && pattern.matches_path_with(rel, GLOB_MATCH_OPTIONS)
    {
        out.push(path);
    }
}

/// Shorthand for `DataFusionError::Execution`.
fn exec_err(msg: String) -> DataFusionError {
    DataFusionError::Execution(msg)
}

#[async_trait]
impl CompiledBackendSource for JsonlCompiledSource {
    fn schema_name(&self) -> &str {
        &self.manifest.common.name
    }

    fn source_name(&self) -> &str {
        &self.manifest.common.name
    }

    async fn register(
        &self,
        _ctx: &datafusion::prelude::SessionContext,
    ) -> Result<BackendRegistration> {
        let mut tables: HashMap<String, Arc<dyn TableProvider>> = HashMap::new();
        let mut table_infos = Vec::with_capacity(self.tables.len());

        for compiled_table in &self.tables {
            let provider = JsonlTableProvider::try_new(compiled_table.clone())?;
            let table_name = compiled_table.table.name().to_string();
            let metadata = registered_table(&compiled_table.table);
            tables.insert(table_name, Arc::new(provider));
            table_infos.push(metadata);
        }

        Ok(BackendRegistration {
            tables,
            source: RegisteredSource {
                schema_name: self.manifest.common.name.clone(),
                tables: table_infos,
            },
        })
    }
}

fn registered_table(table: &FileTableSpec) -> RegisteredTable {
    let required_filters = required_filter_names(table.filters());
    let columns = registered_columns_from_specs(table.columns(), &required_filters);
    build_registered_table(&table.common, columns, required_filters)
}

fn compile_jsonl_table(
    source_schema: &str,
    table: FileTableSpec,
    home_dir: Option<&Path>,
) -> Result<CompiledJsonlTable, CoreError> {
    let base_dir = resolve_file_location(&table.source.location, home_dir)?;
    let glob_str = table.source.jsonl_glob_or_default();
    let glob = glob::Pattern::new(glob_str).map_err(|error| {
        CoreError::FailedPrecondition(format!(
            "{source_schema}.{} has invalid glob pattern '{glob_str}': {error}",
            table.name()
        ))
    })?;
    Ok(CompiledJsonlTable {
        source_schema: source_schema.to_string(),
        location: table.source.location.clone(),
        table,
        base_dir,
        glob,
    })
}

pub(crate) fn resolve_file_location(
    location: &str,
    home_dir: Option<&Path>,
) -> Result<PathBuf, CoreError> {
    let location = if let Some(rest) = location.strip_prefix("file://localhost/") {
        std::borrow::Cow::Owned(format!("file:///{rest}"))
    } else {
        std::borrow::Cow::Borrowed(location)
    };
    let decoded;
    if let Some(rest) = location.strip_prefix("file://~/") {
        let home = home_dir.ok_or_else(|| {
            CoreError::FailedPrecondition(
                "source.location uses '~' but home directory is not available".to_string(),
            )
        })?;
        decoded = urlencoding::decode(rest).map_err(|error| {
            CoreError::InvalidInput(format!("source.location has invalid encoding: {error}"))
        })?;
        Ok(home.join(decoded.as_ref()))
    } else if let Some(path) = location.strip_prefix("file://") {
        decoded = urlencoding::decode(path).map_err(|error| {
            CoreError::InvalidInput(format!("source.location has invalid encoding: {error}"))
        })?;
        Ok(PathBuf::from(decoded.as_ref()))
    } else {
        Err(CoreError::InvalidInput(format!(
            "source.location must start with file://, got '{location}'"
        )))
    }
}

#[async_trait]
impl TableProvider for JsonlTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        Ok(filters
            .iter()
            .map(|_| TableProviderFilterPushDown::Unsupported)
            .collect())
    }

    async fn scan(
        &self,
        _state: &dyn datafusion::catalog::Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        // Not pushed down: DataFusion applies WHERE/ORDER BY above the scan,
        // so truncating raw rows would produce incorrect results.
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.validate_required_filters(filters)?;

        let fetcher = self.build_fetch_plan();
        let converter = self.build_converter(filters);

        let exec = JsonExec::new(
            &self.source_schema,
            self.table.name(),
            self.schema.clone(),
            fetcher,
            converter,
            projection.cloned(),
        )?;

        Ok(Arc::new(exec))
    }
}

impl JsonlTableProvider {
    fn validate_required_filters(&self, filters: &[Expr]) -> Result<()> {
        let filter_values = extract_filter_values(filters, self.table.filters());
        for required in self.table.filters().iter().filter(|f| f.required) {
            if !filter_values.contains_key(&required.name) {
                return Err(DataFusionError::Execution(format!(
                    "{}.{} table requires a constant equality filter: WHERE {} = <constant>",
                    self.source_schema,
                    self.table.name(),
                    required.name
                )));
            }
        }
        Ok(())
    }

    /// Build a fetch plan that reads JSONL files from the filesystem.
    fn build_fetch_plan(&self) -> Fetcher {
        Arc::new(JsonlFetchPlan {
            base_dir: self.base_dir.clone(),
            pattern: self.pattern.clone(),
        })
    }

    fn build_converter(&self, filters: &[Expr]) -> Converter {
        let schema = self.schema.clone();
        let table = Arc::clone(&self.table);
        let filter_values = extract_filter_values(filters, table.filters());
        Arc::new(move |items: &[Value]| {
            convert_items(table.columns(), schema.clone(), &filter_values, items)
        })
    }
}

#[derive(Debug)]
struct JsonlFetchPlan {
    base_dir: PathBuf,
    pattern: glob::Pattern,
}

#[async_trait]
impl RowFetcher for JsonlFetchPlan {
    async fn fetch(&self) -> Result<Vec<Value>> {
        let base_dir = self.base_dir.clone();
        let pattern = self.pattern.clone();
        tokio::task::spawn_blocking(move || read_jsonl_files(&base_dir, &pattern))
            .await
            .map_err(|e| exec_err(format!("task join error: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use crate::QueryRuntimeContext;
    use crate::backends::{CompiledBackendSource, compile_source_manifest};
    use crate::runtime::catalog;
    use crate::runtime::registry::register_sources_blocking;
    use coral_spec::{ValidatedSourceManifest, parse_source_manifest_value};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::prelude::SessionContext;
    use serde_json::{Value, json};
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::PathBuf;
    use tempfile::tempdir;

    fn compile_sources(
        manifests: Vec<ValidatedSourceManifest>,
    ) -> Vec<Box<dyn CompiledBackendSource>> {
        manifests
            .into_iter()
            .map(|manifest| {
                compile_source_manifest(
                    &manifest,
                    BTreeMap::new(),
                    BTreeMap::new(),
                    &QueryRuntimeContext::default(),
                )
                .expect("manifest should compile")
            })
            .collect()
    }

    fn jsonl_manifest(location: &str, columns: &[Value]) -> ValidatedSourceManifest {
        parse_source_manifest_value(json!({
            "dsl_version": 3,
            "name": "test_jsonl",
            "version": "0.1.0",
            "backend": "jsonl",
            "tables": [{
                "name": "events",
                "description": "test",
                "source": {
                    "location": location,
                    "glob": "**/*.jsonl",
                    "partitions": [],
                },
                "columns": columns,
            }]
        }))
        .expect("jsonl manifest should parse")
    }

    fn column(name: &str, data_type: &str) -> Value {
        json!({
            "name": name,
            "type": data_type,
        })
    }

    fn simple_columns() -> Vec<Value> {
        vec![
            column("type", "Utf8"),
            column("name", "Utf8"),
            column("value", "Int64"),
        ]
    }

    // --- Integration tests ---

    #[tokio::test]
    async fn reads_files_across_multiple_subdirectories() {
        let fixture_dir = tempdir().expect("tempdir should be created");
        let dir_a = fixture_dir.path().join("project-a");
        let dir_b = fixture_dir.path().join("project-b");
        fs::create_dir_all(&dir_a).expect("create dir a");
        fs::create_dir_all(&dir_b).expect("create dir b");

        fs::write(
            dir_a.join("session.jsonl"),
            r#"{"type":"user","name":"alice","value":1}
{"type":"assistant","name":"bob","value":2}
"#,
        )
        .expect("write fixture a");

        fs::write(
            dir_b.join("session.jsonl"),
            r#"{"type":"user","name":"carol","value":3}
"#,
        )
        .expect("write fixture b");

        let ctx = SessionContext::new();
        let location = format!("file://{}/", fixture_dir.path().display());
        let columns = simple_columns();
        let manifest = jsonl_manifest(&location, &columns);

        register_sources_blocking(&ctx, compile_sources(vec![manifest]))
            .expect("jsonl plugin should register");

        let batches = ctx
            .sql("SELECT type, name, value FROM test_jsonl.events ORDER BY value")
            .await
            .expect("query should plan")
            .collect()
            .await
            .expect("query should execute");

        let total_rows: usize = batches
            .iter()
            .map(datafusion::arrow::record_batch::RecordBatch::num_rows)
            .sum();
        assert_eq!(total_rows, 3, "should find rows across both subdirectories");

        let rendered = pretty_format_batches(&batches)
            .expect("batches should render")
            .to_string();

        assert!(rendered.contains("| alice"));
        assert!(rendered.contains("| bob"));
        assert!(rendered.contains("| carol"));
    }

    #[tokio::test]
    async fn exposes_schema_in_coral_columns() {
        let fixture_dir = tempdir().expect("tempdir should be created");
        fs::write(
            fixture_dir.path().join("test.jsonl"),
            r#"{"type":"x","name":"y","value":1}
"#,
        )
        .expect("write fixture");

        let ctx = SessionContext::new();
        let location = format!("file://{}/", fixture_dir.path().display());
        let columns = simple_columns();
        let manifest = jsonl_manifest(&location, &columns);

        let active_plugins = register_sources_blocking(&ctx, compile_sources(vec![manifest]))
            .expect("jsonl plugin should register");
        catalog::register(&ctx, &active_plugins.active_sources)
            .expect("metadata tables should register");

        let batches = ctx
            .sql(
                "SELECT column_name, data_type \
                 FROM coral.columns \
                 WHERE schema_name = 'test_jsonl' AND table_name = 'events' \
                 ORDER BY column_name",
            )
            .await
            .expect("metadata query should plan")
            .collect()
            .await
            .expect("metadata query should execute");

        let rendered = pretty_format_batches(&batches)
            .expect("batches should render")
            .to_string();

        assert!(rendered.contains("| type"));
        assert!(rendered.contains("| name"));
        assert!(rendered.contains("| value"));
    }

    // --- Unit tests: file reading ---

    #[test]
    fn read_jsonl_files_returns_empty_for_no_matches() {
        let dir = tempdir().expect("tempdir");
        fs::write(dir.path().join("not_jsonl.txt"), "hello").expect("write");
        let pattern = glob::Pattern::new("**/*.jsonl").unwrap();
        let result = super::read_jsonl_files(dir.path(), &pattern).unwrap();
        assert!(result.is_empty());
    }

    #[test]
    fn read_jsonl_files_skips_malformed_lines() {
        let dir = tempdir().expect("tempdir");
        fs::write(
            dir.path().join("mixed.jsonl"),
            "{\"a\":1}\nnot json\n{\"a\":2}\n",
        )
        .expect("write");
        let pattern = glob::Pattern::new("**/*.jsonl").unwrap();
        let result = super::read_jsonl_files(dir.path(), &pattern).unwrap();
        assert_eq!(
            result.len(),
            2,
            "malformed line should be skipped, valid lines kept"
        );
    }

    #[test]
    fn read_jsonl_files_skips_blank_lines() {
        let dir = tempdir().expect("tempdir");
        fs::write(
            dir.path().join("with_blanks.jsonl"),
            "{\"a\":1}\n\n  \n{\"a\":2}\n",
        )
        .expect("write");
        let pattern = glob::Pattern::new("**/*.jsonl").unwrap();
        let result = super::read_jsonl_files(dir.path(), &pattern).unwrap();
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn read_jsonl_files_returns_empty_for_nonexistent_directory() {
        let pattern = glob::Pattern::new("**/*.jsonl").unwrap();
        let result = super::read_jsonl_files(&PathBuf::from("/nonexistent/dir"), &pattern).unwrap();
        assert!(
            result.is_empty(),
            "nonexistent directory should return empty, not error"
        );
    }

    // --- Unit tests: construction ---

    #[tokio::test]
    async fn default_glob_matches_jsonl_files() {
        let fixture_dir = tempdir().expect("tempdir should be created");
        fs::write(
            fixture_dir.path().join("data.jsonl"),
            r#"{"type":"user","name":"alice","value":1}
"#,
        )
        .expect("write fixture");

        let ctx = SessionContext::new();
        let manifest = parse_source_manifest_value(json!({
            "dsl_version": 3,
            "name": "test_default_glob",
            "version": "0.1.0",
            "backend": "jsonl",
            "tables": [{
                "name": "events",
                "description": "test",
                "source": {
                    "location": format!("file://{}/", fixture_dir.path().display()),
                    "partitions": [],
                },
                "columns": simple_columns(),
            }]
        }))
        .expect("jsonl manifest should parse");

        register_sources_blocking(&ctx, compile_sources(vec![manifest]))
            .expect("jsonl source with default glob should register");

        let batches = ctx
            .sql("SELECT name FROM test_default_glob.events")
            .await
            .expect("query should plan")
            .collect()
            .await
            .expect("query should execute");

        let total_rows: usize = batches
            .iter()
            .map(datafusion::arrow::record_batch::RecordBatch::num_rows)
            .sum();
        assert_eq!(
            total_rows, 1,
            "default glob **/*.jsonl should match the fixture file"
        );
    }

    #[tokio::test]
    async fn missing_source_directory_skips_registration() {
        let ctx = SessionContext::new();
        let columns = simple_columns();
        let manifest = jsonl_manifest("file:///nonexistent/path/that/does/not/exist/", &columns);

        let result = register_sources_blocking(&ctx, compile_sources(vec![manifest]));
        // Registration succeeds (doesn't error) but the source is skipped.
        assert!(result.is_ok());
        let active = result.unwrap();
        assert!(
            active.active_sources.is_empty(),
            "source with missing source directory should be skipped, not registered"
        );
        assert_eq!(active.failures.len(), 1);
    }

    #[test]
    fn try_new_rejects_empty_columns() {
        let error = parse_source_manifest_value(json!({
            "dsl_version": 3,
            "name": "test_jsonl",
            "version": "0.1.0",
            "backend": "jsonl",
            "tables": [{
                "name": "test",
                "description": "test",
                "source": {
                    "location": "file:///tmp/",
                    "glob": "*.jsonl",
                    "partitions": [],
                },
                "columns": [],
            }]
        }))
        .expect_err("jsonl manifest with empty columns should fail");
        assert!(error.to_string().contains("must define columns"));
    }
}
