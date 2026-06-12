use std::collections::BTreeMap;
use std::fs;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, anyhow, bail};
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use clap::{Args, Parser, Subcommand};
use globset::{Glob, GlobSet, GlobSetBuilder};
use ignore::WalkBuilder;
use kreuzberg::{ExtractionConfig, OutputFormat, ResultFormat, extract_file};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Semaphore;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};

const DEFAULT_PORT: u16 = 8765;
const DEFAULT_INDEX_RELATIVE: &str = ".coral-files/index.sqlite";
const DEFAULT_MAX_FILE_SIZE_MB: u64 = 50;
const DEFAULT_CONCURRENCY: usize = 4;
const DEFAULT_CHUNK_SIZE: usize = 4000;
const DEFAULT_LIMIT: u32 = 100;
const DEFAULT_SEARCH_LIMIT: u32 = 20;
const MAX_LIMIT: u32 = 1000;
const EXTRACTION_CONFIG_VERSION: &str = "markdown-json-structured-v1";

#[derive(Debug, Parser)]
#[command(name = "coral-files", version, arg_required_else_help = false)]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Index a directory and expose it over localhost REST
    Serve(ServeArgs),
}

#[derive(Debug, Args, Clone)]
struct ServeArgs {
    /// Directory to index. Defaults to the current directory.
    root_arg: Option<PathBuf>,

    /// Directory to index. Conflicts with positional ROOT.
    #[arg(long)]
    root: Option<PathBuf>,

    /// Port to bind on 127.0.0.1.
    #[arg(long, default_value_t = DEFAULT_PORT)]
    port: u16,

    /// SQLite index path. Relative paths are resolved from ROOT.
    #[arg(long)]
    index: Option<PathBuf>,

    /// Include glob. May be repeated.
    #[arg(long = "include", default_value = "**/*")]
    includes: Vec<String>,

    /// Exclude glob. May be repeated.
    #[arg(long = "exclude")]
    excludes: Vec<String>,

    /// Maximum file size to index, in megabytes.
    #[arg(long, default_value_t = DEFAULT_MAX_FILE_SIZE_MB)]
    max_file_size_mb: u64,

    /// Maximum concurrent extraction jobs.
    #[arg(long, default_value_t = DEFAULT_CONCURRENCY)]
    concurrency: usize,

    /// Delete the existing index and rebuild from scratch.
    #[arg(long)]
    reindex: bool,
}

#[derive(Debug, Clone)]
struct Config {
    root: PathBuf,
    index_path: PathBuf,
    port: u16,
    includes: GlobSet,
    excludes: GlobSet,
    max_file_size_bytes: u64,
    concurrency: usize,
    reindex: bool,
}

#[derive(Clone)]
struct AppState {
    db_path: Arc<PathBuf>,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    root: String,
    pending_files: u64,
    indexed_files: u64,
    unsupported_files: u64,
    failed_files: u64,
    skipped_files: u64,
}

#[derive(Debug, Serialize)]
struct FileRecord {
    id: String,
    path: String,
    extension: Option<String>,
    mime_type: Option<String>,
    size_bytes: u64,
    modified_at: Option<String>,
    indexed_at: Option<String>,
    status: String,
    status_reason: Option<String>,
    error_summary: Option<String>,
}

#[derive(Debug, Serialize)]
struct ListResponse<T> {
    items: Vec<T>,
    limit: u32,
    offset: u32,
}

#[derive(Debug, Serialize)]
struct ContentResponse {
    id: String,
    path: String,
    content_markdown: String,
    content_format: &'static str,
}

#[derive(Debug, Serialize)]
struct MetadataResponse {
    id: String,
    path: String,
    metadata: Value,
}

#[derive(Debug, Serialize)]
struct StructureResponse {
    id: String,
    path: String,
    structure: Value,
}

#[derive(Debug, Serialize)]
struct ElementRecord {
    file_id: String,
    path: String,
    element_index: u32,
    kind: String,
    text: String,
    page: Option<u32>,
    bbox_json: Option<Value>,
    confidence: Option<f64>,
}

#[derive(Debug, Serialize)]
struct TableCellRecord {
    file_id: String,
    path: String,
    table_index: u32,
    row_index: u32,
    column_index: u32,
    text: String,
    page: Option<u32>,
    bbox_json: Option<Value>,
}

#[derive(Debug, Serialize)]
struct ChunkRecord {
    file_id: String,
    path: String,
    chunk_index: u32,
    content: String,
    start_offset: u32,
    end_offset: u32,
}

#[derive(Debug, Clone, Serialize)]
struct StructureNodeRecord {
    file_id: String,
    path: String,
    node_index: u32,
    parent_index: Option<u32>,
    node_type: Option<String>,
    heading_level: Option<u32>,
    heading_text: Option<String>,
    text: Option<String>,
    page: Option<u32>,
    bbox_json: Option<Value>,
    children_json: Option<Value>,
    content_json: Value,
}

#[derive(Debug, Clone, Serialize)]
struct HeadingRecord {
    file_id: String,
    path: String,
    node_index: u32,
    parent_index: Option<u32>,
    level: u32,
    text: String,
    page: Option<u32>,
    bbox_json: Option<Value>,
    source_node_type: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct SectionRecord {
    file_id: String,
    path: String,
    section_index: u32,
    heading_node_index: Option<u32>,
    heading_level: Option<u32>,
    heading_text: Option<String>,
    text: String,
    node_indexes_json: Value,
}

#[derive(Debug, Serialize)]
struct SearchRecord {
    id: String,
    path: String,
    score: f64,
    snippet: String,
}

#[derive(Debug, Deserialize)]
struct FilesQuery {
    path_prefix: Option<String>,
    extension: Option<String>,
    mime_type: Option<String>,
    status: Option<String>,
    limit: Option<u32>,
    offset: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct PageQuery {
    limit: Option<u32>,
    offset: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct ChunksQuery {
    path_prefix: Option<String>,
    status: Option<String>,
    limit: Option<u32>,
    offset: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct StructureRowsQuery {
    path_prefix: Option<String>,
    status: Option<String>,
    node_type: Option<String>,
    limit: Option<u32>,
    offset: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct SearchQuery {
    q: String,
    limit: Option<u32>,
    offset: Option<u32>,
}

#[derive(Debug, thiserror::Error)]
enum ApiError {
    #[error("not found")]
    NotFound,
    #[error("{0}")]
    BadRequest(String),
    #[error(transparent)]
    Internal(#[from] anyhow::Error),
}

impl From<rusqlite::Error> for ApiError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Internal(error.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::NotFound => (StatusCode::NOT_FOUND, "not found".to_string()),
            Self::BadRequest(message) => (StatusCode::BAD_REQUEST, message),
            Self::Internal(error) => {
                error!("request failed: {error:#}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_string(),
                )
            }
        };
        (status, Json(json!({ "error": message }))).into_response()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "coral_files=info,tower_http=warn".into()),
        )
        .init();

    let cli = Cli::parse();
    let args = match cli.command {
        Some(Command::Serve(args)) => args,
        None => ServeArgs {
            root_arg: None,
            root: None,
            port: DEFAULT_PORT,
            index: None,
            includes: vec!["**/*".to_string()],
            excludes: Vec::new(),
            max_file_size_mb: DEFAULT_MAX_FILE_SIZE_MB,
            concurrency: DEFAULT_CONCURRENCY,
            reindex: false,
        },
    };

    serve(args).await
}

async fn serve(args: ServeArgs) -> anyhow::Result<()> {
    let config = Config::from_args(args)?;
    if config.reindex && config.index_path.exists() {
        fs::remove_file(&config.index_path)
            .with_context(|| format!("delete existing index {}", config.index_path.display()))?;
    }
    if let Some(parent) = config.index_path.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    initialize_database(&config)?;

    let db_path = Arc::new(config.index_path.clone());
    let state = AppState {
        db_path: Arc::clone(&db_path),
    };
    let app = build_router(state);
    let addr = SocketAddr::from((Ipv4Addr::LOCALHOST, config.port));
    let listener = tokio::net::TcpListener::bind(addr).await.with_context(|| {
        format!(
            "port {} is busy or unavailable; rerun with --port <PORT>",
            config.port
        )
    })?;

    info!("serving http://{addr}");
    info!("root {}", config.root.display());
    info!("index {}", config.index_path.display());

    let indexing_config = config.clone();
    tokio::spawn(async move {
        if let Err(error) = index_root(indexing_config).await {
            error!("indexing failed: {error:#}");
        }
    });

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("serve HTTP API")
}

fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/files", get(list_files))
        .route("/v1/files/{id}", get(get_file))
        .route("/v1/files/{id}/content", get(get_content))
        .route("/v1/files/{id}/structure", get(get_structure))
        .route("/v1/files/{id}/elements", get(get_elements))
        .route("/v1/files/{id}/tables", get(get_tables))
        .route("/v1/files/{id}/metadata", get(get_metadata))
        .route("/v1/files/{id}/chunks", get(get_file_chunks))
        .route("/v1/files/{id}/nodes", get(get_file_nodes))
        .route("/v1/files/{id}/headings", get(get_file_headings))
        .route("/v1/files/{id}/sections", get(get_file_sections))
        .route("/v1/chunks", get(list_chunks))
        .route("/v1/nodes", get(list_nodes))
        .route("/v1/headings", get(list_headings))
        .route("/v1/sections", get(list_sections))
        .route("/v1/search", get(search))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        warn!("failed to install Ctrl-C handler: {error}");
    }
}

impl Config {
    fn from_args(args: ServeArgs) -> anyhow::Result<Self> {
        if args.root_arg.is_some() && args.root.is_some() {
            bail!("use either positional ROOT or --root, not both");
        }
        let root = args
            .root
            .or(args.root_arg)
            .unwrap_or_else(|| PathBuf::from("."));
        let root = root
            .canonicalize()
            .with_context(|| format!("resolve root {}", root.display()))?;
        if !root.is_dir() {
            bail!("root {} is not a directory", root.display());
        }

        let index_path = args
            .index
            .unwrap_or_else(|| PathBuf::from(DEFAULT_INDEX_RELATIVE));
        let index_path = if index_path.is_absolute() {
            index_path
        } else {
            root.join(index_path)
        };
        let includes = compile_globs(&args.includes, "include")?;
        let mut excludes = vec![
            ".git/**".to_string(),
            ".coral-files/**".to_string(),
            "node_modules/**".to_string(),
            "target/**".to_string(),
            ".cache/**".to_string(),
        ];
        excludes.extend(args.excludes);
        let excludes = compile_globs(&excludes, "exclude")?;
        let max_file_size_bytes = args
            .max_file_size_mb
            .checked_mul(1024 * 1024)
            .ok_or_else(|| anyhow!("--max-file-size-mb is too large"))?;
        if args.concurrency == 0 {
            bail!("--concurrency must be greater than 0");
        }

        Ok(Self {
            root,
            index_path,
            port: args.port,
            includes,
            excludes,
            max_file_size_bytes,
            concurrency: args.concurrency,
            reindex: args.reindex,
        })
    }
}

fn compile_globs(patterns: &[String], label: &str) -> anyhow::Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder
            .add(Glob::new(pattern).with_context(|| format!("invalid {label} glob {pattern:?}"))?);
    }
    builder
        .build()
        .with_context(|| format!("compile {label} globs"))
}

fn initialize_database(config: &Config) -> anyhow::Result<()> {
    let conn = open_connection(&config.index_path)?;
    conn.execute_batch(
        r#"
        PRAGMA journal_mode = WAL;
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS files (
            id TEXT PRIMARY KEY,
            path TEXT NOT NULL UNIQUE,
            extension TEXT,
            mime_type TEXT,
            size_bytes INTEGER NOT NULL,
            modified_at TEXT,
            modified_unix INTEGER,
            indexed_at TEXT,
            status TEXT NOT NULL,
            status_reason TEXT,
            error_summary TEXT,
            content_markdown TEXT NOT NULL DEFAULT '',
            content_json TEXT,
            content_structured_json TEXT,
            structure_json TEXT,
            metadata_json TEXT,
            tables_json TEXT,
            content_hash TEXT,
            fingerprint TEXT,
            extraction_config_version TEXT
        );

        CREATE TABLE IF NOT EXISTS elements (
            file_id TEXT NOT NULL,
            element_index INTEGER NOT NULL,
            kind TEXT NOT NULL,
            text TEXT NOT NULL,
            page INTEGER,
            bbox_json TEXT,
            confidence REAL,
            PRIMARY KEY (file_id, element_index),
            FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS table_cells (
            file_id TEXT NOT NULL,
            table_index INTEGER NOT NULL,
            row_index INTEGER NOT NULL,
            column_index INTEGER NOT NULL,
            text TEXT NOT NULL,
            page INTEGER,
            bbox_json TEXT,
            PRIMARY KEY (file_id, table_index, row_index, column_index),
            FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS chunks (
            file_id TEXT NOT NULL,
            chunk_index INTEGER NOT NULL,
            content TEXT NOT NULL,
            start_offset INTEGER NOT NULL,
            end_offset INTEGER NOT NULL,
            PRIMARY KEY (file_id, chunk_index),
            FOREIGN KEY (file_id) REFERENCES files(id) ON DELETE CASCADE
        );

        CREATE VIRTUAL TABLE IF NOT EXISTS files_fts USING fts5(
            file_id UNINDEXED,
            path,
            content,
            tokenize='unicode61'
        );
        "#,
    )?;
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('root', ?1)",
        params![config.root.to_string_lossy()],
    )?;
    Ok(())
}

fn open_connection(index_path: &Path) -> anyhow::Result<Connection> {
    let conn = Connection::open(index_path)
        .with_context(|| format!("open SQLite index {}", index_path.display()))?;
    conn.pragma_update(None, "foreign_keys", "ON")?;
    Ok(conn)
}

async fn index_root(config: Config) -> anyhow::Result<()> {
    let files = discover_files(&config)?;
    info!("discovered {} files", files.len());
    let semaphore = Arc::new(Semaphore::new(config.concurrency));
    let mut handles = Vec::with_capacity(files.len());
    for discovered in files {
        let permit = Arc::clone(&semaphore).acquire_owned().await?;
        let cfg = config.clone();
        handles.push(tokio::spawn(async move {
            let result = process_file(cfg, discovered).await;
            drop(permit);
            result
        }));
    }

    let mut indexed = 0_u64;
    let mut unsupported = 0_u64;
    let mut failed = 0_u64;
    let mut skipped = 0_u64;
    for handle in handles {
        match handle.await {
            Ok(Ok(status)) => match status.as_str() {
                "indexed" => indexed += 1,
                "unsupported" => unsupported += 1,
                "skipped" => skipped += 1,
                "failed" => failed += 1,
                _ => {}
            },
            Ok(Err(error)) => {
                failed += 1;
                warn!("file indexing task failed: {error:#}");
            }
            Err(error) => {
                failed += 1;
                warn!("file indexing task panicked or was cancelled: {error}");
            }
        }
        let done = indexed + unsupported + failed + skipped;
        if done == 1 || done.is_multiple_of(25) {
            info!(
                "indexing progress: done={done} indexed={indexed} unsupported={unsupported} failed={failed} skipped={skipped}"
            );
        }
    }
    info!(
        "indexing complete: indexed={indexed} unsupported={unsupported} failed={failed} skipped={skipped}"
    );
    Ok(())
}

#[derive(Debug, Clone)]
struct DiscoveredFile {
    id: String,
    absolute_path: PathBuf,
    relative_path: String,
    extension: Option<String>,
    mime_type: Option<String>,
    size_bytes: u64,
    modified_at: Option<String>,
    modified_unix: Option<i64>,
    fingerprint: String,
}

fn discover_files(config: &Config) -> anyhow::Result<Vec<DiscoveredFile>> {
    let mut builder = WalkBuilder::new(&config.root);
    builder.hidden(false);
    builder.git_ignore(true);
    builder.git_global(true);
    builder.git_exclude(true);
    builder.follow_links(false);

    let mut files = Vec::new();
    for entry in builder.build() {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let rel = relative_path(&config.root, path)?;
        if !config.includes.is_match(&rel) || config.excludes.is_match(&rel) {
            continue;
        }
        let metadata = fs::metadata(path).with_context(|| format!("stat {}", path.display()))?;
        let size_bytes = metadata.len();
        let modified_unix = metadata.modified().ok().and_then(system_time_secs);
        let modified_at = metadata.modified().ok().map(system_time_iso);
        let extension = path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.to_ascii_lowercase());
        let mime_type = mime_guess::from_path(path)
            .first_raw()
            .map(ToString::to_string);
        let id = path_id(&rel);
        let fingerprint = fingerprint(&rel, size_bytes, modified_unix, EXTRACTION_CONFIG_VERSION);
        let discovered = DiscoveredFile {
            id,
            absolute_path: path.to_path_buf(),
            relative_path: rel,
            extension,
            mime_type,
            size_bytes,
            modified_at,
            modified_unix,
            fingerprint,
        };
        upsert_pending(config, &discovered)?;
        files.push(discovered);
    }
    Ok(files)
}

fn relative_path(root: &Path, path: &Path) -> anyhow::Result<String> {
    let root = root
        .canonicalize()
        .with_context(|| format!("canonicalize root {}", root.display()))?;
    let canonical = path
        .canonicalize()
        .with_context(|| format!("canonicalize {}", path.display()))?;
    if !canonical.starts_with(&root) {
        bail!("{} is outside root {}", path.display(), root.display());
    }
    let relative = canonical
        .strip_prefix(&root)
        .with_context(|| format!("strip root from {}", canonical.display()))?;
    let mut parts = Vec::new();
    for component in relative.components() {
        match component {
            Component::Normal(part) => parts.push(part.to_string_lossy().into_owned()),
            Component::CurDir => {}
            _ => bail!("unsupported relative path component in {}", path.display()),
        }
    }
    Ok(parts.join("/"))
}

fn upsert_pending(config: &Config, file: &DiscoveredFile) -> anyhow::Result<()> {
    let conn = open_connection(&config.index_path)?;
    let status = if file.size_bytes > config.max_file_size_bytes {
        "skipped"
    } else {
        "pending"
    };
    let reason = if status == "skipped" {
        Some("file_too_large")
    } else {
        None
    };
    conn.execute(
        r#"
        INSERT INTO files (
            id, path, extension, mime_type, size_bytes, modified_at, modified_unix,
            status, status_reason, error_summary, fingerprint, extraction_config_version
        )
        VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, NULL, ?10, ?11)
        ON CONFLICT(id) DO UPDATE SET
            path = excluded.path,
            extension = excluded.extension,
            mime_type = excluded.mime_type,
            size_bytes = excluded.size_bytes,
            modified_at = excluded.modified_at,
            modified_unix = excluded.modified_unix,
            status = CASE
                WHEN files.fingerprint = excluded.fingerprint AND files.status = 'indexed' THEN files.status
                ELSE excluded.status
            END,
            status_reason = CASE
                WHEN files.fingerprint = excluded.fingerprint AND files.status = 'indexed' THEN files.status_reason
                ELSE excluded.status_reason
            END,
            error_summary = CASE
                WHEN files.fingerprint = excluded.fingerprint AND files.status = 'indexed' THEN files.error_summary
                ELSE NULL
            END,
            fingerprint = excluded.fingerprint,
            extraction_config_version = excluded.extraction_config_version
        "#,
        params![
            file.id,
            file.relative_path,
            file.extension,
            file.mime_type,
            i64::try_from(file.size_bytes).unwrap_or(i64::MAX),
            file.modified_at,
            file.modified_unix,
            status,
            reason,
            file.fingerprint,
            EXTRACTION_CONFIG_VERSION,
        ],
    )?;
    Ok(())
}

async fn process_file(config: Config, file: DiscoveredFile) -> anyhow::Result<String> {
    if file.size_bytes > config.max_file_size_bytes {
        return Ok("skipped".to_string());
    }
    if cached_indexed(&config, &file)? {
        return Ok("indexed".to_string());
    }

    let extraction = extract_all_formats(&file.absolute_path, file.mime_type.clone()).await;
    match extraction {
        Ok(extracted) => {
            store_extraction(&config, &file, extracted)?;
            Ok("indexed".to_string())
        }
        Err(error) => {
            let (status, summary) = classify_extraction_error(&error);
            warn!("{}: {summary}", file.relative_path);
            store_failure(&config, &file, status, &summary)?;
            Ok(status.to_string())
        }
    }
}

fn cached_indexed(config: &Config, file: &DiscoveredFile) -> anyhow::Result<bool> {
    let conn = open_connection(&config.index_path)?;
    let cached: Option<String> = conn
        .query_row(
            "SELECT status FROM files WHERE id = ?1 AND fingerprint = ?2 AND status = 'indexed'",
            params![file.id, file.fingerprint],
            |row| row.get(0),
        )
        .optional()?;
    Ok(cached.is_some())
}

#[derive(Debug)]
struct ExtractedOutputs {
    markdown: String,
    json_tree: Value,
    structured_json: Value,
    metadata_json: Value,
    tables_json: Value,
    elements: Vec<ElementRecordDraft>,
    table_cells: Vec<TableCellDraft>,
    structure_json: Value,
}

#[derive(Debug)]
struct ElementRecordDraft {
    element_index: u32,
    kind: String,
    text: String,
    page: Option<u32>,
    bbox_json: Option<Value>,
    confidence: Option<f64>,
}

#[derive(Debug)]
struct TableCellDraft {
    table_index: u32,
    row_index: u32,
    column_index: u32,
    text: String,
    page: Option<u32>,
    bbox_json: Option<Value>,
}

async fn extract_all_formats(
    path: &Path,
    mime_type: Option<String>,
) -> anyhow::Result<ExtractedOutputs> {
    let markdown_result = extract_with_format(path, mime_type.clone(), OutputFormat::Markdown)
        .await
        .context("extract markdown")?;
    let json_result = extract_with_format(path, mime_type.clone(), OutputFormat::Json)
        .await
        .unwrap_or_else(|error| {
            warn!(
                "JSON tree extraction failed for {}: {error}",
                path.display()
            );
            markdown_result.clone()
        });
    let structured_result = extract_with_format(path, mime_type, OutputFormat::Structured)
        .await
        .unwrap_or_else(|error| {
            warn!(
                "structured extraction failed for {}: {error}",
                path.display()
            );
            markdown_result.clone()
        });

    let metadata_json = serde_json::to_value(&markdown_result.metadata)?;
    let tables_json = serde_json::to_value(&markdown_result.tables)?;
    let structure_json = serde_json::to_value(&markdown_result.document)?;
    let json_tree = serde_json::from_str(&json_result.content)
        .unwrap_or_else(|_| json!({ "content": json_result.content }));
    let structured_json = serde_json::from_str(&structured_result.content)
        .unwrap_or_else(|_| serde_json::to_value(&structured_result).unwrap_or(Value::Null));
    let elements = normalize_elements(path, &markdown_result)?;
    let table_cells = normalize_tables(&markdown_result)?;

    Ok(ExtractedOutputs {
        markdown: markdown_result.content,
        json_tree,
        structured_json,
        metadata_json,
        tables_json,
        elements,
        table_cells,
        structure_json,
    })
}

async fn extract_with_format(
    path: &Path,
    mime_type: Option<String>,
    output_format: OutputFormat,
) -> kreuzberg::Result<kreuzberg::ExtractionResult> {
    let mut config = ExtractionConfig {
        output_format,
        include_document_structure: true,
        result_format: ResultFormat::ElementBased,
        max_concurrent_extractions: Some(1),
        use_cache: false,
        ..ExtractionConfig::default()
    };
    config.disable_ocr = true;
    extract_file(path.to_path_buf(), mime_type.as_deref(), &config).await
}

fn normalize_elements(
    path: &Path,
    result: &kreuzberg::ExtractionResult,
) -> anyhow::Result<Vec<ElementRecordDraft>> {
    let Some(elements) = result.elements.as_ref() else {
        return Ok(Vec::new());
    };
    elements
        .iter()
        .enumerate()
        .map(|(index, element)| {
            let bbox_json = element
                .metadata
                .coordinates
                .map(serde_json::to_value)
                .transpose()
                .with_context(|| format!("serialize bbox for {}", path.display()))?;
            Ok(ElementRecordDraft {
                element_index: u32::try_from(index).unwrap_or(u32::MAX),
                kind: format!("{:?}", element.element_type),
                text: element.text.clone(),
                page: element.metadata.page_number,
                bbox_json,
                confidence: None,
            })
        })
        .collect()
}

fn normalize_tables(result: &kreuzberg::ExtractionResult) -> anyhow::Result<Vec<TableCellDraft>> {
    let mut cells = Vec::new();
    for (table_index, table) in result.tables.iter().enumerate() {
        let bbox_json = table.bounding_box.map(serde_json::to_value).transpose()?;
        for (row_index, row) in table.cells.iter().enumerate() {
            for (column_index, text) in row.iter().enumerate() {
                cells.push(TableCellDraft {
                    table_index: u32::try_from(table_index).unwrap_or(u32::MAX),
                    row_index: u32::try_from(row_index).unwrap_or(u32::MAX),
                    column_index: u32::try_from(column_index).unwrap_or(u32::MAX),
                    text: text.clone(),
                    page: Some(table.page_number),
                    bbox_json: bbox_json.clone(),
                });
            }
        }
    }
    Ok(cells)
}

fn store_extraction(
    config: &Config,
    file: &DiscoveredFile,
    extracted: ExtractedOutputs,
) -> anyhow::Result<()> {
    let conn = open_connection(&config.index_path)?;
    let tx = conn.unchecked_transaction()?;
    let now = Utc::now().to_rfc3339();
    let content_hash = content_hash(&extracted.markdown);
    tx.execute(
        r#"
        UPDATE files SET
            indexed_at = ?2,
            status = 'indexed',
            status_reason = NULL,
            error_summary = NULL,
            content_markdown = ?3,
            content_json = ?4,
            content_structured_json = ?5,
            structure_json = ?6,
            metadata_json = ?7,
            tables_json = ?8,
            content_hash = ?9,
            fingerprint = ?10,
            extraction_config_version = ?11
        WHERE id = ?1
        "#,
        params![
            file.id,
            now,
            extracted.markdown,
            serde_json::to_string(&extracted.json_tree)?,
            serde_json::to_string(&extracted.structured_json)?,
            serde_json::to_string(&extracted.structure_json)?,
            serde_json::to_string(&extracted.metadata_json)?,
            serde_json::to_string(&extracted.tables_json)?,
            content_hash,
            file.fingerprint,
            EXTRACTION_CONFIG_VERSION,
        ],
    )?;
    tx.execute("DELETE FROM elements WHERE file_id = ?1", params![file.id])?;
    tx.execute(
        "DELETE FROM table_cells WHERE file_id = ?1",
        params![file.id],
    )?;
    tx.execute("DELETE FROM chunks WHERE file_id = ?1", params![file.id])?;
    tx.execute("DELETE FROM files_fts WHERE file_id = ?1", params![file.id])?;

    for element in extracted.elements {
        tx.execute(
            r#"
            INSERT INTO elements (file_id, element_index, kind, text, page, bbox_json, confidence)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
            params![
                file.id,
                element.element_index,
                element.kind,
                element.text,
                element.page,
                element.bbox_json.map(|value| value.to_string()),
                element.confidence,
            ],
        )?;
    }

    for cell in extracted.table_cells {
        tx.execute(
            r#"
            INSERT INTO table_cells (file_id, table_index, row_index, column_index, text, page, bbox_json)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
            params![
                file.id,
                cell.table_index,
                cell.row_index,
                cell.column_index,
                cell.text,
                cell.page,
                cell.bbox_json.map(|value| value.to_string()),
            ],
        )?;
    }

    for chunk in chunk_text(&extracted.markdown, DEFAULT_CHUNK_SIZE) {
        tx.execute(
            r#"
            INSERT INTO chunks (file_id, chunk_index, content, start_offset, end_offset)
            VALUES (?1, ?2, ?3, ?4, ?5)
            "#,
            params![
                file.id,
                chunk.chunk_index,
                chunk.content,
                chunk.start_offset,
                chunk.end_offset,
            ],
        )?;
    }

    tx.execute(
        "INSERT INTO files_fts (file_id, path, content) VALUES (?1, ?2, ?3)",
        params![file.id, file.relative_path, extracted.markdown],
    )?;
    tx.commit()?;
    Ok(())
}

fn store_failure(
    config: &Config,
    file: &DiscoveredFile,
    status: &str,
    summary: &str,
) -> anyhow::Result<()> {
    let conn = open_connection(&config.index_path)?;
    conn.execute(
        r#"
        UPDATE files SET
            indexed_at = ?2,
            status = ?3,
            status_reason = ?4,
            error_summary = ?5,
            content_markdown = '',
            content_json = NULL,
            content_structured_json = NULL,
            structure_json = NULL,
            metadata_json = NULL,
            tables_json = NULL
        WHERE id = ?1
        "#,
        params![file.id, Utc::now().to_rfc3339(), status, status, summary],
    )?;
    conn.execute("DELETE FROM elements WHERE file_id = ?1", params![file.id])?;
    conn.execute(
        "DELETE FROM table_cells WHERE file_id = ?1",
        params![file.id],
    )?;
    conn.execute("DELETE FROM chunks WHERE file_id = ?1", params![file.id])?;
    conn.execute("DELETE FROM files_fts WHERE file_id = ?1", params![file.id])?;
    Ok(())
}

#[derive(Debug)]
struct ChunkDraft {
    chunk_index: u32,
    content: String,
    start_offset: u32,
    end_offset: u32,
}

fn chunk_text(text: &str, chunk_size: usize) -> Vec<ChunkDraft> {
    if text.is_empty() {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < text.len() {
        let mut end = (start + chunk_size).min(text.len());
        while end < text.len() && !text.is_char_boundary(end) {
            end -= 1;
        }
        if end == start {
            end = text.len();
        }
        chunks.push(ChunkDraft {
            chunk_index: u32::try_from(chunks.len()).unwrap_or(u32::MAX),
            content: text[start..end].to_string(),
            start_offset: u32::try_from(start).unwrap_or(u32::MAX),
            end_offset: u32::try_from(end).unwrap_or(u32::MAX),
        });
        start = end;
    }
    chunks
}

async fn health(State(state): State<AppState>) -> Result<Json<HealthResponse>, ApiError> {
    let conn = open_connection(&state.db_path)?;
    let root: String = conn
        .query_row("SELECT value FROM meta WHERE key = 'root'", [], |row| {
            row.get(0)
        })
        .unwrap_or_else(|_| ".".to_string());
    Ok(Json(HealthResponse {
        status: "ok",
        root,
        pending_files: count_status(&conn, "pending")?,
        indexed_files: count_status(&conn, "indexed")?,
        unsupported_files: count_status(&conn, "unsupported")?,
        failed_files: count_status(&conn, "failed")?,
        skipped_files: count_status(&conn, "skipped")?,
    }))
}

fn count_status(conn: &Connection, status: &str) -> anyhow::Result<u64> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM files WHERE status = ?1",
        params![status],
        |row| row.get(0),
    )?;
    Ok(u64::try_from(count).unwrap_or(0))
}

async fn list_files(
    State(state): State<AppState>,
    Query(query): Query<FilesQuery>,
) -> Result<Json<ListResponse<FileRecord>>, ApiError> {
    let (limit, offset) = limit_offset(query.limit, query.offset, DEFAULT_LIMIT);
    let conn = open_connection(&state.db_path)?;
    let mut sql = String::from(
        "SELECT id, path, extension, mime_type, size_bytes, modified_at, indexed_at, status, status_reason, error_summary FROM files WHERE 1=1",
    );
    let mut values = Vec::new();
    append_optional_filter(
        &mut sql,
        &mut values,
        "path",
        "LIKE",
        query.path_prefix.map(|v| format!("{v}%")),
    );
    append_optional_filter(&mut sql, &mut values, "extension", "=", query.extension);
    append_optional_filter(&mut sql, &mut values, "mime_type", "=", query.mime_type);
    append_optional_filter(&mut sql, &mut values, "status", "=", query.status);
    sql.push_str(" ORDER BY path LIMIT ? OFFSET ?");

    let mut stmt = conn.prepare(&sql)?;
    let mut bind_values: Vec<&dyn rusqlite::ToSql> = values
        .iter()
        .map(|value| value as &dyn rusqlite::ToSql)
        .collect();
    let limit_i64 = i64::from(limit);
    let offset_i64 = i64::from(offset);
    bind_values.push(&limit_i64);
    bind_values.push(&offset_i64);
    let items = stmt
        .query_map(bind_values.as_slice(), row_to_file_record)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(ListResponse {
        items,
        limit,
        offset,
    }))
}

fn append_optional_filter(
    sql: &mut String,
    values: &mut Vec<String>,
    column: &str,
    op: &str,
    value: Option<String>,
) {
    if let Some(value) = value {
        sql.push_str(" AND ");
        sql.push_str(column);
        sql.push(' ');
        sql.push_str(op);
        sql.push_str(" ?");
        values.push(value);
    }
}

async fn get_file(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<FileRecord>, ApiError> {
    let conn = open_connection(&state.db_path)?;
    get_file_record(&conn, &id).map(Json)
}

async fn get_content(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<ContentResponse>, ApiError> {
    let conn = open_connection(&state.db_path)?;
    let response = conn
        .query_row(
            "SELECT id, path, content_markdown FROM files WHERE id = ?1",
            params![id],
            |row| {
                Ok(ContentResponse {
                    id: row.get(0)?,
                    path: row.get(1)?,
                    content_markdown: row.get(2)?,
                    content_format: "markdown",
                })
            },
        )
        .optional()?
        .ok_or(ApiError::NotFound)?;
    Ok(Json(response))
}

async fn get_metadata(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<MetadataResponse>, ApiError> {
    let conn = open_connection(&state.db_path)?;
    get_json_resource(&conn, &id, "metadata_json", |id, path, value| {
        MetadataResponse {
            id,
            path,
            metadata: value,
        }
    })
    .map(Json)
}

async fn get_structure(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
) -> Result<Json<StructureResponse>, ApiError> {
    let conn = open_connection(&state.db_path)?;
    get_json_resource(&conn, &id, "structure_json", |id, path, value| {
        StructureResponse {
            id,
            path,
            structure: value,
        }
    })
    .map(Json)
}

fn get_json_resource<T, F>(
    conn: &Connection,
    id: &str,
    column: &str,
    build: F,
) -> Result<T, ApiError>
where
    F: FnOnce(String, String, Value) -> T,
{
    let sql = format!("SELECT id, path, {column} FROM files WHERE id = ?1");
    let (id, path, raw): (String, String, Option<String>) = conn
        .query_row(&sql, params![id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .optional()?
        .ok_or(ApiError::NotFound)?;
    let value = raw
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|error| ApiError::Internal(error.into()))?
        .unwrap_or(Value::Null);
    Ok(build(id, path, value))
}

#[derive(Debug)]
struct StoredStructure {
    file_id: String,
    path: String,
    structure: Value,
}

async fn get_file_nodes(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<StructureRowsQuery>,
) -> Result<Json<ListResponse<StructureNodeRecord>>, ApiError> {
    let (limit, offset) = limit_offset(query.limit, query.offset, DEFAULT_LIMIT);
    let conn = open_connection(&state.db_path)?;
    let source = get_stored_structure(&conn, &id)?;
    let items = filter_nodes_by_type(
        structure_nodes_for_file(&source.file_id, &source.path, &source.structure),
        query.node_type,
    );
    Ok(Json(ListResponse {
        items: paginate(items, limit, offset),
        limit,
        offset,
    }))
}

async fn get_file_headings(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<ListResponse<HeadingRecord>>, ApiError> {
    let (limit, offset) = limit_offset(query.limit, query.offset, DEFAULT_LIMIT);
    let conn = open_connection(&state.db_path)?;
    let source = get_stored_structure(&conn, &id)?;
    let nodes = structure_nodes_for_file(&source.file_id, &source.path, &source.structure);
    let items = structure_headings_from_nodes(&nodes);
    Ok(Json(ListResponse {
        items: paginate(items, limit, offset),
        limit,
        offset,
    }))
}

async fn get_file_sections(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<ListResponse<SectionRecord>>, ApiError> {
    let (limit, offset) = limit_offset(query.limit, query.offset, DEFAULT_LIMIT);
    let conn = open_connection(&state.db_path)?;
    let source = get_stored_structure(&conn, &id)?;
    let nodes = structure_nodes_for_file(&source.file_id, &source.path, &source.structure);
    let items = structure_sections_from_nodes(&nodes);
    Ok(Json(ListResponse {
        items: paginate(items, limit, offset),
        limit,
        offset,
    }))
}

async fn list_nodes(
    State(state): State<AppState>,
    Query(query): Query<StructureRowsQuery>,
) -> Result<Json<ListResponse<StructureNodeRecord>>, ApiError> {
    let StructureRowsQuery {
        path_prefix,
        status,
        node_type,
        limit,
        offset,
    } = query;
    let (limit, offset) = limit_offset(limit, offset, DEFAULT_LIMIT);
    let conn = open_connection(&state.db_path)?;
    let sources = list_stored_structures(&conn, path_prefix, status)?;
    let items = sources
        .iter()
        .flat_map(|source| {
            structure_nodes_for_file(&source.file_id, &source.path, &source.structure)
        })
        .collect();
    let items = filter_nodes_by_type(items, node_type);
    Ok(Json(ListResponse {
        items: paginate(items, limit, offset),
        limit,
        offset,
    }))
}

async fn list_headings(
    State(state): State<AppState>,
    Query(query): Query<StructureRowsQuery>,
) -> Result<Json<ListResponse<HeadingRecord>>, ApiError> {
    let StructureRowsQuery {
        path_prefix,
        status,
        limit,
        offset,
        ..
    } = query;
    let (limit, offset) = limit_offset(limit, offset, DEFAULT_LIMIT);
    let conn = open_connection(&state.db_path)?;
    let sources = list_stored_structures(&conn, path_prefix, status)?;
    let items = sources
        .iter()
        .flat_map(|source| {
            let nodes = structure_nodes_for_file(&source.file_id, &source.path, &source.structure);
            structure_headings_from_nodes(&nodes)
        })
        .collect();
    Ok(Json(ListResponse {
        items: paginate(items, limit, offset),
        limit,
        offset,
    }))
}

async fn list_sections(
    State(state): State<AppState>,
    Query(query): Query<StructureRowsQuery>,
) -> Result<Json<ListResponse<SectionRecord>>, ApiError> {
    let StructureRowsQuery {
        path_prefix,
        status,
        limit,
        offset,
        ..
    } = query;
    let (limit, offset) = limit_offset(limit, offset, DEFAULT_LIMIT);
    let conn = open_connection(&state.db_path)?;
    let sources = list_stored_structures(&conn, path_prefix, status)?;
    let items = sources
        .iter()
        .flat_map(|source| {
            let nodes = structure_nodes_for_file(&source.file_id, &source.path, &source.structure);
            structure_sections_from_nodes(&nodes)
        })
        .collect();
    Ok(Json(ListResponse {
        items: paginate(items, limit, offset),
        limit,
        offset,
    }))
}

fn get_stored_structure(conn: &Connection, id: &str) -> Result<StoredStructure, ApiError> {
    let (file_id, path, raw): (String, String, Option<String>) = conn
        .query_row(
            "SELECT id, path, structure_json FROM files WHERE id = ?1",
            params![id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()?
        .ok_or(ApiError::NotFound)?;
    Ok(StoredStructure {
        file_id,
        path,
        structure: parse_optional_json(raw)?,
    })
}

fn list_stored_structures(
    conn: &Connection,
    path_prefix: Option<String>,
    status: Option<String>,
) -> Result<Vec<StoredStructure>, ApiError> {
    let mut sql =
        String::from("SELECT id, path, structure_json FROM files WHERE structure_json IS NOT NULL");
    let mut values = Vec::new();
    append_optional_filter(
        &mut sql,
        &mut values,
        "path",
        "LIKE",
        path_prefix.map(|v| format!("{v}%")),
    );
    append_optional_filter(&mut sql, &mut values, "status", "=", status);
    sql.push_str(" ORDER BY path");

    let mut stmt = conn.prepare(&sql)?;
    let bind_values = values
        .iter()
        .map(|value| value as &dyn rusqlite::ToSql)
        .collect::<Vec<_>>();
    let rows = stmt
        .query_map(bind_values.as_slice(), |row| {
            let file_id: String = row.get(0)?;
            let path: String = row.get(1)?;
            let raw: Option<String> = row.get(2)?;
            Ok((file_id, path, raw))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    rows.into_iter()
        .map(|(file_id, path, raw)| {
            Ok(StoredStructure {
                file_id,
                path,
                structure: parse_optional_json(raw)?,
            })
        })
        .collect()
}

fn parse_optional_json(raw: Option<String>) -> Result<Value, ApiError> {
    raw.as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|error| ApiError::Internal(error.into()))
        .map(|value| value.unwrap_or(Value::Null))
}

fn structure_nodes_for_file(
    file_id: &str,
    path: &str,
    structure: &Value,
) -> Vec<StructureNodeRecord> {
    structure
        .get("nodes")
        .and_then(Value::as_array)
        .map(|nodes| {
            nodes
                .iter()
                .enumerate()
                .map(|(index, node)| structure_node_for_file(file_id, path, index, node))
                .collect()
        })
        .unwrap_or_default()
}

fn structure_node_for_file(
    file_id: &str,
    path: &str,
    index: usize,
    node: &Value,
) -> StructureNodeRecord {
    let content = node.get("content").cloned().unwrap_or(Value::Null);
    let node_type = json_string(&content, "node_type");
    let heading_level = json_u32(&content, "heading_level").or_else(|| json_u32(&content, "level"));
    let heading_text = json_string(&content, "heading_text").or_else(|| {
        if node_type.as_deref() == Some("heading") {
            json_string(&content, "text")
        } else {
            None
        }
    });
    StructureNodeRecord {
        file_id: file_id.to_string(),
        path: path.to_string(),
        node_index: u32::try_from(index).unwrap_or(u32::MAX),
        parent_index: json_u32(node, "parent"),
        node_type,
        heading_level,
        heading_text,
        text: node_text(&content),
        page: json_u32(node, "page"),
        bbox_json: node.get("bbox").cloned(),
        children_json: node.get("children").cloned(),
        content_json: content,
    }
}

fn structure_headings_from_nodes(nodes: &[StructureNodeRecord]) -> Vec<HeadingRecord> {
    heading_positions(nodes)
        .into_iter()
        .filter_map(|position| {
            let node = &nodes[position];
            Some(HeadingRecord {
                file_id: node.file_id.clone(),
                path: node.path.clone(),
                node_index: node.node_index,
                parent_index: node.parent_index,
                level: node.heading_level?,
                text: node.heading_text.clone()?,
                page: node.page,
                bbox_json: node.bbox_json.clone(),
                source_node_type: node.node_type.clone(),
            })
        })
        .collect()
}

fn structure_sections_from_nodes(nodes: &[StructureNodeRecord]) -> Vec<SectionRecord> {
    let heading_positions = heading_positions(nodes);
    if heading_positions.is_empty() {
        let text_parts = nodes
            .iter()
            .filter_map(|node| node.text.as_deref())
            .filter(|text| !text.trim().is_empty())
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        if text_parts.is_empty() {
            return Vec::new();
        }
        return vec![SectionRecord {
            file_id: nodes[0].file_id.clone(),
            path: nodes[0].path.clone(),
            section_index: 0,
            heading_node_index: None,
            heading_level: None,
            heading_text: None,
            text: text_parts.join("\n\n"),
            node_indexes_json: json!(nodes.iter().map(|node| node.node_index).collect::<Vec<_>>()),
        }];
    }

    heading_positions
        .iter()
        .enumerate()
        .map(|(section_index, heading_position)| {
            let heading = &nodes[*heading_position];
            let level = heading.heading_level.unwrap_or(u32::MAX);
            let end = heading_positions
                .iter()
                .skip(section_index + 1)
                .copied()
                .find(|position| nodes[*position].heading_level.unwrap_or(u32::MAX) <= level)
                .unwrap_or(nodes.len());

            let mut node_indexes = vec![heading.node_index];
            let mut text_parts = Vec::new();
            for node in nodes.iter().take(end).skip(heading_position + 1) {
                if node.heading_level.is_some() && node.heading_text.is_some() {
                    continue;
                }
                let Some(text) = node.text.as_deref() else {
                    continue;
                };
                if text.trim().is_empty() {
                    continue;
                }
                node_indexes.push(node.node_index);
                text_parts.push(text.to_string());
            }

            SectionRecord {
                file_id: heading.file_id.clone(),
                path: heading.path.clone(),
                section_index: u32::try_from(section_index).unwrap_or(u32::MAX),
                heading_node_index: Some(heading.node_index),
                heading_level: heading.heading_level,
                heading_text: heading.heading_text.clone(),
                text: text_parts.join("\n\n"),
                node_indexes_json: json!(node_indexes),
            }
        })
        .collect()
}

fn heading_positions(nodes: &[StructureNodeRecord]) -> Vec<usize> {
    let direct = nodes
        .iter()
        .enumerate()
        .filter(|(_, node)| {
            node.node_type.as_deref() == Some("heading")
                && node.heading_level.is_some()
                && node.heading_text.is_some()
        })
        .map(|(index, _)| index)
        .collect::<Vec<_>>();
    if !direct.is_empty() {
        return direct;
    }
    nodes
        .iter()
        .enumerate()
        .filter(|(_, node)| node.heading_level.is_some() && node.heading_text.is_some())
        .map(|(index, _)| index)
        .collect()
}

fn filter_nodes_by_type(
    nodes: Vec<StructureNodeRecord>,
    node_type: Option<String>,
) -> Vec<StructureNodeRecord> {
    let Some(node_type) = node_type else {
        return nodes;
    };
    nodes
        .into_iter()
        .filter(|node| node.node_type.as_deref() == Some(node_type.as_str()))
        .collect()
}

fn paginate<T>(items: Vec<T>, limit: u32, offset: u32) -> Vec<T> {
    let skip = usize::try_from(offset).unwrap_or(usize::MAX);
    let take = usize::try_from(limit).unwrap_or(usize::MAX);
    items.into_iter().skip(skip).take(take).collect()
}

fn node_text(content: &Value) -> Option<String> {
    json_string(content, "text")
        .or_else(|| json_string(content, "heading_text"))
        .or_else(|| table_grid_text(content))
}

fn table_grid_text(content: &Value) -> Option<String> {
    let grid = content.get("grid")?;
    let rows = usize::try_from(grid.get("rows")?.as_u64()?).ok()?;
    let cols = usize::try_from(grid.get("cols")?.as_u64()?).ok()?;
    if rows == 0 || cols == 0 {
        return None;
    }
    let mut table = vec![vec![String::new(); cols]; rows];
    for cell in grid.get("cells")?.as_array()? {
        let row = usize::try_from(cell.get("row")?.as_u64()?).ok()?;
        let col = usize::try_from(cell.get("col")?.as_u64()?).ok()?;
        if row >= rows || col >= cols {
            continue;
        }
        table[row][col] = cell
            .get("content")
            .and_then(value_to_text)
            .unwrap_or_default();
    }
    Some(
        table
            .into_iter()
            .map(|row| row.join("\t"))
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

fn json_u32(value: &Value, key: &str) -> Option<u32> {
    value
        .get(key)
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
}

fn json_string(value: &Value, key: &str) -> Option<String> {
    value.get(key).and_then(value_to_text)
}

fn value_to_text(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        _ => None,
    }
}

async fn get_elements(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<ListResponse<ElementRecord>>, ApiError> {
    let (limit, offset) = limit_offset(query.limit, query.offset, DEFAULT_LIMIT);
    let conn = open_connection(&state.db_path)?;
    ensure_file_exists(&conn, &id)?;
    let mut stmt = conn.prepare(
        r#"
        SELECT e.file_id, f.path, e.element_index, e.kind, e.text, e.page, e.bbox_json, e.confidence
        FROM elements e
        JOIN files f ON f.id = e.file_id
        WHERE e.file_id = ?1
        ORDER BY e.element_index
        LIMIT ?2 OFFSET ?3
        "#,
    )?;
    let items = stmt
        .query_map(params![id, limit, offset], row_to_element)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(ListResponse {
        items,
        limit,
        offset,
    }))
}

async fn get_tables(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<ListResponse<TableCellRecord>>, ApiError> {
    let (limit, offset) = limit_offset(query.limit, query.offset, DEFAULT_LIMIT);
    let conn = open_connection(&state.db_path)?;
    ensure_file_exists(&conn, &id)?;
    let mut stmt = conn.prepare(
        r#"
        SELECT t.file_id, f.path, t.table_index, t.row_index, t.column_index, t.text, t.page, t.bbox_json
        FROM table_cells t
        JOIN files f ON f.id = t.file_id
        WHERE t.file_id = ?1
        ORDER BY t.table_index, t.row_index, t.column_index
        LIMIT ?2 OFFSET ?3
        "#,
    )?;
    let items = stmt
        .query_map(params![id, limit, offset], row_to_table_cell)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(ListResponse {
        items,
        limit,
        offset,
    }))
}

async fn get_file_chunks(
    State(state): State<AppState>,
    AxumPath(id): AxumPath<String>,
    Query(query): Query<PageQuery>,
) -> Result<Json<ListResponse<ChunkRecord>>, ApiError> {
    let (limit, offset) = limit_offset(query.limit, query.offset, DEFAULT_LIMIT);
    let conn = open_connection(&state.db_path)?;
    ensure_file_exists(&conn, &id)?;
    let mut stmt = conn.prepare(
        r#"
        SELECT c.file_id, f.path, c.chunk_index, c.content, c.start_offset, c.end_offset
        FROM chunks c
        JOIN files f ON f.id = c.file_id
        WHERE c.file_id = ?1
        ORDER BY c.chunk_index
        LIMIT ?2 OFFSET ?3
        "#,
    )?;
    let items = stmt
        .query_map(params![id, limit, offset], row_to_chunk)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(ListResponse {
        items,
        limit,
        offset,
    }))
}

async fn list_chunks(
    State(state): State<AppState>,
    Query(query): Query<ChunksQuery>,
) -> Result<Json<ListResponse<ChunkRecord>>, ApiError> {
    let (limit, offset) = limit_offset(query.limit, query.offset, DEFAULT_LIMIT);
    let conn = open_connection(&state.db_path)?;
    let mut sql = String::from(
        r#"
        SELECT c.file_id, f.path, c.chunk_index, c.content, c.start_offset, c.end_offset
        FROM chunks c
        JOIN files f ON f.id = c.file_id
        WHERE 1=1
        "#,
    );
    let mut values = Vec::new();
    append_optional_filter(
        &mut sql,
        &mut values,
        "f.path",
        "LIKE",
        query.path_prefix.map(|v| format!("{v}%")),
    );
    append_optional_filter(&mut sql, &mut values, "f.status", "=", query.status);
    sql.push_str(" ORDER BY f.path, c.chunk_index LIMIT ? OFFSET ?");
    let mut stmt = conn.prepare(&sql)?;
    let mut bind_values: Vec<&dyn rusqlite::ToSql> = values
        .iter()
        .map(|value| value as &dyn rusqlite::ToSql)
        .collect();
    let limit_i64 = i64::from(limit);
    let offset_i64 = i64::from(offset);
    bind_values.push(&limit_i64);
    bind_values.push(&offset_i64);
    let items = stmt
        .query_map(bind_values.as_slice(), row_to_chunk)?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(ListResponse {
        items,
        limit,
        offset,
    }))
}

async fn search(
    State(state): State<AppState>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<ListResponse<SearchRecord>>, ApiError> {
    if query.q.trim().is_empty() {
        return Err(ApiError::BadRequest("q must not be empty".to_string()));
    }
    let (limit, offset) = limit_offset(query.limit, query.offset, DEFAULT_SEARCH_LIMIT);
    let conn = open_connection(&state.db_path)?;
    let mut stmt = conn.prepare(
        r#"
        SELECT f.id, f.path, bm25(files_fts), snippet(files_fts, 2, '', '', '...', 24)
        FROM files_fts
        JOIN files f ON f.id = files_fts.file_id
        WHERE files_fts MATCH ?1
        ORDER BY bm25(files_fts)
        LIMIT ?2 OFFSET ?3
        "#,
    )?;
    let items = stmt
        .query_map(params![query.q, limit, offset], |row| {
            Ok(SearchRecord {
                id: row.get(0)?,
                path: row.get(1)?,
                score: row.get(2)?,
                snippet: row.get(3)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(ListResponse {
        items,
        limit,
        offset,
    }))
}

fn ensure_file_exists(conn: &Connection, id: &str) -> Result<(), ApiError> {
    let exists: Option<i64> = conn
        .query_row("SELECT 1 FROM files WHERE id = ?1", params![id], |row| {
            row.get(0)
        })
        .optional()?;
    exists.map(|_| ()).ok_or(ApiError::NotFound)
}

fn get_file_record(conn: &Connection, id: &str) -> Result<FileRecord, ApiError> {
    conn.query_row(
        "SELECT id, path, extension, mime_type, size_bytes, modified_at, indexed_at, status, status_reason, error_summary FROM files WHERE id = ?1",
        params![id],
        row_to_file_record,
    )
    .optional()?
    .ok_or(ApiError::NotFound)
}

fn row_to_file_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<FileRecord> {
    let size: i64 = row.get(4)?;
    Ok(FileRecord {
        id: row.get(0)?,
        path: row.get(1)?,
        extension: row.get(2)?,
        mime_type: row.get(3)?,
        size_bytes: u64::try_from(size).unwrap_or(0),
        modified_at: row.get(5)?,
        indexed_at: row.get(6)?,
        status: row.get(7)?,
        status_reason: row.get(8)?,
        error_summary: row.get(9)?,
    })
}

fn row_to_element(row: &rusqlite::Row<'_>) -> rusqlite::Result<ElementRecord> {
    let bbox: Option<String> = row.get(6)?;
    Ok(ElementRecord {
        file_id: row.get(0)?,
        path: row.get(1)?,
        element_index: row.get(2)?,
        kind: row.get(3)?,
        text: row.get(4)?,
        page: row.get(5)?,
        bbox_json: bbox.and_then(|raw| serde_json::from_str(&raw).ok()),
        confidence: row.get(7)?,
    })
}

fn row_to_table_cell(row: &rusqlite::Row<'_>) -> rusqlite::Result<TableCellRecord> {
    let bbox: Option<String> = row.get(7)?;
    Ok(TableCellRecord {
        file_id: row.get(0)?,
        path: row.get(1)?,
        table_index: row.get(2)?,
        row_index: row.get(3)?,
        column_index: row.get(4)?,
        text: row.get(5)?,
        page: row.get(6)?,
        bbox_json: bbox.and_then(|raw| serde_json::from_str(&raw).ok()),
    })
}

fn row_to_chunk(row: &rusqlite::Row<'_>) -> rusqlite::Result<ChunkRecord> {
    Ok(ChunkRecord {
        file_id: row.get(0)?,
        path: row.get(1)?,
        chunk_index: row.get(2)?,
        content: row.get(3)?,
        start_offset: row.get(4)?,
        end_offset: row.get(5)?,
    })
}

fn limit_offset(limit: Option<u32>, offset: Option<u32>, default_limit: u32) -> (u32, u32) {
    (
        limit.unwrap_or(default_limit).clamp(1, MAX_LIMIT),
        offset.unwrap_or(0),
    )
}

fn path_id(relative_path: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(relative_path.as_bytes());
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

fn content_hash(content: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(content.as_bytes());
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

fn fingerprint(
    relative_path: &str,
    size_bytes: u64,
    modified_unix: Option<i64>,
    config: &str,
) -> String {
    let mut fields = BTreeMap::new();
    fields.insert("path", relative_path.to_string());
    fields.insert("size", size_bytes.to_string());
    fields.insert("modified", modified_unix.unwrap_or_default().to_string());
    fields.insert("config", config.to_string());
    let mut hasher = Sha256::new();
    for (key, value) in fields {
        hasher.update(key.as_bytes());
        hasher.update(b"=");
        hasher.update(value.as_bytes());
        hasher.update(b"\n");
    }
    format!("sha256:{}", hex::encode(hasher.finalize()))
}

fn system_time_secs(time: SystemTime) -> Option<i64> {
    time.duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|duration| i64::try_from(duration.as_secs()).ok())
}

fn system_time_iso(time: SystemTime) -> String {
    let datetime: DateTime<Utc> = time.into();
    datetime.to_rfc3339()
}

fn classify_extraction_error(error: &anyhow::Error) -> (&'static str, String) {
    let messages = error.chain().map(ToString::to_string).collect::<Vec<_>>();
    let unsupported = messages
        .iter()
        .find(|message| message.to_ascii_lowercase().contains("unsupported"));
    if let Some(message) = unsupported {
        return ("unsupported", sanitize_message(message));
    }
    let fallback = messages.first().map_or("extraction failed", String::as_str);
    ("failed", sanitize_message(fallback))
}

fn sanitize_message(raw: &str) -> String {
    let first_line = raw.lines().next().unwrap_or("extraction failed").trim();
    if first_line.is_empty() {
        "extraction failed".to_string()
    } else {
        first_line.chars().take(300).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn path_ids_are_stable() {
        assert_eq!(path_id("docs/a.md"), path_id("docs/a.md"));
        assert_ne!(path_id("docs/a.md"), path_id("docs/b.md"));
    }

    #[test]
    fn chunks_respect_utf8_boundaries() {
        let chunks = chunk_text("éééabc", 3);
        assert_eq!(chunks[0].content, "é");
        assert_eq!(chunks.last().expect("chunk").end_offset, 9);
    }

    #[test]
    fn relative_path_rejects_outside_root() {
        let root = tempdir().expect("root");
        let outside = tempdir().expect("outside");
        let file = outside.path().join("x.txt");
        fs::write(&file, "x").expect("write");
        let err = relative_path(root.path(), &file).expect_err("outside root should fail");
        assert!(err.to_string().contains("outside root"));
    }

    #[test]
    fn relative_path_uses_forward_slashes() {
        let root = tempdir().expect("root");
        let dir = root.path().join("a");
        fs::create_dir_all(&dir).expect("mkdir");
        let file = dir.join("b.txt");
        fs::write(&file, "x").expect("write");
        assert_eq!(
            relative_path(root.path(), &file).expect("relative path"),
            "a/b.txt"
        );
    }

    #[test]
    fn structure_headings_prefer_direct_heading_nodes() {
        let structure = json!({
            "nodes": [
                {
                    "content": {
                        "node_type": "group",
                        "heading_level": 1,
                        "heading_text": "Title"
                    },
                    "children": [1]
                },
                {
                    "content": {
                        "node_type": "heading",
                        "level": 1,
                        "text": "Title"
                    },
                    "parent": 0
                },
                {
                    "content": {
                        "node_type": "paragraph",
                        "text": "Body"
                    }
                }
            ]
        });
        let nodes = structure_nodes_for_file("file", "doc.md", &structure);
        let headings = structure_headings_from_nodes(&nodes);
        assert_eq!(headings.len(), 1);
        assert_eq!(headings[0].node_index, 1);
        assert_eq!(headings[0].text, "Title");
    }

    #[test]
    fn structure_sections_include_table_grid_text() {
        let structure = json!({
            "nodes": [
                {
                    "content": {
                        "node_type": "heading",
                        "level": 2,
                        "text": "Metrics"
                    }
                },
                {
                    "content": {
                        "node_type": "table",
                        "grid": {
                            "rows": 2,
                            "cols": 2,
                            "cells": [
                                {"row": 0, "col": 0, "content": "Metric"},
                                {"row": 0, "col": 1, "content": "Value"},
                                {"row": 1, "col": 0, "content": "Revenue"},
                                {"row": 1, "col": 1, "content": "12"}
                            ]
                        }
                    }
                }
            ]
        });
        let nodes = structure_nodes_for_file("file", "sheet.xlsx", &structure);
        let sections = structure_sections_from_nodes(&nodes);
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].heading_text.as_deref(), Some("Metrics"));
        assert_eq!(sections[0].text, "Metric\tValue\nRevenue\t12");
    }
}
