//! `v4-metadata-report` xtask subcommand.
//!
//! Emits one deterministic line per imported v4 operation describing the
//! inference outcomes that are opinions rather than facts: where the rows live
//! and what pagination contract the operation was given.
//!
//! The point is diffing. Row-path and pagination inference are heuristics over
//! vendor descriptors, so a change to either can quietly reshape a relation in
//! a source nobody was thinking about. Running this before and after such a
//! change turns "did anything else move?" from a question into a `diff`.
//!
//! Deliberately not wired into CI: it fetches multi-megabyte descriptors over
//! the network, and vendor descriptors change under us, so a green run proves
//! nothing about the commit that produced it.

use std::collections::BTreeMap;
use std::fs;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use coral_spec::PaginationMode;
use coral_spec::v4::{
    OperationMetadata, SurfaceDescriptor, SurfaceType, V4SourceManifest, import_openapi_surface,
};

use crate::sources::load_catalog_manifests;

/// Descriptor ceiling, matching `MAX_DESCRIPTOR_BYTES` in
/// `coral-app`'s materialization path.
///
/// The report exists to reproduce what the app infers, so it reads descriptors
/// the same way the app does: raw bytes straight into the importer, with no
/// reference hydration and the same size ceiling. Microsoft Graph's v1.0
/// description alone is ~38 MB.
const MAX_DESCRIPTOR_BYTES: u64 = 64 * 1024 * 1024;

/// Timeout for descriptor fetches.
const FETCH_TIMEOUT: Duration = Duration::from_mins(2);

const HEADER: &str =
    "source,operation_id,row_path,mode,page_size_param,cursor_param,offset_param,next_url_path";

/// Arguments for the `v4-metadata-report` subcommand.
#[derive(Debug, clap::Args)]
pub(crate) struct Args {
    /// Directory holding one subdirectory per v4 source manifest.
    #[arg(long, default_value = "sources/v4")]
    sources: PathBuf,

    /// File to write the report to. Writes to stdout when omitted.
    #[arg(long)]
    out: Option<PathBuf>,

    /// Directory for cached descriptor downloads. Reused across runs so a
    /// before/after comparison fetches each descriptor once.
    #[arg(long)]
    cache_dir: Option<PathBuf>,

    /// Restrict the report to these schema names. Repeatable.
    #[arg(long = "source-name")]
    source_names: Vec<String>,
}

/// One row of the report.
struct OperationRow {
    source: String,
    operation_id: String,
    row_path: String,
    mode: String,
    page_size_param: String,
    cursor_param: String,
    offset_param: String,
    next_url_path: String,
}

impl OperationRow {
    fn render(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{}",
            self.source,
            self.operation_id,
            self.row_path,
            self.mode,
            self.page_size_param,
            self.cursor_param,
            self.offset_param,
            self.next_url_path,
        )
    }
}

/// Import every v4 `OpenAPI` source under `--sources` and write the report.
pub(crate) fn run(args: &Args) -> Result<bool> {
    let manifests = load_catalog_manifests(&args.sources)
        .with_context(|| format!("loading manifests from {}", args.sources.display()))?;

    let mut rows = Vec::new();
    for manifest in &manifests {
        let schema_name = manifest.schema_name().to_owned();
        if !args.source_names.is_empty() && !args.source_names.contains(&schema_name) {
            continue;
        }
        let Some(v4) = manifest.as_v4() else {
            eprintln!("skipping {schema_name}: not a DSL v4 manifest");
            continue;
        };
        // An MCP surface enumerates its operations by calling a live,
        // authenticated server, so it cannot be imported from a checkout.
        if v4.surface.surface_type == SurfaceType::Mcp {
            eprintln!("skipping {schema_name}: MCP surfaces need a live server to enumerate tools");
            continue;
        }
        eprintln!("importing {schema_name} ...");
        rows.extend(
            import_source_rows(&schema_name, v4, args.cache_dir.as_deref())
                .with_context(|| format!("importing {schema_name}"))?,
        );
    }

    rows.sort_by(|left, right| {
        (&left.source, &left.operation_id).cmp(&(&right.source, &right.operation_id))
    });

    let mut report = String::from(HEADER);
    report.push('\n');
    for row in &rows {
        report.push_str(&row.render());
        report.push('\n');
    }

    match &args.out {
        Some(path) => {
            if let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                fs::create_dir_all(parent)
                    .with_context(|| format!("creating {}", parent.display()))?;
            }
            fs::write(path, &report).with_context(|| format!("writing {}", path.display()))?;
            eprintln!("wrote {} operations to {}", rows.len(), path.display());
        }
        None => {
            io::stdout().lock().write_all(report.as_bytes())?;
        }
    }

    Ok(true)
}

fn import_source_rows(
    schema_name: &str,
    manifest: &V4SourceManifest,
    cache_dir: Option<&Path>,
) -> Result<Vec<OperationRow>> {
    let document = load_descriptor(schema_name, &manifest.surface.descriptor, cache_dir)?;
    let imported = import_openapi_surface(manifest, &manifest.surface, &document)?;

    let metadata: BTreeMap<&str, &OperationMetadata> = imported
        .operation_metadata
        .operations
        .iter()
        .map(|(id, entry)| (id.as_str(), entry))
        .collect();

    Ok(imported
        .semantic_ir
        .operations
        .iter()
        .map(|operation| {
            let entry = metadata.get(operation.id.as_str());
            let (row_path, pagination) = match entry {
                Some(OperationMetadata::Rest {
                    row_path,
                    pagination,
                    ..
                }) => (join_path(row_path), Some(pagination)),
                Some(OperationMetadata::Mcp { row_path, .. }) => (join_path(row_path), None),
                None => (String::new(), None),
            };
            OperationRow {
                source: schema_name.to_owned(),
                operation_id: operation.id.clone(),
                row_path,
                mode: pagination.map_or_else(|| "-".to_owned(), |spec| mode_name(spec.mode)),
                page_size_param: pagination
                    .and_then(|spec| spec.page_size.as_ref())
                    .and_then(|size| size.query_param.clone())
                    .unwrap_or_default(),
                cursor_param: pagination
                    .and_then(|spec| spec.cursor_param.clone())
                    .unwrap_or_default(),
                offset_param: pagination
                    .and_then(|spec| spec.offset_param.clone())
                    .unwrap_or_default(),
                // Always empty today: no pagination contract reads a next URL
                // out of the response body yet. The column exists so the
                // baseline captured before that lands has the same shape as the
                // reports compared against it.
                next_url_path: String::new(),
            }
        })
        .collect())
}

/// Reads the descriptor, using `cache_dir` as a read-through cache so a
/// before/after comparison does not re-download tens of megabytes.
fn load_descriptor(
    schema_name: &str,
    descriptor: &SurfaceDescriptor,
    cache_dir: Option<&Path>,
) -> Result<Vec<u8>> {
    let cached = cache_dir.map(|dir| dir.join(format!("{schema_name}.descriptor")));
    if let Some(path) = &cached
        && path.exists()
    {
        eprintln!("  using cached descriptor {}", path.display());
        return fs::read(path).with_context(|| format!("reading {}", path.display()));
    }

    let bytes = match descriptor {
        SurfaceDescriptor::File { file } => {
            fs::read(file).with_context(|| format!("reading {}", file.display()))?
        }
        SurfaceDescriptor::Url { url } => fetch_descriptor(url)?,
        SurfaceDescriptor::McpServer { .. } => {
            bail!("MCP surfaces have no OpenAPI descriptor")
        }
    };
    if bytes.len() as u64 > MAX_DESCRIPTOR_BYTES {
        bail!(
            "descriptor for {schema_name} is {} bytes, over the {MAX_DESCRIPTOR_BYTES} byte ceiling",
            bytes.len()
        );
    }

    if let Some(path) = &cached {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).with_context(|| format!("creating {}", parent.display()))?;
        }
        fs::write(path, &bytes).with_context(|| format!("writing {}", path.display()))?;
    }
    Ok(bytes)
}

fn fetch_descriptor(url: &str) -> Result<Vec<u8>> {
    let client = reqwest::blocking::Client::builder()
        .timeout(FETCH_TIMEOUT)
        .build()?;
    let response = client
        .get(url)
        .send()
        .with_context(|| format!("fetching {url}"))?;
    let status = response.status();
    if !status.is_success() {
        bail!("{url} returned HTTP {status}");
    }
    Ok(response.bytes()?.to_vec())
}

/// Renders a JSON path so an empty path is visibly empty rather than absent,
/// and a nested path stays on one CSV field.
fn join_path(path: &[String]) -> String {
    path.join(".")
}

/// Renders the mode as the same token the artifact serializes.
///
/// Matched exhaustively on purpose: a new pagination mode must fail to compile
/// here rather than silently report as something else.
fn mode_name(mode: PaginationMode) -> String {
    match mode {
        PaginationMode::None => "none",
        PaginationMode::Auto => "auto",
        PaginationMode::CursorQuery => "cursor_query",
        PaginationMode::CursorBody => "cursor_body",
        PaginationMode::Page => "page",
        PaginationMode::Offset => "offset",
        PaginationMode::LinkHeader => "link_header",
    }
    .to_owned()
}

#[cfg(test)]
mod tests {
    use super::{HEADER, OperationRow, join_path};

    #[test]
    fn renders_a_row_with_the_same_field_count_as_the_header() {
        let row = OperationRow {
            source: "github_v4".to_owned(),
            operation_id: "issues_list".to_owned(),
            row_path: "items".to_owned(),
            mode: "link_header".to_owned(),
            page_size_param: "per_page".to_owned(),
            cursor_param: String::new(),
            offset_param: String::new(),
            next_url_path: String::new(),
        };

        assert_eq!(
            row.render().split(',').count(),
            HEADER.split(',').count(),
            "a row must line up with the header for the diff to be readable"
        );
    }

    #[test]
    fn joins_nested_row_paths_into_one_field() {
        assert_eq!(join_path(&[]), "");
        assert_eq!(join_path(&["value".to_owned()]), "value");
        assert_eq!(
            join_path(&["results".to_owned(), "data".to_owned()]),
            "results.data"
        );
        assert!(
            !join_path(&["results".to_owned(), "data".to_owned()]).contains(','),
            "a nested path must not split across CSV fields"
        );
    }
}
