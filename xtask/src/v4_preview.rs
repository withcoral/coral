//! `v4-preview` xtask subcommand.
//!
//! Runs Coral's own DSL v4 importer and projection derivation over a manifest,
//! then renders the SQL surface the manifest would produce as markdown.
//!
//! The point is reviewability. A hand-authored or generated `openapi.yaml`
//! diff says nothing about whether a table kept its columns, whether cursor
//! pagination was still detected, or whether row-path inference silently
//! collapsed a relation into one JSON column. The derived catalog says all
//! three, so the rendered preview is committed next to the descriptor and
//! reviewed with it.
//!
//! This calls the real importer rather than reimplementing its heuristics, and
//! needs no running server, workspace, or credentials — a file descriptor is
//! previewed entirely offline.

#![expect(
    clippy::let_underscore_must_use,
    reason = "The rendering below writes markdown into a String, where fmt::Write is infallible; handling the Result at every line would add noise without adding safety."
)]

use std::fmt::Write as _;
use std::fs;
use std::io::Write as _;
use std::io::{self};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use coral_spec::v4::{
    HttpMethod, ImportedSurface, IrExecutionAttachment, IrInputLocation, IrOperation,
    OperationMetadata, Projection, ProjectionCatalog, ProjectionKind, ProjectionVisibility,
    SqlInputExposure, SurfaceDescriptor, SurfaceType, V4SourceManifest, ValidatedSurfacePlan,
    generate_projection_catalog, import_openapi_surface,
};
use coral_spec::{ManifestDataType, PaginationMode, PaginationSpec, parse_source_manifest_yaml};

/// Descriptors are fetched only when the manifest declares a URL surface.
const FETCH_TIMEOUT: Duration = Duration::from_secs(30);
const USER_AGENT: &str = "coral-xtask-v4-preview";

/// Arguments for the `v4-preview` subcommand.
#[derive(Debug, clap::Args)]
pub(crate) struct PreviewArgs {
    /// Path to a DSL v4 `manifest.yaml`.
    manifest: PathBuf,

    /// Write the preview here instead of standard output.
    #[arg(long)]
    out: Option<PathBuf>,

    /// Fail when the file at `--out` does not already match the preview.
    #[arg(long, requires = "out")]
    check: bool,
}

/// Render the catalog preview, then write or verify it.
pub(crate) fn preview(args: &PreviewArgs) -> Result<bool> {
    let rendered = render_preview(&args.manifest)?;
    let Some(out) = args.out.as_deref() else {
        io::stdout().lock().write_all(rendered.as_bytes())?;
        return Ok(true);
    };
    if args.check {
        let current = fs::read_to_string(out)
            .with_context(|| format!("failed to read '{}' for --check", out.display()))?;
        if current == rendered {
            return Ok(true);
        }
        eprintln!(
            "xtask: '{}' is stale; re-run without --check to regenerate",
            out.display()
        );
        return Ok(false);
    }
    fs::write(out, &rendered).with_context(|| format!("failed to write '{}'", out.display()))?;
    Ok(true)
}

fn render_preview(manifest_path: &Path) -> Result<String> {
    let raw = fs::read_to_string(manifest_path)
        .with_context(|| format!("failed to read manifest '{}'", manifest_path.display()))?;
    let validated = parse_source_manifest_yaml(&raw)
        .with_context(|| format!("failed to parse manifest '{}'", manifest_path.display()))?;
    let Some(manifest) = validated.as_v4() else {
        bail!(
            "'{}' is not a DSL v4 manifest; v4-preview requires `dsl_version: 4`",
            manifest_path.display()
        );
    };
    let surface = &manifest.surface;
    if surface.surface_type != SurfaceType::OpenApi {
        bail!(
            "source '{}' declares a non-OpenAPI surface; v4-preview supports `type: openapi` only",
            manifest.common.name
        );
    }

    let descriptor = read_descriptor(manifest_path, &surface.descriptor)?;
    let imported = import_openapi_surface(manifest, surface, &descriptor.bytes)
        .with_context(|| format!("failed to import source '{}' surface", manifest.common.name))?;
    let plan = imported
        .validated_plan()
        .with_context(|| format!("failed to validate source '{}' plan", manifest.common.name))?;
    let catalog = generate_projection_catalog(manifest, &plan).with_context(|| {
        format!(
            "failed to derive projections for source '{}'",
            manifest.common.name
        )
    })?;

    Ok(render(
        manifest,
        &descriptor.label,
        &imported,
        &plan,
        &catalog,
    ))
}

struct Descriptor {
    label: String,
    bytes: Vec<u8>,
}

/// Resolve and load the descriptor the surface points at.
///
/// A relative `file:` descriptor resolves against the manifest's own
/// directory, matching how `coral source add --file <manifest>` canonicalizes
/// it before install.
fn read_descriptor(manifest_path: &Path, descriptor: &SurfaceDescriptor) -> Result<Descriptor> {
    match descriptor {
        SurfaceDescriptor::File { file } => {
            let resolved = if file.is_absolute() {
                file.clone()
            } else {
                manifest_path
                    .parent()
                    .unwrap_or_else(|| Path::new("."))
                    .join(file)
            };
            let bytes = fs::read(&resolved).with_context(|| {
                format!("failed to read OpenAPI descriptor '{}'", resolved.display())
            })?;
            Ok(Descriptor {
                label: format!("`file: {}`", file.display()),
                bytes,
            })
        }
        SurfaceDescriptor::Url { url } => {
            let bytes = fetch_descriptor(url)?;
            Ok(Descriptor {
                label: format!("`url: {url}`"),
                bytes,
            })
        }
        SurfaceDescriptor::McpServer { location } => bail!(
            "surface descriptor '{location}' is an MCP server; v4-preview supports OpenAPI descriptors only"
        ),
    }
}

fn fetch_descriptor(url: &str) -> Result<Vec<u8>> {
    let client = reqwest::blocking::Client::builder()
        .timeout(FETCH_TIMEOUT)
        .user_agent(USER_AGENT)
        .build()
        .context("failed to build HTTP client")?;
    let response = client
        .get(url)
        .send()
        .with_context(|| format!("failed to fetch descriptor '{url}'"))?;
    let status = response.status();
    if !status.is_success() {
        bail!("descriptor '{url}' returned HTTP {status}");
    }
    let bytes = response
        .bytes()
        .with_context(|| format!("failed to read descriptor body from '{url}'"))?;
    Ok(bytes.to_vec())
}

fn render(
    manifest: &V4SourceManifest,
    descriptor_label: &str,
    imported: &ImportedSurface,
    plan: &ValidatedSurfacePlan,
    catalog: &ProjectionCatalog,
) -> String {
    let ir = &imported.semantic_ir;
    let published = catalog
        .projections
        .iter()
        .filter(|projection| projection.visibility == ProjectionVisibility::Published)
        .count();
    let hidden = catalog.projections.len() - published;

    let mut out = String::new();
    let name = &manifest.common.name;
    let _ = writeln!(out, "# `{name}` — DSL v4 catalog preview");
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "Generated by `xtask v4-preview`. Do not edit by hand; regenerate instead."
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "| | |");
    let _ = writeln!(out, "| --- | --- |");
    let _ = writeln!(out, "| Descriptor | {descriptor_label} |");
    let _ = writeln!(out, "| Operations imported | {} |", ir.operations.len());
    let _ = writeln!(out, "| Published projections | {published} |");
    let _ = writeln!(out, "| Hidden projections | {hidden} |");
    let _ = writeln!(out);

    render_diagnostics(&mut out, imported, catalog);

    let _ = writeln!(out, "## Projections");
    let _ = writeln!(out);
    if catalog.projections.is_empty() {
        let _ = writeln!(out, "_None._");
        return out;
    }

    let mut projections: Vec<&Projection> = catalog.projections.iter().collect();
    projections.sort_by(|left, right| left.name.cmp(&right.name));
    for projection in projections {
        render_projection(&mut out, projection, ir.operations.as_slice(), plan);
    }
    out
}

/// Collect every diagnostic the import and derivation produced.
///
/// Diagnostics are attached at four levels — surface, operation, catalog, and
/// projection — and a reviewer wants all of them in one place, since any one of
/// them can explain a missing table.
fn render_diagnostics(out: &mut String, imported: &ImportedSurface, catalog: &ProjectionCatalog) {
    let ir = &imported.semantic_ir;
    let mut entries: Vec<(Option<&str>, &str)> = Vec::new();
    for diagnostic in &ir.diagnostics {
        entries.push((diagnostic.operation_id.as_deref(), &diagnostic.message));
    }
    for operation in &ir.operations {
        for diagnostic in &operation.diagnostics {
            entries.push((Some(operation.id.as_str()), &diagnostic.message));
        }
    }
    for diagnostic in &catalog.diagnostics {
        entries.push((diagnostic.operation_id.as_deref(), &diagnostic.message));
    }
    for projection in &catalog.projections {
        for diagnostic in &projection.diagnostics {
            entries.push((Some(projection.operation_id.as_str()), &diagnostic.message));
        }
    }

    let _ = writeln!(out, "## Diagnostics");
    let _ = writeln!(out);
    if entries.is_empty() {
        let _ = writeln!(out, "_None._");
        let _ = writeln!(out);
        return;
    }
    entries.sort_unstable();
    entries.dedup();
    for (operation_id, message) in entries {
        match operation_id {
            Some(id) => {
                let _ = writeln!(out, "- `{id}`: {message}");
            }
            None => {
                let _ = writeln!(out, "- {message}");
            }
        }
    }
    let _ = writeln!(out);
}

fn render_projection(
    out: &mut String,
    projection: &Projection,
    operations: &[IrOperation],
    plan: &ValidatedSurfacePlan,
) {
    let operation = operations
        .iter()
        .find(|candidate| candidate.id == projection.operation_id);
    let metadata = plan.metadata_for_operation(&projection.operation_id);

    let _ = writeln!(out, "### `{}`", projection.name);
    let _ = writeln!(out);
    let _ = writeln!(out, "| | |");
    let _ = writeln!(out, "| --- | --- |");
    let _ = writeln!(
        out,
        "| Kind | {} |",
        projection_kind_label(&projection.kind)
    );
    let _ = writeln!(out, "| Operation | `{}` |", projection.operation_id);
    if let Some(request) = operation.and_then(request_label) {
        let _ = writeln!(out, "| Request | `{request}` |");
    }
    let _ = writeln!(
        out,
        "| Visibility | {} |",
        visibility_label(projection.visibility)
    );
    let row_path = metadata.row_path();
    let _ = writeln!(
        out,
        "| Row path | {} |",
        if row_path.is_empty() {
            "_response root_".to_string()
        } else {
            format!("`{}`", row_path.join("."))
        }
    );
    if let OperationMetadata::Rest { pagination, .. } = metadata {
        render_pagination_rows(out, pagination);
    }
    if !projection.description.is_empty() {
        let _ = writeln!(out, "| Description | {} |", cell(&projection.description));
    }
    let _ = writeln!(out);

    render_inputs(out, projection);
    render_columns(out, projection);
}

fn render_pagination_rows(out: &mut String, pagination: &PaginationSpec) {
    let _ = writeln!(
        out,
        "| Pagination | `{}` |",
        pagination_mode_label(pagination.mode)
    );
    if let Some(cursor_param) = &pagination.cursor_param {
        let _ = writeln!(out, "| Cursor param | `{cursor_param}` |");
    }
    if !pagination.response_cursor_path.is_empty() {
        let _ = writeln!(
            out,
            "| Response cursor | `{}` |",
            pagination.response_cursor_path.join(".")
        );
    }
    if let Some(header) = &pagination.response_cursor_header {
        let _ = writeln!(out, "| Response cursor header | `{header}` |");
    }
    if let Some(page_param) = &pagination.page_param {
        let _ = writeln!(out, "| Page param | `{page_param}` |");
    }
    if let Some(offset_param) = &pagination.offset_param {
        let _ = writeln!(out, "| Offset param | `{offset_param}` |");
    }
    if let Some(page_size) = &pagination.page_size {
        let via = page_size
            .query_param
            .as_deref()
            .map_or_else(String::new, |param| format!(" via `{param}`"));
        let _ = writeln!(
            out,
            "| Page size | {} (max {}){via} |",
            page_size.default, page_size.max
        );
    }
}

fn render_inputs(out: &mut String, projection: &Projection) {
    if projection.inputs.is_empty() {
        let _ = writeln!(out, "No inputs.");
        let _ = writeln!(out);
        return;
    }
    let _ = writeln!(out, "Inputs ({}):", projection.inputs.len());
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "| Name | Exposure | Wire | In | Required | Type | Default | Description |"
    );
    let _ = writeln!(out, "| --- | --- | --- | --- | --- | --- | --- | --- |");
    for input in &projection.inputs {
        let _ = writeln!(
            out,
            "| `{}` | {} | `{}` | {} | {} | {} | {} | {} |",
            input.name,
            exposure_label(input.sql_exposure),
            input.wire_name,
            location_label(input.source_location),
            if input.required { "yes" } else { "no" },
            data_type_label(input.data_type),
            input
                .default_value
                .as_deref()
                .map_or_else(String::new, |value| format!("`{}`", cell(value))),
            cell(&input.description),
        );
    }
    let _ = writeln!(out);
}

fn render_columns(out: &mut String, projection: &Projection) {
    if projection.columns.is_empty() {
        let _ = writeln!(out, "No columns.");
        let _ = writeln!(out);
        return;
    }
    let _ = writeln!(out, "Columns ({}):", projection.columns.len());
    let _ = writeln!(out);
    let _ = writeln!(out, "| Column | Type | Source path | Description |");
    let _ = writeln!(out, "| --- | --- | --- | --- |");
    for column in &projection.columns {
        let source_path = if column.source_path.is_empty() {
            "_whole row_".to_string()
        } else {
            format!("`{}`", column.source_path.join("."))
        };
        let _ = writeln!(
            out,
            "| `{}` | {} | {source_path} | {} |",
            column.name,
            data_type_label(column.data_type),
            cell(&column.description),
        );
    }
    let _ = writeln!(out);
}

fn request_label(operation: &IrOperation) -> Option<String> {
    let IrExecutionAttachment::Rest(rest) = &operation.execution else {
        return None;
    };
    Some(format!(
        "{} {}",
        http_method_label(rest.method),
        rest.path_template
    ))
}

/// Flatten a value into one markdown table cell.
///
/// Pipes would end the cell early and newlines would end the row, so both are
/// neutralized rather than trusted.
fn cell(value: &str) -> String {
    value
        .split_whitespace()
        .collect::<Vec<&str>>()
        .join(" ")
        .replace('|', "\\|")
}

fn projection_kind_label(kind: &ProjectionKind) -> String {
    match kind {
        ProjectionKind::Table => "table".to_string(),
        ProjectionKind::TableFunction { function_kind } => {
            format!("table function ({})", function_kind.as_str())
        }
    }
}

fn visibility_label(visibility: ProjectionVisibility) -> &'static str {
    match visibility {
        ProjectionVisibility::Published => "published",
        ProjectionVisibility::Hidden => "hidden",
    }
}

fn exposure_label(exposure: SqlInputExposure) -> &'static str {
    match exposure {
        SqlInputExposure::Filter => "filter",
        SqlInputExposure::FunctionArg => "function arg",
        SqlInputExposure::Internal => "internal",
    }
}

fn location_label(location: IrInputLocation) -> &'static str {
    match location {
        IrInputLocation::Path => "path",
        IrInputLocation::Query => "query",
        IrInputLocation::Header => "header",
        IrInputLocation::Cookie => "cookie",
        IrInputLocation::Body => "body",
        IrInputLocation::ToolArg => "tool arg",
    }
}

fn data_type_label(data_type: ManifestDataType) -> &'static str {
    match data_type {
        ManifestDataType::Utf8 => "Utf8",
        ManifestDataType::Int64 => "Int64",
        ManifestDataType::Boolean => "Boolean",
        ManifestDataType::Float64 => "Float64",
        ManifestDataType::Timestamp => "Timestamp",
        ManifestDataType::Json => "Json",
    }
}

fn pagination_mode_label(mode: PaginationMode) -> &'static str {
    match mode {
        PaginationMode::None => "none",
        PaginationMode::Auto => "auto",
        PaginationMode::CursorQuery => "cursor_query",
        PaginationMode::CursorBody => "cursor_body",
        PaginationMode::Page => "page",
        PaginationMode::Offset => "offset",
        PaginationMode::LinkHeader => "link_header",
        PaginationMode::NextUrlBody => "next_url_body",
    }
}

fn http_method_label(method: HttpMethod) -> &'static str {
    match method {
        HttpMethod::Get => "GET",
        HttpMethod::Post => "POST",
        HttpMethod::Put => "PUT",
        HttpMethod::Patch => "PATCH",
        HttpMethod::Delete => "DELETE",
        HttpMethod::Head => "HEAD",
        HttpMethod::Options => "OPTIONS",
        HttpMethod::Trace => "TRACE",
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::indexing_slicing,
        missing_docs,
        reason = "Tests use fixture-shaped string assertions."
    )]

    use std::fs;
    use std::path::PathBuf;

    use tempfile::TempDir;

    use super::render_preview;

    const MANIFEST: &str = r"
dsl_version: 4
name: demo_v4
description: Fixture source.
surface:
  type: openapi
  file: ./openapi.yaml
  base_url: https://example.com/api
";

    struct Fixture {
        dir: TempDir,
    }

    impl Fixture {
        fn new(descriptor: &str) -> Self {
            Self::with_manifest(MANIFEST, descriptor)
        }

        fn with_manifest(manifest: &str, descriptor: &str) -> Self {
            let dir = TempDir::new().expect("create temp dir");
            fs::write(dir.path().join("manifest.yaml"), manifest).expect("write manifest");
            fs::write(dir.path().join("openapi.yaml"), descriptor).expect("write descriptor");
            Self { dir }
        }

        fn manifest_path(&self) -> PathBuf {
            self.dir.path().join("manifest.yaml")
        }

        fn render(&self) -> String {
            render_preview(&self.manifest_path()).expect("render preview")
        }
    }

    /// The shape every Slack list method returns: an `ok` flag, one row array
    /// named after the resource, and a `response_metadata.next_cursor` string.
    fn slack_shaped_descriptor() -> String {
        r#"
openapi: 3.0.3
info: {title: Demo, version: "1"}
servers:
  - url: https://example.com/api
paths:
  /conversations.list:
    get:
      operationId: conversations/list
      tags: [conversations]
      description: List conversations.
      parameters:
        - name: cursor
          in: query
          required: false
          description: Pagination cursor.
          schema: {type: string}
        - name: limit
          in: query
          required: false
          schema: {type: integer, default: 200}
        - name: types
          in: query
          required: false
          description: Conversation types to include.
          schema: {type: string}
      responses:
        "200":
          description: OK
          content:
            application/json:
              schema:
                type: object
                properties:
                  ok: {type: boolean}
                  error: {type: string}
                  channels:
                    type: array
                    items: {$ref: '#/components/schemas/Channel'}
                  response_metadata:
                    type: object
                    properties:
                      next_cursor: {type: string}
components:
  schemas:
    Channel:
      type: object
      properties:
        id: {type: string, description: Channel ID.}
        name: {type: string}
        num_members: {type: integer}
        is_archived: {type: boolean}
        purpose: {type: object}
"#
        .to_string()
    }

    #[test]
    fn renders_row_path_pagination_and_columns_for_a_slack_shaped_envelope() {
        let rendered = Fixture::new(&slack_shaped_descriptor()).render();

        assert!(
            rendered.contains("| Row path | `channels` |"),
            "sole-array fallback should select the row array: {rendered}"
        );
        assert!(
            rendered.contains("| Pagination | `cursor_query` |"),
            "cursor pagination should be detected: {rendered}"
        );
        assert!(
            rendered.contains("| Response cursor | `response_metadata.next_cursor` |"),
            "nested response cursor should be found: {rendered}"
        );
        assert!(
            rendered.contains("| Page size | 200 (max 200) via `limit` |"),
            "declared limit default should drive page size: {rendered}"
        );
        assert!(
            rendered.contains("| `num_members` | Int64 |"),
            "scalar row properties should become typed columns: {rendered}"
        );
        assert!(
            rendered.contains("| `purpose` | Json |"),
            "nested objects should collapse to a JSON column: {rendered}"
        );
        assert!(
            rendered.contains("| Request | `GET /conversations.list` |"),
            "the request line should be shown: {rendered}"
        );
    }

    /// Row-path inference gives up when an envelope declares more than one
    /// candidate array. That silently turns a relation into a single JSON
    /// column, so the preview has to make it visible.
    #[test]
    fn reports_no_row_path_when_the_envelope_declares_two_arrays() {
        let descriptor = slack_shaped_descriptor().replace(
            "                  response_metadata:",
            "                  pinned_items:\n                    type: array\n                    items: {type: object}\n                  response_metadata:",
        );
        let rendered = Fixture::new(&descriptor).render();

        assert!(
            rendered.contains("| Row path | _response root_ |"),
            "a two-array envelope should lose its row path: {rendered}"
        );
        assert!(
            rendered.contains("| `channels` | Json |"),
            "the envelope itself becomes the row: {rendered}"
        );
    }

    #[test]
    fn reports_diagnostics_raised_during_import() {
        let descriptor = slack_shaped_descriptor().replace(
            "items: {$ref: '#/components/schemas/Channel'}",
            "items: {$ref: 'https://example.com/other.yaml#/Channel'}",
        );
        let rendered = Fixture::new(&descriptor).render();

        assert!(
            !rendered.contains("## Diagnostics\n\n_None._"),
            "an external ref should be reported: {rendered}"
        );
        assert!(
            rendered.contains("external reference"),
            "the diagnostic should name the cause: {rendered}"
        );
    }

    #[test]
    fn rejects_a_manifest_that_is_not_dsl_v4() {
        let fixture = Fixture::with_manifest(
            "
name: demo
version: 0.1.0
dsl_version: 3
backend: http
description: A DSL v3 source.
base_url: https://example.com
tables:
  - name: entries
    description: Fetch entries.
    request:
      method: GET
      path: /entries
    response:
      row_strategy: direct
    pagination:
      mode: none
    columns:
      - name: id
        type: Utf8
        expr: {kind: path, path: [id]}
",
            &slack_shaped_descriptor(),
        );

        let error = render_preview(&fixture.manifest_path()).expect_err("should reject v3");

        assert!(
            format!("{error:#}").contains("dsl_version: 4"),
            "error should name the requirement: {error:#}"
        );
    }

    #[test]
    fn reports_a_missing_descriptor_by_resolved_path() {
        let dir = TempDir::new().expect("create temp dir");
        fs::write(dir.path().join("manifest.yaml"), MANIFEST).expect("write manifest");

        let error =
            render_preview(&dir.path().join("manifest.yaml")).expect_err("should fail to read");

        assert!(
            format!("{error:#}").contains("openapi.yaml"),
            "error should name the descriptor: {error:#}"
        );
    }

    /// The preview is committed and drift-checked in CI, so identical inputs
    /// must render byte-identical output.
    #[test]
    fn renders_deterministically() {
        let fixture = Fixture::new(&slack_shaped_descriptor());

        assert_eq!(fixture.render(), fixture.render());
    }
}
