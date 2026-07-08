//! `export-openapi`: convert v3 HTTP source manifests to OpenAPI documents.
//!
//! One document is written per manifest, named `<source>.openapi.yaml`.
//! Manifests with nothing to describe are skipped with a note: non-HTTP
//! backends (file, MCP, DSL v4) have no HTTP endpoints, and GraphQL sources
//! collapse into one meaningless path/method slot. Conversion itself never
//! fails: any construct OpenAPI cannot express becomes a warning plus an
//! `x-coral*` extension in the emitted document (see `convert`).

mod convert;
mod schema_tree;

#[cfg(test)]
mod tests;

use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use coral_spec::parse_source_manifest_yaml;

use crate::sources::iter_manifest_files;
use convert::{convert_http_manifest, is_graphql_source};

#[derive(Debug, clap::Args)]
pub(crate) struct Args {
    /// Manifest files or directories to convert. Defaults to `sources/`
    /// when no paths are given.
    paths: Vec<PathBuf>,

    /// Directory that receives the generated `<source>.openapi.yaml` files.
    #[arg(long, default_value = "target/openapi")]
    out: PathBuf,

    /// Print every conversion warning instead of only per-source counts.
    #[arg(long)]
    verbose: bool,
}

/// Returns `Ok(false)` when any manifest failed to parse or convert; skipped
/// non-HTTP manifests do not count as failures.
pub(crate) fn run(args: &Args) -> Result<bool> {
    let paths = if args.paths.is_empty() {
        vec![PathBuf::from("sources")]
    } else {
        args.paths.clone()
    };
    let manifest_files = iter_manifest_files(&paths);
    if manifest_files.is_empty() {
        eprintln!("xtask: no manifest.y{{a,}}ml files found under the given paths");
        return Ok(false);
    }
    fs::create_dir_all(&args.out)
        .with_context(|| format!("creating output directory {}", args.out.display()))?;

    let mut converted = 0usize;
    let mut skipped = 0usize;
    let mut failed = 0usize;
    let mut total_warnings = 0usize;
    for manifest_path in manifest_files {
        let raw = match fs::read_to_string(&manifest_path) {
            Ok(raw) => raw,
            Err(error) => {
                eprintln!("xtask: reading {}: {error}", manifest_path.display());
                failed += 1;
                continue;
            }
        };
        let manifest = match parse_source_manifest_yaml(&raw) {
            Ok(manifest) => manifest,
            Err(error) => {
                eprintln!("xtask: parsing {}: {error}", manifest_path.display());
                failed += 1;
                continue;
            }
        };
        let Some(http) = manifest.as_http() else {
            println!(
                "skip  {} ({}, not an HTTP v3 manifest)",
                manifest.schema_name(),
                manifest_path.display()
            );
            skipped += 1;
            continue;
        };
        if is_graphql_source(http) {
            println!(
                "skip  {} ({}, GraphQL source; OpenAPI cannot describe its operations)",
                manifest.schema_name(),
                manifest_path.display()
            );
            skipped += 1;
            continue;
        }

        let conversion = convert_http_manifest(http);
        let body = serde_yaml::to_string(&conversion.document)
            .with_context(|| format!("serializing OpenAPI document for {}", http.common.name))?;
        let out_path = args.out.join(format!("{}.openapi.yaml", http.common.name));
        fs::write(&out_path, body).with_context(|| format!("writing {}", out_path.display()))?;

        let operations = count_operations(&conversion.document);
        println!(
            "write {} ({operations} operations, {} warnings) -> {}",
            http.common.name,
            conversion.warnings.len(),
            out_path.display()
        );
        if args.verbose {
            for warning in &conversion.warnings {
                println!("      warning: {warning}");
            }
        }
        total_warnings += conversion.warnings.len();
        converted += 1;
    }

    println!(
        "export-openapi: {converted} converted, {skipped} skipped, {failed} failed, \
         {total_warnings} warnings"
    );
    Ok(failed == 0)
}

fn count_operations(document: &serde_json::Value) -> usize {
    document
        .get("paths")
        .and_then(serde_json::Value::as_object)
        .map_or(0, |paths| {
            paths
                .values()
                .filter_map(serde_json::Value::as_object)
                .map(serde_json::Map::len)
                .sum()
        })
}
