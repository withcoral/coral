//! Developer tooling for the Coral source bundle.
//!
//! This binary exposes two subcommands that share workspace conventions but
//! serve different workflows:
//!   - `generate-docs` regenerates the bundled-sources Mintlify page and nav
//!     from `sources/*/manifest.y{a,}ml`.
//!   - `detect-truncations` scans manifests for likely-truncated descriptions
//!     (the regression gate for the SOURCE-465 manifest cleanup).

#![allow(
    clippy::print_stderr,
    clippy::print_stdout,
    reason = "CLI intentionally writes human-readable diagnostics to stdout/stderr"
)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use coral_spec::{ValidatedSourceManifest, parse_source_manifest_yaml};

mod detect;
mod nav;
mod render;

#[derive(Debug, Parser)]
#[command(name = "xtask", about = "Developer tooling for Coral bundled sources")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Regenerate the bundled-sources docs page and Mintlify nav.
    GenerateDocs(GenerateDocsArgs),
    /// Scan manifests for likely-truncated descriptions.
    DetectTruncations(DetectArgs),
}

#[derive(Debug, clap::Args)]
struct GenerateDocsArgs {
    /// Directory containing one subdirectory per source, each holding a
    /// `manifest.yaml` or `manifest.yml` file.
    #[arg(long, default_value = "sources")]
    sources_dir: PathBuf,

    /// Path to the index page to regenerate.
    #[arg(long, default_value = "docs/reference/bundled-sources.mdx")]
    index: PathBuf,

    /// Path to the Mintlify navigation file to update.
    #[arg(long, default_value = "docs/docs.json")]
    docs_json: PathBuf,

    /// Render everything in memory and diff against disk instead of writing.
    /// Exits non-zero if any generated file differs from its on-disk copy.
    #[arg(long)]
    check: bool,
}

#[derive(Debug, clap::Args)]
struct DetectArgs {
    /// Manifest files or directories to scan. Defaults to `sources/` when
    /// no paths are given.
    paths: Vec<PathBuf>,

    /// Print one line per manifest scanned, including those with no hits.
    #[arg(long)]
    verbose: bool,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(&cli.command) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(1),
        Err(err) => {
            eprintln!("xtask: {err:#}");
            ExitCode::from(2)
        }
    }
}

/// Returns `Ok(true)` on success, `Ok(false)` on a detected regression
/// (stale generated file or suspected truncation).
fn run(command: &Command) -> Result<bool> {
    match command {
        Command::GenerateDocs(args) => generate_docs(args),
        Command::DetectTruncations(args) => {
            let paths: Vec<PathBuf> = if args.paths.is_empty() {
                vec![PathBuf::from("sources")]
            } else {
                args.paths.clone()
            };
            detect::run(&paths, args.verbose)
        }
    }
}

fn generate_docs(args: &GenerateDocsArgs) -> Result<bool> {
    let manifests = load_manifests(&args.sources_dir)?;

    let index = render::index_page(&manifests);

    let existing_json = fs::read_to_string(&args.docs_json)
        .with_context(|| format!("reading {}", args.docs_json.display()))?;
    let updated_json = nav::update_docs_json(&existing_json)?;

    if args.check {
        Ok(check_mode(args, &index, &updated_json))
    } else {
        write_mode(args, &index, &updated_json)?;
        Ok(true)
    }
}

fn check_mode(args: &GenerateDocsArgs, index: &str, docs_json: &str) -> bool {
    let mut stale = Vec::new();

    if fs::read_to_string(&args.index).ok().as_deref() != Some(index) {
        stale.push(args.index.clone());
    }

    if fs::read_to_string(&args.docs_json).ok().as_deref() != Some(docs_json) {
        stale.push(args.docs_json.clone());
    }

    if stale.is_empty() {
        true
    } else {
        eprintln!("xtask: the following files are out of date:");
        for path in &stale {
            eprintln!("  {}", path.display());
        }
        eprintln!("Run `make docs-generate` to regenerate.");
        false
    }
}

fn write_mode(args: &GenerateDocsArgs, index: &str, docs_json: &str) -> Result<()> {
    write_if_changed(&args.index, index)?;
    write_if_changed(&args.docs_json, docs_json)?;
    Ok(())
}

fn write_if_changed(path: &Path, body: &str) -> Result<()> {
    if fs::read_to_string(path).ok().as_deref() == Some(body) {
        return Ok(());
    }
    fs::write(path, body).with_context(|| format!("writing {}", path.display()))
}

/// Discover every `manifest.y{a,}ml` beneath `sources_dir`, parse it, and
/// return the validated manifests sorted by schema name.
fn load_manifests(sources_dir: &Path) -> Result<Vec<ValidatedSourceManifest>> {
    let entries =
        fs::read_dir(sources_dir).with_context(|| format!("reading {}", sources_dir.display()))?;

    let mut manifests = Vec::new();
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let Some(manifest_path) = find_manifest_file(&entry.path()) else {
            bail!(
                "missing manifest.y{{a,}}ml for bundled source '{}'",
                entry.path().display()
            );
        };
        let raw = fs::read_to_string(&manifest_path)
            .with_context(|| format!("reading {}", manifest_path.display()))?;
        let manifest = parse_source_manifest_yaml(&raw)
            .with_context(|| format!("parsing {}", manifest_path.display()))?;
        manifests.push(manifest);
    }

    manifests.sort_by(|left, right| left.schema_name().cmp(right.schema_name()));
    Ok(manifests)
}

/// Mirrors `crates/coral-app/build.rs::find_manifest_file`: prefer the
/// `.yaml` extension but accept `.yml` as a fallback.
fn find_manifest_file(dir: &Path) -> Option<PathBuf> {
    ["manifest.yaml", "manifest.yml"]
        .into_iter()
        .map(|name| dir.join(name))
        .find(|path| path.exists())
}
