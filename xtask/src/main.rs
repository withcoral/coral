//! Generates the Mintlify bundled-sources docs page from source manifests.
//!
//! Reads every `sources/<name>/manifest.y{a,}ml`, parses it via
//! [`coral_spec::parse_source_manifest_yaml`], and writes a regenerated
//! `bundled-sources.mdx` index plus an updated Mintlify navigation file.
//! In `--check` mode, renders everything in memory and exits non-zero when
//! the outputs differ from disk — suitable for CI freshness enforcement.

#![allow(
    clippy::print_stderr,
    reason = "CLI intentionally renders stale-file diagnostics to stderr"
)]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use anyhow::{Context, Result, bail};
use clap::Parser;
use coral_spec::{ValidatedSourceManifest, parse_source_manifest_yaml};

mod nav;
mod render;

/// CLI for regenerating the bundled-source docs from source manifests.
#[derive(Debug, Parser)]
#[command(
    name = "xtask",
    about = "Regenerate Coral source docs from sources/*/manifest.y{a,}ml"
)]
struct Cli {
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

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(&cli) {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(1),
        Err(err) => {
            eprintln!("xtask: {err:#}");
            ExitCode::from(2)
        }
    }
}

/// Returns `Ok(true)` on success, `Ok(false)` if `--check` found a stale file.
fn run(cli: &Cli) -> Result<bool> {
    let manifests = load_manifests(&cli.sources_dir)?;

    let index = render::index_page(&manifests);

    let existing_json = fs::read_to_string(&cli.docs_json)
        .with_context(|| format!("reading {}", cli.docs_json.display()))?;
    let updated_json = nav::update_docs_json(&existing_json)?;

    if cli.check {
        Ok(check_mode(cli, &index, &updated_json))
    } else {
        write_mode(cli, &index, &updated_json)?;
        Ok(true)
    }
}

fn check_mode(cli: &Cli, index: &str, docs_json: &str) -> bool {
    let mut stale = Vec::new();

    if fs::read_to_string(&cli.index).ok().as_deref() != Some(index) {
        stale.push(cli.index.clone());
    }

    if fs::read_to_string(&cli.docs_json).ok().as_deref() != Some(docs_json) {
        stale.push(cli.docs_json.clone());
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

fn write_mode(cli: &Cli, index: &str, docs_json: &str) -> Result<()> {
    write_if_changed(&cli.index, index)?;
    write_if_changed(&cli.docs_json, docs_json)?;
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
