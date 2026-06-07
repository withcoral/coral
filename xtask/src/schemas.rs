//! Generate checked-in JSON schemas owned by Coral repo automation.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

#[derive(Debug, clap::Args)]
pub(crate) struct Args {
    /// Path to the generated `SourceSpec` schema.
    #[arg(
        long,
        default_value = "crates/coral-spec/src/schema/source_spec.schema.json"
    )]
    source_spec_schema: PathBuf,

    /// Render in memory and diff against disk instead of writing.
    #[arg(long)]
    check: bool,
}

pub(crate) fn run(args: &Args) -> Result<bool> {
    let body = generated_source_spec_schema_body()?;
    if args.check {
        Ok(check_file(&args.source_spec_schema, &body))
    } else {
        write_if_changed(&args.source_spec_schema, &body)?;
        Ok(true)
    }
}

fn generated_source_spec_schema_body() -> Result<String> {
    let schema = coral_spec::generated_source_spec_schema();
    let mut body =
        serde_json::to_string_pretty(&schema).context("serializing generated SourceSpec schema")?;
    body.push('\n');
    Ok(body)
}

fn check_file(path: &Path, body: &str) -> bool {
    if fs::read_to_string(path).ok().as_deref() == Some(body) {
        true
    } else {
        eprintln!("xtask: {} is out of date", path.display());
        eprintln!("Run `make schema-generate` to regenerate.");
        false
    }
}

fn write_if_changed(path: &Path, body: &str) -> Result<()> {
    if fs::read_to_string(path).ok().as_deref() == Some(body) {
        return Ok(());
    }
    fs::write(path, body).with_context(|| format!("writing {}", path.display()))
}
