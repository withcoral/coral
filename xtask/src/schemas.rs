//! Generate checked-in JSON schemas owned by Coral repo automation.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

#[derive(Debug, clap::Args)]
pub(crate) struct Args {
    /// Path to the generated DSL v4 source manifest schema.
    #[arg(
        long,
        default_value = "crates/coral-spec/src/schema/source_manifest_v4.schema.json"
    )]
    v4_schema: PathBuf,

    /// Render in memory and diff against disk instead of writing.
    #[arg(long)]
    check: bool,
}

pub(crate) fn run(args: &Args) -> Result<bool> {
    let body = generated_v4_schema_body()?;
    if args.check {
        Ok(check_file(&args.v4_schema, &body))
    } else {
        write_if_changed(&args.v4_schema, &body)?;
        Ok(true)
    }
}

fn generated_v4_schema_body() -> Result<String> {
    let schema = coral_spec::v4::generated_v4_source_manifest_schema();
    let mut body =
        serde_json::to_string_pretty(&schema).context("serializing generated DSL v4 schema")?;
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
