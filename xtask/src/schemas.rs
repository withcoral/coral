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

    /// Path to the generated identity manifest schema.
    #[arg(
        long,
        default_value = "crates/coral-spec/src/schema/identity_manifest.schema.json"
    )]
    identity_schema: PathBuf,

    /// Render in memory and diff against disk instead of writing.
    #[arg(long)]
    check: bool,
}

pub(crate) fn run(args: &Args) -> Result<bool> {
    let v4_body = generated_v4_schema_body()?;
    let identity_body = generated_identity_schema_body()?;
    if args.check {
        let v4_ok = check_file(&args.v4_schema, &v4_body);
        let identity_ok = check_file(&args.identity_schema, &identity_body);
        Ok(v4_ok && identity_ok)
    } else {
        write_if_changed(&args.v4_schema, &v4_body)?;
        write_if_changed(&args.identity_schema, &identity_body)?;
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

fn generated_identity_schema_body() -> Result<String> {
    let schema = coral_spec::generated_identity_manifest_schema();
    // The typed generator is the review surface; keep this large derived artifact compact.
    let mut body =
        serde_json::to_string(&schema).context("serializing generated identity schema")?;
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
