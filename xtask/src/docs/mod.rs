//! Generate the Mintlify pages owned by Coral repo automation.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::sources;

mod nav;
mod render;

#[derive(Debug, clap::Args)]
pub(crate) struct Args {
    /// Directory containing one subdirectory per bundled source, each holding a
    /// `manifest.yaml` or `manifest.yml` file.
    #[arg(long, default_value = "sources/core")]
    sources_dir: PathBuf,

    /// Path to the bundled source catalog page to regenerate.
    #[arg(long, default_value = "docs/reference/bundled-sources.mdx")]
    index: PathBuf,

    /// Directory containing community source manifests to render into the
    /// community source catalog.
    #[arg(long, default_value = "sources/community")]
    community_sources_dir: PathBuf,

    /// Path to the community source catalog page to regenerate.
    #[arg(long, default_value = "docs/reference/community-sources.mdx")]
    community_index: PathBuf,

    /// Skip rendering and checking the community source catalog.
    #[arg(long)]
    skip_community_sources: bool,

    /// Path to the Mintlify navigation file to update.
    #[arg(long, default_value = "docs/docs.json")]
    docs_json: PathBuf,

    /// Path to the source CHANGELOG.md to render into the docs.
    #[arg(long, default_value = "CHANGELOG.md")]
    changelog_source: PathBuf,

    /// Path to the changelog page to regenerate.
    #[arg(long, default_value = "docs/project/changelog.mdx")]
    changelog_out: PathBuf,

    /// Render everything in memory and diff against disk instead of writing.
    /// Exits non-zero if any generated file differs from its on-disk copy.
    #[arg(long)]
    check: bool,
}

/// One generator-owned output: where it lives on disk and the body it
/// should contain. `run` builds a vector of these and the check/write helpers
/// iterate over the same list.
struct GeneratedFile {
    path: PathBuf,
    body: String,
}

pub(crate) fn run(args: &Args) -> Result<bool> {
    let manifests = sources::load_catalog_manifests(&args.sources_dir)?;
    let index_body = render::index_page(&manifests);

    let existing_json = fs::read_to_string(&args.docs_json)
        .with_context(|| format!("reading {}", args.docs_json.display()))?;
    let updated_json = nav::update_docs_json(&existing_json)?;

    let raw_changelog = fs::read_to_string(&args.changelog_source)
        .with_context(|| format!("reading {}", args.changelog_source.display()))?;
    let changelog_body = render::changelog_page(&raw_changelog);

    let mut outputs = vec![GeneratedFile {
        path: args.index.clone(),
        body: index_body,
    }];

    if !args.skip_community_sources {
        let community_manifests = sources::load_catalog_manifests(&args.community_sources_dir)?;
        let community_index_body = render::community_sources_page(&community_manifests);
        outputs.push(GeneratedFile {
            path: args.community_index.clone(),
            body: community_index_body,
        });
    }

    outputs.extend([
        GeneratedFile {
            path: args.docs_json.clone(),
            body: updated_json,
        },
        GeneratedFile {
            path: args.changelog_out.clone(),
            body: changelog_body,
        },
    ]);

    if args.check {
        Ok(check_mode(&outputs))
    } else {
        write_mode(&outputs)?;
        Ok(true)
    }
}

fn check_mode(outputs: &[GeneratedFile]) -> bool {
    let stale: Vec<&Path> = outputs
        .iter()
        .filter(|file| fs::read_to_string(&file.path).ok().as_deref() != Some(&file.body))
        .map(|file| file.path.as_path())
        .collect();

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

fn write_mode(outputs: &[GeneratedFile]) -> Result<()> {
    for file in outputs {
        write_if_changed(&file.path, &file.body)?;
    }
    Ok(())
}

fn write_if_changed(path: &Path, body: &str) -> Result<()> {
    if fs::read_to_string(path).ok().as_deref() == Some(body) {
        return Ok(());
    }
    fs::write(path, body).with_context(|| format!("writing {}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{Args, run};

    const MINIMAL_MANIFEST: &str = r"
name: minimal
version: 0.1.0
dsl_version: 3
backend: http
base_url: https://api.example.com
tables:
  - name: pings
    description: Ping events
    request:
      method: GET
      path: /ping
    response:
      rows_path: []
    columns:
      - name: id
        type: Utf8
        nullable: false
        description: Ping id
        expr:
          kind: path
          path: [id]
";

    const MINIMAL_DOCS_JSON: &str = r#"{
  "navigation": {
    "groups": [
      {
        "group": "Reference",
        "pages": [
          "reference/source-spec-reference"
        ]
      },
      {
        "group": "Project",
        "pages": []
      }
    ]
  }
}
"#;

    #[test]
    fn generate_docs_check_skips_community_catalog_when_requested() {
        let root = unique_temp_dir("generate-docs-skip-community");
        let source_dir = root.join("sources/core/minimal");
        let docs_reference_dir = root.join("docs/reference");
        let docs_project_dir = root.join("docs/project");
        fs::create_dir_all(&source_dir).expect("create source dir");
        fs::create_dir_all(&docs_reference_dir).expect("create reference docs dir");
        fs::create_dir_all(&docs_project_dir).expect("create project docs dir");
        fs::write(source_dir.join("manifest.yaml"), MINIMAL_MANIFEST).expect("write manifest");

        let docs_json = root.join("docs/docs.json");
        let changelog_source = root.join("CHANGELOG.md");
        let community_index = docs_reference_dir.join("community-sources.mdx");
        fs::write(&docs_json, MINIMAL_DOCS_JSON).expect("write docs json");
        fs::write(&changelog_source, "# Changelog\n").expect("write changelog");

        let mut args = Args {
            sources_dir: root.join("sources/core"),
            index: docs_reference_dir.join("bundled-sources.mdx"),
            community_sources_dir: root.join("missing-community"),
            community_index: community_index.clone(),
            skip_community_sources: true,
            docs_json,
            changelog_source,
            changelog_out: docs_project_dir.join("changelog.mdx"),
            check: false,
        };

        assert!(run(&args).expect("write generated docs"));
        fs::write(&community_index, "stale community catalog")
            .expect("write stale community index");

        args.check = true;
        assert!(run(&args).expect("check generated docs"));

        fs::remove_dir_all(&root).expect("remove temp dir");
    }

    fn unique_temp_dir(name: &str) -> PathBuf {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("coral-xtask-{name}-{}-{nonce}", std::process::id()))
    }
}
