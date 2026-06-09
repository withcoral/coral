use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};
use coral_spec::parse_source_manifest_yaml;
use serde::Serialize;
use serde_yaml::Value;
use sha2::{Digest, Sha256};

use crate::GenerateCommunityIndexArgs;
use crate::{find_manifest_file, write_if_changed};

const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Serialize)]
struct CommunityIndex {
    schema_version: u32,
    repository: CommunityIndexRepository,
    sources: Vec<CommunitySourceEntry>,
}

#[derive(Debug, Serialize)]
struct CommunityIndexRepository {
    full_name: String,
    #[serde(rename = "ref")]
    git_ref: String,
    commit_sha: String,
}

#[derive(Debug, Serialize)]
struct CommunitySourceEntry {
    name: String,
    description: String,
    version: String,
    dsl_version: u32,
    backend: String,
    paths: CommunitySourcePaths,
    hashes: CommunitySourceHashes,
    git: CommunitySourceGit,
}

#[derive(Debug, Serialize)]
struct CommunitySourcePaths {
    manifest: String,
    readme: String,
}

#[derive(Debug, Serialize)]
struct CommunitySourceHashes {
    manifest_sha256: String,
    readme_sha256: String,
}

#[derive(Debug, Serialize)]
struct CommunitySourceGit {
    #[serde(rename = "commit_sha")]
    commit: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "manifest_blob_sha")]
    manifest_blob: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "readme_blob_sha")]
    readme_blob: Option<String>,
}

pub(crate) fn run(args: &GenerateCommunityIndexArgs) -> Result<bool> {
    let commit_sha = args
        .commit_sha
        .clone()
        .map_or_else(resolve_head_commit_sha, Ok)?;
    let index = CommunityIndex {
        schema_version: SCHEMA_VERSION,
        repository: CommunityIndexRepository {
            full_name: args.repo.clone(),
            git_ref: args.git_ref.clone(),
            commit_sha: commit_sha.clone(),
        },
        sources: collect_sources(&args.sources_dir, &commit_sha)?,
    };
    let body = serde_json::to_string_pretty(&index).context("serializing community index")? + "\n";
    if args.check {
        Ok(fs::read_to_string(&args.out).ok().as_deref() == Some(&body))
    } else {
        write_if_changed(&args.out, &body)?;
        Ok(true)
    }
}

fn collect_sources(sources_dir: &Path, commit_sha: &str) -> Result<Vec<CommunitySourceEntry>> {
    let entries =
        fs::read_dir(sources_dir).with_context(|| format!("reading {}", sources_dir.display()))?;

    let mut sources = Vec::new();
    for entry in entries {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let community_dir = entry.path();
        let Some(manifest_path) = find_manifest_file(&community_dir) else {
            bail!(
                "missing manifest.y{{a,}}ml for community source '{}'",
                community_dir.display()
            );
        };
        let readme_path = find_readme_file(&community_dir).with_context(|| {
            format!(
                "missing README.md for community source '{}'",
                community_dir.display()
            )
        })?;
        sources.push(source_entry(&manifest_path, &readme_path, commit_sha)?);
    }

    sources.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(sources)
}

fn source_entry(
    manifest_path: &Path,
    readme_path: &Path,
    commit_sha: &str,
) -> Result<CommunitySourceEntry> {
    let manifest_raw = fs::read_to_string(manifest_path)
        .with_context(|| format!("reading {}", manifest_path.display()))?;
    let readme_raw = fs::read_to_string(readme_path)
        .with_context(|| format!("reading {}", readme_path.display()))?;
    let manifest = parse_source_manifest_yaml(&manifest_raw)
        .with_context(|| format!("parsing {}", manifest_path.display()))?;
    let manifest_value: Value = serde_yaml::from_str(&manifest_raw)
        .with_context(|| format!("parsing metadata from {}", manifest_path.display()))?;
    let dsl_version = manifest_value
        .get("dsl_version")
        .and_then(Value::as_u64)
        .and_then(|value| u32::try_from(value).ok())
        .with_context(|| format!("reading dsl_version from {}", manifest_path.display()))?;
    let backend = manifest_value
        .get("backend")
        .and_then(Value::as_str)
        .with_context(|| format!("reading backend from {}", manifest_path.display()))?
        .to_string();

    Ok(CommunitySourceEntry {
        name: manifest.schema_name().to_string(),
        description: manifest.description().to_string(),
        version: manifest.source_version().to_string(),
        dsl_version,
        backend,
        paths: CommunitySourcePaths {
            manifest: repo_path(manifest_path)?,
            readme: repo_path(readme_path)?,
        },
        hashes: CommunitySourceHashes {
            manifest_sha256: sha256_hex(&manifest_raw),
            readme_sha256: sha256_hex(&readme_raw),
        },
        git: CommunitySourceGit {
            commit: commit_sha.to_string(),
            manifest_blob: git_blob_sha(manifest_path)?,
            readme_blob: git_blob_sha(readme_path)?,
        },
    })
}

fn find_readme_file(dir: &Path) -> Option<PathBuf> {
    ["README.md", "readme.md"]
        .into_iter()
        .map(|name| dir.join(name))
        .find(|path| path.exists())
}

fn sha256_hex(raw: &str) -> String {
    let digest = Sha256::digest(raw.as_bytes());
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        write!(&mut encoded, "{byte:02x}").expect("writing to String cannot fail");
    }
    encoded
}

fn repo_path(path: &Path) -> Result<String> {
    let root = workspace_root()?;
    let absolute_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };
    let stripped = absolute_path
        .strip_prefix(&root)
        .with_context(|| format!("{} is outside {}", absolute_path.display(), root.display()))?;
    Ok(stripped
        .components()
        .map(|component| component.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/"))
}

fn workspace_root() -> Result<PathBuf> {
    let output = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .output()
        .context("running git rev-parse --show-toplevel")?;
    if !output.status.success() {
        bail!("git rev-parse --show-toplevel failed");
    }
    Ok(PathBuf::from(
        String::from_utf8(output.stdout)
            .context("decoding git root")?
            .trim(),
    ))
}

fn resolve_head_commit_sha() -> Result<String> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .context("running git rev-parse HEAD")?;
    if !output.status.success() {
        bail!("git rev-parse HEAD failed");
    }
    Ok(String::from_utf8(output.stdout)
        .context("decoding git commit")?
        .trim()
        .to_string())
}

fn git_blob_sha(path: &Path) -> Result<Option<String>> {
    let output = Command::new("git")
        .arg("hash-object")
        .arg(path)
        .output()
        .with_context(|| format!("running git hash-object {}", path.display()))?;
    if !output.status.success() {
        return Ok(None);
    }
    let value = String::from_utf8(output.stdout)
        .context("decoding git hash-object output")?
        .trim()
        .to_string();
    Ok((!value.is_empty()).then_some(value))
}
