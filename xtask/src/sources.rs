//! Shared source-manifest discovery helpers for xtask commands.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use coral_spec::{ValidatedSourceManifest, parse_source_manifest_yaml};
use walkdir::WalkDir;

/// Discover every immediate `manifest.y{a,}ml` beneath `sources_dir`, parse it,
/// and return the validated manifests sorted by schema name.
pub(crate) fn load_catalog_manifests(sources_dir: &Path) -> Result<Vec<ValidatedSourceManifest>> {
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
                "missing manifest.y{{a,}}ml for source '{}'",
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

/// Expand the caller's path list into concrete manifest files, deduplicated.
pub(crate) fn iter_manifest_files(paths: &[PathBuf]) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = Vec::new();
    for path in paths {
        if path.is_file()
            && matches!(
                path.extension().and_then(|extension| extension.to_str()),
                Some("yaml" | "yml")
            )
        {
            out.push(path.clone());
            continue;
        }
        if path.is_dir() {
            out.extend(manifest_files_under(path));
        }
    }
    out.sort();
    let mut seen: HashSet<PathBuf> = HashSet::new();
    let mut unique: Vec<PathBuf> = Vec::new();
    for path in out {
        let resolved = path.canonicalize().unwrap_or_else(|_| path.clone());
        if seen.insert(resolved) {
            unique.push(path);
        }
    }
    unique
}

fn manifest_files_under(dir: &Path) -> Vec<PathBuf> {
    WalkDir::new(dir)
        .follow_links(false)
        .into_iter()
        .filter_map(std::result::Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .filter_map(|entry| {
            let file_name = entry.file_name().to_str()?;
            matches!(file_name, "manifest.yaml" | "manifest.yml").then(|| entry.into_path())
        })
        .collect()
}

/// Mirrors `crates/coral-app/build.rs::find_manifest_file`: prefer the
/// `.yaml` extension but accept `.yml` as a fallback.
fn find_manifest_file(dir: &Path) -> Option<PathBuf> {
    ["manifest.yaml", "manifest.yml"]
        .into_iter()
        .map(|name| dir.join(name))
        .find(|path| path.exists())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use super::iter_manifest_files;

    #[test]
    fn iter_manifest_files_recurses_nested_source_groups() {
        let root = unique_temp_dir("iter-manifests");
        let core_manifest = root.join("sources/core/github/manifest.yaml");
        let community_manifest = root.join("sources/community/hn/manifest.yaml");
        fs::create_dir_all(core_manifest.parent().expect("core parent")).expect("create core");
        fs::create_dir_all(community_manifest.parent().expect("community parent"))
            .expect("create community");
        fs::write(&core_manifest, "name: github\n").expect("write core manifest");
        fs::write(&community_manifest, "name: hn\n").expect("write community manifest");

        let manifests = iter_manifest_files(&[root.join("sources")]);

        fs::remove_dir_all(&root).expect("remove temp dir");
        assert_eq!(manifests, vec![community_manifest, core_manifest]);
    }

    #[cfg(unix)]
    #[test]
    fn iter_manifest_files_does_not_follow_symlinked_directories() {
        let root = unique_temp_dir("iter-manifests-symlink");
        let real_manifest = root.join("real/manifest.yaml");
        let linked_manifest = root.join("sources/linked/manifest.yaml");
        fs::create_dir_all(real_manifest.parent().expect("real parent")).expect("create real");
        fs::create_dir_all(root.join("sources")).expect("create sources");
        fs::write(&real_manifest, "name: real\n").expect("write manifest");
        std::os::unix::fs::symlink(root.join("real"), root.join("sources/linked"))
            .expect("create symlink");

        let manifests = iter_manifest_files(&[root.join("sources")]);

        fs::remove_dir_all(&root).expect("remove temp dir");
        assert!(
            manifests.is_empty(),
            "symlinked manifest should not be traversed: {linked_manifest:?}"
        );
    }

    fn unique_temp_dir(name: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time after epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("coral-xtask-{name}-{}-{nonce}", std::process::id()))
    }
}
