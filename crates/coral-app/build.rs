//! Build script for bundled source manifests.

#![allow(
    clippy::disallowed_methods,
    reason = "Cargo build scripts read build-time environment variables directly."
)]

use serde_yaml::Value;
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir"));
    let bundled_root = manifest_dir.join("../../sources/core");
    println!("cargo:rerun-if-changed={}", bundled_root.display());

    let mut generated = String::new();
    let entries = bundled_entries(&bundled_root);
    write_entries(&mut generated, "BUNDLED_SOURCES", &entries);

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("out dir"));
    fs::write(out_dir.join("bundled_sources.rs"), generated).expect("write bundled source table");
}

struct BundledEntry {
    name: String,
    manifest_yaml: String,
    assets: Vec<BundledAsset>,
}

struct BundledAsset {
    relative_path: String,
    source_path: PathBuf,
}

fn bundled_entries(root: &Path) -> Vec<BundledEntry> {
    assert!(
        root.exists(),
        "bundled source root '{}' does not exist",
        root.display()
    );
    let mut entries = fs::read_dir(root)
        .unwrap_or_else(|error| panic!("read bundled sources '{}': {error}", root.display()))
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().to_string();
            let manifest_path = find_manifest_file(&entry.path()).unwrap_or_else(|| {
                panic!(
                    "missing manifest.y*ml for bundled source '{}'",
                    entry.path().display()
                )
            });
            println!("cargo:rerun-if-changed={}", manifest_path.display());
            let raw = fs::read_to_string(&manifest_path).expect("read bundled manifest");
            if !is_source_spec_manifest(&raw) {
                return None;
            }
            let manifest_name = manifest_name(&raw).unwrap_or_else(|| {
                panic!(
                    "bundled source '{}' is missing a top-level string name",
                    manifest_path.display()
                )
            });
            assert_eq!(
                manifest_name, name,
                "bundled source directory '{name}' must match manifest name '{manifest_name}'"
            );
            let assets = bundled_assets(&entry.path(), &manifest_path);
            Some(BundledEntry {
                name,
                manifest_yaml: raw,
                assets,
            })
        })
        .collect::<Vec<_>>();
    assert!(
        !entries.is_empty(),
        "bundled source root '{}' must contain at least one active SourceSpec manifest",
        root.display()
    );
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    entries
}

fn is_source_spec_manifest(raw: &str) -> bool {
    let Ok(root) = serde_yaml::from_str::<Value>(raw) else {
        return false;
    };
    root.get("spec_version").and_then(Value::as_i64) == Some(1)
        && root.get("kind").and_then(Value::as_str) == Some("source")
}

fn write_entries(generated: &mut String, const_name: &str, entries: &[BundledEntry]) {
    writeln!(
        generated,
        "pub(crate) const {const_name}: &[BundledSourceEntry] = &["
    )
    .expect("writing to String is infallible");
    for entry in entries {
        writeln!(
            generated,
            "    BundledSourceEntry {{ name: {:?}, manifest_yaml: {:?}, assets: &[",
            entry.name, entry.manifest_yaml,
        )
        .expect("writing to String is infallible");
        for asset in &entry.assets {
            writeln!(
                generated,
                "        BundledSourceAsset {{ relative_path: {:?}, bytes: include_bytes!({:?}) }},",
                asset.relative_path,
                asset.source_path.display().to_string(),
            )
            .expect("writing to String is infallible");
        }
        generated.push_str("    ] },\n");
    }
    generated.push_str("];\n");
}

fn find_manifest_file(dir: &Path) -> Option<PathBuf> {
    ["manifest.yaml", "manifest.yml"]
        .into_iter()
        .map(|name| dir.join(name))
        .find(|path| path.exists())
}

fn manifest_name(raw: &str) -> Option<String> {
    let root: Value = serde_yaml::from_str(raw).ok()?;
    root.get("name")
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn bundled_assets(source_dir: &Path, manifest_path: &Path) -> Vec<BundledAsset> {
    let mut assets = Vec::new();
    collect_bundled_assets(source_dir, source_dir, manifest_path, &mut assets);
    assets.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    assets
}

fn collect_bundled_assets(
    root: &Path,
    dir: &Path,
    manifest_path: &Path,
    assets: &mut Vec<BundledAsset>,
) {
    for entry in fs::read_dir(dir)
        .unwrap_or_else(|error| panic!("read bundled source assets '{}': {error}", dir.display()))
    {
        let entry = entry.unwrap_or_else(|error| {
            panic!(
                "read bundled source asset entry '{}': {error}",
                dir.display()
            )
        });
        let path = entry.path();
        let file_type = entry.file_type().unwrap_or_else(|error| {
            panic!("stat bundled source asset '{}': {error}", path.display())
        });
        if file_type.is_dir() {
            collect_bundled_assets(root, &path, manifest_path, assets);
            continue;
        }
        if !file_type.is_file() || path == manifest_path {
            continue;
        }
        println!("cargo:rerun-if-changed={}", path.display());
        let relative_path = path
            .strip_prefix(root)
            .unwrap_or_else(|error| {
                panic!(
                    "bundled source asset '{}' was not under '{}': {error}",
                    path.display(),
                    root.display()
                )
            })
            .to_string_lossy()
            .replace('\\', "/");
        assets.push(BundledAsset {
            relative_path,
            source_path: path,
        });
    }
}
