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
}

fn bundled_entries(root: &Path) -> Vec<BundledEntry> {
    let mut entries = fs::read_dir(root)
        .unwrap_or_else(|error| panic!("read bundled sources '{}': {error}", root.display()))
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .map(|entry| {
            let name = entry.file_name().to_string_lossy().to_string();
            let manifest_path = find_manifest_file(&entry.path()).unwrap_or_else(|| {
                panic!(
                    "missing manifest.y*ml for bundled source '{}'",
                    entry.path().display()
                )
            });
            println!("cargo:rerun-if-changed={}", manifest_path.display());
            let raw = fs::read_to_string(&manifest_path).expect("read bundled manifest");
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
            BundledEntry {
                name,
                manifest_yaml: raw,
            }
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    entries
}

fn write_entries(generated: &mut String, const_name: &str, entries: &[BundledEntry]) {
    writeln!(
        generated,
        "pub(crate) const {const_name}: &[(&str, &str)] = &["
    )
    .expect("writing to String is infallible");
    for entry in entries {
        writeln!(
            generated,
            "    ({:?}, {:?}),",
            entry.name, entry.manifest_yaml
        )
        .expect("writing to String is infallible");
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
