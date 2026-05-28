//! Build script for bundled source manifests.

#![allow(
    clippy::disallowed_methods,
    reason = "Cargo build scripts read build-time environment variables directly."
)]

use serde_yaml::Value;
use std::collections::BTreeMap;
use std::env;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};

fn main() {
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir"));
    let bundled_root = manifest_dir.join("../../sources/core");
    let dsl_v4_bundled_root = manifest_dir.join("../../sources/core-v4");
    println!("cargo:rerun-if-changed={}", bundled_root.display());
    println!("cargo:rerun-if-changed={}", dsl_v4_bundled_root.display());

    let entries = bundled_entries(&bundled_root, false);
    let dsl_v4_entries = bundled_entries(&dsl_v4_bundled_root, true);

    let mut generated = String::new();
    write_entries(&mut generated, "BUNDLED_SOURCES", &entries);
    write_entries(&mut generated, "DSL_V4_BUNDLED_SOURCES", &dsl_v4_entries);

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("out dir"));
    fs::write(out_dir.join("bundled_sources.rs"), generated).expect("write bundled source table");
}

struct BundledEntry {
    name: String,
    manifest_yaml: String,
    descriptors: Vec<BundledDescriptor>,
}

struct BundledDescriptor {
    surface_id: String,
    path: PathBuf,
}

fn bundled_entries(root: &Path, collect_descriptors: bool) -> Vec<BundledEntry> {
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
            let descriptors = if collect_descriptors {
                bundled_v4_descriptors(&manifest_path, &raw)
            } else {
                Vec::new()
            };
            BundledEntry {
                name,
                manifest_yaml: raw,
                descriptors,
            }
        })
        .collect::<Vec<_>>();
    entries.sort_by(|left, right| left.name.cmp(&right.name));
    entries
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
            "    BundledSourceEntry {{ name: {:?}, manifest_yaml: {:?}, descriptors: &[",
            entry.name, entry.manifest_yaml
        )
        .expect("writing to String is infallible");
        for descriptor in &entry.descriptors {
            writeln!(
                generated,
                "        BundledV4Descriptor {{ surface_id: {:?}, bytes: include_bytes!({:?}) }},",
                descriptor.surface_id,
                descriptor.path.display().to_string()
            )
            .expect("writing to String is infallible");
        }
        generated.push_str("    ] },\n");
    }
    generated.push_str("];\n");
}

fn bundled_v4_descriptors(manifest_path: &Path, raw: &str) -> Vec<BundledDescriptor> {
    let root: Value = serde_yaml::from_str(raw).unwrap_or_else(|error| {
        panic!(
            "parse bundled v4 source '{}': {error}",
            manifest_path.display()
        )
    });
    if root.get("dsl_version").and_then(Value::as_i64) != Some(4) {
        return Vec::new();
    }
    let manifest_dir = manifest_path
        .parent()
        .expect("manifest path should have parent directory");
    let surfaces = root
        .get("surfaces")
        .and_then(Value::as_sequence)
        .unwrap_or_else(|| panic!("v4 source '{}' has no surfaces", manifest_path.display()));
    surfaces
        .iter()
        .filter_map(|surface| {
            let surface = surface.as_mapping().unwrap_or_else(|| {
                panic!(
                    "v4 source '{}' contains non-object surface",
                    manifest_path.display()
                )
            });
            let values = surface
                .iter()
                .filter_map(|(key, value)| Some((key.as_str()?, value)))
                .collect::<BTreeMap<_, _>>();
            let surface_id = values
                .get("id")
                .and_then(|value| value.as_str())
                .unwrap_or_else(|| {
                    panic!(
                        "v4 source '{}' contains surface without id",
                        manifest_path.display()
                    )
                });
            let file = values.get("file").and_then(|value| value.as_str())?;
            let path = manifest_dir.join(file);
            println!("cargo:rerun-if-changed={}", path.display());
            assert!(
                path.exists(),
                "v4 source '{}' surface '{}' references missing descriptor '{}'",
                manifest_path.display(),
                surface_id,
                path.display()
            );
            Some(BundledDescriptor {
                surface_id: surface_id.to_string(),
                path,
            })
        })
        .collect()
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
