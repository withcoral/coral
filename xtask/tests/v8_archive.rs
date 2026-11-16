//! A build that ships links only the V8 archive that `Cargo.toml` records.
//!
//! The v8 crate's build script downloads a prebuilt archive and checks it
//! against `RUSTY_V8_ARCHIVE_SHA256`. Desktop's staging reads the digests from
//! `[workspace.metadata.v8-archive]`, and its own test proves that it passes
//! them on: `apps/desktop/scripts/runtime.test.mjs`. The Dockerfile cannot
//! read the table. It names the values again, and these tests keep the table,
//! the v8 pin and the builds in step. Only a build proves that a digest is the
//! archive's.

#![allow(
    missing_docs,
    reason = "The test names say what each one keeps in step."
)]
#![allow(
    unused_crate_dependencies,
    reason = "Cargo compiles each integration test as a separate crate with the package dependencies."
)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

/// The runner labels the shipping workflows use, by the word that names the
/// operating system, and the target a plain `cargo build` has there.
const RUNNER_TARGETS: [(&str, &str); 3] = [
    ("ubuntu", "x86_64-unknown-linux-gnu"),
    ("macos", "aarch64-apple-darwin"),
    ("windows", "x86_64-pc-windows-msvc"),
];

/// The image's `TARGETARCH` values and the target `cargo build` has in each.
const IMAGE_TARGETS: [(&str, &str); 2] = [
    ("amd64", "x86_64-unknown-linux-gnu"),
    ("arm64", "aarch64-unknown-linux-gnu"),
];

fn read(path: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join(path);
    fs::read_to_string(&path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

/// The value at `path` under `[workspace]` in the root `Cargo.toml`.
fn workspace_value(path: &[&str]) -> toml::Value {
    let cargo: toml::Table = toml::from_str(&read("Cargo.toml")).expect("parse Cargo.toml");
    let mut value = &toml::Value::Table(cargo);
    for key in ["workspace"].iter().chain(path) {
        value = value
            .get(key)
            .unwrap_or_else(|| panic!("Cargo.toml has no `{key}` on the way to {path:?}"));
    }
    value.clone()
}

/// The digest of each target's archive, from `[workspace.metadata.v8-archive.sha256]`.
fn recorded_digests() -> BTreeMap<String, String> {
    workspace_value(&["metadata", "v8-archive", "sha256"])
        .as_table()
        .expect("[workspace.metadata.v8-archive.sha256] is a table")
        .iter()
        .map(|(target, digest)| {
            let digest = digest.as_str().expect("a digest is a string");
            assert!(
                digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()),
                "the digest for {target} is not a SHA-256: {digest:?}"
            );
            (target.clone(), digest.to_owned())
        })
        .collect()
}

#[test]
fn the_table_holds_the_digests_of_the_pinned_v8_version() {
    let pin = workspace_value(&["dependencies", "v8"]);
    let pin = pin.as_str().expect("the v8 dependency is a version string");
    let recorded = workspace_value(&["metadata", "v8-archive", "version"]);
    let recorded = recorded.as_str().expect("the version is a string");

    assert_eq!(
        pin,
        format!("={recorded}"),
        "[workspace.metadata.v8-archive] holds the digests of v8 {recorded}. \
         Replace every row with the digest of the pinned version's archive, \
         and set its version."
    );
}

#[test]
fn the_server_image_checks_the_archive_of_each_architecture() {
    let digests = recorded_digests();
    let dockerfile = read("docker/Dockerfile");

    let expected = IMAGE_TARGETS
        .iter()
        .map(|(arch, target)| {
            let digest = digests
                .get(*target)
                .unwrap_or_else(|| panic!("the table has no row for {target}"));
            format!("{arch}) v8_sha={digest} ;;")
        })
        .collect::<BTreeSet<_>>();
    // A `case "$TARGETARCH"` arm, without the line continuation.
    let named = dockerfile
        .lines()
        .filter(|line| line.contains("v8_sha="))
        .map(|line| line.trim().trim_end_matches('\\').trim_end().to_owned())
        .collect::<BTreeSet<_>>();

    assert_eq!(
        named, expected,
        "docker/Dockerfile and the table name other archives"
    );
    assert!(
        dockerfile.contains(r#"RUSTY_V8_ARCHIVE_SHA256="$v8_sha" cargo build"#),
        "docker/Dockerfile builds without RUSTY_V8_ARCHIVE_SHA256"
    );
}

#[test]
fn the_table_has_a_row_for_every_target_a_shipping_workflow_builds() {
    let digests = recorded_digests();

    for workflow in [
        ".github/workflows/release.yml",
        ".github/workflows/desktop.yml",
    ] {
        let text = read(workflow);
        let mut targets = BTreeSet::new();
        for line in text.lines().map(str::trim) {
            if let Some(runner) = line.strip_prefix("runs-on: ") {
                targets.insert(runner_target(workflow, runner).to_owned());
            }
            if let Some((_, added)) = line.split_once("rustup target add ") {
                targets.extend(added.split_whitespace().map(str::to_owned));
            }
        }
        assert!(!targets.is_empty(), "{workflow} names no runner");

        let missing = targets
            .iter()
            .filter(|target| !digests.contains_key(*target))
            .collect::<Vec<_>>();
        assert!(
            missing.is_empty(),
            "{workflow} builds for {missing:?}, which have no row in \
             [workspace.metadata.v8-archive.sha256]"
        );
    }
}

/// The target of a runner label. A label this test does not know stops it:
/// an Arm Linux or Windows runner builds for a target of its own.
fn runner_target(workflow: &str, runner: &str) -> &'static str {
    RUNNER_TARGETS
        .iter()
        .find(|(os, _)| !runner.ends_with("-arm") && runner.split('-').any(|part| part == *os))
        .map_or_else(
            || {
                panic!(
                    "{workflow} runs on {runner}. Add its target to RUNNER_TARGETS and \
                     the archive's digest to [workspace.metadata.v8-archive.sha256]."
                )
            },
            |(_, target)| *target,
        )
}
