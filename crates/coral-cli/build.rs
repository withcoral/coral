//! Build hints for optional CLI assets and embedded version metadata.

#![allow(
    clippy::disallowed_methods,
    clippy::print_stdout,
    reason = "Cargo build scripts read build-time environment variables directly."
)]

use std::process::Command;

fn main() {
    let sha = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map_or_else(
            || "unknown".to_owned(),
            |out| String::from_utf8_lossy(&out.stdout).trim().to_owned(),
        );
    println!("cargo:rustc-env=CORAL_GIT_SHA={sha}");

    // Trigger rebuilds when HEAD or the checked-out branch's ref moves so the
    // embedded SHA stays current.
    if let Some(head_path) = git_path("HEAD") {
        println!("cargo:rerun-if-changed={head_path}");
        if let Ok(head) = std::fs::read_to_string(&head_path)
            && let Some(reference) = head.trim().strip_prefix("ref: ")
            && let Some(reference_path) = git_path(reference)
            && std::path::Path::new(&reference_path).exists()
        {
            println!("cargo:rerun-if-changed={reference_path}");
        }
    }
    if let Some(packed_refs_path) = git_path("packed-refs")
        && std::path::Path::new(&packed_refs_path).exists()
    {
        println!("cargo:rerun-if-changed={packed_refs_path}");
    }

    if std::env::var_os("CARGO_FEATURE_EMBEDDED_UI").is_some() {
        println!("cargo:rerun-if-changed=../../ui/dist");
        println!("cargo:rerun-if-changed=../../ui/dist/index.html");
        println!("cargo:rerun-if-env-changed=CORAL_REQUIRE_UI_DIST");

        if std::env::var_os("CORAL_REQUIRE_UI_DIST").is_some() {
            validate_embedded_ui_dist();
        }
    }
}

fn git_path(path: &str) -> Option<String> {
    Command::new("git")
        .args(["rev-parse", "--git-path", path])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
        .filter(|path| !path.is_empty())
}

fn validate_embedded_ui_dist() {
    use std::path::Path;

    let index_path = Path::new("../../ui/dist/index.html");
    let index_metadata = std::fs::metadata(index_path).unwrap_or_else(|error| {
        panic!(
            "CORAL_REQUIRE_UI_DIST=1 requires embedded UI assets, but {} is missing or unreadable: {error}",
            index_path.display()
        )
    });
    assert!(
        index_metadata.is_file(),
        "CORAL_REQUIRE_UI_DIST=1 requires embedded UI assets, but {} is not a file",
        index_path.display()
    );
    assert!(
        index_metadata.len() != 0,
        "CORAL_REQUIRE_UI_DIST=1 requires embedded UI assets, but {} is empty",
        index_path.display()
    );

    let assets_dir = Path::new("../../ui/dist/assets");
    let has_assets = std::fs::read_dir(assets_dir).is_ok_and(|entries| {
        entries
            .flatten()
            .any(|entry| entry.file_type().is_ok_and(|file_type| file_type.is_file()))
    });
    assert!(
        has_assets,
        "CORAL_REQUIRE_UI_DIST=1 requires embedded UI assets, but {} has no built files",
        assets_dir.display()
    );
}
