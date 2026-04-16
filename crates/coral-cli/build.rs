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
