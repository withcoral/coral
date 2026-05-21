//! Build hints for optional CLI assets and embedded version metadata.

#![allow(
    clippy::disallowed_methods,
    clippy::print_stdout,
    reason = "Cargo build scripts read build-time environment variables directly."
)]

use std::process::Command;
use std::{
    env,
    path::{Path, PathBuf},
};

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

    if env::var_os("CARGO_FEATURE_EMBEDDED_UI").is_some() {
        let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").expect("manifest dir"));
        let ui_dist_dir = manifest_dir.join("../../ui/dist");
        emit_ui_rerun_hints(&ui_dist_dir);
        validate_embedded_ui_dist(&ui_dist_dir);
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

fn emit_ui_rerun_hints(ui_dist_dir: &Path) {
    println!("cargo:rerun-if-changed={}", ui_dist_dir.display());
    println!(
        "cargo:rerun-if-changed={}",
        ui_dist_dir.join("index.html").display()
    );
}

fn validate_embedded_ui_dist(ui_dist_dir: &Path) {
    let index_path = ui_dist_dir.join("index.html");
    if index_path.is_file() {
        return;
    }

    fail_build(format!(
        "embedded-ui is enabled by default, but the compiled UI was not found at {}.\n\
         Run `make ui-build`, then retry. To compile without the UI, pass `--no-default-features`.",
        index_path.display()
    ));
}

fn fail_build(message: impl AsRef<str>) -> ! {
    for line in message.as_ref().lines() {
        println!("cargo::error={line}");
    }
    std::process::exit(1);
}
