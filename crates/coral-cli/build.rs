//! Embed the git commit SHA into the binary so `coral --version` reports which
//! commit a build came from. Falls back to `unknown` when git is unavailable,
//! (e.g. building from a source tarball).

#![allow(
    clippy::print_stdout,
    reason = "Build scripts communicate with cargo via stdout."
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
    if let Ok(output) = Command::new("git")
        .args(["rev-parse", "--git-dir"])
        .output()
        && output.status.success()
    {
        let git_dir = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        println!("cargo:rerun-if-changed={git_dir}/HEAD");
        println!("cargo:rerun-if-changed={git_dir}/packed-refs");
        if let Ok(head) = std::fs::read_to_string(format!("{git_dir}/HEAD"))
            && let Some(reference) = head.trim().strip_prefix("ref: ")
        {
            println!("cargo:rerun-if-changed={git_dir}/{reference}");
        }
    }
}
