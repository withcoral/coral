// SPDX-License-Identifier: LicenseRef-Coral-Enterprise

//! Fixes this build's Release Date into the crate (ADR 0038).
//!
//! The release workflow sets `CORAL_RELEASE_DATE` for every build it
//! publishes, and such a build trusts the production signing keys only. Any
//! other build is a development build: its date is its own commit's, or
//! today's when git is unavailable, and it trusts only the test signing key.
//! A release build (`CORAL_RELEASE_BUILD=1`) without a Release Date does not
//! compile.

#![allow(
    clippy::disallowed_methods,
    clippy::print_stdout,
    reason = "Cargo build scripts read build-time environment variables and the build machine's clock directly."
)]

use std::process::Command;

use chrono::NaiveDate;

fn main() {
    println!("cargo:rerun-if-env-changed=CORAL_RELEASE_DATE");
    println!("cargo:rerun-if-env-changed=CORAL_RELEASE_BUILD");
    let released = std::env::var("CORAL_RELEASE_DATE")
        .ok()
        .filter(|date| !date.is_empty());
    let (date, kind) = match released {
        Some(date) => match NaiveDate::parse_from_str(&date, "%Y-%m-%d") {
            Ok(date) => (date, "released"),
            Err(error) => panic!("CORAL_RELEASE_DATE `{date}` is not a YYYY-MM-DD date: {error}"),
        },
        None if std::env::var("CORAL_RELEASE_BUILD").as_deref() == Ok("1") => {
            panic!("a release build (CORAL_RELEASE_BUILD=1) needs CORAL_RELEASE_DATE")
        }
        None => {
            watch_head();
            (commit_date().unwrap_or_else(today), "development")
        }
    };
    println!("cargo:rustc-env=CORAL_LICENSE_RELEASE_DATE={date}");
    println!("cargo:rustc-env=CORAL_LICENSE_BUILD_KIND={kind}");
}

/// Reruns this script when git's HEAD moves, so that a development build
/// keeps its own commit's date. The paths come from git, since a worktree's
/// `.git` is a file.
fn watch_head() {
    for path in ["HEAD", "packed-refs"] {
        if let Some(path) = git(&["rev-parse", "--git-path", path]) {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    if let Some(reference) = git(&["symbolic-ref", "-q", "HEAD"])
        && let Some(path) = git(&["rev-parse", "--git-path", &reference])
    {
        println!("cargo:rerun-if-changed={path}");
    }
}

/// What `git args` prints, trimmed, if it succeeds.
fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8(output.stdout).ok())
        .flatten()
        .map(|text| text.trim().to_owned())
        .filter(|text| !text.is_empty())
}

/// The UTC committer date of the commit this source was built from.
fn commit_date() -> Option<NaiveDate> {
    let output = Command::new("git")
        .args(["log", "-1", "--date=format-local:%Y-%m-%d", "--format=%cd"])
        .env("TZ", "UTC")
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let date = String::from_utf8(output.stdout).ok()?;
    NaiveDate::parse_from_str(date.trim(), "%Y-%m-%d").ok()
}

/// Today's UTC date, from the build machine's clock.
fn today() -> NaiveDate {
    let seconds = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs());
    chrono::DateTime::from_timestamp(i64::try_from(seconds).unwrap_or(0), 0)
        .unwrap_or_default()
        .date_naive()
}
