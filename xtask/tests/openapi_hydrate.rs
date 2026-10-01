//! Command-level coverage for `xtask openapi-hydrate`.

#![allow(
    clippy::indexing_slicing,
    missing_docs,
    reason = "The test asserts values in fixture-shaped JSON output."
)]
#![allow(
    unused_crate_dependencies,
    reason = "Cargo compiles each integration test as a separate crate with the package dependencies."
)]

use std::fs;
use std::path::Path;

use tempfile::TempDir;

#[test]
fn hydrate_prints_pretty_json() {
    let fixture = Fixture::new();
    fixture.write(
        "openapi.yaml",
        r#"
openapi: 3.1.0
info: {title: Test, version: "1"}
paths:
  /pets:
    get:
      responses:
        "200": {description: ok}
"#,
    );

    let output = assert_cmd::Command::cargo_bin("xtask")
        .expect("binary exists")
        .args([
            "openapi-hydrate",
            &fixture.path("openapi.yaml").display().to_string(),
        ])
        .output()
        .expect("command runs");

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    assert!(stdout.contains('\n'));
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("stdout is JSON");
    assert_eq!(
        parsed["paths"]["/pets"]["get"]["responses"]["200"]["description"],
        "ok"
    );
}

#[test]
fn hydrate_failed_ref_exits_nonzero_with_useful_error() {
    let fixture = Fixture::new();
    fixture.write(
        "openapi.yaml",
        r#"
openapi: 3.1.0
info: {title: Test, version: "1"}
paths:
  /pets:
    $ref: missing.yaml
"#,
    );

    let output = assert_cmd::Command::cargo_bin("xtask")
        .expect("binary exists")
        .args([
            "openapi-hydrate",
            &fixture.path("openapi.yaml").display().to_string(),
        ])
        .output()
        .expect("command runs");

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(stderr.contains("missing.yaml"), "{stderr}");
}

struct Fixture {
    dir: TempDir,
}

impl Fixture {
    fn new() -> Self {
        Self {
            dir: TempDir::new().expect("tempdir"),
        }
    }

    fn path(&self, relative: impl AsRef<Path>) -> std::path::PathBuf {
        self.dir.path().join(relative)
    }

    fn write(&self, relative: impl AsRef<Path>, contents: &str) {
        let path = self.path(relative);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).expect("parent dirs");
        }
        fs::write(path, contents).expect("fixture write");
    }
}
