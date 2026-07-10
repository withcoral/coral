//! Integration coverage for `OpenAPI` hydration.

#![allow(
    clippy::indexing_slicing,
    clippy::needless_raw_string_hashes,
    missing_docs,
    unused_crate_dependencies,
    reason = "Integration tests use fixture-shaped JSON assertions and exercise public binary/library surfaces."
)]

use std::fs;
use std::path::Path;

use openapi::{OpenApiToolsError, hydrate_openapi, hydrate_openapi_from_location};
use serde_json::Value;
use tempfile::TempDir;
use url::Url;

#[test]
fn local_file_refs_across_path_item_parameter_and_schema_files() {
    let fixture = Fixture::new();
    fixture.write(
        "openapi.yaml",
        r#"
openapi: 3.1.0
info: {title: Test, version: "1"}
paths:
  /pets:
    $ref: paths/pets.yaml
"#,
    );
    fixture.write(
        "paths/pets.yaml",
        r#"
get:
  parameters:
    - $ref: ../parameters/limit.yaml
  responses:
    "200":
      description: ok
      content:
        application/json:
          schema:
            $ref: ../schemas/pet.yaml
"#,
    );
    fixture.write(
        "parameters/limit.yaml",
        r#"
name: limit
in: query
schema:
  type: integer
"#,
    );
    fixture.write(
        "schemas/pet.yaml",
        r#"
type: object
properties:
  name:
    type: string
"#,
    );

    let hydrated =
        hydrate_openapi_from_location(&fixture.path("openapi.yaml").display().to_string())
            .expect("hydrate succeeds");

    assert_eq!(
        hydrated["paths"]["/pets"]["get"]["parameters"][0]["name"],
        "limit"
    );
    assert_eq!(
        hydrated["paths"]["/pets"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["properties"]["name"]["type"],
        "string"
    );
}

#[test]
fn nested_external_refs_are_hydrated() {
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
        "200":
          description: ok
          content:
            application/json:
              schema:
                $ref: schemas/pet.yaml
"#,
    );
    fixture.write(
        "schemas/pet.yaml",
        r#"
type: object
properties:
  category:
    $ref: category.yaml
"#,
    );
    fixture.write("schemas/category.yaml", "type: string\n");

    let hydrated =
        hydrate_openapi_from_location(&fixture.path("openapi.yaml").display().to_string())
            .expect("hydrate succeeds");

    assert_eq!(
        hydrated["paths"]["/pets"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["properties"]["category"]["type"],
        "string"
    );
}

#[test]
fn same_document_uri_refs_are_treated_as_local_refs() {
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
        "200":
          $ref: openapi.yaml#/components/responses/Ok
components:
  responses:
    Ok:
      description: ok
"#,
    );

    let hydrated =
        hydrate_openapi_from_location(&fixture.path("openapi.yaml").display().to_string())
            .expect("hydrate succeeds");

    assert_eq!(
        hydrated["paths"]["/pets"]["get"]["responses"]["200"]["description"],
        "ok"
    );
}

#[test]
fn descriptor_query_and_fragment_are_ignored_for_ref_resolution() {
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
        "200":
          description: ok
          content:
            application/json:
              schema:
                $ref: schemas.yaml#/Pet
"#,
    );
    fixture.write("schemas.yaml", "Pet:\n  type: object\n");
    let bytes = fs::read(fixture.path("openapi.yaml")).expect("fixture reads");
    let base = format!(
        "{}?cache=false#/ignored",
        Url::from_file_path(fixture.path("openapi.yaml"))
            .expect("file URL")
            .as_str()
    );

    let hydrated = hydrate_openapi(&bytes, &base).expect("hydrate succeeds");

    assert_eq!(
        hydrated["paths"]["/pets"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["type"],
        "object"
    );
}

#[test]
fn percent_encoded_fragments_are_decoded() {
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
        "200":
          description: ok
          content:
            application/json:
              schema:
                $ref: schemas.yaml#/Pet%20Model
"#,
    );
    fixture.write("schemas.yaml", "\"Pet Model\":\n  type: object\n");

    let hydrated =
        hydrate_openapi_from_location(&fixture.path("openapi.yaml").display().to_string())
            .expect("hydrate succeeds");

    assert_eq!(
        hydrated["paths"]["/pets"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
            ["type"],
        "object"
    );
}

#[test]
fn arrays_referenced_by_json_pointers_are_preserved() {
    let fixture = Fixture::new();
    fixture.write(
        "openapi.yaml",
        r#"
openapi: 3.1.0
info: {title: Test, version: "1"}
paths:
  /pets:
    get:
      parameters:
        $ref: params.yaml#/all
      responses:
        "200": {description: ok}
"#,
    );
    fixture.write(
        "params.yaml",
        r#"
all:
  - name: limit
    in: query
    schema: {type: integer}
"#,
    );

    let hydrated =
        hydrate_openapi_from_location(&fixture.path("openapi.yaml").display().to_string())
            .expect("hydrate succeeds");

    assert!(hydrated["paths"]["/pets"]["get"]["parameters"].is_array());
    assert_eq!(
        hydrated["paths"]["/pets"]["get"]["parameters"][0]["schema"]["type"],
        "integer"
    );
}

#[test]
fn unused_external_refs_under_root_components_are_ignored() {
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
        "200":
          description: ok
components:
  schemas:
    Unused:
      $ref: missing.yaml
"#,
    );

    let hydrated =
        hydrate_openapi_from_location(&fixture.path("openapi.yaml").display().to_string())
            .expect("hydrate succeeds");

    assert_eq!(
        hydrated["paths"]["/pets"]["get"]["responses"]["200"]["description"],
        "ok"
    );
}

#[test]
fn root_refs_outside_components_are_hydrated() {
    let fixture = Fixture::new();
    fixture.write(
        "openapi.yaml",
        r#"
openapi: 3.1.0
info:
  title: Test
  version: "1"
  description:
    $ref: description.yaml
paths:
  /pets:
    get:
      responses:
        "200":
          description: ok
components:
  schemas:
    Unused:
      $ref: missing.yaml
"#,
    );
    fixture.write("description.yaml", "Root description\n");

    let hydrated =
        hydrate_openapi_from_location(&fixture.path("openapi.yaml").display().to_string())
            .expect("hydrate succeeds");

    assert_eq!(hydrated["info"]["description"], "Root description");
    assert_eq!(
        hydrated["paths"]["/pets"]["get"]["responses"]["200"]["description"],
        "ok"
    );
}

#[test]
fn missing_reachable_external_refs_fail() {
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

    let error = hydrate_openapi_from_location(&fixture.path("openapi.yaml").display().to_string())
        .expect_err("missing ref fails");

    assert!(matches!(error, OpenApiToolsError::ReadFailure { .. }));
}

#[test]
fn missing_reachable_external_target_fails() {
    let fixture = Fixture::new();
    fixture.write(
        "openapi.yaml",
        r#"
openapi: 3.1.0
info: {title: Test, version: "1"}
paths:
  /pets:
    $ref: paths.yaml#/missing
"#,
    );
    fixture.write("paths.yaml", "present: {}\n");

    let error = hydrate_openapi_from_location(&fixture.path("openapi.yaml").display().to_string())
        .expect_err("missing pointer target fails");

    assert!(matches!(
        error,
        OpenApiToolsError::UnresolvedRefTarget { .. }
    ));
}

#[test]
fn https_document_cannot_load_file_refs() {
    let input = br#"
openapi: 3.1.0
info: {title: Test, version: "1"}
paths:
  /pets:
    $ref: file:///tmp/pets.yaml
"#;

    let error =
        hydrate_openapi(input, "https://example.com/openapi.yaml").expect_err("file ref fails");

    assert!(matches!(
        error,
        OpenApiToolsError::UnsupportedScheme { scheme, .. } if scheme == "file"
    ));
}

#[test]
fn local_file_refs_cannot_escape_descriptor_directory() {
    let fixture = Fixture::new();
    let outside = Fixture::new();
    outside.write("schema.yaml", "type: object\n");
    fixture.write(
        "openapi.yaml",
        &format!(
            r#"
openapi: 3.1.0
info: {{title: Test, version: "1"}}
paths:
  /pets:
    get:
      responses:
        "200":
          description: ok
          content:
            application/json:
              schema:
                $ref: {}
"#,
            Url::from_file_path(outside.path("schema.yaml"))
                .expect("outside file URL")
                .as_str()
        ),
    );

    let error = hydrate_openapi_from_location(&fixture.path("openapi.yaml").display().to_string())
        .expect_err("escaped ref fails");

    assert!(matches!(
        error,
        OpenApiToolsError::LocalFileConfinementViolation { .. }
    ));
}

#[cfg(unix)]
#[test]
fn symlinked_local_refs_are_rejected() {
    use std::os::unix::fs::symlink;

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
        "200":
          description: ok
          content:
            application/json:
              schema:
                $ref: schema-link.yaml
"#,
    );
    fixture.write("schema.yaml", "type: object\n");
    symlink(
        fixture.path("schema.yaml"),
        fixture.path("schema-link.yaml"),
    )
    .expect("symlink");

    let error = hydrate_openapi_from_location(&fixture.path("openapi.yaml").display().to_string())
        .expect_err("symlink ref fails");

    assert!(matches!(
        error,
        OpenApiToolsError::LocalFileConfinementViolation { .. }
    ));
}

#[test]
fn cli_hydrate_prints_pretty_json() {
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

    let output = assert_cmd::Command::cargo_bin("openapi")
        .expect("binary exists")
        .args([
            "hydrate",
            &fixture.path("openapi.yaml").display().to_string(),
        ])
        .output()
        .expect("command runs");

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("stdout is UTF-8");
    assert!(stdout.contains('\n'));
    let parsed: Value = serde_json::from_str(&stdout).expect("stdout is JSON");
    assert_eq!(
        parsed["paths"]["/pets"]["get"]["responses"]["200"]["description"],
        "ok"
    );
}

#[test]
fn cli_failed_ref_exits_nonzero_with_useful_error() {
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

    let output = assert_cmd::Command::cargo_bin("openapi")
        .expect("binary exists")
        .args([
            "hydrate",
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
