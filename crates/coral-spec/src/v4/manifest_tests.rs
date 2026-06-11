use crate::parse_source_manifest_yaml;
use serde_json::json;

/// Renders a one-surface v4 manifest: `header` provides the top-level lines
/// (name and optional version) and `surface_extra` lines are appended under
/// the openapi surface entry (4-space indent).
fn v4_yaml(header: &str, surface_extra: &str) -> String {
    format!(
        "{header}\ndsl_version: 4\nsurfaces:\n  - id: rest\n    type: openapi\n    file: /tmp/openapi.yaml\n    sha256: 0000000000000000000000000000000000000000000000000000000000000000\n{surface_extra}"
    )
}

/// Asserts the manifest fails to parse with an error containing `expected`.
fn expect_v4_error(raw: &str, label: &str, expected: &str) {
    let error = parse_source_manifest_yaml(raw).expect_err(label);
    assert!(
        error.to_string().contains(expected),
        "unexpected error: {error}"
    );
}

#[test]
fn parses_v4_manifest_and_unions_surface_inputs() {
    let manifest = parse_source_manifest_yaml(&v4_yaml(
        "name: demo\nversion: 1.2.3",
        r#"    inputs:
      AAA_BASE: {kind: variable, default: 'https://api.example.com'}
      ZZZ_TENANT: {kind: variable, default: demo}
    base_url: "{{input.AAA_BASE}}""#,
    ))
    .expect("v4 manifest");
    assert_eq!(manifest.dsl_version(), 4);
    assert_eq!(manifest.source_version(), Some("1.2.3"));
    let v4 = manifest.as_v4().expect("v4");
    let surface = v4.surface("rest").expect("rest surface");
    assert_eq!(
        surface.descriptor.sha256(),
        Some("0000000000000000000000000000000000000000000000000000000000000000")
    );
    assert_eq!(manifest.declared_inputs().len(), 2);
    let keys = manifest
        .declared_inputs()
        .iter()
        .map(|input| input.key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(keys, ["AAA_BASE", "ZZZ_TENANT"]);
}

#[test]
fn parses_v4_openapi_surface_without_base_url() {
    let manifest = parse_source_manifest_yaml(&v4_yaml("name: demo", "")).expect("v4 manifest");
    let v4 = manifest.as_v4().expect("v4");
    assert_eq!(
        v4.surfaces
            .first()
            .expect("surface")
            .openapi_runtime()
            .expect("OpenAPI runtime")
            .base_url
            .raw(),
        ""
    );
}

#[test]
fn single_surface_relation_namespace_defaults_to_source_name() {
    let manifest =
        parse_source_manifest_yaml(&v4_yaml("name: demo_source", "")).expect("v4 manifest");
    let v4 = manifest.as_v4().expect("v4");

    assert_eq!(
        v4.surfaces.first().expect("surface").relation_namespace,
        "demo_source"
    );
}

#[test]
fn parses_v4_identity_requirements() {
    let manifest = parse_source_manifest_yaml(&v4_yaml(
        "name: github",
        r#"    identity_requirements:
      accepts:
        - id: " github-rest-read "
          identity_specs: [" github_oauth ", " github_pat "]
          audience: {host: github.com, port: 443}"#,
    ))
    .expect("manifest");
    let surface = manifest
        .as_v4()
        .expect("v4")
        .surface("rest")
        .expect("surface");
    let requirements = surface
        .identity_requirements
        .as_ref()
        .expect("identity requirements");
    let accepted = requirements.accepts.first().expect("accepted identity");
    assert_eq!(accepted.id, "github-rest-read");
    assert_eq!(
        accepted.identity_specs,
        vec!["github_oauth".to_string(), "github_pat".to_string()]
    );
    assert_eq!(accepted.audience.get("host"), Some(&json!("github.com")));
    assert_eq!(accepted.audience.get("port"), Some(&json!(443)));
}

#[test]
fn rejects_empty_v4_identity_requirement_accepts() {
    expect_v4_error(
        &v4_yaml("name: github", "    identity_requirements: {accepts: []}"),
        "empty accepts should fail",
        "identity_requirements.accepts must contain at least one accepted identity",
    );
}

#[test]
fn reserved_default_relation_namespace_is_rejected() {
    let error = parse_source_manifest_yaml(&v4_yaml("name: public", ""))
        .expect_err("reserved relation namespace should fail");

    assert!(
        error
            .to_string()
            .contains("source surface relation namespace 'public' is reserved"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_v4_identity_requirement_without_identity_specs() {
    expect_v4_error(
        &v4_yaml(
            "name: github",
            "    identity_requirements:\n      accepts:\n        - {id: github-rest-read, identity_specs: []}",
        ),
        "empty identity spec ids should fail",
        "identity_specs must contain at least one identity spec id",
    );
}

#[test]
fn rejects_multiple_surfaces_omitting_namespace_suffix() {
    let error = parse_source_manifest_yaml(
        r"
name: github_v4
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    sha256: 0000000000000000000000000000000000000000000000000000000000000000
  - id: mcp
    type: mcp
    server:
      transport: stdio
      command: demo-mcp-server
",
    )
    .expect_err("only one surface should be allowed to omit namespace_suffix");

    let message = error.to_string();
    assert!(
        message.contains("surfaces 'rest' and 'mcp' both omit namespace_suffix")
            && message
                .contains("at most one surface may use the default relation namespace 'github_v4'"),
        "unexpected error: {message}"
    );
}

#[test]
fn rejects_v4_identity_requirement_duplicate_identity_spec_ids() {
    expect_v4_error(
        &v4_yaml(
            "name: github",
            "    identity_requirements:\n      accepts:\n        - {id: github-rest-read, identity_specs: [github_oauth, github_oauth]}",
        ),
        "duplicate identity spec ids should fail",
        "has duplicate identity spec id 'github_oauth'",
    );
}

#[test]
fn multi_surface_namespace_suffixes_are_source_relative() {
    let manifest = parse_source_manifest_yaml(
        r"
name: github_v4
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    sha256: 0000000000000000000000000000000000000000000000000000000000000000
  - id: mcp
    namespace_suffix: mcp
    type: mcp
    server:
      transport: stdio
      command: demo-mcp-server
",
    )
    .expect("v4 manifest");
    let v4 = manifest.as_v4().expect("v4");
    let namespaces = v4
        .surfaces
        .iter()
        .map(|surface| surface.relation_namespace.as_str())
        .collect::<Vec<_>>();

    assert_eq!(namespaces, ["github_v4", "github_v4_mcp"]);
}

#[test]
fn explicit_surface_namespace_suffix_appends_to_source_name() {
    let manifest =
        parse_source_manifest_yaml(&v4_yaml("name: github_v4", "    namespace_suffix: api"))
            .expect("v4 manifest");
    let v4 = manifest.as_v4().expect("v4");

    assert_eq!(
        v4.surfaces.first().expect("surface").relation_namespace,
        "github_v4_api"
    );
}

#[test]
fn unrelated_namespace_suffix_cannot_impersonate_another_source_relation_namespace() {
    let manifest = parse_source_manifest_yaml(&v4_yaml(
        "name: linear",
        "    namespace_suffix: github_rest",
    ))
    .expect("v4 manifest");
    let v4 = manifest.as_v4().expect("v4");

    assert_eq!(
        v4.surfaces.first().expect("surface").relation_namespace,
        "linear_github_rest"
    );
}

#[test]
fn rejects_duplicate_effective_surface_relation_namespaces() {
    let error = parse_source_manifest_yaml(
        r"
name: github_v4
dsl_version: 4
surfaces:
  - id: rest
    namespace_suffix: api
    type: openapi
    file: /tmp/openapi.yaml
    sha256: 0000000000000000000000000000000000000000000000000000000000000000
  - id: mcp
    namespace_suffix: api
    type: mcp
    server:
      transport: stdio
      command: demo-mcp-server
",
    )
    .expect_err("duplicate namespace should fail");

    assert!(error.to_string().contains(
        "surfaces 'rest' and 'mcp' declare duplicate relation namespace 'github_v4_api'"
    ));
}

#[test]
fn rejects_v4_surface_auth() {
    let error = parse_source_manifest_yaml(&v4_yaml(
        "name: demo",
        "    auth: {type: HeaderAuth, headers: []}",
    ))
    .expect_err("v4 auth should fail");

    assert!(
        error.to_string().contains("auth") && error.to_string().contains("not valid"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_v4_surface_secret_inputs() {
    let error = parse_source_manifest_yaml(&v4_yaml(
        "name: demo",
        "    inputs:\n      API_TOKEN: {kind: secret}",
    ))
    .expect_err("v4 secret input should fail");

    assert!(
        error.to_string().contains("/surfaces/0/inputs/API_TOKEN")
            && error.to_string().contains("schema validation"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_v4_openapi_base_url_runtime_controlled_tokens() {
    for token in [
        "filter.host",
        "arg.host",
        "state.next",
        "expr.host",
        "custom.host",
    ] {
        let raw = v4_yaml(
            "name: demo",
            &format!("    base_url: \"https://{{{{{token}}}}}\""),
        );
        let error = parse_source_manifest_yaml(&raw).expect_err("runtime token should be rejected");
        let message = error.to_string();
        assert!(
            message.contains("base_url may only reference source inputs"),
            "unexpected error for {token}: {message}"
        );
    }
}

#[test]
fn rejects_v4_openapi_base_url_input_token_defaults() {
    let error = parse_source_manifest_yaml(&v4_yaml(
        "name: demo",
        r#"    inputs:
      API_BASE: {kind: variable, default: 'https://api.example.com'}
    base_url: "{{input.API_BASE|https://fallback.example.com}}""#,
    ))
    .expect_err("base_url token default should be rejected");

    let message = error.to_string();
    assert!(
        message.contains("must declare defaults under top-level inputs")
            || message.contains("must declare defaults under source inputs"),
        "unexpected error: {message}"
    );
}

#[test]
fn parses_v4_base_url_referencing_surface_variables() {
    let manifest = parse_source_manifest_yaml(&v4_yaml(
        "name: demo",
        "    inputs:\n      TENANT_ID: {kind: variable, default: organizations}\n    base_url: https://{{input.TENANT_ID}}.example.com",
    ))
    .expect("v4 manifest");

    assert_eq!(manifest.declared_inputs().len(), 1);
}
