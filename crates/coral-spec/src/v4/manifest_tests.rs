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
        "0000000000000000000000000000000000000000000000000000000000000000"
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
            .openapi_runtime
            .base_url
            .raw(),
        ""
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
fn rejects_v4_surface_auth() {
    let error = parse_source_manifest_yaml(&v4_yaml(
        "name: demo",
        "    auth: {type: HeaderAuth, headers: []}",
    ))
    .expect_err("v4 auth should fail");

    assert!(
        error
            .to_string()
            .contains("Additional properties are not allowed ('auth' was unexpected)")
            || error.to_string().contains("unknown field `auth`"),
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
