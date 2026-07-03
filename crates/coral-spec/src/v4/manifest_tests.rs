use crate::{
    ManifestCredentialMethodKind, ManifestOAuthDynamicClientRegistrationAuthMethod,
    ManifestOAuthFlowKind, ManifestOAuthPkceMode, ManifestOAuthRedirectUriPortMode,
    ManifestOAuthScopeDelimiter, parse_source_manifest_yaml,
};

#[test]
fn parses_v4_manifest_and_unions_surface_inputs() {
    let manifest = parse_source_manifest_yaml(
        r#"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    inputs:
      ZZZ_TOKEN:
        kind: secret
      AAA_BASE:
        kind: variable
        default: https://api.example.com
    base_url: "{{input.AAA_BASE}}"
    auth:
      type: HeaderAuth
      headers:
        - name: Authorization
          from: template
          template: Bearer {{input.ZZZ_TOKEN}}
"#,
    )
    .expect("v4 manifest");
    assert_eq!(manifest.dsl_version(), 4);
    assert!(manifest.as_v4().is_some());
    assert_eq!(manifest.declared_inputs().len(), 2);
    let keys = manifest
        .declared_inputs()
        .iter()
        .map(|input| input.key.as_str())
        .collect::<Vec<_>>();
    assert_eq!(keys, ["ZZZ_TOKEN", "AAA_BASE"]);
}

#[test]
fn parses_v4_manifest_version_and_descriptor_sha256() {
    let manifest = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
version: 1.2.3
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    sha256: 0000000000000000000000000000000000000000000000000000000000000000
",
    )
    .expect("v4 manifest");
    assert_eq!(manifest.source_version(), Some("1.2.3"));
    let v4 = manifest.as_v4().expect("v4");
    assert_eq!(v4.common.version.as_deref(), Some("1.2.3"));
    assert_eq!(
        v4.surfaces.first().expect("surface").descriptor.sha256(),
        Some("0000000000000000000000000000000000000000000000000000000000000000")
    );
}

#[test]
fn rejects_empty_v4_manifest_version() {
    let error = parse_source_manifest_yaml(
        r#"
name: demo
dsl_version: 4
version: ""
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
"#,
    )
    .expect_err("empty version should fail");

    assert!(
        error.to_string().contains("/version"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_invalid_v4_descriptor_sha256() {
    for sha256 in [
        "abc",
        "000000000000000000000000000000000000000000000000000000000000000G",
    ] {
        let raw = format!(
            r"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    sha256: {sha256}
"
        );
        let error = parse_source_manifest_yaml(&raw).expect_err("invalid sha256 should fail");
        assert!(
            error.to_string().contains("sha256"),
            "unexpected error for {sha256}: {error}"
        );
    }
}

#[test]
fn rejects_v4_mcp_descriptor_sha256() {
    let error = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
surfaces:
  - id: mcp
    type: mcp
    sha256: 0000000000000000000000000000000000000000000000000000000000000000
    server:
      transport: stdio
      command: demo-mcp-server
",
    )
    .expect_err("mcp sha256 should fail");

    assert!(
        error.to_string().contains("sha256"),
        "unexpected error: {error}"
    );
}

#[test]
fn parses_v4_openapi_surface_without_base_url() {
    let manifest = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
",
    )
    .expect("v4 manifest");
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
    let manifest = parse_source_manifest_yaml(
        r"
name: demo_source
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
",
    )
    .expect("v4 manifest");
    let v4 = manifest.as_v4().expect("v4");

    assert_eq!(
        v4.surfaces.first().expect("surface").relation_namespace,
        "demo_source"
    );
}

#[test]
fn reserved_default_relation_namespace_is_rejected() {
    let error = parse_source_manifest_yaml(
        r"
name: public
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
",
    )
    .expect_err("reserved relation namespace should fail");

    assert!(
        error
            .to_string()
            .contains("source surface relation namespace 'public' is reserved"),
        "unexpected error: {error}"
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
fn multi_surface_namespace_suffixes_are_source_relative() {
    let manifest = parse_source_manifest_yaml(
        r"
name: github_v4
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
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
    let manifest = parse_source_manifest_yaml(
        r"
name: github_v4
dsl_version: 4
surfaces:
  - id: rest
    namespace_suffix: api
    type: openapi
    file: /tmp/openapi.yaml
",
    )
    .expect("v4 manifest");
    let v4 = manifest.as_v4().expect("v4");

    assert_eq!(
        v4.surfaces.first().expect("surface").relation_namespace,
        "github_v4_api"
    );
}

#[test]
fn unrelated_namespace_suffix_cannot_impersonate_another_source_relation_namespace() {
    let manifest = parse_source_manifest_yaml(
        r"
name: linear
dsl_version: 4
surfaces:
  - id: rest
    namespace_suffix: github_rest
    type: openapi
    file: /tmp/openapi.yaml
",
    )
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
fn rejects_v4_oauth_endpoint_templates_referencing_runtime_tokens() {
    let error = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    inputs:
      ACCESS_TOKEN:
        kind: secret
        credential:
          methods:
            - type: oauth
              oauth:
                flow:
                  type: authorization_code
                  pkce: required
                redirect_uri: http://127.0.0.1:53682/oauth/callback
                endpoints:
                  authorization_url: https://provider.example.com/oauth/authorize
                  token_url: https://provider.example.com/{{filter.tenant}}/oauth/token
                client:
                  id:
                    default: demo-client
",
    )
    .expect_err("runtime token in oauth endpoint should fail");

    assert!(
        error
            .to_string()
            .contains("oauth.endpoints.token_url uses unsupported template token 'filter.tenant'")
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
        let raw = format!(
            r#"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    base_url: "https://{{{{{token}}}}}"
"#
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
    let error = parse_source_manifest_yaml(
        r#"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    inputs:
      API_BASE:
        kind: variable
        default: https://api.example.com
    base_url: "{{input.API_BASE|https://fallback.example.com}}"
"#,
    )
    .expect_err("base_url token default should be rejected");

    let message = error.to_string();
    assert!(
        message.contains("must declare defaults under top-level inputs")
            || message.contains("must declare defaults under source inputs"),
        "unexpected error: {message}"
    );
}

#[test]
fn rejects_v4_oauth_endpoint_templates_referencing_undeclared_surface_inputs() {
    let error = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    inputs:
      ACCESS_TOKEN:
        kind: secret
        credential:
          methods:
            - type: oauth
              oauth:
                flow:
                  type: authorization_code
                  pkce: required
                redirect_uri: http://127.0.0.1:53682/oauth/callback
                endpoints:
                  authorization_url: https://provider.example.com/oauth/authorize
                  token_url: https://provider.example.com/{{input.TENANT_ID}}/oauth/token
                client:
                  id:
                    default: demo-client
",
    )
    .expect_err("undeclared endpoint input should fail");

    assert!(error.to_string().contains(
        "manifest input 'TENANT_ID' is referenced but not declared under surface inputs"
    ));
}

#[test]
fn parses_v4_oauth_endpoint_templates_referencing_surface_variables() {
    let manifest = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    inputs:
      TENANT_ID:
        kind: variable
        default: organizations
      ACCESS_TOKEN:
        kind: secret
        credential:
          methods:
            - type: oauth
              oauth:
                flow:
                  type: authorization_code
                  pkce: required
                redirect_uri: http://127.0.0.1:53682/oauth/callback
                endpoints:
                  authorization_url: https://login.example.com/{{input.TENANT_ID}}/oauth/authorize
                  token_url: https://login.example.com/{{input.TENANT_ID}}/oauth/token
                client:
                  id:
                    default: demo-client
",
    )
    .expect("v4 manifest");

    assert_eq!(manifest.declared_inputs().len(), 2);
}

#[test]
fn parses_v4_surface_input_oauth_credential_metadata() {
    let manifest = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    inputs:
      TENANT_ID:
        kind: variable
        default: organizations
      ACCESS_TOKEN:
        kind: secret
        credential:
          methods:
            - type: oauth
              label: Connect with Demo
              description: Use OAuth.
              hint: Authorize in your browser.
              oauth:
                flow:
                  type: authorization_code
                  pkce: required
                resource: https://api.example.com/
                redirect_uri: http://127.0.0.1:0/oauth/callback
                redirect_uri_port_mode: random
                endpoints:
                  authorization_url: https://login.example.com/{{input.TENANT_ID}}/oauth/authorize
                  token_url: https://login.example.com/{{input.TENANT_ID}}/oauth/token
                client:
                  dynamic_registration:
                    registration_url: https://login.example.com/{{input.TENANT_ID}}/oauth/register
                    client_name: Coral Demo
                    token_endpoint_auth_method: client_secret_post
                    request_refresh_token_grant: true
                scopes:
                  scope:
                    delimiter: comma
                    values:
                      - read
                      - offline_access
            - type: source_config
              label: Paste token
",
    )
    .expect("v4 manifest");

    let access_token = manifest
        .declared_inputs()
        .iter()
        .find(|input| input.key == "ACCESS_TOKEN")
        .expect("ACCESS_TOKEN input");
    let credential = access_token.credential.as_ref().expect("credential");
    let [oauth_method, source_config_method] = credential.methods.as_slice() else {
        panic!(
            "expected two credential methods, got {:?}",
            credential.methods
        );
    };
    assert_eq!(oauth_method.kind, ManifestCredentialMethodKind::OAuth);
    assert_eq!(oauth_method.label.as_deref(), Some("Connect with Demo"));
    assert_eq!(oauth_method.description.as_deref(), Some("Use OAuth."));
    assert_eq!(
        oauth_method.hint.as_deref(),
        Some("Authorize in your browser.")
    );
    assert_eq!(
        source_config_method.kind,
        ManifestCredentialMethodKind::SourceConfig
    );

    let oauth = oauth_method.oauth.as_ref().expect("oauth");
    assert_eq!(oauth.flow.kind, ManifestOAuthFlowKind::AuthorizationCode);
    assert_eq!(oauth.flow.pkce, ManifestOAuthPkceMode::Required);
    assert_eq!(
        oauth.redirect_uri_port_mode,
        ManifestOAuthRedirectUriPortMode::Random
    );
    assert_eq!(oauth.resource.as_deref(), Some("https://api.example.com/"));
    assert_eq!(
        oauth.authorization_url.as_deref(),
        Some("https://login.example.com/{{input.TENANT_ID}}/oauth/authorize")
    );
    assert_eq!(
        oauth.token_url,
        "https://login.example.com/{{input.TENANT_ID}}/oauth/token"
    );
    let registration = oauth
        .client
        .dynamic_registration
        .as_ref()
        .expect("dynamic registration");
    assert_eq!(
        registration.token_endpoint_auth_method,
        ManifestOAuthDynamicClientRegistrationAuthMethod::ClientSecretPost
    );
    assert!(registration.request_refresh_token_grant);
    assert_eq!(
        oauth.scopes.as_ref().expect("scopes").scope.delimiter,
        ManifestOAuthScopeDelimiter::Comma
    );
}

#[test]
fn rejects_v4_surfaces_with_incompatible_oauth_input_metadata() {
    let error = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    namespace_suffix: rest
    type: openapi
    file: /tmp/openapi.yaml
    inputs:
      ACCESS_TOKEN:
        kind: secret
        credential:
          methods:
            - type: oauth
              oauth:
                flow:
                  type: authorization_code
                  pkce: required
                redirect_uri: http://127.0.0.1:0/oauth/callback
                endpoints:
                  authorization_url: https://login.example.com/oauth/authorize
                  token_url: https://login.example.com/oauth/token
                client:
                  id:
                    default: rest-client
  - id: mcp
    namespace_suffix: mcp
    type: mcp
    inputs:
      ACCESS_TOKEN:
        kind: secret
        credential:
          methods:
            - type: oauth
              oauth:
                flow:
                  type: device_code
                endpoints:
                  device_authorization_url: https://login.example.com/oauth/device
                  token_url: https://login.example.com/oauth/token
                client:
                  id:
                    default: mcp-client
    server:
      transport: stdio
      command: demo-mcp-server
",
    )
    .expect_err("incompatible duplicate input metadata should fail");

    assert!(
        error
            .to_string()
            .contains("declare incompatible input 'ACCESS_TOKEN'"),
        "unexpected error: {error}"
    );
}

#[test]
fn parses_and_normalizes_v4_identity_requirements() {
    let manifest = parse_source_manifest_yaml(
        r#"
name: github
dsl_version: 4
identity_requirements:
  accepts:
    - id: " github_rest_read "
      identity_specs: [" github_oauth ", github_pat]
      audience: {host: github.com, port: 443}
surfaces:
  - id: rest
    type: openapi
    file: /tmp/github-openapi.yaml
"#,
    )
    .expect("v4 manifest");
    let accepted = manifest
        .as_v4()
        .and_then(|v4| v4.identity_requirements.as_ref())
        .and_then(|requirements| requirements.accepts.first())
        .expect("accepted identity requirement");

    assert_eq!(accepted.id, "github_rest_read");
    assert_eq!(accepted.identity_specs, ["github_oauth", "github_pat"]);
    assert_eq!(
        accepted.audience.get("host"),
        Some(&serde_json::json!("github.com"))
    );
    assert_eq!(accepted.audience.get("port"), Some(&serde_json::json!(443)));
}

#[test]
fn rejects_invalid_v4_identity_requirement_entries() {
    let invalid = [
        (
            "  accepts:
    - {id: github_rest_read, identity_specs: [github_oauth]}
    - {id: \" github_rest_read \", identity_specs: [github_pat]}
",
            "duplicate identity requirement id 'github_rest_read'",
        ),
        (
            "  accepts:
    - {id: github_rest_read, identity_specs: [github_oauth, \" github_oauth \"]}
",
            "duplicate identity spec id 'github_oauth'",
        ),
    ];

    for (requirements, expected) in invalid {
        let raw = format!(
            "name: github\ndsl_version: 4\nidentity_requirements:\n{requirements}surfaces:\n  - id: rest\n    type: openapi\n    file: /tmp/github-openapi.yaml\n"
        );
        let error = parse_source_manifest_yaml(&raw).expect_err("duplicates should fail");
        assert!(
            error.to_string().contains(expected),
            "unexpected error: {error}"
        );
    }
}

#[test]
fn rejects_secret_inputs_anywhere_in_identity_gated_sources() {
    let error = parse_source_manifest_yaml(
        r"
name: github
dsl_version: 4
identity_requirements:
  accepts:
    - {id: github_rest_read, identity_specs: [github_oauth]}
surfaces:
  - id: rest
    type: openapi
    file: /tmp/github-openapi.yaml
    inputs:
      TOKEN: {kind: secret}
",
    )
    .expect_err("gated surface should reject legacy secrets");

    assert!(
        error.to_string().contains(
            "input 'TOKEN' must not use kind: secret in DSL v4; use identity_requirements and identity specs for credentials"
        ),
        "unexpected error: {error}"
    );

    for (fixture, secret_input) in [("github", "GITHUB_TOKEN"), ("stripe", "STRIPE_API_KEY")] {
        let raw = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../sources/v4")
                .join(fixture)
                .join("manifest.yaml"),
        )
        .expect("core v4 fixture");
        let manifest = parse_source_manifest_yaml(&raw).expect("ungated fixture remains valid");
        assert!(
            manifest
                .declared_inputs()
                .iter()
                .any(|input| input.key == secret_input),
            "{fixture} should retain {secret_input}"
        );
    }

    let error = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
identity_requirements:
  accepts:
    - {id: demo_read, identity_specs: [demo_oauth]}
surfaces:
  - id: legacy
    namespace_suffix: legacy
    type: openapi
    file: /tmp/legacy-openapi.yaml
    inputs:
      TOKEN: {kind: secret}
  - id: gated
    namespace_suffix: gated
    type: openapi
    file: /tmp/gated-openapi.yaml
",
    )
    .expect_err("a source-level identity requirement must reject secrets on every surface");

    assert!(
        error.to_string().contains(
            "source 'demo' input 'TOKEN' must not use kind: secret in DSL v4; use identity_requirements and identity specs for credentials"
        ),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_identity_requirements_on_sources_with_mcp_surfaces() {
    let error = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
identity_requirements:
  accepts:
    - {id: demo, identity_specs: [demo_oauth]}
surfaces:
  - id: rest
    type: openapi
    file: /tmp/demo-openapi.yaml
  - id: mcp
    namespace_suffix: mcp
    type: mcp
    server:
      transport: stdio
      command: demo-mcp-server
",
    )
    .expect_err("identity requirements are unsupported when any surface is MCP");

    assert!(
        error
            .to_string()
            .contains("identity_requirements are only supported when every surface is OpenAPI"),
        "unexpected error: {error}"
    );
}
