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
