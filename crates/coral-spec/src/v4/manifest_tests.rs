use crate::{
    ManifestCredentialMethodKind, ManifestOAuthDynamicClientRegistrationAuthMethod,
    ManifestOAuthFlowKind, ManifestOAuthPkceMode, ManifestOAuthRedirectUriPortMode,
    ManifestOAuthScopeDelimiter, parse_source_manifest_yaml,
};

#[test]
fn parses_v4_manifest_top_level_inputs() {
    let manifest = parse_source_manifest_yaml(
        r#"
name: demo
dsl_version: 4
inputs:
  ZZZ_TOKEN:
    kind: secret
  AAA_BASE:
    kind: variable
    default: https://api.example.com
surface:
    type: openapi
    file: /tmp/openapi.yaml
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
surface:
    type: openapi
    file: /tmp/openapi.yaml
",
    )
    .expect("v4 manifest");
    let v4 = manifest.as_v4().expect("v4");
    assert_eq!(
        v4.surface
            .openapi_runtime()
            .expect("OpenAPI runtime")
            .base_url
            .raw(),
        ""
    );
}

#[test]
fn reserved_source_namespace_is_rejected() {
    let error = parse_source_manifest_yaml(
        r"
name: public
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
",
    )
    .expect_err("reserved relation namespace should fail");

    assert!(
        error
            .to_string()
            .contains("source name 'public' is reserved"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_invalid_v4_source_names() {
    for name in ["Demo", "demo-api", "4demo"] {
        let raw = format!(
            r"
name: {name}
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
"
        );
        let error = parse_source_manifest_yaml(&raw).expect_err("invalid v4 name should fail");
        assert!(
            error.to_string().contains("^[a-z][a-z0-9_]*$")
                || error.to_string().contains("must match [a-z][a-z0-9_]*"),
            "unexpected error for {name}: {error}"
        );
    }
}

#[test]
fn rejects_plural_surfaces_field() {
    let error = parse_source_manifest_yaml(
        r"
name: github_v4
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
",
    )
    .expect_err("plural surfaces should be rejected");

    assert!(
        error.to_string().contains("surfaces"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_surface_id() {
    let error = parse_source_manifest_yaml(
        r"
name: github_v4
dsl_version: 4
surface:
    id: rest
    type: openapi
    file: /tmp/openapi.yaml
",
    )
    .expect_err("surface id should be rejected");

    assert!(
        error.to_string().contains("id"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_surface_namespace_suffix() {
    let error = parse_source_manifest_yaml(
        r"
name: github_v4
dsl_version: 4
surface:
    namespace_suffix: api
    type: openapi
    file: /tmp/openapi.yaml
",
    )
    .expect_err("namespace suffix should be rejected");

    assert!(
        error.to_string().contains("namespace_suffix"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_surface_inputs() {
    let error = parse_source_manifest_yaml(
        r"
name: github_v4
dsl_version: 4
surface:
    type: openapi
    file: /tmp/openapi.yaml
    inputs:
      GITHUB_TOKEN:
        kind: secret
",
    )
    .expect_err("surface inputs should be rejected");

    assert!(
        error.to_string().contains("inputs"),
        "unexpected error: {error}"
    );
}

#[test]
fn rejects_v4_oauth_endpoint_templates_referencing_runtime_tokens() {
    let error = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
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
surface:
    type: openapi
    file: /tmp/openapi.yaml
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
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: "https://{{{{{token}}}}}"
"#
        );
        let error = parse_source_manifest_yaml(&raw).expect_err("runtime token should be rejected");
        let message = error.to_string();
        assert!(
            message.contains("base_url may only reference top-level inputs"),
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
inputs:
  API_BASE:
    kind: variable
    default: https://api.example.com
surface:
    type: openapi
    file: /tmp/openapi.yaml
    base_url: "{{input.API_BASE|https://fallback.example.com}}"
"#,
    )
    .expect_err("base_url token default should be rejected");

    let message = error.to_string();
    assert!(
        message.contains("must declare defaults under top-level inputs"),
        "unexpected error: {message}"
    );
}

#[test]
fn rejects_v4_oauth_endpoint_templates_referencing_undeclared_top_level_inputs() {
    let error = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
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
surface:
    type: openapi
    file: /tmp/openapi.yaml
",
    )
    .expect_err("undeclared endpoint input should fail");

    assert!(error.to_string().contains(
        "manifest input 'TENANT_ID' is referenced but not declared under top-level inputs"
    ));
}

#[test]
fn parses_v4_oauth_endpoint_templates_referencing_top_level_variables() {
    let manifest = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
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
surface:
    type: openapi
    file: /tmp/openapi.yaml
",
    )
    .expect("v4 manifest");

    assert_eq!(manifest.declared_inputs().len(), 2);
}

#[test]
fn parses_v4_top_level_input_oauth_credential_metadata() {
    let manifest = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
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
surface:
    type: openapi
    file: /tmp/openapi.yaml
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
