use crate::parse_source_manifest_yaml;

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
            .openapi_runtime
            .base_url
            .raw(),
        ""
    );
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
