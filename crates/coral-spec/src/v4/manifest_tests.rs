use crate::{
    ManifestCredentialMethodKind, ManifestOAuthDynamicClientRegistrationAuthMethod,
    ManifestOAuthFlowKind, ManifestOAuthPkceMode, ManifestOAuthRedirectUriPortMode,
    ManifestOAuthScopeDelimiter, parse_source_manifest_yaml,
};

use super::manifest::V4UniversalSearchInputLocation;

fn v4_manifest_with_routes(routes: &str) -> String {
    format!(
        r"
name: demo
dsl_version: 4
universal_search:
  routes:
{routes}
surface:
  type: openapi
  file: /tmp/openapi.yaml
"
    )
}

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
fn parses_v4_universal_search_allow_deny_and_result_pointers() {
    let raw = v4_manifest_with_routes(
        r"    issue_search:
      execute: true
      target:
        operation_id: search_issues
      query_input:
        location: query
        name: q
      result:
        entity_type: issue
        identity_fields: [/node_id, /repository/owner~1name]
        provider_id: /id
        title: /title
        url: /html_url
        snippet: /body~0text
        attributes: [/state, /author/login]
    issue_search_disabled:
      execute: false
      target:
        operation_id: search_issues_legacy
",
    );

    let manifest = parse_source_manifest_yaml(&raw).expect("v4 Universal Search policy");
    let policy = manifest
        .as_v4()
        .expect("v4 manifest")
        .universal_search
        .as_ref()
        .expect("Universal Search policy");
    let allowed = policy.routes.get("issue_search").expect("allowed route");
    assert!(allowed.execute);
    assert_eq!(allowed.target.operation_id, "search_issues");
    let query_input = allowed.query_input.as_ref().expect("query input");
    assert_eq!(query_input.location, V4UniversalSearchInputLocation::Query);
    assert_eq!(query_input.name, "q");
    let result = allowed.result.as_ref().expect("result mapping");
    assert_eq!(result.entity_type.as_deref(), Some("issue"));
    assert_eq!(
        result.identity_fields,
        ["/node_id", "/repository/owner~1name"]
    );
    assert_eq!(result.snippet.as_deref(), Some("/body~0text"));

    let denied = policy
        .routes
        .get("issue_search_disabled")
        .expect("denied route");
    assert!(!denied.execute);
    assert!(denied.query_input.is_none());
}

#[test]
fn parses_v4_universal_search_path_and_tool_arg_locations() {
    let raw = v4_manifest_with_routes(
        r"    rest_search:
      execute: true
      target:
        operation_id: find_by_path
      query_input:
        location: path
        name: query
    mcp_search:
      execute: true
      target:
        operation_id: search_tool
      query_input:
        location: tool_arg
        name: query
",
    );

    let manifest = parse_source_manifest_yaml(&raw).expect("valid input locations");
    let routes = &manifest
        .as_v4()
        .expect("v4 manifest")
        .universal_search
        .as_ref()
        .expect("Universal Search policy")
        .routes;
    assert_eq!(
        routes
            .get("rest_search")
            .and_then(|route| route.query_input.as_ref())
            .map(|input| input.location),
        Some(V4UniversalSearchInputLocation::Path)
    );
    assert_eq!(
        routes
            .get("mcp_search")
            .and_then(|route| route.query_input.as_ref())
            .map(|input| input.location),
        Some(V4UniversalSearchInputLocation::ToolArg)
    );
}

#[test]
fn rejects_invalid_v4_universal_search_route_ids() {
    for route_id in ["IssueSearch", "9search", "issue-search"] {
        let raw = v4_manifest_with_routes(&format!(
            r"    {route_id}:
      execute: false
      target:
        operation_id: search_issues
"
        ));
        let error = parse_source_manifest_yaml(&raw).expect_err("invalid route id should fail");
        assert!(
            error.to_string().contains(route_id),
            "unexpected error for {route_id}: {error}"
        );
    }
}

#[test]
fn rejects_duplicate_v4_universal_search_operation_targets() {
    let raw = v4_manifest_with_routes(
        r"    issue_search:
      execute: true
      target:
        operation_id: search_issues
      query_input:
        location: query
        name: q
    issue_search_disabled:
      execute: false
      target:
        operation_id: search_issues
",
    );

    let error = parse_source_manifest_yaml(&raw)
        .expect_err("distinct route ids must not target the same operation");
    let message = error.to_string();
    assert!(
        message.contains("operation_id 'search_issues'")
            && message.contains("declared by more than one route"),
        "unexpected duplicate-target error: {message}"
    );
}

#[test]
fn enforces_v4_universal_search_allow_deny_query_input_shape() {
    let allow_without_query = v4_manifest_with_routes(
        r"    issue_search:
      execute: true
      target:
        operation_id: search_issues
",
    );
    let error = parse_source_manifest_yaml(&allow_without_query)
        .expect_err("an executable route needs query_input");
    assert!(error.to_string().contains("query_input"));

    let deny_with_query = v4_manifest_with_routes(
        r"    issue_search:
      execute: false
      target:
        operation_id: search_issues
      query_input:
        location: query
        name: q
",
    );
    let error = parse_source_manifest_yaml(&deny_with_query)
        .expect_err("a denied route must not declare query_input");
    assert!(error.to_string().contains("query_input"));
}

#[test]
fn rejects_empty_v4_universal_search_target_and_input_fields() {
    for (field, operation_id, query_name) in
        [("operation_id", "", "q"), ("name", "search_issues", "   ")]
    {
        let raw = v4_manifest_with_routes(&format!(
            r#"    issue_search:
      execute: true
      target:
        operation_id: "{operation_id}"
      query_input:
        location: query
        name: "{query_name}"
"#
        ));
        let error = parse_source_manifest_yaml(&raw).expect_err("empty field should fail");
        assert!(
            error.to_string().contains(field),
            "unexpected error for {field}: {error}"
        );
    }
}

#[test]
fn rejects_unsupported_v4_universal_search_input_locations() {
    for location in ["header", "cookie", "body"] {
        let raw = v4_manifest_with_routes(&format!(
            r"    issue_search:
      execute: true
      target:
        operation_id: search_issues
      query_input:
        location: {location}
        name: q
"
        ));
        let error =
            parse_source_manifest_yaml(&raw).expect_err("unsupported input location should fail");
        assert!(
            error.to_string().contains(location),
            "unexpected error for {location}: {error}"
        );
    }
}

#[test]
fn rejects_invalid_v4_universal_search_result_pointers() {
    for result_field in [
        "identity_fields: [node_id]",
        "provider_id: ''",
        "provider_id: /provider~2id",
        "title: /title~",
        "url: '#/url'",
        "snippet: body",
        "attributes: [/state, /author~name]",
    ] {
        let raw = v4_manifest_with_routes(&format!(
            r"    issue_search:
      execute: true
      target:
        operation_id: search_issues
      query_input:
        location: query
        name: q
      result:
        entity_type: issue
        {result_field}
"
        ));
        let error = parse_source_manifest_yaml(&raw).expect_err("invalid pointer should fail");
        assert!(
            error.to_string().contains("RFC 6901") || error.to_string().contains("does not match"),
            "unexpected error for {result_field}: {error}"
        );
    }
}

#[test]
fn rejects_duplicate_v4_universal_search_result_pointers() {
    let raw = v4_manifest_with_routes(
        r"    issue_search:
      execute: true
      target:
        operation_id: search_issues
      query_input:
        location: query
        name: q
      result:
        identity_fields: [/node_id, /node_id]
",
    );
    let error = parse_source_manifest_yaml(&raw).expect_err("duplicate pointers should fail");

    let message = error.to_string();
    assert!(
        message.contains("duplicate pointer") || message.contains("unique"),
        "unexpected duplicate-pointer error: {message}"
    );
}

#[test]
fn rejects_unknown_v4_universal_search_fields() {
    let raw = v4_manifest_with_routes(
        r"    issue_search:
      execute: true
      target:
        operation_id: search_issues
        fallback_operation_id: find_issues
      query_input:
        location: query
        name: q
",
    );

    let error = parse_source_manifest_yaml(&raw).expect_err("unknown route field should fail");
    assert!(error.to_string().contains("fallback_operation_id"));
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
        let message = error.to_string();
        assert!(
            message.contains("/name") && message.contains("^[a-z][a-z0-9_]*$"),
            "unexpected error for {name}: {message}"
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

    let message = error.to_string();
    assert!(
        message.contains("/: Additional properties are not allowed ('surfaces' was unexpected)"),
        "unexpected error: {message}"
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

    let message = error.to_string();
    assert!(
        message.contains("/surface:")
            && message.contains(r#"{"id":"rest","type":"openapi","file":"/tmp/openapi.yaml"}"#)
            && message.contains("not valid under any of the schemas listed in the 'oneOf' keyword"),
        "unexpected error: {message}"
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

    let message = error.to_string();
    assert!(
        message.contains("/surface:")
            && message.contains(
                r#"{"namespace_suffix":"api","type":"openapi","file":"/tmp/openapi.yaml"}"#
            )
            && message.contains("not valid under any of the schemas listed in the 'oneOf' keyword"),
        "unexpected error: {message}"
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

    let message = error.to_string();
    assert!(
        message.contains("/surface:")
            && message.contains(
                r#"{"type":"openapi","file":"/tmp/openapi.yaml","inputs":{"GITHUB_TOKEN":{"kind":"secret"}}}"#
            )
            && message.contains("not valid under any of the schemas listed in the 'oneOf' keyword"),
        "unexpected error: {message}"
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
surface:
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
            "  accepts:\n    - {id: github_rest_read, identity_specs: [github_oauth]}\n    - {id: \" github_rest_read \", identity_specs: [github_pat]}\n",
            "duplicate identity requirement id 'github_rest_read'",
        ),
        (
            "  accepts:\n    - {id: github_rest_read, identity_specs: [github_oauth, \" github_oauth \"]}\n",
            "duplicate identity spec id 'github_oauth'",
        ),
    ];

    for (requirements, expected) in invalid {
        let raw = format!(
            "name: github\ndsl_version: 4\nidentity_requirements:\n{requirements}surface:\n  type: openapi\n  file: /tmp/github-openapi.yaml\n"
        );
        let error = parse_source_manifest_yaml(&raw).expect_err("duplicates should fail");
        assert!(
            error.to_string().contains(expected),
            "unexpected error: {error}"
        );
    }
}

#[test]
fn rejects_secret_inputs_only_on_identity_gated_sources() {
    let error = parse_source_manifest_yaml(
        r"
name: github
dsl_version: 4
inputs:
  TOKEN: {kind: secret}
identity_requirements:
  accepts:
    - {id: github_rest_read, identity_specs: [github_oauth]}
surface:
  type: openapi
  file: /tmp/github-openapi.yaml
",
    )
    .expect_err("gated source should reject legacy secrets");

    assert!(
        error.to_string().contains(
            "input 'TOKEN' must not use kind: secret in DSL v4; use identity_requirements and identity specs for credentials"
        ),
        "unexpected error: {error}"
    );

    parse_source_manifest_yaml(
        r"
name: github
dsl_version: 4
inputs:
  TOKEN: {kind: secret}
surface:
  type: openapi
  file: /tmp/github-openapi.yaml
",
    )
    .expect("ungated source retains legacy secret inputs");
}

#[test]
fn rejects_identity_requirements_on_mcp_sources() {
    let error = parse_source_manifest_yaml(
        r"
name: demo
dsl_version: 4
identity_requirements:
  accepts:
    - {id: demo, identity_specs: [demo_oauth]}
surface:
  type: mcp
  server:
    transport: stdio
    command: demo-mcp-server
",
    )
    .expect_err("identity requirements are OpenAPI-only");

    assert!(
        error
            .to_string()
            .contains("identity_requirements are only supported for OpenAPI sources"),
        "unexpected error: {error}"
    );
}
