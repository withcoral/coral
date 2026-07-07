use crate::backends::database::{DatabaseConnectionSpec, DatabaseProvider};
use crate::parse_source_manifest_yaml;
use crate::v4::{SurfaceDescriptor, SurfaceRuntimeConfig, SurfaceType};

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
fn parses_v4_database_surface_with_provider_specific_connection() {
    let manifest = parse_source_manifest_yaml(
        r#"
name: pickl_db
dsl_version: 4
surfaces:
  - id: db
    type: database
    provider: postgres
    inputs:
      DB_HOST:
        kind: variable
        default: localhost
      DB_PORT:
        kind: variable
        default: "5432"
      DB_USER:
        kind: variable
        default: pickl_reader
      DB_PASSWORD:
        kind: secret
    connection:
      host: "{{input.DB_HOST}}"
      port: "{{input.DB_PORT}}"
      database: pickl
      user: "{{input.DB_USER}}"
      password: "{{input.DB_PASSWORD}}"
      sslmode: require
"#,
    )
    .expect("v4 manifest");

    let v4 = manifest.as_v4().expect("v4");
    let surface = v4.surfaces.first().expect("surface");
    assert_eq!(surface.surface_type, SurfaceType::Database);
    assert_eq!(surface.relation_namespace, "pickl_db");
    assert!(matches!(
        &surface.descriptor,
        SurfaceDescriptor::Database {
            provider: DatabaseProvider::Postgres,
        }
    ));

    let SurfaceRuntimeConfig::Database(runtime) = &surface.runtime else {
        panic!("database runtime");
    };
    assert_eq!(runtime.provider, DatabaseProvider::Postgres);
    let DatabaseConnectionSpec::Postgres(connection) = &runtime.connection else {
        panic!("postgres connection");
    };
    assert_eq!(connection.host.raw(), "{{input.DB_HOST}}");
    assert_eq!(connection.port.raw(), "{{input.DB_PORT}}");
    assert_eq!(connection.database.raw(), "pickl");
    assert_eq!(connection.user.raw(), "{{input.DB_USER}}");
    assert_eq!(connection.password.raw(), "{{input.DB_PASSWORD}}");
    assert_eq!(
        connection.sslmode.as_ref().expect("sslmode template").raw(),
        "require"
    );
}

#[test]
fn rejects_v4_database_connection_templates_referencing_runtime_tokens() {
    let error = parse_source_manifest_yaml(
        r#"
name: pickl_db
dsl_version: 4
surfaces:
  - id: db
    type: database
    provider: postgres
    connection:
      host: localhost
      port: "5432"
      database: "{{filter.tenant}}"
      user: pickl_reader
      password: "{{input.DB_PASSWORD}}"
    inputs:
      DB_PASSWORD:
        kind: secret
"#,
    )
    .expect_err("runtime token should fail");

    assert!(
        error.to_string().contains(
            "connection.database may only reference source inputs; unsupported template token '{{filter.tenant}}'"
        ),
        "unexpected error: {error}"
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
