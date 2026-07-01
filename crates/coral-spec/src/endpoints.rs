//! Outbound-host extraction for source specs.
//!
//! Source setup surfaces every network host a source will contact so a user
//! can review and confirm them before setup proceeds. Hosts are derived
//! statically from the manifest — the base URL, OAuth provider endpoints,
//! remote MCP server URLs, and S3 service endpoints. Local filesystem paths and
//! object-store locations are not themselves network hosts.

use std::collections::{BTreeMap, BTreeSet};

use url::Url;

use crate::{
    ManifestError, ManifestInputSpec, McpServerSpec, ParsedTemplate, Result, TemplateNamespace,
    TemplatePart, ValidatedSourceManifest,
    backends::file::{
        FileObjectStoreSpec, s3_endpoint_dns_suffix_for_region, validate_s3_region_name,
    },
    v4::{SurfaceDescriptor, openapi_document_metadata},
};

const UNRESOLVED_OPENAPI_SERVER_HOST: &str =
    "OpenAPI servers[0].url runtime host; declare base_url to review before import";
const UNRESOLVED_S3_REGION_HOST_PREFIX: &str = "S3 service endpoint for unresolved region ";

/// Concrete and unresolved outbound-host review data for source setup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundHostReview {
    /// Concrete network hosts, with ports when non-default ports are authored.
    pub hosts: Vec<String>,
    /// Host values that could not be resolved before setup.
    pub unresolved_hosts: Vec<String>,
}

impl ValidatedSourceManifest {
    /// Returns every network host this source will contact, de-duplicated and
    /// sorted.
    ///
    /// Hosts are collected from the HTTP base URL, OAuth provider
    /// authorization/token endpoints, remote MCP server URLs, and S3 service
    /// endpoints. This compatibility helper returns only concrete hosts; use
    /// [`ValidatedSourceManifest::outbound_host_review`] when callers also need
    /// to surface unresolved input-driven endpoints.
    pub fn outbound_hosts(&self) -> Result<Vec<String>> {
        Ok(self.outbound_host_review()?.hosts)
    }

    /// Returns every network host this source will contact after substituting
    /// resolved non-secret source input values, de-duplicated and sorted.
    ///
    /// Input values override manifest defaults. This compatibility helper
    /// returns only concrete hosts; use
    /// [`ValidatedSourceManifest::outbound_host_review_with_input_values`] when
    /// callers also need to surface unresolved input-driven endpoints.
    pub fn outbound_hosts_with_input_values(
        &self,
        source_inputs: &BTreeMap<String, String>,
    ) -> Result<Vec<String>> {
        Ok(self
            .outbound_host_review_with_input_values(source_inputs)?
            .hosts)
    }

    /// Returns concrete and unresolved outbound-host review data.
    pub fn outbound_host_review(&self) -> Result<OutboundHostReview> {
        self.outbound_host_review_with_input_values(&BTreeMap::new())
    }

    /// Returns concrete and unresolved outbound-host review data after
    /// substituting resolved non-secret source input values.
    pub fn outbound_host_review_with_input_values(
        &self,
        source_inputs: &BTreeMap<String, String>,
    ) -> Result<OutboundHostReview> {
        let mut hosts = BTreeSet::new();
        let mut unresolved_hosts = BTreeSet::new();
        let inputs = self.declared_inputs();

        if let Some(http) = self.as_http() {
            collect_host(
                &mut hosts,
                &mut unresolved_hosts,
                &render_with_input_values(&http.base_url, inputs, source_inputs),
            );
        }

        // OAuth endpoints are declared on secret inputs regardless of backend.
        for input in inputs {
            let Some(credential) = input.credential.as_ref() else {
                continue;
            };
            for method in &credential.methods {
                let Some(oauth) = method.oauth.as_ref() else {
                    continue;
                };
                let endpoint_urls = [
                    oauth.authorization_url.as_deref(),
                    oauth.device_authorization_url.as_deref(),
                    Some(oauth.token_url.as_str()),
                ];
                for url in endpoint_urls.into_iter().flatten() {
                    collect_host(
                        &mut hosts,
                        &mut unresolved_hosts,
                        &render_string_template_with_input_values(url, inputs, source_inputs),
                    );
                }
            }
        }

        if let Some(file) = self.as_file() {
            for table in &file.tables {
                let table_context = format!("{}.{}", self.schema_name(), table.name());
                collect_file_source_hosts(
                    &table_context,
                    &mut hosts,
                    &mut unresolved_hosts,
                    &render_with_input_values(&table.source.location, inputs, source_inputs),
                    table.source.object_store.as_ref(),
                    inputs,
                    source_inputs,
                )?;
            }
        }

        if let Some(mcp) = self.as_mcp() {
            collect_mcp_server_host(
                &mut hosts,
                &mut unresolved_hosts,
                &mcp.server,
                inputs,
                source_inputs,
            );
        }

        if let Some(v4) = self.as_v4() {
            for surface in &v4.surfaces {
                if let SurfaceDescriptor::Url { url } = &surface.descriptor {
                    collect_host(&mut hosts, &mut unresolved_hosts, url);
                }

                if let Some(runtime) = surface.openapi_runtime() {
                    if runtime.base_url.raw().trim().is_empty() {
                        collect_v4_derived_openapi_host(
                            &mut hosts,
                            &mut unresolved_hosts,
                            &surface.descriptor,
                        );
                    } else {
                        collect_host(
                            &mut hosts,
                            &mut unresolved_hosts,
                            &render_with_input_values(&runtime.base_url, inputs, source_inputs),
                        );
                    }
                }

                if let Some(runtime) = surface.mcp_runtime() {
                    collect_mcp_server_host(
                        &mut hosts,
                        &mut unresolved_hosts,
                        &runtime.server,
                        inputs,
                        source_inputs,
                    );
                }
            }
        }

        Ok(OutboundHostReview {
            hosts: hosts.into_iter().collect(),
            unresolved_hosts: unresolved_hosts.into_iter().collect(),
        })
    }
}

fn collect_v4_derived_openapi_host(
    hosts: &mut BTreeSet<String>,
    unresolved_hosts: &mut BTreeSet<String>,
    descriptor: &SurfaceDescriptor,
) {
    if let SurfaceDescriptor::File { file } = descriptor
        && let Ok(bytes) = std::fs::read(file)
        && let Ok(metadata) = openapi_document_metadata(&bytes)
        && let Some(server_url) = metadata.server_url
        && insert_parsed_url_host(hosts, &server_url)
    {
        return;
    }
    unresolved_hosts.insert(UNRESOLVED_OPENAPI_SERVER_HOST.to_string());
}

fn collect_mcp_server_host(
    hosts: &mut BTreeSet<String>,
    unresolved_hosts: &mut BTreeSet<String>,
    server: &McpServerSpec,
    inputs: &[ManifestInputSpec],
    source_inputs: &BTreeMap<String, String>,
) {
    if let McpServerSpec::StreamableHttp { url, .. } = server {
        collect_host(
            hosts,
            unresolved_hosts,
            &render_string_template_with_input_values(url, inputs, source_inputs),
        );
    }
}

/// Renders a template, substituting input tokens with their manifest default
/// value. Tokens with no usable default are left as their `{{...}}` literal so
/// the resulting string still signals an unresolved, input-driven endpoint.
fn render_with_input_values(
    template: &ParsedTemplate,
    inputs: &[ManifestInputSpec],
    source_inputs: &BTreeMap<String, String>,
) -> String {
    let mut rendered = String::new();
    for part in template.parts() {
        match part {
            TemplatePart::Literal(text) => rendered.push_str(text),
            TemplatePart::Token(token) => {
                let resolved = match token.namespace() {
                    TemplateNamespace::Input => source_inputs
                        .get(token.key())
                        .map(|value| value.trim())
                        .filter(|value| !value.is_empty())
                        .map(str::to_string)
                        .or_else(|| token.default_value().map(str::to_string))
                        .or_else(|| {
                            inputs
                                .iter()
                                .find(|input| input.key == token.key())
                                .map(|input| input.default_value.clone())
                                .filter(|value| !value.is_empty())
                        }),
                    _ => token.default_value().map(str::to_string),
                };
                if let Some(value) = resolved {
                    rendered.push_str(&value);
                } else {
                    rendered.push_str("{{");
                    rendered.push_str(token.raw());
                    rendered.push_str("}}");
                }
            }
        }
    }
    rendered
}

fn collect_file_source_hosts(
    table_context: &str,
    hosts: &mut BTreeSet<String>,
    unresolved_hosts: &mut BTreeSet<String>,
    rendered_location: &str,
    object_store: Option<&FileObjectStoreSpec>,
    inputs: &[ManifestInputSpec],
    source_inputs: &BTreeMap<String, String>,
) -> Result<()> {
    let rendered_location = rendered_location.trim();
    if rendered_location.is_empty() || has_scheme(rendered_location, "file") {
        return Ok(());
    }
    if has_scheme(rendered_location, "s3") {
        collect_s3_service_host(
            table_context,
            hosts,
            unresolved_hosts,
            object_store,
            inputs,
            source_inputs,
        )?;
        return Ok(());
    }
    // File-source validation rejects unsupported remote schemes before this
    // point. Keep a defensive host extraction path so future supported schemes
    // are still visible during setup.
    collect_host(hosts, unresolved_hosts, rendered_location);
    Ok(())
}

fn collect_s3_service_host(
    table_context: &str,
    hosts: &mut BTreeSet<String>,
    unresolved_hosts: &mut BTreeSet<String>,
    object_store: Option<&FileObjectStoreSpec>,
    inputs: &[ManifestInputSpec],
    source_inputs: &BTreeMap<String, String>,
) -> Result<()> {
    let region = match object_store {
        Some(FileObjectStoreSpec::S3 {
            region: Some(region),
            ..
        }) => render_with_input_values(region, inputs, source_inputs),
        _ => "us-east-1".to_string(),
    };
    let region = region.trim();
    if region.is_empty() {
        return Ok(());
    }
    if is_unresolved_host(region) {
        unresolved_hosts.insert(format!("{UNRESOLVED_S3_REGION_HOST_PREFIX}{region}"));
        return Ok(());
    }
    validate_s3_region_name(region).map_err(|error| {
        ManifestError::validation(format!(
            "{table_context} source.object_store.region {error}"
        ))
    })?;
    let host = format!("s3.{region}.{}", s3_endpoint_dns_suffix_for_region(region));
    if is_unresolved_host(&host) {
        unresolved_hosts.insert(host);
    } else {
        hosts.insert(host);
    }
    Ok(())
}

fn render_string_template_with_input_values(
    raw: &str,
    inputs: &[ManifestInputSpec],
    source_inputs: &BTreeMap<String, String>,
) -> String {
    ParsedTemplate::parse(raw).map_or_else(
        |_| raw.to_string(),
        |template| render_with_input_values(&template, inputs, source_inputs),
    )
}

/// Extracts a displayable host from a URL string or records the raw value as
/// unresolved when no concrete remote host can be derived.
fn collect_host(hosts: &mut BTreeSet<String>, unresolved_hosts: &mut BTreeSet<String>, raw: &str) {
    let raw = raw.trim();
    if raw.is_empty() {
        return;
    }
    match parsed_url_host(raw) {
        HostExtraction::Host(host) => {
            hosts.insert(host);
        }
        HostExtraction::NoRemoteHost => {}
        HostExtraction::Unresolved => {
            // Hostless, templated, or otherwise unparseable: surface the raw
            // string so callers can show that an input-driven endpoint exists.
            unresolved_hosts.insert(raw.to_string());
        }
    }
}

fn insert_parsed_url_host(hosts: &mut BTreeSet<String>, raw: &str) -> bool {
    match parsed_url_host(raw) {
        HostExtraction::Host(host) => {
            hosts.insert(host);
            true
        }
        HostExtraction::NoRemoteHost | HostExtraction::Unresolved => false,
    }
}

enum HostExtraction {
    Host(String),
    NoRemoteHost,
    Unresolved,
}

fn parsed_url_host(raw: &str) -> HostExtraction {
    let Ok(url) = Url::parse(raw) else {
        return HostExtraction::Unresolved;
    };
    // Both `file://...` and `file:/...` parse as local filesystem URLs with no
    // remote network host.
    if url.scheme() == "file" {
        return HostExtraction::NoRemoteHost;
    }
    let Some(host) = url.host_str() else {
        return HostExtraction::Unresolved;
    };
    let host = match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_string(),
    };
    HostExtraction::Host(host)
}

fn has_scheme(raw: &str, expected: &str) -> bool {
    let Some((scheme, _rest)) = raw.split_once("://") else {
        return false;
    };
    scheme.eq_ignore_ascii_case(expected)
}

fn is_unresolved_host(host: &str) -> bool {
    host.contains("{{") || host.contains("}}")
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::parse_source_manifest_yaml;

    use super::UNRESOLVED_OPENAPI_SERVER_HOST;

    fn hosts(manifest_yaml: &str) -> Vec<String> {
        parse_source_manifest_yaml(manifest_yaml)
            .expect("manifest should parse")
            .outbound_hosts()
            .expect("outbound hosts should resolve")
    }

    #[test]
    fn extracts_literal_base_url_host() {
        let found = hosts(
            r"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://api.example.com/v1
tables:
  - name: messages
    description: Demo messages
    request:
      method: GET
      path: /messages
    response: {}
    columns:
      - name: id
        type: Utf8
",
        );
        assert_eq!(found, vec!["api.example.com".to_string()]);
    }

    #[test]
    fn resolves_templated_base_url_against_input_default() {
        let found = hosts(
            r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  API_BASE:
    kind: variable
    default: https://api.github.com
base_url: "{{input.API_BASE}}"
tables:
  - name: messages
    description: Demo messages
    request:
      method: GET
      path: /messages
    response: {}
    columns:
      - name: id
        type: Utf8
"#,
        );
        assert_eq!(found, vec!["api.github.com".to_string()]);
    }

    #[test]
    fn resolved_input_values_override_templated_base_url_defaults() {
        let manifest = parse_source_manifest_yaml(
            r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  API_BASE:
    kind: variable
    default: https://api.github.com
base_url: "{{input.API_BASE}}"
tables:
  - name: messages
    description: Demo messages
    request:
      method: GET
      path: /messages
    response: {}
    columns:
      - name: id
        type: Utf8
"#,
        )
        .expect("manifest should parse");
        let source_inputs = BTreeMap::from([(
            "API_BASE".to_string(),
            "https://gitlab.internal/api/v4".to_string(),
        )]);

        assert_eq!(
            manifest
                .outbound_hosts_with_input_values(&source_inputs)
                .expect("outbound hosts should resolve"),
            vec!["gitlab.internal".to_string()]
        );
    }

    #[test]
    fn trims_resolved_input_values_and_falls_back_for_whitespace_only_overrides() {
        let manifest = parse_source_manifest_yaml(
            r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  API_BASE:
    kind: variable
    default: https://api.github.com
base_url: "{{input.API_BASE}}"
tables:
  - name: messages
    description: Demo messages
    request:
      method: GET
      path: /messages
    response: {}
    columns:
      - name: id
        type: Utf8
"#,
        )
        .expect("manifest should parse");
        let source_inputs = BTreeMap::from([(
            "API_BASE".to_string(),
            "  https://gitlab.internal/api/v4  ".to_string(),
        )]);
        assert_eq!(
            manifest
                .outbound_hosts_with_input_values(&source_inputs)
                .expect("outbound hosts should resolve"),
            vec!["gitlab.internal".to_string()]
        );

        let source_inputs = BTreeMap::from([("API_BASE".to_string(), "   ".to_string())]);
        assert_eq!(
            manifest
                .outbound_hosts_with_input_values(&source_inputs)
                .expect("outbound hosts should resolve"),
            vec!["api.github.com".to_string()]
        );
    }

    #[test]
    fn surfaces_unresolved_templated_base_url() {
        let manifest = parse_source_manifest_yaml(
            r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  API_BASE:
    kind: variable
base_url: "{{input.API_BASE}}"
tables:
  - name: messages
    description: Demo messages
    request:
      method: GET
      path: /messages
    response: {}
    columns:
      - name: id
        type: Utf8
"#,
        )
        .expect("manifest should parse");
        let review = manifest
            .outbound_host_review()
            .expect("outbound host review should resolve");

        assert!(review.hosts.is_empty());
        assert_eq!(
            review.unresolved_hosts,
            vec!["{{input.API_BASE}}".to_string()]
        );
    }

    #[test]
    fn includes_v4_descriptor_and_explicit_runtime_hosts() {
        let manifest = parse_source_manifest_yaml(
            r"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    url: https://specs.example.com/openapi.yaml
    base_url: https://api.example.com/v1
",
        )
        .expect("manifest should parse");

        assert_eq!(
            manifest
                .outbound_hosts()
                .expect("outbound hosts should resolve"),
            vec![
                "api.example.com".to_string(),
                "specs.example.com".to_string()
            ]
        );
    }

    #[test]
    fn includes_v4_file_descriptor_and_derived_runtime_host() {
        let openapi_file = write_openapi_fixture(
            r"
openapi: 3.0.3
info:
  title: Demo
  version: 1.0.0
servers:
  - url: https://api.example.com/v1
paths: {}
",
        );
        let manifest = parse_source_manifest_yaml(&format!(
            r#"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: "{}"
"#,
            openapi_file.display()
        ))
        .expect("manifest should parse");

        assert_eq!(
            manifest
                .outbound_hosts()
                .expect("outbound hosts should resolve"),
            vec!["api.example.com".to_string()]
        );
        std::fs::remove_file(openapi_file).expect("remove OpenAPI fixture");
    }

    #[test]
    fn deduplicates_v4_file_descriptor_runtime_hosts_without_unresolved_marker() {
        let openapi_file = write_openapi_fixture(
            r"
openapi: 3.0.3
info:
  title: Demo
  version: 1.0.0
servers:
  - url: https://api.example.com/v1
paths: {}
",
        );
        let manifest = parse_source_manifest_yaml(&format!(
            r#"
name: demo
dsl_version: 4
surfaces:
  - id: rest_a
    type: openapi
    namespace_suffix: rest_a
    file: "{}"
  - id: rest_b
    type: openapi
    namespace_suffix: rest_b
    file: "{}"
"#,
            openapi_file.display(),
            openapi_file.display()
        ))
        .expect("manifest should parse");

        let review = manifest
            .outbound_host_review()
            .expect("outbound host review should resolve");
        assert_eq!(review.hosts, vec!["api.example.com".to_string()]);
        assert!(review.unresolved_hosts.is_empty());
        std::fs::remove_file(openapi_file).expect("remove OpenAPI fixture");
    }

    #[test]
    fn includes_unresolved_marker_for_v4_file_descriptor_with_relative_server_url() {
        let openapi_file = write_openapi_fixture(
            r"
openapi: 3.0.3
info:
  title: Demo
  version: 1.0.0
servers:
  - url: /v1
paths: {}
",
        );
        let manifest = parse_source_manifest_yaml(&format!(
            r#"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: "{}"
"#,
            openapi_file.display()
        ))
        .expect("manifest should parse");

        let review = manifest
            .outbound_host_review()
            .expect("outbound host review should resolve");
        assert!(review.hosts.is_empty());
        assert_eq!(
            review.unresolved_hosts,
            vec![UNRESOLVED_OPENAPI_SERVER_HOST.to_string()]
        );
        std::fs::remove_file(openapi_file).expect("remove OpenAPI fixture");
    }

    #[test]
    fn includes_v4_url_descriptor_and_unresolved_derived_runtime_host_marker() {
        let manifest = parse_source_manifest_yaml(
            r"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    url: https://specs.example.com/openapi.yaml
",
        )
        .expect("manifest should parse");

        let review = manifest
            .outbound_host_review()
            .expect("outbound host review should resolve");
        assert_eq!(review.hosts, vec!["specs.example.com".to_string()]);
        assert_eq!(
            review.unresolved_hosts,
            vec![UNRESOLVED_OPENAPI_SERVER_HOST.to_string()]
        );
    }

    #[test]
    fn includes_v4_streamable_http_mcp_surface_host() {
        let manifest = parse_source_manifest_yaml(
            r"
name: demo
dsl_version: 4
surfaces:
  - id: mcp
    type: mcp
    server:
      transport: streamable_http
      url: https://mcp.example.com:8443/mcp
",
        )
        .expect("manifest should parse");

        let review = manifest
            .outbound_host_review()
            .expect("outbound host review should resolve");
        assert_eq!(review.hosts, vec!["mcp.example.com:8443".to_string()]);
        assert!(review.unresolved_hosts.is_empty());
    }

    #[test]
    fn omits_v4_stdio_mcp_surface_host_without_openapi_marker() {
        let manifest = parse_source_manifest_yaml(
            r"
name: demo
dsl_version: 4
surfaces:
  - id: mcp
    type: mcp
    server:
      transport: stdio
      command: demo-mcp-server
",
        )
        .expect("manifest should parse");

        let review = manifest
            .outbound_host_review()
            .expect("outbound host review should resolve");
        assert!(review.hosts.is_empty());
        assert!(review.unresolved_hosts.is_empty());
    }

    fn write_openapi_fixture(contents: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time")
            .as_nanos();
        let file = std::env::temp_dir().join(format!(
            "coral-spec-openapi-{}-{unique}.yaml",
            std::process::id()
        ));
        std::fs::write(&file, contents).expect("write OpenAPI fixture");
        file
    }

    #[test]
    fn includes_oauth_endpoint_hosts() {
        let found = hosts(
            r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  API_BASE:
    kind: variable
    default: https://api.example.com
  API_TOKEN:
    kind: secret
    credential:
      methods:
        - type: oauth
          label: Connect
          oauth:
            flow:
              type: authorization_code
              pkce: required
            redirect_uri: http://127.0.0.1:53682/oauth/callback
            endpoints:
              authorization_url: https://auth.example.com/oauth/authorize
              token_url: https://tokens.example.com/oauth/token
            client:
              id:
                default: default-client
base_url: "{{input.API_BASE}}"
tables:
  - name: messages
    description: Demo messages
    request:
      method: GET
      path: /messages
    response: {}
    columns:
      - name: id
        type: Utf8
"#,
        );
        assert_eq!(
            found,
            vec![
                "api.example.com".to_string(),
                "auth.example.com".to_string(),
                "tokens.example.com".to_string(),
            ]
        );
    }

    #[test]
    fn resolves_templated_oauth_endpoint_hosts_against_input_defaults() {
        let found = hosts(
            r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  API_BASE:
    kind: variable
    default: https://api.example.com
  AUTH_BASE:
    kind: variable
    default: https://auth.example.com
  EXCHANGE_BASE:
    kind: variable
    default: https://tokens.example.com
  API_TOKEN:
    kind: secret
    credential:
      methods:
        - type: oauth
          label: Connect
          oauth:
            flow:
              type: authorization_code
              pkce: required
            redirect_uri: http://127.0.0.1:53682/oauth/callback
            endpoints:
              authorization_url: "{{input.AUTH_BASE}}/oauth/authorize"
              token_url: "{{input.EXCHANGE_BASE}}/oauth/token"
            client:
              id:
                default: default-client
base_url: "{{input.API_BASE}}"
tables:
  - name: messages
    description: Demo messages
    request:
      method: GET
      path: /messages
    response: {}
    columns:
      - name: id
        type: Utf8
"#,
        );
        assert_eq!(
            found,
            vec![
                "api.example.com".to_string(),
                "auth.example.com".to_string(),
                "tokens.example.com".to_string(),
            ]
        );
    }

    #[test]
    fn includes_device_oauth_endpoint_hosts() {
        let found = hosts(
            r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  API_BASE:
    kind: variable
    default: https://api.example.com
  API_TOKEN:
    kind: secret
    credential:
      methods:
        - type: oauth
          label: Connect
          oauth:
            flow:
              type: device_code
              pkce: disabled
            endpoints:
              device_authorization_url: https://device.example.com/oauth/device/code
              token_url: https://tokens.example.com/oauth/token
            client:
              id:
                default: default-client
base_url: "{{input.API_BASE}}"
tables:
  - name: messages
    description: Demo messages
    request:
      method: GET
      path: /messages
    response: {}
    columns:
      - name: id
        type: Utf8
"#,
        );
        assert_eq!(
            found,
            vec![
                "api.example.com".to_string(),
                "device.example.com".to_string(),
                "tokens.example.com".to_string(),
            ]
        );
    }

    #[test]
    fn includes_streamable_http_mcp_server_host() {
        let found = hosts(
            r"
name: demo_mcp
version: 1.0.0
dsl_version: 3
backend: mcp
server:
  transport: streamable_http
  url: https://mcp.internal.example:8443/mcp
tables:
  - name: issues
    description: Demo issues
    tool: list_issues
    response: {}
    columns:
      - name: id
        type: Utf8
",
        );
        assert_eq!(found, vec!["mcp.internal.example:8443".to_string()]);
    }

    #[test]
    fn omits_local_file_locations() {
        let found = hosts(
            r"
name: demo
version: 1.0.0
dsl_version: 3
backend: file
tables:
  - name: events
    description: Demo events
    format: jsonl
    source:
      location: file:///tmp/demo/
    columns:
      - name: kind
        type: Utf8
",
        );
        assert!(found.is_empty());
    }

    #[test]
    fn omits_local_file_urls_without_double_slashes() {
        let mut found = BTreeSet::new();
        let mut unresolved = BTreeSet::new();
        super::collect_host(&mut found, &mut unresolved, "file:/tmp/demo");

        assert!(found.is_empty());
        assert!(unresolved.is_empty());
    }

    #[test]
    fn includes_s3_service_host_with_default_region() {
        let found = hosts(
            r"
name: demo
version: 1.0.0
dsl_version: 3
backend: file
tables:
  - name: events
    description: Demo events
    format: jsonl
    source:
      location: s3://example-bucket/events/
      object_store:
        type: s3
        auth:
          type: instance_profile
    columns:
      - name: kind
        type: Utf8
",
        );
        assert_eq!(found, vec!["s3.us-east-1.amazonaws.com".to_string()]);
    }

    #[test]
    fn includes_s3_service_host_with_templated_region() {
        let manifest = parse_source_manifest_yaml(
            r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: file
inputs:
  S3_BUCKET:
    kind: variable
  AWS_REGION:
    kind: variable
tables:
  - name: events
    description: Demo events
    format: jsonl
    source:
      location: s3://{{input.S3_BUCKET}}/events/
      object_store:
        type: s3
        region: "{{input.AWS_REGION}}"
        auth:
          type: instance_profile
    columns:
      - name: kind
        type: Utf8
"#,
        )
        .expect("manifest should parse");

        let review = manifest
            .outbound_host_review()
            .expect("outbound host review should resolve");
        assert!(review.hosts.is_empty());
        assert_eq!(
            review.unresolved_hosts,
            vec!["S3 service endpoint for unresolved region {{input.AWS_REGION}}".to_string()]
        );

        let source_inputs = BTreeMap::from([("AWS_REGION".to_string(), "eu-west-1".to_string())]);
        assert_eq!(
            manifest
                .outbound_hosts_with_input_values(&source_inputs)
                .expect("outbound hosts should resolve"),
            vec!["s3.eu-west-1.amazonaws.com".to_string()]
        );
    }

    #[test]
    fn rejects_invalid_rendered_s3_region_before_host_review() {
        let manifest = parse_source_manifest_yaml(
            r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: file
inputs:
  AWS_REGION:
    kind: variable
    default: us-east-1
tables:
  - name: events
    description: Demo events
    format: jsonl
    source:
      location: s3://example-bucket/events/
      object_store:
        type: s3
        region: "{{input.AWS_REGION}}"
        auth:
          type: instance_profile
    columns:
      - name: kind
        type: Utf8
"#,
        )
        .expect("manifest should parse");
        let source_inputs = BTreeMap::from([(
            "AWS_REGION".to_string(),
            "cn-north-1.evil.example/path".to_string(),
        )]);

        let error = manifest
            .outbound_hosts_with_input_values(&source_inputs)
            .expect_err("invalid rendered S3 region should fail before host review");

        assert!(
            error.to_string().contains(
                "demo.events source.object_store.region must contain only lowercase ASCII letters"
            ),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_invalid_defaulted_s3_region_before_host_review() {
        let manifest = parse_source_manifest_yaml(
            r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: file
inputs:
  AWS_REGION:
    kind: variable
    default: cn-north-1.evil.example/path
tables:
  - name: events
    description: Demo events
    format: jsonl
    source:
      location: s3://example-bucket/events/
      object_store:
        type: s3
        region: "{{input.AWS_REGION}}"
        auth:
          type: instance_profile
    columns:
      - name: kind
        type: Utf8
"#,
        )
        .expect("manifest should parse");

        let error = manifest
            .outbound_hosts()
            .expect_err("invalid defaulted S3 region should fail before host review");

        assert!(
            error.to_string().contains(
                "demo.events source.object_store.region must contain only lowercase ASCII letters"
            ),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn includes_s3_service_host_with_china_region_suffix() {
        let manifest = parse_source_manifest_yaml(
            r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: file
inputs:
  AWS_REGION:
    kind: variable
tables:
  - name: events
    description: Demo events
    format: jsonl
    source:
      location: s3://example-bucket/events/
      object_store:
        type: s3
        region: "{{input.AWS_REGION}}"
        auth:
          type: instance_profile
    columns:
      - name: kind
        type: Utf8
"#,
        )
        .expect("manifest should parse");

        let source_inputs = BTreeMap::from([("AWS_REGION".to_string(), "cn-north-1".to_string())]);
        assert_eq!(
            manifest
                .outbound_hosts_with_input_values(&source_inputs)
                .expect("outbound hosts should resolve"),
            vec!["s3.cn-north-1.amazonaws.com.cn".to_string()]
        );
    }

    #[test]
    fn allows_secret_input_in_base_url_path() {
        // A secret in the path (e.g. a Telegram bot token in `/bot<TOKEN>`)
        // does not determine the host, so it is permitted; the host is the
        // static authority and is reported correctly.
        let manifest = parse_source_manifest_yaml(
            r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  API_TOKEN:
    kind: secret
base_url: "https://api.telegram.org/bot{{input.API_TOKEN}}"
tables:
  - name: messages
    description: Demo messages
    request:
      method: GET
      path: /getUpdates
    response: {}
    columns:
      - name: id
        type: Utf8
"#,
        )
        .expect("a secret in the base_url path should be allowed");
        assert_eq!(
            manifest
                .outbound_hosts()
                .expect("outbound hosts should resolve"),
            vec!["api.telegram.org".to_string()]
        );
    }

    #[test]
    fn rejects_base_url_templating_a_secret_input() {
        // A host that resolves from a secret can never be shown for
        // outbound-host confirmation (secrets are collected afterward), so the
        // manifest must not declare one. See `validate_host_template_inputs`.
        let error = parse_source_manifest_yaml(
            r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  SECRET_HOST:
    kind: secret
base_url: "https://{{input.SECRET_HOST}}.example.com"
tables:
  - name: messages
    description: Demo messages
    request:
      method: GET
      path: /messages
    response: {}
    columns:
      - name: id
        type: Utf8
"#,
        )
        .expect_err("secret-templated base_url host should be rejected");
        assert!(
            error
                .to_string()
                .contains("only `kind: variable` inputs may determine an outbound host"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_s3_region_templating_a_secret_input() {
        let error = parse_source_manifest_yaml(
            r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: file
inputs:
  AWS_REGION:
    kind: secret
tables:
  - name: events
    description: Demo events
    format: jsonl
    source:
      location: s3://example-bucket/events/
      object_store:
        type: s3
        region: "{{input.AWS_REGION}}"
        auth:
          type: instance_profile
    columns:
      - name: kind
        type: Utf8
"#,
        )
        .expect_err("secret-templated S3 region should be rejected");
        assert!(
            error
                .to_string()
                .contains("only `kind: variable` inputs may determine an outbound host"),
            "unexpected error: {error}"
        );
    }
}
