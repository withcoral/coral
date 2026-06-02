//! Outbound-host extraction for source specs.
//!
//! Source setup surfaces every network host a source will contact so a user
//! can review and confirm them before installation. Hosts are derived
//! statically from the manifest — the base URL, OAuth provider endpoints, and
//! file or object-store table locations.

use std::collections::BTreeSet;

use url::Url;

use crate::{
    ManifestInputSpec, ParsedTemplate, TemplateNamespace, TemplatePart, ValidatedSourceManifest,
};

impl ValidatedSourceManifest {
    /// Returns every network host this source will contact, de-duplicated and
    /// sorted.
    ///
    /// Hosts are collected from the HTTP base URL, OAuth provider
    /// authorization/token endpoints, and file-backed table locations. A URL
    /// whose host depends on an install-time input with no manifest default
    /// cannot be resolved statically; its unresolved `{{...}}` template string
    /// is returned verbatim so callers can still show it to the user.
    #[must_use]
    pub fn outbound_hosts(&self) -> Vec<String> {
        let mut hosts = BTreeSet::new();
        let inputs = self.declared_inputs();

        if let Some(http) = self.as_http() {
            collect_host(&mut hosts, &render_with_defaults(&http.base_url, inputs));
        }

        // OAuth endpoints are declared on secret inputs regardless of backend.
        for input in inputs {
            let Some(credential) = input.credential.as_ref() else {
                continue;
            };
            for method in &credential.methods {
                if let Some(oauth) = method.oauth.as_ref() {
                    if let Some(authorization_url) = oauth.authorization_url.as_deref() {
                        collect_host(
                            &mut hosts,
                            &render_string_template_with_defaults(authorization_url, inputs),
                        );
                    }
                    if let Some(device_authorization_url) =
                        oauth.device_authorization_url.as_deref()
                    {
                        collect_host(
                            &mut hosts,
                            &render_string_template_with_defaults(device_authorization_url, inputs),
                        );
                    }
                    collect_host(
                        &mut hosts,
                        &render_string_template_with_defaults(&oauth.token_url, inputs),
                    );
                }
            }
        }

        if let Some(file) = self.as_file() {
            for table in &file.tables {
                collect_host(
                    &mut hosts,
                    &render_with_defaults(&table.source.location, inputs),
                );
            }
        }

        hosts.into_iter().collect()
    }
}

/// Renders a template, substituting input tokens with their manifest default
/// value. Tokens with no usable default are left as their `{{...}}` literal so
/// the resulting string still signals an unresolved, input-driven endpoint.
fn render_with_defaults(template: &ParsedTemplate, inputs: &[ManifestInputSpec]) -> String {
    let mut rendered = String::new();
    for part in template.parts() {
        match part {
            TemplatePart::Literal(text) => rendered.push_str(text),
            TemplatePart::Token(token) => {
                let resolved = token.default_value().map(str::to_string).or_else(|| {
                    if matches!(token.namespace(), TemplateNamespace::Input) {
                        inputs
                            .iter()
                            .find(|input| input.key == token.key())
                            .map(|input| input.default_value.clone())
                            .filter(|value| !value.is_empty())
                    } else {
                        None
                    }
                });
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

fn render_string_template_with_defaults(raw: &str, inputs: &[ManifestInputSpec]) -> String {
    ParsedTemplate::parse(raw).map_or_else(
        |_| raw.to_string(),
        |template| render_with_defaults(&template, inputs),
    )
}

/// Extracts a displayable host from a (possibly templated) URL string and adds
/// it to `hosts`.
fn collect_host(hosts: &mut BTreeSet<String>, raw: &str) {
    let raw = raw.trim();
    if raw.is_empty() {
        return;
    }
    match Url::parse(raw) {
        Ok(url) => {
            // A `file://` location reads from the local filesystem; there is no
            // remote host to report.
            if url.scheme() == "file" {
                return;
            }
            if let Some(host) = url.host_str() {
                match url.port() {
                    Some(port) => hosts.insert(format!("{host}:{port}")),
                    None => hosts.insert(host.to_string()),
                };
                return;
            }
            hosts.insert(raw.to_string());
        }
        // Templated or otherwise unparseable: surface the raw string so the
        // user still sees that an input-driven endpoint exists.
        Err(_) => {
            hosts.insert(raw.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::parse_source_manifest_yaml;

    fn hosts(manifest_yaml: &str) -> Vec<String> {
        parse_source_manifest_yaml(manifest_yaml)
            .expect("manifest should parse")
            .outbound_hosts()
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
    fn surfaces_unresolved_templated_base_url() {
        let found = hosts(
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
        );
        assert_eq!(found, vec!["{{input.API_BASE}}".to_string()]);
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
}
