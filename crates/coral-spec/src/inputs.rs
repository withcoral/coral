//! Extracts interactive source inputs from validated source-spec models.
//!
//! Walks the parsed, typed source-spec model and collects install-time inputs
//! in declaration order. Input help is sourced from the parsed `Onboarding`
//! metadata — no raw YAML re-scraping.

use std::collections::BTreeMap;

use crate::{
    HeaderSpec, ManifestError, ParsedTemplate, RequestRouteSpec, RequestSpec, Result,
    TemplateNamespace, ValidatedSourceManifest, ValueSourceSpec,
};

/// The kind of interactive input required by one validated source spec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputKind {
    /// A non-secret input persisted in source variables.
    Variable,
    /// A secret input persisted separately from source variables.
    Secret,
}
/// One install-time input extracted from a validated source spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputSpec {
    /// The source-spec-declared input key.
    pub key: String,
    /// Whether this input is a variable or a secret.
    pub kind: InputKind,
    /// Whether the user must provide an explicit value.
    pub required: bool,
    /// The source-spec-declared default value, if any.
    pub default_value: String,
    /// Optional human-readable help describing how to acquire this value.
    pub help: Option<String>,
}

/// Collect interactive source inputs from a validated source manifest.
///
/// # Errors
///
/// Returns a [`ManifestError`] if the same input key is declared with
/// inconsistent kind or default value.
pub fn collect_inputs(manifest: &ValidatedSourceManifest) -> Result<Vec<InputSpec>> {
    InputCollector::new().collect(manifest)
}

struct InputCollector {
    ordered: Vec<InputSpec>,
    seen: BTreeMap<String, (InputKind, Option<String>)>,
}

impl InputCollector {
    fn new() -> Self {
        Self {
            ordered: Vec::new(),
            seen: BTreeMap::new(),
        }
    }

    fn collect(mut self, manifest: &ValidatedSourceManifest) -> Result<Vec<InputSpec>> {
        if let Some(http) = manifest.as_http() {
            self.visit_template(&http.base_url)?;

            for header in &http.auth.headers {
                self.visit_value_source(&header.value)?;
            }

            for table in &http.tables {
                self.visit_request(&table.request)?;
                for route in &table.requests {
                    self.visit_route(route)?;
                }
            }
        }

        if let Some(onboarding) = manifest.common().onboarding.as_ref() {
            for input in &mut self.ordered {
                input.help = onboarding.help_for_input(&input.key).map(str::to_string);
            }
        }

        Ok(self.ordered)
    }

    fn register(
        &mut self,
        key: &str,
        kind: InputKind,
        default_value: Option<String>,
    ) -> Result<()> {
        if let Some(existing) = self.seen.get(key) {
            if existing.0 != kind || existing.1 != default_value {
                return Err(ManifestError::validation(format!(
                    "manifest input '{key}' is declared inconsistently"
                )));
            }
            return Ok(());
        }

        self.ordered.push(InputSpec {
            key: key.to_string(),
            kind,
            required: default_value.is_none(),
            default_value: default_value.clone().unwrap_or_default(),
            help: None,
        });
        self.seen.insert(key.to_string(), (kind, default_value));
        Ok(())
    }

    fn visit_template(&mut self, template: &ParsedTemplate) -> Result<()> {
        for token in template.tokens() {
            match token.namespace() {
                TemplateNamespace::Secret => {
                    self.register(
                        token.key(),
                        InputKind::Secret,
                        token.default_value().map(str::to_string),
                    )?;
                }
                TemplateNamespace::Variable => {
                    self.register(
                        token.key(),
                        InputKind::Variable,
                        token.default_value().map(str::to_string),
                    )?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn visit_value_source(&mut self, source: &ValueSourceSpec) -> Result<()> {
        match source {
            ValueSourceSpec::Secret { key, default } => {
                self.register(key, InputKind::Secret, default.clone())?;
            }
            ValueSourceSpec::Variable { key, default } => {
                self.register(key, InputKind::Variable, default.clone())?;
            }
            ValueSourceSpec::Template { template } => {
                self.visit_template(template)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn visit_headers(&mut self, headers: &[HeaderSpec]) -> Result<()> {
        for header in headers {
            self.visit_value_source(&header.value)?;
        }
        Ok(())
    }

    fn visit_request(&mut self, request: &RequestSpec) -> Result<()> {
        self.visit_template(&request.path)?;
        for param in &request.query {
            self.visit_value_source(&param.value)?;
        }
        for field in &request.body {
            self.visit_value_source(&field.value)?;
        }
        self.visit_headers(&request.headers)?;
        Ok(())
    }

    fn visit_route(&mut self, route: &RequestRouteSpec) -> Result<()> {
        self.visit_request(&route.request)
    }
}

#[cfg(test)]
mod tests {
    use crate::parse_source_manifest_yaml;

    use super::{InputKind, collect_inputs};

    #[test]
    fn extracts_variables_and_secrets_in_manifest_order() {
        let manifest = parse_source_manifest_yaml(
            r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: "{{variable.API_BASE|https://example.com}}"
onboarding:
  input_help:
    API_TOKEN: "Create a token at https://example.com/settings/tokens"
    API_BASE: "Your API base URL for self-hosted instances"
auth:
  headers:
    - name: Authorization
      from: template
      template: Bearer {{secret.API_TOKEN}}
tables:
  - name: messages
    description: Demo messages
    request:
      method: GET
      path: /messages
    columns:
      - name: id
        type: Utf8
"#,
        )
        .expect("parse");

        let inputs = collect_inputs(&manifest).expect("collect");
        assert_eq!(inputs.len(), 2);

        assert_eq!(inputs[0].key, "API_BASE");
        assert_eq!(inputs[0].kind, InputKind::Variable);
        assert!(!inputs[0].required);
        assert_eq!(inputs[0].default_value, "https://example.com");
        assert_eq!(
            inputs[0].help.as_deref(),
            Some("Your API base URL for self-hosted instances")
        );

        assert_eq!(inputs[1].key, "API_TOKEN");
        assert_eq!(inputs[1].kind, InputKind::Secret);
        assert!(inputs[1].required);
        assert_eq!(
            inputs[1].help.as_deref(),
            Some("Create a token at https://example.com/settings/tokens")
        );
    }

    #[test]
    fn onboarding_metadata_does_not_register_phantom_inputs() {
        let manifest = parse_source_manifest_yaml(
            r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: "{{variable.API_BASE|https://example.com}}"
onboarding:
  input_help:
    API_BASE: "Use {{secret.PHANTOM}} to authenticate"
auth:
  headers:
    - name: Authorization
      from: template
      template: Bearer {{secret.API_TOKEN}}
tables:
  - name: messages
    description: Demo messages
    request:
      method: GET
      path: /messages
    columns:
      - name: id
        type: Utf8
"#,
        )
        .expect("parse");

        let inputs = collect_inputs(&manifest).expect("collect");
        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[0].key, "API_BASE");
        assert_eq!(inputs[1].key, "API_TOKEN");
        assert!(
            !inputs.iter().any(|i| i.key == "PHANTOM"),
            "onboarding metadata should not register inputs"
        );
    }

    #[test]
    fn file_backend_returns_empty_inputs() {
        let manifest = parse_source_manifest_yaml(
            r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: parquet
tables:
  - name: data
    description: Some data
    source:
      location: file:///tmp/demo/
      glob: "**/*.parquet"
    columns: []
"#,
        )
        .expect("parse");

        let inputs = collect_inputs(&manifest).expect("collect");
        assert!(inputs.is_empty());
    }

    #[test]
    fn collects_input_help_from_onboarding() {
        let manifest = parse_source_manifest_yaml(
            r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: "https://example.com"
onboarding:
  input_help:
    API_TOKEN: "Create a token at https://example.com/settings/tokens"
auth:
  headers:
    - name: Authorization
      from: template
      template: Bearer {{secret.API_TOKEN}}
tables:
  - name: items
    description: Demo items
    request:
      method: GET
      path: /items
    columns:
      - name: id
        type: Utf8
"#,
        )
        .expect("parse");

        let inputs = collect_inputs(&manifest).expect("collect");
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].kind, InputKind::Secret);
        assert_eq!(
            inputs[0].help.as_deref(),
            Some("Create a token at https://example.com/settings/tokens")
        );
    }
}
