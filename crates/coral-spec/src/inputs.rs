//! Extracts interactive source inputs from source-spec documents.
//!
//! Sources that need interactive configuration declare their inputs under a
//! top-level `inputs` map. Each entry fixes the input's kind (`variable` or
//! `secret`), an optional default, and an optional hint. References elsewhere
//! in the manifest use `{{input.KEY}}` templates or `from: input` value
//! sources; the declared kind determines whether the value is resolved from
//! the variable or secret store. Manifests that take no interactive inputs
//! may omit the block entirely.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};

use crate::{ManifestError, ParsedTemplate, Result, TemplateNamespace};

/// The kind of interactive input required by one validated source spec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestInputKind {
    /// A non-secret input persisted in source variables.
    Variable,
    /// A secret input persisted separately from source variables.
    Secret,
}

/// One interactive input extracted from a validated source spec.
///
/// The app and CLI can map this into prompts, persisted variables, or secret
/// collection flows without depending on protobuf-specific types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestInputSpec {
    /// The source-spec-declared input key.
    pub key: String,
    /// Whether this input is a variable or a secret.
    pub kind: ManifestInputKind,
    /// Whether the user must provide an explicit value.
    pub required: bool,
    /// The source-spec-declared default value, if any.
    pub default_value: String,
    /// Optional authored hint shown to the user when collecting the input.
    pub hint: Option<String>,
}

/// Merge user-provided secrets and variables with manifest defaults into one
/// runtime-ready input map.
#[must_use]
pub fn resolve_inputs(
    declared: &[ManifestInputSpec],
    source_secrets: &BTreeMap<String, String>,
    source_variables: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut resolved = BTreeMap::new();
    for input in declared {
        let value = match input.kind {
            ManifestInputKind::Secret => source_secrets.get(&input.key).cloned(),
            ManifestInputKind::Variable => source_variables
                .get(&input.key)
                .cloned()
                .or_else(|| (!input.required).then(|| input.default_value.clone())),
        };
        if let Some(value) = value {
            resolved.insert(input.key.clone(), value);
        }
    }
    resolved
}

/// Collect interactive source inputs from an already-parsed manifest value.
///
/// # Errors
///
/// Returns a [`ManifestError`] when an input is declared incorrectly or the
/// manifest references an input that is not declared under the top-level
/// `inputs` block.
pub(crate) fn collect_source_inputs_value(root: &Value) -> Result<Vec<ManifestInputSpec>> {
    let inputs = collect_declared_inputs(root)?;
    validate_input_references(root, &inputs)?;
    Ok(inputs)
}

fn collect_declared_inputs(root: &Value) -> Result<Vec<ManifestInputSpec>> {
    let root = root
        .as_object()
        .ok_or_else(|| ManifestError::validation("manifest must be a mapping"))?;
    let Some(inputs) = root.get("inputs") else {
        return Ok(Vec::new());
    };
    let inputs = inputs.as_object().ok_or_else(|| {
        ManifestError::validation("manifest `inputs` must be declared as a mapping")
    })?;

    let mut ordered = Vec::new();
    for (key, value) in inputs {
        let input = value.as_object().ok_or_else(|| {
            ManifestError::validation(format!(
                "manifest input '{key}' must be declared as a mapping"
            ))
        })?;
        let kind = match input.get("kind").and_then(Value::as_str) {
            Some("variable") => ManifestInputKind::Variable,
            Some("secret") => ManifestInputKind::Secret,
            Some(other) => {
                return Err(ManifestError::validation(format!(
                    "manifest input '{key}' has unsupported kind '{other}'"
                )));
            }
            None => {
                return Err(ManifestError::validation(format!(
                    "manifest input '{key}' is missing kind"
                )));
            }
        };
        let default_value = input
            .get("default")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        if kind == ManifestInputKind::Secret && default_value.is_some() {
            return Err(ManifestError::validation(format!(
                "manifest secret input '{key}' must not declare a default"
            )));
        }
        let hint = input
            .get("hint")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        ordered.push(ManifestInputSpec {
            key: key.clone(),
            kind,
            required: default_value.is_none(),
            default_value: default_value.unwrap_or_default(),
            hint,
        });
    }

    Ok(ordered)
}

fn validate_input_references(root: &Value, inputs: &[ManifestInputSpec]) -> Result<()> {
    let declared: BTreeSet<String> = inputs.iter().map(|input| input.key.clone()).collect();
    validate_value(root, true, &declared)
}

fn validate_value(value: &Value, is_root: bool, declared: &BTreeSet<String>) -> Result<()> {
    match value {
        Value::Object(map) => {
            validate_mapping(map, declared)?;
            for (key, nested) in map {
                if is_root && key == "inputs" {
                    continue;
                }
                validate_value(nested, false, declared)?;
            }
        }
        Value::Array(items) => {
            for item in items {
                validate_value(item, false, declared)?;
            }
        }
        Value::String(raw) => validate_template(raw, declared)?,
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    Ok(())
}

fn validate_mapping(map: &Map<String, Value>, declared: &BTreeSet<String>) -> Result<()> {
    if map.get("from").and_then(Value::as_str) != Some("input") {
        return Ok(());
    }

    let key = map
        .get("key")
        .and_then(Value::as_str)
        .ok_or_else(|| ManifestError::validation("manifest 'input' value source is missing key"))?;
    if !declared.contains(key) {
        return Err(ManifestError::validation(format!(
            "manifest input '{key}' is referenced but not declared under top-level inputs"
        )));
    }
    if map.contains_key("default") {
        return Err(ManifestError::validation(format!(
            "manifest input '{key}' must declare defaults under top-level inputs"
        )));
    }
    Ok(())
}

fn validate_template(template: &str, declared: &BTreeSet<String>) -> Result<()> {
    let template = ParsedTemplate::parse(template)?;
    for token in template.tokens() {
        if !matches!(token.namespace(), TemplateNamespace::Input) {
            continue;
        }
        if !declared.contains(token.key()) {
            return Err(ManifestError::validation(format!(
                "manifest input '{}' is referenced but not declared under top-level inputs",
                token.key()
            )));
        }
        if token.default_value().is_some() {
            return Err(ManifestError::validation(format!(
                "manifest input '{}' must declare defaults under top-level inputs",
                token.key()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ManifestInputKind, ManifestInputSpec, collect_source_inputs_value};
    use crate::{ManifestError, Result};

    fn collect(raw: &str) -> Result<Vec<ManifestInputSpec>> {
        let root: serde_json::Value =
            serde_yaml::from_str(raw).map_err(ManifestError::parse_yaml)?;
        collect_source_inputs_value(&root)
    }

    #[test]
    fn declared_inputs_are_parsed_in_manifest_order() {
        let manifest = r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  GITHUB_API_BASE:
    kind: variable
    default: https://api.github.com
    hint: For GitHub Enterprise, use https://<host>/api/v3
  GITHUB_TOKEN:
    kind: secret
    hint: Run `gh auth token` or create a PAT
base_url: "{{input.GITHUB_API_BASE}}"
auth:
  type: HeaderAuth
  headers:
    - name: Authorization
      from: template
      template: Bearer {{input.GITHUB_TOKEN}}
tables: []
"#;

        let inputs = collect(manifest).expect("inputs");
        let [api_base, token] = inputs.as_slice() else {
            panic!("expected two inputs, got {inputs:?}");
        };
        assert_eq!(api_base.key, "GITHUB_API_BASE");
        assert_eq!(api_base.kind, ManifestInputKind::Variable);
        assert!(!api_base.required);
        assert_eq!(api_base.default_value, "https://api.github.com");
        assert_eq!(
            api_base.hint.as_deref(),
            Some("For GitHub Enterprise, use https://<host>/api/v3")
        );
        assert_eq!(token.key, "GITHUB_TOKEN");
        assert_eq!(token.kind, ManifestInputKind::Secret);
        assert!(token.required);
        assert_eq!(token.default_value, "");
        assert_eq!(
            token.hint.as_deref(),
            Some("Run `gh auth token` or create a PAT")
        );
    }

    #[test]
    fn from_input_value_source_resolves_against_declarations() {
        let manifest = r"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  GITHUB_TOKEN:
    kind: secret
auth:
  type: HeaderAuth
  headers:
    - name: Authorization
      from: input
      key: GITHUB_TOKEN
tables: []
";
        let inputs = collect(manifest).expect("inputs");
        let [input] = inputs.as_slice() else {
            panic!("expected one input, got {inputs:?}");
        };
        assert_eq!(input.kind, ManifestInputKind::Secret);
    }

    #[test]
    fn manifests_without_inputs_block_are_allowed() {
        let manifest = r"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://api.github.com
tables: []
";
        let inputs = collect(manifest).expect("no inputs is fine");
        assert!(inputs.is_empty());
    }

    #[test]
    fn references_without_inputs_block_are_rejected() {
        let manifest = r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: "{{input.GITHUB_API_BASE}}"
tables: []
"#;
        let error = collect(manifest).expect_err("undeclared reference");
        assert!(
            error
                .to_string()
                .contains("referenced but not declared under top-level inputs"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn undeclared_reference_is_rejected() {
        let manifest = r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  GITHUB_TOKEN:
    kind: secret
base_url: "{{input.GITHUB_API_BASE}}"
tables: []
"#;
        let error = collect(manifest).expect_err("undeclared input");
        assert!(
            error
                .to_string()
                .contains("referenced but not declared under top-level inputs")
        );
    }

    #[test]
    fn inline_template_defaults_are_rejected() {
        let manifest = r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  GITHUB_API_BASE:
    kind: variable
    default: https://api.github.com
base_url: "{{input.GITHUB_API_BASE|https://other.example.com}}"
tables: []
"#;
        let error = collect(manifest).expect_err("inline default");
        assert!(
            error
                .to_string()
                .contains("must declare defaults under top-level inputs")
        );
    }

    #[test]
    fn secret_defaults_are_rejected() {
        let manifest = r"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  GITHUB_TOKEN:
    kind: secret
    default: abc123
tables: []
";
        let error = collect(manifest).expect_err("secret default");
        assert!(error.to_string().contains("must not declare a default"));
    }
}
