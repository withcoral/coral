//! Extracts interactive source inputs from source-spec documents.
//!
//! These helpers walk the source-spec DSL and collect install-time inputs in
//! declaration order. They stay close to the authored file format so callers
//! can use them before any app- or transport-level mapping.

use std::collections::BTreeMap;

use serde_yaml::{Mapping, Value};

use crate::{ManifestError, Result};

/// The kind of interactive input required by one validated source spec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestInputKind {
    /// A non-secret input persisted in source variables.
    Variable,
    /// A secret input persisted separately from source variables.
    Secret,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputKind {
    Variable,
    Secret,
}

impl InputKind {
    fn as_manifest_kind(self) -> ManifestInputKind {
        match self {
            Self::Variable => ManifestInputKind::Variable,
            Self::Secret => ManifestInputKind::Secret,
        }
    }
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
}

#[derive(Debug, Clone)]
struct InputState {
    kind: InputKind,
    default_value: Option<String>,
}

/// Collect interactive source inputs from structured source-spec data.
///
/// # Errors
///
/// Returns a [`ManifestError`] when the source spec contains unsupported legacy
/// source-input forms or malformed template tokens.
pub fn collect_source_inputs_value(root: &Value) -> Result<Vec<ManifestInputSpec>> {
    let mut ordered = Vec::new();
    let mut seen = BTreeMap::<String, InputState>::new();
    collect_from_value(root, &mut ordered, &mut seen)?;
    Ok(ordered)
}

/// Collect interactive source inputs from raw source-spec YAML.
///
/// # Errors
///
/// Returns a [`ManifestError`] when the YAML cannot be parsed or when the
/// source spec contains unsupported legacy source-input forms or malformed
/// template tokens.
pub fn collect_source_inputs_yaml(raw: &str) -> Result<Vec<ManifestInputSpec>> {
    let root: Value = serde_yaml::from_str(raw).map_err(ManifestError::parse_yaml)?;
    collect_source_inputs_value(&root)
}

fn collect_from_value(
    value: &Value,
    ordered: &mut Vec<ManifestInputSpec>,
    seen: &mut BTreeMap<String, InputState>,
) -> Result<()> {
    match value {
        Value::Mapping(map) => {
            collect_from_mapping(map, ordered, seen)?;
            for nested in map.values() {
                collect_from_value(nested, ordered, seen)?;
            }
        }
        Value::Sequence(items) => {
            for item in items {
                collect_from_value(item, ordered, seen)?;
            }
        }
        Value::String(raw) => collect_from_template(raw, ordered, seen)?,
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::Tagged(_) => {}
    }
    Ok(())
}

fn collect_from_mapping(
    map: &Mapping,
    ordered: &mut Vec<ManifestInputSpec>,
    seen: &mut BTreeMap<String, InputState>,
) -> Result<()> {
    let Some(from) = map
        .get(Value::String("from".to_string()))
        .and_then(Value::as_str)
    else {
        return Ok(());
    };

    let kind = match from {
        "secret" => Some(InputKind::Secret),
        "variable" => Some(InputKind::Variable),
        "env" | "env_any" | "secret_any" | "variable_any" => {
            return Err(ManifestError::validation(format!(
                "unsupported manifest input source '{from}'"
            )));
        }
        _ => None,
    };
    let Some(kind) = kind else {
        return Ok(());
    };

    let key = map
        .get(Value::String("key".to_string()))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            ManifestError::validation(format!("manifest '{from}' input is missing key"))
        })?;
    let default_value = map
        .get(Value::String("default".to_string()))
        .and_then(Value::as_str)
        .map(ToString::to_string);
    register_input(key, kind, default_value, ordered, seen)
}

fn collect_from_template(
    template: &str,
    ordered: &mut Vec<ManifestInputSpec>,
    seen: &mut BTreeMap<String, InputState>,
) -> Result<()> {
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        let token_start = start + 2;
        let Some(end_rel) = rest[token_start..].find("}}") else {
            return Err(ManifestError::validation(format!(
                "unclosed template token in '{template}'"
            )));
        };
        let end = token_start + end_rel;
        let token = rest[token_start..end].trim();
        let (raw_key, default_value) = match token.split_once('|') {
            Some((key, default)) => (key.trim(), Some(default.to_string())),
            None => (token, None),
        };
        if let Some(key) = raw_key.strip_prefix("secret.") {
            register_input(key, InputKind::Secret, default_value, ordered, seen)?;
        } else if let Some(key) = raw_key.strip_prefix("variable.") {
            register_input(key, InputKind::Variable, default_value, ordered, seen)?;
        } else if raw_key.starts_with("env.") {
            return Err(ManifestError::validation(format!(
                "unsupported template namespace '{raw_key}'"
            )));
        }
        rest = &rest[end + 2..];
    }
    Ok(())
}

fn register_input(
    key: &str,
    kind: InputKind,
    default_value: Option<String>,
    ordered: &mut Vec<ManifestInputSpec>,
    seen: &mut BTreeMap<String, InputState>,
) -> Result<()> {
    if let Some(existing) = seen.get(key) {
        if existing.kind != kind || existing.default_value != default_value {
            return Err(ManifestError::validation(format!(
                "manifest input '{key}' is declared inconsistently"
            )));
        }
        return Ok(());
    }

    ordered.push(ManifestInputSpec {
        key: key.to_string(),
        kind: kind.as_manifest_kind(),
        required: default_value.is_none(),
        default_value: default_value.clone().unwrap_or_default(),
    });
    seen.insert(
        key.to_string(),
        InputState {
            kind,
            default_value,
        },
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{ManifestInputKind, collect_source_inputs_yaml};

    #[test]
    fn extracts_variables_and_secrets_in_manifest_order() {
        let manifest = r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: "{{variable.API_BASE|https://example.com}}"
auth:
  headers:
    - name: Authorization
      from: template
      template: Bearer {{secret.API_TOKEN}}
tables: []
"#;

        let inputs = collect_source_inputs_yaml(manifest).expect("inputs");
        assert_eq!(inputs.len(), 2);
        assert_eq!(inputs[0].key, "API_BASE");
        assert_eq!(inputs[0].kind, ManifestInputKind::Variable);
        assert!(!inputs[0].required);
        assert_eq!(inputs[0].default_value, "https://example.com");
        assert_eq!(inputs[1].key, "API_TOKEN");
        assert_eq!(inputs[1].kind, ManifestInputKind::Secret);
        assert!(inputs[1].required);
    }

    #[test]
    fn rejects_legacy_env_inputs() {
        let manifest = r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: "{{env.API_BASE}}"
tables: []
"#;
        let error = collect_source_inputs_yaml(manifest).expect_err("legacy env unsupported");
        assert!(error.to_string().contains("unsupported"));
    }
}
