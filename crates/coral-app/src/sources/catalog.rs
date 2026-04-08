//! Bundled source catalog and source-spec description helpers.

use std::collections::BTreeSet;

use coral_api::v1::{AvailableSource, SourceInputKind, SourceInputSpec, SourceOrigin, Workspace};
use coral_spec::{
    InputSpec, InputKind, collect_source_inputs_value, parse_source_manifest_value,
};
use serde_yaml::Value;

use crate::bootstrap::AppError;

include!(concat!(env!("OUT_DIR"), "/bundled_sources.rs"));

#[derive(Debug, Clone)]
pub(crate) struct BundledSourceManifest {
    pub(crate) manifest_yaml: String,
}

pub(crate) fn list_bundled_sources(
    _workspace: &Workspace,
    installed_source_names: &BTreeSet<String>,
) -> Result<Vec<AvailableSource>, AppError> {
    let mut available = BUNDLED_SOURCES
        .iter()
        .map(|(name, manifest_yaml)| {
            let mut source = describe_manifest(
                manifest_yaml,
                SourceOrigin::Bundled,
                installed_source_names.contains(*name),
            )?;
            source.name = (*name).to_string();
            Ok(source)
        })
        .collect::<Result<Vec<_>, AppError>>()?;
    available.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(available)
}

pub(crate) fn load_bundled_source(name: &str) -> Result<BundledSourceManifest, AppError> {
    let Some((_, manifest_yaml)) = BUNDLED_SOURCES
        .iter()
        .find(|(candidate, _)| *candidate == name)
    else {
        return Err(AppError::InvalidInput(format!(
            "unknown bundled source '{name}'"
        )));
    };
    Ok(BundledSourceManifest {
        manifest_yaml: (*manifest_yaml).to_string(),
    })
}

pub(crate) fn describe_manifest(
    manifest_yaml: &str,
    origin: SourceOrigin,
    installed: bool,
) -> Result<AvailableSource, AppError> {
    let root: Value = serde_yaml::from_str(manifest_yaml)?;
    let manifest = parse_source_manifest_value(serde_json::to_value(&root)?)
        .map_err(|error| AppError::InvalidInput(error.to_string()))?;
    let description = manifest_description(&root);
    let inputs = collect_source_inputs_value(&root)
        .map(|inputs| inputs.into_iter().map(proto_input_spec).collect())
        .map_err(|error| AppError::InvalidInput(error.to_string()))?;
    Ok(AvailableSource {
        name: manifest.schema_name().to_string(),
        description,
        version: manifest.source_version().to_string(),
        inputs,
        installed,
        origin: origin as i32,
    })
}

fn proto_input_spec(input: InputSpec) -> SourceInputSpec {
    SourceInputSpec {
        key: input.key,
        kind: proto_input_kind(input.kind) as i32,
        required: input.required,
        default_value: input.default_value,
        help: input.help.unwrap_or_default(),
    }
}

fn proto_input_kind(kind: InputKind) -> SourceInputKind {
    match kind {
        InputKind::Variable => SourceInputKind::Variable,
        InputKind::Secret => SourceInputKind::Secret,
    }
}

fn manifest_description(root: &Value) -> String {
    root.get("description")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use coral_api::v1::{SourceInputKind, Workspace};

    use super::{describe_manifest, list_bundled_sources};
    use crate::workspaces::WorkspaceManager;

    fn default_workspace() -> Workspace {
        WorkspaceManager::new().default_workspace()
    }

    #[test]
    fn bundled_sources_load_through_catalog() {
        let sources =
            list_bundled_sources(&default_workspace(), &BTreeSet::new()).expect("bundled sources");
        assert!(!sources.is_empty());
        assert!(sources.iter().any(|source| source.name == "github"));
        assert!(sources.iter().any(|source| source.name == "stripe"));
        assert!(sources.iter().all(|source| !source.version.is_empty()));
    }

    #[test]
    fn describe_manifest_extracts_variable_and_secret_inputs() {
        let source = describe_manifest(
            r#"
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
            coral_api::v1::SourceOrigin::Imported,
            false,
        )
        .expect("describe manifest");
        assert_eq!(source.inputs.len(), 2);
        assert_eq!(source.inputs[0].key, "API_BASE");
        assert_eq!(source.inputs[0].kind, SourceInputKind::Variable as i32);
        assert_eq!(source.inputs[1].key, "API_TOKEN");
        assert_eq!(source.inputs[1].kind, SourceInputKind::Secret as i32);
    }

    #[test]
    fn describe_manifest_rejects_legacy_env_inputs() {
        let error = describe_manifest(
            r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: "{{env.API_BASE}}"
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
            coral_api::v1::SourceOrigin::Imported,
            false,
        )
        .expect_err("legacy env input should fail");
        assert!(error.to_string().contains("unsupported"));
    }

    #[test]
    fn describe_manifest_rejects_legacy_schema_field() {
        let error = describe_manifest(
            r"
name: demo
schema: demo
version: 1.0.0
dsl_version: 3
backend: http
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
            coral_api::v1::SourceOrigin::Imported,
            false,
        )
        .expect_err("legacy schema field should fail");
        assert!(error.to_string().contains("unknown field `schema`"));
    }

    #[test]
    fn describe_manifest_attaches_onboarding_help_to_inputs() {
        let source = describe_manifest(
            r#"
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
onboarding:
  input_help:
    API_TOKEN: "Create a token at https://example.com/settings/tokens"
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
            coral_api::v1::SourceOrigin::Imported,
            false,
        )
        .expect("describe manifest");
        assert_eq!(source.inputs[0].help, "");
        assert_eq!(
            source.inputs[1].help,
            "Create a token at https://example.com/settings/tokens"
        );
    }
}
