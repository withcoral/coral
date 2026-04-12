//! Bundled source catalog and source-spec description helpers.

use std::collections::BTreeSet;

use coral_api::v1::{AvailableSource, SourceInputKind, SourceInputSpec, SourceOrigin, Workspace};
use coral_spec::{ManifestInputKind, ManifestInputSpec, parse_manifest_and_inputs};

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
    let (manifest, inputs) = parse_manifest_and_inputs(manifest_yaml)
        .map_err(|error| AppError::InvalidInput(error.to_string()))?;
    Ok(AvailableSource {
        name: manifest.schema_name().to_string(),
        description: manifest.description().to_string(),
        version: manifest.source_version().to_string(),
        inputs: inputs.into_iter().map(proto_input_spec).collect(),
        installed,
        origin: origin as i32,
    })
}

fn proto_input_spec(input: ManifestInputSpec) -> SourceInputSpec {
    SourceInputSpec {
        key: input.key,
        kind: proto_input_kind(input.kind) as i32,
        required: input.required,
        default_value: input.default_value,
    }
}

fn proto_input_kind(kind: ManifestInputKind) -> SourceInputKind {
    match kind {
        ManifestInputKind::Variable => SourceInputKind::Variable,
        ManifestInputKind::Secret => SourceInputKind::Secret,
    }
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
base_url: https://example.com
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
        let message = error.to_string();
        assert!(message.starts_with("invalid input: source manifest failed schema validation:"));
        assert!(message.contains("'schema'"));
    }
}
