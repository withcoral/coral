//! Bundled source catalog and installed-manifest resolution helpers.

use std::collections::BTreeSet;

use coral_spec::{SourceSpec, parse_source_manifest_yaml};

use crate::bootstrap::AppError;
use crate::sources::SourceName;
use crate::sources::model::{CandidateSource, InstalledSource, SourceOrigin};
use crate::state::AppStateLayout;
use crate::workspaces::WorkspaceName;

include!(concat!(env!("OUT_DIR"), "/bundled_sources.rs"));

#[derive(Debug, Clone, Copy)]
pub(crate) struct BundledSourceEntry {
    pub(crate) name: &'static str,
    pub(crate) manifest_yaml: &'static str,
    pub(crate) assets: &'static [BundledSourceAsset],
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BundledSourceAsset {
    pub(crate) relative_path: &'static str,
    pub(crate) bytes: &'static [u8],
}

#[derive(Debug, Clone)]
pub(crate) struct BundledSourceManifest {
    pub(crate) manifest_yaml: String,
    pub(crate) assets: Vec<BundledSourceAsset>,
}

#[derive(Debug, Clone)]
pub(crate) struct InstalledSourceManifest {
    pub(crate) source_spec: SourceSpec,
    pub(crate) candidate: CandidateSource,
    pub(crate) manifest_yaml: String,
}

pub(crate) fn list_bundled_sources(
    installed_source_names: &BTreeSet<SourceName>,
) -> Result<Vec<CandidateSource>, AppError> {
    let mut candidates = BUNDLED_SOURCES
        .iter()
        .map(|entry| {
            let bundled_name = SourceName::parse(entry.name)?;
            let mut candidate = describe_manifest(
                entry.manifest_yaml,
                SourceOrigin::Bundled,
                installed_source_names.contains(&bundled_name),
            )?;
            candidate.name = bundled_name;
            Ok(candidate)
        })
        .collect::<Result<Vec<_>, AppError>>()?;
    candidates.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(candidates)
}

pub(crate) fn load_bundled_source(name: &SourceName) -> Result<BundledSourceManifest, AppError> {
    let Some(entry) = BUNDLED_SOURCES
        .iter()
        .find(|candidate| candidate.name == name.as_str())
    else {
        return Err(AppError::InvalidInput(format!(
            "unknown bundled source '{name}'"
        )));
    };
    Ok(BundledSourceManifest {
        manifest_yaml: entry.manifest_yaml.to_string(),
        assets: entry.assets.to_vec(),
    })
}

/// Resolve the effective installed manifest and verify it still matches the
/// installed source identity in app state.
pub(crate) fn resolve_installed_manifest(
    workspace_name: &WorkspaceName,
    source: &InstalledSource,
    layout: &AppStateLayout,
) -> Result<InstalledSourceManifest, AppError> {
    let manifest_yaml = match source.origin {
        SourceOrigin::Bundled => {
            let persisted = layout.manifest_file(workspace_name, &source.name);
            if persisted.exists() {
                std::fs::read_to_string(persisted)?
            } else {
                load_bundled_source(&source.name)?.manifest_yaml
            }
        }
        SourceOrigin::Imported => {
            std::fs::read_to_string(layout.manifest_file(workspace_name, &source.name))?
        }
    };
    let source_spec = parse_source_manifest_yaml(&manifest_yaml)
        .map_err(|error| AppError::InvalidInput(error.to_string()))?;
    let mut candidate = candidate_from_manifest(&source_spec, source.origin, false)?;
    if candidate.name != source.name {
        return Err(AppError::FailedPrecondition(format!(
            "installed source '{}' does not match manifest name '{}'",
            source.name, candidate.name
        )));
    }
    candidate.installed = true;
    candidate.credential_storage = source.credential_storage_for_material();
    Ok(InstalledSourceManifest {
        source_spec,
        candidate,
        manifest_yaml,
    })
}

pub(crate) fn describe_manifest(
    manifest_yaml: &str,
    origin: SourceOrigin,
    installed: bool,
) -> Result<CandidateSource, AppError> {
    let manifest = parse_source_manifest_yaml(manifest_yaml)
        .map_err(|error| AppError::InvalidInput(error.to_string()))?;
    candidate_from_manifest(&manifest, origin, installed)
}

fn candidate_from_manifest(
    manifest: &SourceSpec,
    origin: SourceOrigin,
    installed: bool,
) -> Result<CandidateSource, AppError> {
    Ok(CandidateSource {
        name: SourceName::parse(&manifest.name)?,
        description: manifest.description.clone(),
        version: None,
        interface_ids: manifest
            .interfaces
            .iter()
            .map(|interface| interface.id().to_string())
            .collect(),
        inputs: manifest.declared_inputs.clone(),
        installed,
        origin,
        credential_storage: None,
    })
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::indexing_slicing,
        reason = "manifest input order assertions intentionally fail loudly in tests"
    )]

    use std::collections::BTreeSet;

    use coral_spec::ManifestInputKind;

    use super::{describe_manifest, list_bundled_sources, load_bundled_source};
    use crate::sources::SourceName;
    use crate::sources::model::SourceOrigin;

    #[test]
    fn bundled_sources_catalog_includes_active_source_specs() {
        let sources = list_bundled_sources(&BTreeSet::new()).expect("bundled sources");
        assert!(
            sources
                .iter()
                .any(|source| source.name.as_str() == "github")
        );
    }

    #[test]
    fn community_sources_are_not_bundled() {
        let hn = SourceName::parse("hn").expect("source");
        let error = load_bundled_source(&hn).expect_err("community source should not be bundled");
        assert!(error.to_string().contains("unknown bundled source 'hn'"));
    }

    #[test]
    fn core_v4_preview_sources_are_not_bundled() {
        let github_v4 = SourceName::parse("github_v4").expect("source");

        let error = load_bundled_source(&github_v4).expect_err("v4 source should not be bundled");
        assert!(error.to_string().contains("unknown bundled source"));
    }

    #[test]
    fn describe_manifest_extracts_declared_inputs() {
        let source = describe_manifest(
            r"
name: demo
spec_version: 1
kind: source
inputs:
  - key: API_BASE
    kind: variable
    default: https://example.com
  - key: API_TOKEN
    kind: secret
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
    auth:
      kind: bearer_input
      key: API_TOKEN
",
            SourceOrigin::Imported,
            false,
        )
        .expect("describe manifest");
        assert_eq!(source.inputs.len(), 2);
        assert_eq!(source.inputs[0].key, "API_BASE");
        assert_eq!(source.inputs[0].kind, ManifestInputKind::Variable);
        assert_eq!(source.inputs[1].key, "API_TOKEN");
        assert_eq!(source.inputs[1].kind, ManifestInputKind::Secret);
    }

    #[test]
    fn describe_manifest_rejects_removed_schema_field() {
        let error = describe_manifest(
            r"
name: demo
schema: demo
spec_version: 1
kind: source
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
",
            SourceOrigin::Imported,
            false,
        )
        .expect_err("removed schema field should fail");
        let message = error.to_string();
        assert!(message.starts_with("invalid input: source manifest failed schema validation:"));
        assert!(message.contains("'schema'"));
    }
}
