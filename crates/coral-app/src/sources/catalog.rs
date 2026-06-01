//! Bundled source catalog and installed-manifest resolution helpers.

use std::collections::BTreeSet;

use coral_spec::{ValidatedSourceManifest, parse_source_manifest_yaml};

use crate::bootstrap::AppError;
use crate::features::{Feature, Features};
use crate::sources::SourceName;
use crate::sources::model::{CandidateSource, InstalledSource, SourceOrigin};
use crate::state::AppStateLayout;
use crate::workspaces::WorkspaceName;

include!(concat!(env!("OUT_DIR"), "/bundled_sources.rs"));

#[derive(Debug, Clone)]
pub(crate) struct BundledSourceManifest {
    pub(crate) manifest_yaml: String,
    pub(crate) descriptors: &'static [BundledV4Descriptor],
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BundledSourceEntry {
    pub(crate) name: &'static str,
    pub(crate) manifest_yaml: &'static str,
    pub(crate) descriptors: &'static [BundledV4Descriptor],
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct BundledV4Descriptor {
    pub(crate) surface_id: &'static str,
    pub(crate) bytes: &'static [u8],
}

#[derive(Debug, Clone)]
pub(crate) struct InstalledSourceManifest {
    pub(crate) source_spec: ValidatedSourceManifest,
    pub(crate) candidate: CandidateSource,
    pub(crate) manifest_yaml: String,
}

pub(crate) fn list_bundled_sources(
    installed_source_names: &BTreeSet<SourceName>,
    features: &Features,
) -> Result<Vec<CandidateSource>, AppError> {
    let mut candidates = enabled_bundled_sources(features)
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

pub(crate) fn load_bundled_source(
    name: &SourceName,
    features: &Features,
) -> Result<BundledSourceManifest, AppError> {
    let Some(entry) = enabled_bundled_sources(features)
        .into_iter()
        .find(|entry| entry.name == name.as_str())
    else {
        return Err(AppError::InvalidInput(format!(
            "unknown bundled source '{name}'"
        )));
    };
    Ok(BundledSourceManifest {
        manifest_yaml: entry.manifest_yaml.to_string(),
        descriptors: entry.descriptors,
    })
}

/// Resolve the effective installed manifest and verify it still matches the
/// installed source identity in app state.
pub(crate) fn resolve_installed_manifest(
    workspace_name: &WorkspaceName,
    source: &InstalledSource,
    layout: &AppStateLayout,
    features: &Features,
) -> Result<InstalledSourceManifest, AppError> {
    let manifest_yaml = match source.origin {
        SourceOrigin::Bundled => load_bundled_source(&source.name, features)?.manifest_yaml,
        SourceOrigin::Imported => {
            std::fs::read_to_string(layout.manifest_file(workspace_name, &source.name))?
        }
    };
    let source_spec = parse_source_manifest_yaml(&manifest_yaml)
        .map_err(|error| AppError::InvalidInput(error.to_string()))?;
    ensure_manifest_allowed(&source_spec, features)?;
    let mut candidate = candidate_from_manifest(&source_spec, source.origin, false)?;
    if candidate.name != source.name {
        return Err(AppError::FailedPrecondition(format!(
            "installed source '{}' does not match manifest name '{}'",
            source.name, candidate.name
        )));
    }
    candidate.installed = true;
    candidate.credential_storage = Some(source.effective_credential_storage());
    Ok(InstalledSourceManifest {
        source_spec,
        candidate,
        manifest_yaml,
    })
}

pub(crate) fn ensure_manifest_allowed(
    manifest: &ValidatedSourceManifest,
    features: &Features,
) -> Result<(), AppError> {
    if manifest.as_v4().is_some() && !features.enabled(Feature::DslV4) {
        return Err(dsl_v4_feature_required_error(&SourceName::parse(
            manifest.schema_name(),
        )?));
    }
    Ok(())
}

pub(crate) fn dsl_v4_feature_required_error(source_name: &SourceName) -> AppError {
    AppError::FailedPrecondition(format!(
        "source '{source_name}' requires experimental feature 'dsl_v4'; run `coral features enable dsl_v4`"
    ))
}

pub(crate) fn is_dsl_v4_feature_required_error(error: &AppError) -> bool {
    matches!(error, AppError::FailedPrecondition(message) if message.contains("requires experimental feature 'dsl_v4'"))
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

fn enabled_bundled_sources(_features: &Features) -> Vec<&'static BundledSourceEntry> {
    BUNDLED_SOURCES.iter().collect::<Vec<_>>()
}

fn candidate_from_manifest(
    manifest: &ValidatedSourceManifest,
    origin: SourceOrigin,
    installed: bool,
) -> Result<CandidateSource, AppError> {
    Ok(CandidateSource {
        name: SourceName::parse(manifest.schema_name())?,
        description: manifest.description().to_string(),
        version: manifest.source_version().to_string(),
        inputs: manifest.declared_inputs().to_vec(),
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
    use crate::features::{Feature, FeatureOverrides, Features};
    use crate::sources::SourceName;
    use crate::sources::model::SourceOrigin;

    #[test]
    fn bundled_sources_load_through_catalog() {
        let sources =
            list_bundled_sources(&BTreeSet::new(), &Features::default()).expect("bundled sources");
        assert!(!sources.is_empty());
        assert!(
            sources
                .iter()
                .any(|source| source.name == SourceName::parse("github").expect("source"))
        );
        assert!(
            sources
                .iter()
                .any(|source| source.name == SourceName::parse("stripe").expect("source"))
        );
        assert!(sources.iter().all(|source| !source.version.is_empty()));
    }

    #[test]
    fn community_sources_are_not_bundled() {
        let hn = SourceName::parse("hn").expect("source");
        let error = load_bundled_source(&hn, &Features::default())
            .expect_err("community source should not be bundled");
        assert!(error.to_string().contains("unknown bundled source 'hn'"));
    }

    #[test]
    fn core_v4_preview_sources_are_not_bundled() {
        let github_v4 = SourceName::parse("github_v4").expect("source");

        let error = load_bundled_source(&github_v4, &Features::default())
            .expect_err("v4 source should not be bundled");
        assert!(error.to_string().contains("unknown bundled source"));

        let mut overrides = FeatureOverrides::default();
        overrides.set(Feature::DslV4, true);
        let mut features = Features::default();
        features.apply_overrides(&overrides);
        let error = load_bundled_source(&github_v4, &features)
            .expect_err("feature flag should not make preview sources bundled");
        assert!(error.to_string().contains("unknown bundled source"));
    }

    #[test]
    fn describe_manifest_extracts_declared_inputs() {
        let source = describe_manifest(
            r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  API_BASE:
    kind: variable
    default: https://example.com
  API_TOKEN:
    kind: secret
base_url: "{{input.API_BASE}}"
auth:
  type: HeaderAuth
  headers:
    - name: Authorization
      from: template
      template: Bearer {{input.API_TOKEN}}
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
            SourceOrigin::Imported,
            false,
        )
        .expect_err("legacy schema field should fail");
        let message = error.to_string();
        assert!(message.starts_with("invalid input: source manifest failed schema validation:"));
        assert!(message.contains("'schema'"));
    }
}
