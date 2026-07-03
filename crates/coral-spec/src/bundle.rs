//! Multi-document manifest bundle parsing.

use std::collections::HashSet;

use serde::Deserialize as _;
use serde_json::Value;

use crate::{
    IdentityManifest, ManifestError, Result, ValidatedSourceManifest,
    parse_identity_manifest_value, parse_source_manifest_value,
};

/// One identity spec document parsed from a manifest bundle.
#[derive(Debug, Clone)]
pub struct IdentityManifestDocument {
    /// Canonical YAML for this identity spec document.
    pub manifest_yaml: String,
    /// Validated identity spec.
    pub manifest: IdentityManifest,
}

/// A source manifest plus any identity specs authored in the same YAML bundle.
#[derive(Debug, Clone)]
pub struct ManifestBundle {
    /// Canonical YAML for the source document only.
    pub source_manifest_yaml: String,
    /// Validated source manifest.
    pub source_manifest: ValidatedSourceManifest,
    /// Validated identity spec documents from the bundle.
    pub identity_manifests: Vec<IdentityManifestDocument>,
}

/// Parses a YAML file that may contain one source manifest and zero or more
/// identity specs separated by `---`.
///
/// Single-document source manifests keep the same acceptance behavior as
/// [`crate::parse_source_manifest_yaml`]. Multi-document files must contain
/// exactly one source manifest document. Identity spec documents are selected
/// with `kind: identity`; any other explicit `kind` fails closed.
pub fn parse_manifest_bundle_yaml(raw: &str) -> Result<ManifestBundle> {
    let mut source = None;
    let mut identities = Vec::new();
    let mut identity_names = HashSet::new();

    for (index, document) in serde_yaml::Deserializer::from_str(raw).enumerate() {
        let value = Value::deserialize(document).map_err(ManifestError::parse_yaml)?;
        if value.is_null() {
            continue;
        }
        let document_number = index + 1;
        match manifest_document_kind(&value, document_number)? {
            ManifestDocumentKind::Source => {
                if source.is_some() {
                    return Err(ManifestError::validation(
                        "manifest bundle must contain exactly one source manifest document",
                    ));
                }
                let manifest = parse_source_manifest_value(value.clone())?;
                source = Some((serialize_document_yaml(&value)?, manifest));
            }
            ManifestDocumentKind::Identity => {
                let manifest = parse_identity_manifest_value(value.clone())?;
                if !identity_names.insert(manifest.name.clone()) {
                    return Err(ManifestError::validation(format!(
                        "manifest bundle declares identity spec '{}' more than once",
                        manifest.name
                    )));
                }
                identities.push(IdentityManifestDocument {
                    manifest_yaml: serialize_document_yaml(&value)?,
                    manifest,
                });
            }
        }
    }

    let Some((source_manifest_yaml, source_manifest)) = source else {
        return Err(ManifestError::validation(
            "manifest bundle must contain exactly one source manifest document",
        ));
    };

    Ok(ManifestBundle {
        source_manifest_yaml,
        source_manifest,
        identity_manifests: identities,
    })
}

enum ManifestDocumentKind {
    Source,
    Identity,
}

fn manifest_document_kind(value: &Value, document_number: usize) -> Result<ManifestDocumentKind> {
    let Some(kind) = value.get("kind") else {
        return Ok(ManifestDocumentKind::Source);
    };
    match kind.as_str() {
        Some("identity") => Ok(ManifestDocumentKind::Identity),
        Some(other) => Err(ManifestError::validation(format!(
            "manifest bundle document {document_number} has unsupported kind '{other}'"
        ))),
        None => Err(ManifestError::validation(format!(
            "manifest bundle document {document_number} kind must be a string"
        ))),
    }
}

fn serialize_document_yaml(value: &Value) -> Result<String> {
    serde_yaml::to_string(value).map_err(ManifestError::parse_yaml)
}

#[cfg(test)]
mod tests {
    use crate::{parse_identity_manifest_yaml, parse_source_manifest_yaml};

    use super::parse_manifest_bundle_yaml;

    fn identity_yaml(name: &str) -> String {
        format!(
            "kind: identity\nspec_version: 1\nname: {name}\nversion: 0.1.0\nissuer: github\ntype: fixed_token\naudience: {{host: api.github.com}}\n"
        )
    }

    fn source_yaml(name: &str) -> String {
        format!(
            "name: {name}\ndsl_version: 4\nidentity_requirements:\n  accepts:\n    - id: github_api\n      identity_specs: [github_oauth, github_pat]\n      audience: {{host: api.github.com}}\nsurface:\n  type: openapi\n  file: /tmp/github-openapi.yaml\n"
        )
    }

    #[test]
    fn parses_single_document_source_manifest_as_bundle() {
        let bundle = parse_manifest_bundle_yaml(&source_yaml("demo")).expect("bundle");

        assert_eq!(bundle.source_manifest.schema_name(), "demo");
        assert!(
            bundle
                .source_manifest
                .as_v4()
                .and_then(|manifest| manifest.identity_requirements.as_ref())
                .is_some(),
            "identity requirements do not require bundled specs at parse time"
        );
        assert!(bundle.identity_manifests.is_empty());
        assert!(bundle.source_manifest_yaml.contains("name: demo"));
    }

    #[test]
    fn parses_v4_source_with_multiple_identity_specs_and_canonicalizes_documents() {
        let raw = format!(
            "---\n\n---\n{}---\n{}---\nnull\n---\n{}",
            identity_yaml("github_oauth"),
            source_yaml("demo"),
            identity_yaml("github_pat")
        );

        let bundle = parse_manifest_bundle_yaml(&raw).expect("bundle");

        assert_eq!(bundle.source_manifest.schema_name(), "demo");
        assert_eq!(
            bundle
                .identity_manifests
                .iter()
                .map(|document| document.manifest.name.as_str())
                .collect::<Vec<_>>(),
            vec!["github_oauth", "github_pat"]
        );
        assert!(
            bundle
                .identity_manifests
                .iter()
                .all(|document| document.manifest_yaml.contains("kind: identity"))
        );

        let reparsed_source = parse_source_manifest_yaml(&bundle.source_manifest_yaml)
            .expect("canonical source document reparses");
        assert_eq!(reparsed_source.schema_name(), "demo");
        for document in &bundle.identity_manifests {
            assert_eq!(
                parse_identity_manifest_yaml(&document.manifest_yaml)
                    .expect("canonical identity document reparses"),
                document.manifest
            );
        }

        let canonical_bundle = std::iter::once(bundle.source_manifest_yaml.as_str())
            .chain(
                bundle
                    .identity_manifests
                    .iter()
                    .map(|document| document.manifest_yaml.as_str()),
            )
            .collect::<Vec<_>>()
            .join("---\n");
        let canonical_reparse = parse_manifest_bundle_yaml(&canonical_bundle)
            .expect("canonical bundle reparses idempotently");
        assert_eq!(
            canonical_reparse.source_manifest_yaml,
            bundle.source_manifest_yaml
        );
        assert_eq!(
            canonical_reparse
                .identity_manifests
                .iter()
                .map(|document| document.manifest_yaml.as_str())
                .collect::<Vec<_>>(),
            bundle
                .identity_manifests
                .iter()
                .map(|document| document.manifest_yaml.as_str())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn rejects_bundle_without_source_manifest() {
        let error = parse_manifest_bundle_yaml(&identity_yaml("github_oauth"))
            .expect_err("source document required");

        assert!(
            error
                .to_string()
                .contains("manifest bundle must contain exactly one source manifest document"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_duplicate_source_manifest() {
        let raw = format!("---\n{}---\n{}", source_yaml("demo"), source_yaml("other"));
        let error = parse_manifest_bundle_yaml(&raw).expect_err("one source document allowed");

        assert!(
            error
                .to_string()
                .contains("manifest bundle must contain exactly one source manifest document"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_duplicate_identity_spec_name() {
        let raw = format!(
            "---\n{}---\n{}---\n{}",
            source_yaml("demo"),
            identity_yaml("github_oauth"),
            identity_yaml("github_oauth")
        );
        let error = parse_manifest_bundle_yaml(&raw).expect_err("identity names must be unique");

        assert!(
            error
                .to_string()
                .contains("declares identity spec 'github_oauth' more than once"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_unsupported_or_non_string_explicit_kind() {
        for (raw, expected) in [
            ("kind: source\nname: demo", "unsupported kind 'source'"),
            ("kind: 1\nname: demo", "kind must be a string"),
            ("kind: null\nname: demo", "kind must be a string"),
        ] {
            let error = parse_manifest_bundle_yaml(raw).expect_err("kind should fail closed");
            assert!(
                error.to_string().contains(expected),
                "unexpected error: {error}"
            );
        }
    }

    #[test]
    fn delegates_document_validation_to_normal_manifest_parsers() {
        let invalid_source = "name: demo\ndsl_version: 4\nsurface: []\n";
        parse_manifest_bundle_yaml(invalid_source).expect_err("invalid source must fail");

        let invalid_identity = format!(
            "{}---\nkind: identity\nspec_version: 1\nname: broken\nversion: 0.1.0\nissuer: github\n",
            source_yaml("demo")
        );
        parse_manifest_bundle_yaml(&invalid_identity).expect_err("invalid identity must fail");
    }
}
