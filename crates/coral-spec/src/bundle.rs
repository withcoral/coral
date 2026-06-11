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

/// A source manifest plus any global identity specs authored in the same file.
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
    use super::parse_manifest_bundle_yaml;

    fn identity_yaml(name: &str) -> String {
        format!(
            "kind: identity\nspec_version: 1\nname: {name}\nversion: 0.1.0\nissuer: github\ntype: fixed_token\n"
        )
    }

    fn source_yaml(name: &str) -> String {
        format!(
            "name: {name}\nversion: 0.1.0\ndsl_version: 3\nbackend: http\nbase_url: https://example.com\ntables:\n  - name: users\n    description: Demo users\n    request: {{method: GET, path: /users}}\n    columns:\n      - {{name: id, type: Utf8}}\n"
        )
    }

    #[test]
    fn parses_single_document_source_manifest_as_bundle() {
        let bundle = parse_manifest_bundle_yaml(&source_yaml("demo")).expect("bundle");

        assert_eq!(bundle.source_manifest.schema_name(), "demo");
        assert!(bundle.identity_manifests.is_empty());
    }

    #[test]
    fn parses_source_with_multiple_identity_specs() {
        let raw = format!(
            "---\n{}---\n{}---\n{}",
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
    fn rejects_unsupported_explicit_kind() {
        let error =
            parse_manifest_bundle_yaml("kind: source\nname: demo").expect_err("unsupported kind");

        assert!(
            error.to_string().contains("unsupported kind 'source'"),
            "unexpected error: {error}"
        );
    }
}
