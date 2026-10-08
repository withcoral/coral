use serde_json::{Map, Value};

use crate::{ManifestError, Result};

pub fn normalize_source_document(bytes: &[u8]) -> Result<String> {
    let value: Value = serde_yaml::from_slice(bytes).map_err(ManifestError::parse_yaml)?;
    serde_yaml::to_string(&value).map_err(ManifestError::serialize_yaml)
}

#[derive(Debug, Clone, Default)]
pub struct OpenApiDocumentMetadata {
    pub description: Option<String>,
    pub server_url: Option<String>,
}

/// Accepts the `OpenAPI` versions the importer reads, and rejects the rest.
///
/// 3.0 and 3.1 share one code path rather than one per version. The 3.1 changes
/// this importer can even observe are how nullability is spelled — a `null`
/// member in a `type` array, or a union whose only other variant is `null` —
/// and [`super::normalize::normalized_schema`] rewrites both into the 3.0 forms
/// as each schema is read. Everything downstream is version-agnostic, so there
/// is nothing left for a version branch to decide.
///
/// The two spellings also coexist in the wild: `OpenAI` publishes a `3.1.0`
/// document that still uses the 3.0 `nullable` keyword in over a hundred
/// places. Dispatching on the declared version would read those as errors in a
/// document that imports correctly today.
pub(super) fn validate_supported_openapi_version(document: &Value) -> Result<()> {
    let openapi = document
        .get("openapi")
        .and_then(Value::as_str)
        .ok_or_else(|| ManifestError::validation("OpenAPI document is missing openapi version"))?;
    if !(openapi.starts_with("3.0.") || openapi.starts_with("3.1.")) {
        return Err(ManifestError::validation(format!(
            "OpenAPI document uses unsupported version '{openapi}'"
        )));
    }
    Ok(())
}

pub fn openapi_document_metadata(document_bytes: &[u8]) -> Result<OpenApiDocumentMetadata> {
    let document: Value =
        serde_yaml::from_slice(document_bytes).map_err(ManifestError::parse_yaml)?;
    validate_supported_openapi_version(&document)?;
    // Deliberately not normalized: this reads `info` and `servers` only, and
    // neither carries a schema.
    Ok(OpenApiDocumentMetadata {
        description: trimmed_string_at(&document, &["info", "description"]),
        server_url: document
            .get("servers")
            .and_then(Value::as_array)
            .and_then(|servers| servers.iter().find_map(openapi_server_url)),
    })
}

fn openapi_server_url(server: &Value) -> Option<String> {
    let url = server
        .get("url")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|url| !url.is_empty())?;
    let variables = server.get("variables").and_then(Value::as_object);
    resolve_openapi_server_url(url, variables)
}

fn resolve_openapi_server_url(url: &str, variables: Option<&Map<String, Value>>) -> Option<String> {
    let mut resolved = String::with_capacity(url.len());
    let mut rest = url;
    while let Some((literal, after_open)) = rest.split_once('{') {
        resolved.push_str(literal);
        let (name, after_close) = after_open.split_once('}')?;
        let default = variables?.get(name)?.get("default")?.as_str()?.trim();
        if default.is_empty() {
            return None;
        }
        resolved.push_str(default);
        rest = after_close;
    }
    resolved.push_str(rest);
    Some(resolved)
}

fn trimmed_string_at(document: &Value, path: &[&str]) -> Option<String> {
    let value = path
        .iter()
        .try_fold(document, |value, key| value.get(*key))?;
    value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}
