use serde_json::{Map, Value};

use crate::{ManifestError, Result};

/// Metadata Coral extracts from an `OpenAPI` source document.
#[derive(Debug, Clone, Default)]
pub struct OpenApiDocumentMetadata {
    /// The trimmed `info.description`, when present.
    pub description: Option<String>,
    /// The first resolvable `servers[].url`, with `OpenAPI` variable defaults applied.
    pub server_url: Option<String>,
}

/// Parses an `OpenAPI` JSON/YAML document and serializes it as normalized YAML.
pub fn normalize_openapi_document(bytes: &[u8]) -> Result<String> {
    let value: Value = serde_yaml::from_slice(bytes).map_err(ManifestError::parse_yaml)?;
    serde_yaml::to_string(&value).map_err(ManifestError::parse_yaml)
}

/// Extracts metadata from an `OpenAPI` JSON/YAML document.
pub fn openapi_document_metadata(document_bytes: &[u8]) -> Result<OpenApiDocumentMetadata> {
    let document: Value =
        serde_yaml::from_slice(document_bytes).map_err(ManifestError::parse_yaml)?;
    openapi_document_metadata_from_value(&document)
}

/// Extracts metadata from an already-parsed `OpenAPI` document value.
pub fn openapi_document_metadata_from_value(document: &Value) -> Result<OpenApiDocumentMetadata> {
    let openapi = document
        .get("openapi")
        .and_then(Value::as_str)
        .ok_or_else(|| ManifestError::validation("OpenAPI document is missing openapi version"))?;
    if !openapi.starts_with("3.0.") {
        return Err(ManifestError::validation(format!(
            "OpenAPI document uses unsupported version '{openapi}'"
        )));
    }
    Ok(OpenApiDocumentMetadata {
        description: trimmed_string_at(document, &["info", "description"]),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_document_metadata() {
        let metadata = openapi_document_metadata(
            r"
openapi: 3.0.3
info:
  title: Demo
  description: Query demo data.
servers:
  - url: https://api.example.com/v1
paths: {}
"
            .as_bytes(),
        )
        .expect("metadata");

        assert_eq!(metadata.description.as_deref(), Some("Query demo data."));
        assert_eq!(
            metadata.server_url.as_deref(),
            Some("https://api.example.com/v1")
        );
    }

    #[test]
    fn extracts_server_url_with_variable_defaults() {
        let metadata = openapi_document_metadata(
            r"
openapi: 3.0.1
info:
  title: StatusGator
  version: v3
servers:
  - url: https://{defaultHost}/api/v3
    variables:
      defaultHost:
        default: statusgator.com
paths: {}
"
            .as_bytes(),
        )
        .expect("metadata");

        assert_eq!(
            metadata.server_url.as_deref(),
            Some("https://statusgator.com/api/v3")
        );
    }
}
