use serde_json::{Map, Value};

use crate::{ManifestError, Result};

pub fn normalize_source_document(bytes: &[u8]) -> Result<String> {
    let value: Value = serde_yaml::from_slice(bytes).map_err(ManifestError::parse_yaml)?;
    serde_yaml::to_string(&value).map_err(ManifestError::parse_yaml)
}

#[derive(Debug, Clone, Default)]
pub struct OpenApiDocumentMetadata {
    pub description: Option<String>,
    pub server_url: Option<String>,
}

pub fn openapi_document_metadata(document_bytes: &[u8]) -> Result<OpenApiDocumentMetadata> {
    let document: Value =
        serde_yaml::from_slice(document_bytes).map_err(ManifestError::parse_yaml)?;
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
