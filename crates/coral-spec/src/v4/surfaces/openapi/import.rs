use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::Read as _;
use std::path::Path;
use std::time::Duration;

use serde_json::Value;
use url::Url;

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{IrType, SemanticIr};
use crate::v4::manifest::{SurfaceDescriptor, V4SourceManifest, V4Surface};
use crate::v4::surfaces::json_schema::{RefError, resolve_local_ref};
use crate::v4::{OPENAPI_IMPORTER_VERSION, V4_ARTIFACT_SCHEMA_VERSION};
use crate::{ManifestError, Result};

const ORIGINAL_REF_EXTENSION: &str = "x-coral-original-ref";
const EXTERNAL_REF_FETCH_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_EXTERNAL_REF_BYTES: u64 = 16 * 1024 * 1024;
const EXTERNAL_REF_USER_AGENT: &str = "coral-openapi-importer";

pub fn import_openapi_surface(
    manifest: &V4SourceManifest,
    surface: &V4Surface,
    document_bytes: &[u8],
) -> Result<SemanticIr> {
    let mut document: Value =
        serde_yaml::from_slice(document_bytes).map_err(ManifestError::parse_yaml)?;
    let openapi = document
        .get("openapi")
        .and_then(Value::as_str)
        .ok_or_else(|| ManifestError::validation("OpenAPI document is missing openapi version"))?;
    if !openapi.starts_with("3.0.") {
        return Err(ManifestError::validation(format!(
            "OpenAPI document for surface '{}' uses unsupported version '{openapi}'",
            surface.id
        )));
    }

    annotate_ref_sites(&mut document);
    let document = dereference_openapi_document(surface, &document)?;
    let mut importer = OpenApiImporter::new(manifest, surface, &document);
    importer.import()
}

fn annotate_ref_sites(value: &mut Value) {
    match value {
        Value::Object(object) => {
            if let Some(reference) = object
                .get("$ref")
                .and_then(Value::as_str)
                .map(str::to_string)
            {
                object.insert(ORIGINAL_REF_EXTENSION.to_string(), Value::String(reference));
            }
            for value in object.values_mut() {
                annotate_ref_sites(value);
            }
        }
        Value::Array(values) => {
            for value in values {
                annotate_ref_sites(value);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn dereference_openapi_document(surface: &V4Surface, document: &Value) -> Result<Value> {
    let base_uri = openapi_document_base_uri(surface)?;
    let parsed_descriptor_url = Url::parse(&base_uri).map_err(|error| {
        ManifestError::validation(format!(
            "OpenAPI descriptor base URI '{base_uri}' for surface '{}' is invalid: {error}",
            surface.id
        ))
    })?;
    let mut seen = BTreeSet::new();
    let mut external_documents = BTreeMap::new();
    collect_external_ref_documents(
        &parsed_descriptor_url,
        document,
        &mut seen,
        &mut external_documents,
    )
    .map_err(|error| {
        ManifestError::validation(format!(
            "failed to load OpenAPI external references for surface '{}': {error}",
            surface.id
        ))
    })?;
    if external_documents.is_empty() {
        return Ok(document.clone());
    }

    let mut registry = jsonschema::Registry::new();
    for (uri, document) in external_documents {
        registry = registry.add(uri, document).map_err(|error| {
            ManifestError::validation(format!(
                "failed to register OpenAPI external reference for surface '{}': {error}",
                surface.id
            ))
        })?;
    }
    let registry = registry.prepare().map_err(|error| {
        ManifestError::validation(format!(
            "failed to prepare OpenAPI external reference registry for surface '{}': {error}",
            surface.id
        ))
    })?;

    jsonschema::options()
        .with_base_uri(base_uri)
        .with_registry(&registry)
        .dereference(document)
        .map_err(|error| {
            ManifestError::validation(format!(
                "failed to dereference OpenAPI document for surface '{}': {error}",
                surface.id
            ))
        })
}

fn collect_external_ref_documents(
    base_uri: &Url,
    value: &Value,
    seen: &mut BTreeSet<String>,
    external_documents: &mut BTreeMap<String, Value>,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match value {
        Value::Object(object) => {
            if let Some(reference) = object.get("$ref").and_then(Value::as_str)
                && let Some(document_uri) = external_ref_document_uri(base_uri, reference)?
            {
                let document_uri_string = document_uri.to_string();
                if seen.insert(document_uri_string.clone()) {
                    let mut document = retrieve_external_ref_document(&document_uri)?;
                    annotate_ref_sites(&mut document);
                    collect_external_ref_documents(
                        &document_uri,
                        &document,
                        seen,
                        external_documents,
                    )?;
                    external_documents.insert(document_uri_string, document);
                }
            }
            for value in object.values() {
                collect_external_ref_documents(base_uri, value, seen, external_documents)?;
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_external_ref_documents(base_uri, value, seen, external_documents)?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(())
}

fn external_ref_document_uri(
    base_uri: &Url,
    reference: &str,
) -> std::result::Result<Option<Url>, Box<dyn std::error::Error + Send + Sync>> {
    if reference.starts_with('#') {
        return Ok(None);
    }
    let mut resolved = base_uri.join(reference)?;
    resolved.set_fragment(None);
    let mut base_document_uri = base_uri.clone();
    base_document_uri.set_fragment(None);
    Ok((resolved != base_document_uri).then_some(resolved))
}

fn openapi_document_base_uri(surface: &V4Surface) -> Result<String> {
    match &surface.descriptor {
        SurfaceDescriptor::Url { url } => Ok(url.clone()),
        SurfaceDescriptor::File { file } => file_descriptor_base_uri(file),
        SurfaceDescriptor::McpServer { .. } => Err(ManifestError::validation(format!(
            "DSL v4 MCP surface '{}' does not have an OpenAPI descriptor",
            surface.id
        ))),
    }
}

fn file_descriptor_base_uri(file: &Path) -> Result<String> {
    Url::from_file_path(file)
        .map(|url| url.to_string())
        .map_err(|()| {
            ManifestError::validation(format!(
                "OpenAPI descriptor '{}' could not be converted to a file URI",
                file.display()
            ))
        })
}

fn retrieve_external_ref_document(
    uri: &Url,
) -> std::result::Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    match uri.scheme() {
        "https" => retrieve_https_external_ref(uri),
        "file" => retrieve_file_external_ref(uri),
        "http" => Err(std::io::Error::other(format!(
            "OpenAPI external reference '{uri}' must use HTTPS"
        ))
        .into()),
        scheme => Err(std::io::Error::other(format!(
            "unsupported OpenAPI external reference scheme '{scheme}' for '{uri}'"
        ))
        .into()),
    }
}

fn retrieve_https_external_ref(
    uri: &Url,
) -> std::result::Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let uri = uri.to_string();
    let panic_uri = uri.clone();
    std::thread::spawn(move || retrieve_https_external_ref_on_blocking_thread(&uri))
        .join()
        .map_err(|_panic| {
            std::io::Error::other(format!(
                "failed to fetch OpenAPI external reference '{panic_uri}': fetch thread panicked"
            ))
        })?
}

fn retrieve_file_external_ref(
    uri: &Url,
) -> std::result::Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let path = uri.to_file_path().map_err(|()| {
        std::io::Error::other(format!(
            "OpenAPI external reference '{uri}' is not a valid file URI"
        ))
    })?;
    let metadata = std::fs::symlink_metadata(&path)?;
    if metadata.file_type().is_symlink() {
        return Err(std::io::Error::other(format!(
            "OpenAPI external reference '{}' must not be a symlink",
            path.display()
        ))
        .into());
    }
    if !metadata.file_type().is_file() {
        return Err(std::io::Error::other(format!(
            "OpenAPI external reference '{}' must be a regular file",
            path.display()
        ))
        .into());
    }
    if metadata.len() > MAX_EXTERNAL_REF_BYTES {
        return Err(std::io::Error::other(format!(
            "OpenAPI external reference '{}' is too large: {} bytes exceeds {MAX_EXTERNAL_REF_BYTES}",
            path.display(),
            metadata.len()
        ))
        .into());
    }
    let bytes = std::fs::read(&path)?;
    parse_external_ref_document(uri.as_str(), &bytes)
}

fn retrieve_https_external_ref_on_blocking_thread(
    uri: &str,
) -> std::result::Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    let client = reqwest::blocking::Client::builder()
        .timeout(EXTERNAL_REF_FETCH_TIMEOUT)
        .https_only(true)
        .redirect(reqwest::redirect::Policy::limited(5))
        .user_agent(EXTERNAL_REF_USER_AGENT)
        .build()?;
    let mut response = client.get(uri).send()?;
    if response.url().scheme() != "https" {
        return Err(std::io::Error::other(format!(
            "OpenAPI external reference '{uri}' redirected to non-HTTPS URL '{}'",
            response.url()
        ))
        .into());
    }
    if !response.status().is_success() {
        return Err(std::io::Error::other(format!(
            "failed to fetch OpenAPI external reference '{uri}': HTTP {}",
            response.status()
        ))
        .into());
    }
    if let Some(length) = response.content_length()
        && length > MAX_EXTERNAL_REF_BYTES
    {
        return Err(std::io::Error::other(format!(
                "OpenAPI external reference '{uri}' is too large: {length} bytes exceeds {MAX_EXTERNAL_REF_BYTES}"
            ))
            .into());
    }
    let mut bytes = Vec::new();
    let mut limited = response.by_ref().take(MAX_EXTERNAL_REF_BYTES + 1);
    limited.read_to_end(&mut bytes)?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_EXTERNAL_REF_BYTES {
        return Err(std::io::Error::other(format!(
                "OpenAPI external reference '{uri}' is too large: exceeds {MAX_EXTERNAL_REF_BYTES} bytes"
            ))
            .into());
    }
    parse_external_ref_document(uri, &bytes)
}

fn parse_external_ref_document(
    uri: &str,
    bytes: &[u8],
) -> std::result::Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    serde_yaml::from_slice(bytes).map_err(|error| {
        std::io::Error::other(format!(
            "failed to parse OpenAPI external reference '{uri}' as YAML/JSON: {error}"
        ))
        .into()
    })
}

pub(super) fn original_ref(value: &Value) -> Option<&str> {
    value
        .get("$ref")
        .or_else(|| value.get(ORIGINAL_REF_EXTENSION))
        .and_then(Value::as_str)
}

pub(super) struct OpenApiImporter<'a> {
    pub(super) manifest: &'a V4SourceManifest,
    pub(super) surface: &'a V4Surface,
    pub(super) document: &'a Value,
    pub(super) types: BTreeMap<String, IrType>,
    pub(super) diagnostics: Vec<Diagnostic>,
}

impl<'a> OpenApiImporter<'a> {
    fn new(manifest: &'a V4SourceManifest, surface: &'a V4Surface, document: &'a Value) -> Self {
        Self {
            manifest,
            surface,
            document,
            types: BTreeMap::new(),
            diagnostics: Vec::new(),
        }
    }

    fn import(&mut self) -> Result<SemanticIr> {
        let paths = self
            .document
            .get("paths")
            .and_then(Value::as_object)
            .ok_or_else(|| ManifestError::validation("OpenAPI document is missing paths"))?;
        let mut operations = Vec::new();
        let mut operation_ids = HashSet::new();
        for (path, path_item) in paths {
            let Some(path_item) = path_item.as_object() else {
                continue;
            };
            for method_name in [
                "get", "head", "options", "post", "put", "patch", "delete", "trace",
            ] {
                let Some(operation_value) = path_item.get(method_name) else {
                    continue;
                };
                let operation =
                    self.import_operation(path, path_item, method_name, operation_value)?;
                if !operation_ids.insert(operation.id.clone()) {
                    return Err(ManifestError::validation(format!(
                        "source '{}' surface '{}' imports duplicate operation id '{}'",
                        self.manifest.common.name, self.surface.id, operation.id
                    )));
                }
                operations.push(operation);
            }
        }
        Ok(SemanticIr {
            artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
            source_name: self.manifest.common.name.clone(),
            surface_id: self.surface.id.clone(),
            surface_type: self.surface.surface_type,
            importer_version: OPENAPI_IMPORTER_VERSION.to_string(),
            operations,
            types: self.types.values().cloned().collect(),
            diagnostics: self.diagnostics.clone(),
        })
    }

    pub(super) fn resolve_ref(
        &self,
        value: &Value,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Option<Value> {
        match resolve_local_ref(self.document, value) {
            Ok(resolved) => Some(resolved.clone()),
            Err(RefError::External(reference)) => {
                diagnostics.push(Diagnostic::warning(
                    "OPENAPI_EXTERNAL_REF_UNSUPPORTED",
                    format!("external reference '{reference}' is unsupported"),
                    self.surface.id.clone(),
                    Some(operation_id.to_string()),
                ));
                None
            }
            Err(RefError::NotFound(reference)) => {
                diagnostics.push(Diagnostic::warning(
                    "OPENAPI_REF_NOT_FOUND",
                    format!("reference '{reference}' was not found"),
                    self.surface.id.clone(),
                    Some(operation_id.to_string()),
                ));
                None
            }
        }
    }
}
