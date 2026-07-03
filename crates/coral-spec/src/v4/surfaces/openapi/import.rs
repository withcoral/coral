use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::Read as _;
use std::path::Path;
use std::time::Duration;

use serde_json::{Map, Value};
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
    let base_uri = openapi_document_base_uri(surface)?;
    import_openapi_surface_with_base_uri(manifest, surface, document_bytes, &base_uri)
}

pub fn import_openapi_surface_with_base_uri(
    manifest: &V4SourceManifest,
    surface: &V4Surface,
    document_bytes: &[u8],
    descriptor_base_uri: &str,
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
    let document = dereference_openapi_document(surface, descriptor_base_uri, &document)?;
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

fn dereference_openapi_document(
    surface: &V4Surface,
    descriptor_base_uri: &str,
    document: &Value,
) -> Result<Value> {
    let parsed_descriptor_url = Url::parse(descriptor_base_uri).map_err(|error| {
        ManifestError::validation(format!(
            "OpenAPI descriptor base URI '{descriptor_base_uri}' for surface '{}' is invalid: {error}",
            surface.id
        ))
    })?;
    let mut document = document.clone();
    normalize_same_document_refs(&parsed_descriptor_url, &mut document);
    let reachable_document = reachable_openapi_document(&document);
    let mut seen = BTreeSet::new();
    let mut external_documents = BTreeMap::new();
    collect_external_ref_documents(
        &parsed_descriptor_url,
        &reachable_document,
        &reachable_document,
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
        return Ok(document);
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
        .with_base_uri(descriptor_base_uri)
        .with_registry(&registry)
        .dereference(&reachable_document)
        .map_err(|error| {
            ManifestError::validation(format!(
                "failed to dereference OpenAPI document for surface '{}': {error}",
                surface.id
            ))
        })
}

fn collect_external_ref_documents(
    base_uri: &Url,
    document: &Value,
    value: &Value,
    seen: &mut BTreeSet<String>,
    external_documents: &mut BTreeMap<String, Value>,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    match value {
        Value::Object(object) => {
            if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
                collect_ref_documents(base_uri, document, reference, seen, external_documents)?;
            }
            for value in object.values() {
                collect_external_ref_documents(
                    base_uri,
                    document,
                    value,
                    seen,
                    external_documents,
                )?;
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_external_ref_documents(
                    base_uri,
                    document,
                    value,
                    seen,
                    external_documents,
                )?;
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
    Ok(())
}

fn collect_ref_documents(
    base_uri: &Url,
    document: &Value,
    reference: &str,
    seen: &mut BTreeSet<String>,
    external_documents: &mut BTreeMap<String, Value>,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let resolved_ref_uri = base_uri.join(reference)?;
    let resolved_ref_uri_string = resolved_ref_uri.to_string();
    if !seen.insert(resolved_ref_uri_string) {
        return Ok(());
    }
    let mut base_document_uri = base_uri.clone();
    base_document_uri.set_fragment(None);
    let mut document_uri = resolved_ref_uri.clone();
    document_uri.set_fragment(None);

    if document_uri == base_document_uri {
        let Ok(target) = ref_target(document, &resolved_ref_uri) else {
            return Ok(());
        };
        let target = target.clone();
        return collect_external_ref_documents(
            base_uri,
            document,
            &target,
            seen,
            external_documents,
        );
    }

    let document_uri_string = document_uri.to_string();
    if !external_documents.contains_key(&document_uri_string) {
        let mut document = retrieve_external_ref_document(base_uri, &document_uri)?;
        annotate_ref_sites(&mut document);
        normalize_same_document_refs(&document_uri, &mut document);
        external_documents.insert(document_uri_string.clone(), document);
    }
    let external_document = external_documents
        .get(&document_uri_string)
        .expect("external document was inserted")
        .clone();
    let target = ref_target(&external_document, &resolved_ref_uri)?.clone();
    collect_external_ref_documents(
        &document_uri,
        &external_document,
        &target,
        seen,
        external_documents,
    )
}

fn ref_target<'a>(
    document: &'a Value,
    resolved_ref_uri: &Url,
) -> std::result::Result<&'a Value, Box<dyn std::error::Error + Send + Sync>> {
    let Some(fragment) = resolved_ref_uri.fragment() else {
        return Ok(document);
    };
    if fragment.is_empty() {
        return Ok(document);
    }
    if !fragment.starts_with('/') {
        return Err(std::io::Error::other(format!(
            "OpenAPI reference '{resolved_ref_uri}' uses unsupported fragment '{fragment}'"
        ))
        .into());
    }
    document.pointer(fragment).ok_or_else(|| {
        std::io::Error::other(format!(
            "OpenAPI reference target '{resolved_ref_uri}' was not found"
        ))
        .into()
    })
}

fn reachable_openapi_document(document: &Value) -> Value {
    let mut reachable = Value::Object(Map::new());
    for key in ["openapi", "paths"] {
        if let Some(value) = document.get(key) {
            reachable
                .as_object_mut()
                .expect("reachable document is an object")
                .insert(key.to_string(), value.clone());
        }
    }

    let mut seen = BTreeSet::new();
    let mut pointers = BTreeSet::new();
    if let Some(paths) = document.get("paths") {
        collect_reachable_local_ref_pointers(document, paths, &mut seen, &mut pointers);
    }
    for pointer in pointers {
        if let Some(value) = document.pointer(&pointer) {
            insert_json_pointer(&mut reachable, &pointer, value.clone());
        }
    }

    reachable
}

fn collect_reachable_local_ref_pointers(
    document: &Value,
    value: &Value,
    seen: &mut BTreeSet<String>,
    pointers: &mut BTreeSet<String>,
) {
    match value {
        Value::Object(object) => {
            if let Some(reference) = object.get("$ref").and_then(Value::as_str)
                && let Some(pointer) = reference.strip_prefix('#')
                && pointer.starts_with('/')
                && seen.insert(pointer.to_string())
            {
                pointers.insert(pointer.to_string());
                if let Some(target) = document.pointer(pointer) {
                    collect_reachable_local_ref_pointers(document, target, seen, pointers);
                }
            }
            for value in object.values() {
                collect_reachable_local_ref_pointers(document, value, seen, pointers);
            }
        }
        Value::Array(values) => {
            for value in values {
                collect_reachable_local_ref_pointers(document, value, seen, pointers);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn insert_json_pointer(document: &mut Value, pointer: &str, value: Value) {
    let Some(pointer) = pointer.strip_prefix('/') else {
        *document = value;
        return;
    };
    let segments = pointer
        .split('/')
        .map(decode_json_pointer_segment)
        .collect::<Vec<_>>();
    insert_json_pointer_segments(document, &segments, value);
}

fn insert_json_pointer_segments(document: &mut Value, segments: &[String], value: Value) {
    let Some((segment, rest)) = segments.split_first() else {
        *document = value;
        return;
    };
    if !document.is_object() {
        *document = Value::Object(Map::new());
    }
    let object = document
        .as_object_mut()
        .expect("JSON pointer destination is an object");
    if rest.is_empty() {
        object.insert(segment.clone(), value);
        return;
    }
    let child = object
        .entry(segment.clone())
        .or_insert_with(|| Value::Object(Map::new()));
    insert_json_pointer_segments(child, rest, value);
}

fn decode_json_pointer_segment(segment: &str) -> String {
    segment.replace("~1", "/").replace("~0", "~")
}

fn normalize_same_document_refs(base_uri: &Url, value: &mut Value) {
    match value {
        Value::Object(object) => {
            if let Some(reference) = object.get("$ref").and_then(Value::as_str)
                && let Some(local_reference) = same_document_local_ref(base_uri, reference)
            {
                object.insert("$ref".to_string(), Value::String(local_reference));
            }
            for value in object.values_mut() {
                normalize_same_document_refs(base_uri, value);
            }
        }
        Value::Array(values) => {
            for value in values {
                normalize_same_document_refs(base_uri, value);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn same_document_local_ref(base_uri: &Url, reference: &str) -> Option<String> {
    if reference.starts_with('#') {
        return None;
    }
    let resolved_ref_uri = base_uri.join(reference).ok()?;
    let fragment = resolved_ref_uri.fragment()?.to_string();
    if !fragment.starts_with('/') {
        return None;
    }
    let mut document_uri = resolved_ref_uri;
    document_uri.set_fragment(None);
    let mut base_document_uri = base_uri.clone();
    base_document_uri.set_fragment(None);
    (document_uri == base_document_uri).then(|| format!("#{fragment}"))
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
    referring_document_uri: &Url,
    uri: &Url,
) -> std::result::Result<Value, Box<dyn std::error::Error + Send + Sync>> {
    match uri.scheme() {
        "https" => retrieve_https_external_ref(uri),
        "file" if referring_document_uri.scheme() == "file" => retrieve_file_external_ref(uri),
        "file" => Err(std::io::Error::other(format!(
            "OpenAPI file external reference '{uri}' is not allowed from non-file document '{referring_document_uri}'"
        ))
        .into()),
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
