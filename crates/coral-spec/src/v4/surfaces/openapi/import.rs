use std::collections::{BTreeMap, HashSet};

use serde_json::Value;

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{IrType, SemanticIr};
use crate::v4::manifest::{V4SourceManifest, V4Surface};
use crate::v4::surfaces::json_schema::{RefError, resolve_local_ref};
use crate::v4::{OPENAPI_IMPORTER_VERSION, V4_ARTIFACT_SCHEMA_VERSION};
use crate::{ManifestError, Result};

pub fn import_openapi_surface(
    manifest: &V4SourceManifest,
    surface: &V4Surface,
    document_bytes: &[u8],
) -> Result<SemanticIr> {
    let document: Value =
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

    let mut importer = OpenApiImporter::new(manifest, surface, &document);
    importer.import()
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
                let Some(operation_value) =
                    self.resolve_operation_ref(path, method_name, operation_value)
                else {
                    continue;
                };
                let operation =
                    self.import_operation(path, path_item, method_name, &operation_value)?;
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

    fn resolve_operation_ref(
        &mut self,
        path: &str,
        method_name: &str,
        operation: &Value,
    ) -> Option<Value> {
        let Some(reference) = operation.get("$ref").and_then(Value::as_str) else {
            return Some(operation.clone());
        };
        match resolve_local_ref(self.document, operation) {
            Ok(resolved) => Some(resolved.clone()),
            Err(RefError::External(_)) => {
                self.diagnostics.push(Diagnostic::warning(
                    "OPENAPI_OPERATION_REF_UNSUPPORTED",
                    format!(
                        "OpenAPI operation {method_name} {path} references external operation '{reference}', but Coral currently requires dereferenced or bundled OpenAPI documents"
                    ),
                    self.surface.id.clone(),
                    None,
                ));
                None
            }
            Err(RefError::NotFound(_)) => {
                self.diagnostics.push(Diagnostic::warning(
                    "OPENAPI_OPERATION_REF_UNRESOLVED",
                    format!("OpenAPI operation {method_name} {path} reference '{reference}' was not found"),
                    self.surface.id.clone(),
                    None,
                ));
                None
            }
        }
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
