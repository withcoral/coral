use std::collections::{BTreeMap, HashSet};

use serde_json::Value;

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{IrType, SemanticIr};
use crate::v4::manifest::{V4SourceManifest, V4Surface};
use crate::v4::surfaces::json_schema::{RefError, resolve_local_ref};
use crate::v4::{
    ImportedSurface, OPENAPI_IMPORTER_VERSION, OPERATION_METADATA_GENERATOR_VERSION,
    OperationMetadataCatalog, V4_ARTIFACT_SCHEMA_VERSION,
};
use crate::{ManifestError, Result};

pub fn import_openapi_surface(
    manifest: &V4SourceManifest,
    surface: &V4Surface,
    document_bytes: &[u8],
) -> Result<ImportedSurface> {
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

enum RefDiagnosticContext<'a> {
    Operation { path: &'a str, method_name: &'a str },
    OperationId(&'a str),
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

    fn import(&mut self) -> Result<ImportedSurface> {
        let paths = self
            .document
            .get("paths")
            .and_then(Value::as_object)
            .ok_or_else(|| ManifestError::validation("OpenAPI document is missing paths"))?;
        let mut operations = Vec::new();
        let mut operation_metadata = BTreeMap::new();
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
                let operation_value = if operation_value.get("$ref").is_some() {
                    match resolve_local_ref(self.document, operation_value) {
                        Ok(operation_value) => operation_value,
                        Err(error) => {
                            let diagnostic = Self::ref_error_diagnostic(
                                error,
                                &RefDiagnosticContext::Operation { path, method_name },
                            );
                            self.diagnostics.push(diagnostic);
                            continue;
                        }
                    }
                } else {
                    operation_value
                };
                let (operation, metadata) =
                    self.import_operation(path, path_item, method_name, operation_value)?;
                if !operation_ids.insert(operation.id.clone()) {
                    return Err(ManifestError::validation(format!(
                        "source '{}' surface imports duplicate operation id '{}'",
                        self.manifest.common.name, operation.id
                    )));
                }
                operation_metadata.insert(operation.id.clone(), metadata);
                operations.push(operation);
            }
        }
        let semantic_ir = SemanticIr {
            artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
            source_name: self.manifest.common.name.clone(),
            surface_type: self.surface.surface_type,
            importer_version: OPENAPI_IMPORTER_VERSION.to_string(),
            operations,
            types: self.types.values().cloned().collect(),
            diagnostics: self.diagnostics.clone(),
        };
        let operation_metadata = OperationMetadataCatalog {
            artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
            source_name: self.manifest.common.name.clone(),
            generator_version: Some(OPERATION_METADATA_GENERATOR_VERSION.to_string()),
            operations: operation_metadata,
        };
        Ok(ImportedSurface {
            semantic_ir,
            operation_metadata,
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
            Err(error) => {
                diagnostics.push(Self::ref_error_diagnostic(
                    error,
                    &RefDiagnosticContext::OperationId(operation_id),
                ));
                None
            }
        }
    }

    fn ref_error_diagnostic(error: RefError<'_>, context: &RefDiagnosticContext<'_>) -> Diagnostic {
        let (code, message) = match error {
            RefError::External(reference) => (
                "OPENAPI_EXTERNAL_REF_UNSUPPORTED",
                format!(
                    "external reference '{reference}' is unsupported; Coral currently requires dereferenced or bundled OpenAPI documents"
                ),
            ),
            RefError::NotFound(reference) => (
                "OPENAPI_REF_NOT_FOUND",
                format!("reference '{reference}' was not found"),
            ),
        };
        let (message, operation_id) = match context {
            RefDiagnosticContext::Operation { path, method_name } => (
                format!("OpenAPI operation {method_name} {path}: {message}"),
                None,
            ),
            RefDiagnosticContext::OperationId(operation_id) => {
                (message, Some(operation_id.to_string()))
            }
        };
        Diagnostic::warning(code, message, operation_id)
    }
}
