use std::borrow::Cow;
use std::collections::{BTreeMap, HashSet};

use serde_json::Value;

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{IrType, SemanticIr};
use crate::v4::manifest::{V4SourceManifest, V4Surface};
use crate::v4::surfaces::json_schema::{RefError, SchemaRoot, resolve_local_ref};
use crate::v4::{
    ImportedSurface, OPENAPI_IMPORTER_VERSION, OPERATION_METADATA_GENERATOR_VERSION,
    OperationMetadataCatalog, V4_ARTIFACT_SCHEMA_VERSION,
};
use crate::{ManifestError, Result};

use super::document::validate_supported_openapi_version;
use super::normalize::normalized_schema;
use super::yaml_value::parse_yaml_json_value;

pub fn import_openapi_surface(
    manifest: &V4SourceManifest,
    surface: &V4Surface,
    document_bytes: &[u8],
) -> Result<ImportedSurface> {
    let document = parse_yaml_json_value(document_bytes).map_err(ManifestError::parse_yaml)?;
    validate_supported_openapi_version(&document)?;

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

pub(super) enum RefDiagnosticContext<'a> {
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

    /// The document a walk should read this surface's schemas through.
    ///
    /// Every schema the importer reads comes from here or from
    /// [`Self::resolve_ref`], so the 3.1 nullability spellings are rewritten
    /// wherever a reader would otherwise misread them — and nowhere else. The
    /// document itself is left as parsed, so every `$ref` keeps resolving,
    /// including one that points into a subschema.
    pub(super) fn schema_root(&self) -> SchemaRoot<'a> {
        SchemaRoot::normalized_by(self.document, normalized_schema)
    }

    /// One schema, in the spelling this importer's readers understand.
    ///
    /// Every read of a schema's own keywords goes through here or through the
    /// shared walk, which is what replaces rewriting the document: a reader
    /// that reached into the parsed tree directly would still see the 3.1
    /// spelling.
    pub(super) fn read_schema<'s>(&self, schema: &'s Value) -> Cow<'s, Value> {
        self.schema_root().read(schema)
    }

    pub(super) fn resolve_ref(
        &self,
        value: &Value,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Option<Value> {
        // Normalized before resolving as well as after: a nullable `$ref` union
        // only becomes a `$ref` once it is unwrapped, and resolving the union
        // itself would hand back the union.
        let value = self.read_schema(value);
        match resolve_local_ref(self.document, &value) {
            Ok(resolved) => Some(self.read_schema(resolved).into_owned()),
            Err(error) => {
                diagnostics.push(Self::ref_error_diagnostic(
                    error,
                    &RefDiagnosticContext::OperationId(operation_id),
                ));
                None
            }
        }
    }

    pub(super) fn ref_error_diagnostic(
        error: RefError<'_>,
        context: &RefDiagnosticContext<'_>,
    ) -> Diagnostic {
        let message = match error {
            RefError::External(reference) => {
                format!(
                    "external reference '{reference}' is unsupported; Coral currently requires dereferenced or bundled OpenAPI documents"
                )
            }
            RefError::NotFound(reference) => {
                format!("reference '{reference}' was not found")
            }
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
        Diagnostic::new(message, operation_id)
    }
}
