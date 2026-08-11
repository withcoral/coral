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

use super::dialect::OpenApiDialect;
use super::json_schema_dialect::validate_json_schema_dialect;
use super::v3_0::OpenApi30Importer;
use super::v3_1::OpenApi31Importer;
use super::version::{OpenApiVersion, parse_openapi_version};

pub fn import_openapi_surface(
    manifest: &V4SourceManifest,
    surface: &V4Surface,
    document_bytes: &[u8],
) -> Result<ImportedSurface> {
    let document: Value =
        serde_yaml::from_slice(document_bytes).map_err(ManifestError::parse_yaml)?;
    let dialect: &dyn OpenApiDialect = match parse_openapi_version(&document)? {
        OpenApiVersion::V3_0 => &OpenApi30Importer,
        OpenApiVersion::V3_1 => {
            // Only 3.1 can choose a dialect. 3.0's schema object is its own,
            // fixed by that specification, so neither keyword means anything
            // there and a document carrying one is not selecting anything.
            validate_json_schema_dialect(&document)?;
            &OpenApi31Importer
        }
    };

    let mut importer = OpenApiImporter::new(manifest, surface, &document, dialect);
    importer.import()
}

pub(super) struct OpenApiImporter<'a> {
    pub(super) manifest: &'a V4SourceManifest,
    pub(super) surface: &'a V4Surface,
    pub(super) document: &'a Value,
    pub(super) dialect: &'a dyn OpenApiDialect,
    pub(super) types: BTreeMap<String, IrType>,
    pub(super) diagnostics: Vec<Diagnostic>,
}

pub(super) enum RefDiagnosticContext<'a> {
    Operation { path: &'a str, method_name: &'a str },
    OperationId(&'a str),
}

impl<'a> OpenApiImporter<'a> {
    fn new(
        manifest: &'a V4SourceManifest,
        surface: &'a V4Surface,
        document: &'a Value,
        dialect: &'a dyn OpenApiDialect,
    ) -> Self {
        Self {
            manifest,
            surface,
            document,
            dialect,
            types: BTreeMap::new(),
            diagnostics: Vec::new(),
        }
    }

    fn import(&mut self) -> Result<ImportedSurface> {
        let paths = self.document.get("paths").and_then(Value::as_object);
        if paths.is_none() && self.dialect.paths_required() {
            return Err(ManifestError::validation(
                "OpenAPI document is missing paths",
            ));
        }
        // A document describing only `webhooks` or only reusable `components` is
        // well-formed under 3.1 and simply has nothing here to import, so it
        // yields an empty surface rather than an error.
        self.report_unimported_sections();
        let mut operations = Vec::new();
        let mut operation_metadata = BTreeMap::new();
        let mut operation_ids = HashSet::new();
        for (path, path_item) in paths.into_iter().flatten() {
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

    /// Records what the document describes that this importer does not read.
    ///
    /// `webhooks` is 3.1's home for provider-initiated callbacks. Coral models a
    /// source as endpoints it calls, so there is nothing to map them onto — but
    /// a document whose whole surface is webhooks would otherwise import to
    /// nothing at all, with no indication of why.
    fn report_unimported_sections(&mut self) {
        let webhooks = self
            .document
            .get("webhooks")
            .and_then(Value::as_object)
            .map_or(0, serde_json::Map::len);
        if webhooks > 0 {
            self.diagnostics.push(Diagnostic::new(
                format!(
                    "OpenAPI document declares {webhooks} webhook(s), which Coral does not import; only operations under 'paths' become tables"
                ),
                None,
            ));
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
            Err(error) => {
                diagnostics.push(Self::ref_error_diagnostic(
                    error,
                    &RefDiagnosticContext::OperationId(operation_id),
                ));
                None
            }
        }
    }

    /// Reports a schema reaching for a keyword its document's version removed.
    ///
    /// Every path that reads a schema has to ask, not just the one that imports
    /// a type from it. A parameter resolves to a scalar through
    /// `import_parameter_scalar` and never reaches `import_schema`, and a
    /// top-level collection response hands `import_schema` its `items` and sets
    /// the schema carrying the keyword aside — so a `nullable: true` in either
    /// place was dropped with nothing said, which is the silence this diagnostic
    /// exists to break.
    ///
    /// `subject` names what carries the keyword — a type, a parameter, a
    /// response — because the operation alone does not locate it.
    pub(super) fn warn_removed_keywords(
        &self,
        schema: &Value,
        subject: &str,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) {
        if let Some(warning) = self.dialect.removed_keyword_warning(schema) {
            diagnostics.push(Diagnostic::new(
                format!("{subject} in operation '{operation_id}': {warning}"),
                Some(operation_id.to_string()),
            ));
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
