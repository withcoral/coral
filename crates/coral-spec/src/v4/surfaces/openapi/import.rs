use std::collections::{BTreeMap, HashSet};

use serde_json::{Map, Value};

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{IrType, SemanticIr};
use crate::v4::manifest::{V4SourceManifest, V4Surface};
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
                let imported_operations =
                    self.import_operation(path, path_item, method_name, operation_value)?;
                for operation in imported_operations {
                    if !operation_ids.insert(operation.id.clone()) {
                        return Err(ManifestError::validation(format!(
                            "source '{}' surface '{}' imports duplicate operation id '{}'",
                            self.manifest.common.name, self.surface.id, operation.id
                        )));
                    }
                    operations.push(operation);
                }
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
        let Some(reference) = value.get("$ref").and_then(Value::as_str) else {
            return Some(value.clone());
        };
        if !reference.starts_with("#/") {
            diagnostics.push(Diagnostic::warning(
                "OPENAPI_EXTERNAL_REF_UNSUPPORTED",
                format!("external reference '{reference}' is unsupported"),
                self.surface.id.clone(),
                Some(operation_id.to_string()),
            ));
            return None;
        }
        let pointer = reference.strip_prefix('#').unwrap_or(reference);
        if let Some(target) = self.document.pointer(pointer) {
            Some(target.clone())
        } else {
            diagnostics.push(Diagnostic::warning(
                "OPENAPI_REF_NOT_FOUND",
                format!("reference '{reference}' was not found"),
                self.surface.id.clone(),
                Some(operation_id.to_string()),
            ));
            None
        }
    }

    pub(super) fn effective_schema(
        &self,
        schema: &Value,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Option<Value> {
        let resolved = self.resolve_ref(schema, operation_id, diagnostics)?;
        let Some(all_of) = resolved.get("allOf").and_then(Value::as_array) else {
            return Some(resolved);
        };

        let mut merged = Map::new();
        for (key, value) in resolved.as_object()? {
            if key != "allOf" {
                merged.insert(key.clone(), value.clone());
            }
        }
        for item in all_of {
            let item = self.effective_schema(item, operation_id, diagnostics)?;
            merge_schema_object(&mut merged, &item);
        }
        Some(Value::Object(merged))
    }
}

fn merge_schema_object(target: &mut Map<String, Value>, item: &Value) {
    let Some(item) = item.as_object() else {
        return;
    };
    for (key, value) in item {
        if key == "properties" {
            let target_properties = target
                .entry(key.clone())
                .or_insert_with(|| Value::Object(Map::new()));
            if let (Some(target_map), Some(value_map)) =
                (target_properties.as_object_mut(), value.as_object())
            {
                for (property_name, property_schema) in value_map {
                    target_map
                        .entry(property_name.clone())
                        .or_insert_with(|| property_schema.clone());
                }
            }
        } else {
            target.entry(key.clone()).or_insert_with(|| value.clone());
        }
    }
}
