use std::collections::BTreeSet;

use serde_json::Value;

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{IrField, IrType, IrTypeShape};
use crate::v4::naming::normalize_identifier;
use crate::v4::surfaces::json_schema::{
    JsonObjectShape, JsonSchemaComparisonError, direct_json_object_shape,
    json_schema_required_fields, json_schema_scalar_type,
    merge_json_object_shape_annotation_insensitive,
};

use super::import::OpenApiImporter;

/// Ceiling on nested `allOf` composition, matching the depth the inference
/// passes walk to. A cyclic `$ref` chain — `A: {allOf: [$ref A]}` — is a schema
/// this recursion would otherwise follow forever.
const MAX_ALL_OF_DEPTH: usize = 8;

/// Ceiling on the walk that compares two declarations of the same property.
/// Separate from [`MAX_ALL_OF_DEPTH`], which bounds composition nesting: this
/// one bounds the property schema itself, and exceeding it discards the whole
/// type. Matches the MCP importer's ceiling for the same comparison.
const MAX_PROPERTY_COMPARISON_DEPTH: usize = 64;

/// Why folding a schema's `allOf` tree stopped.
enum AllOfMergeError {
    /// A branch could not be resolved. `resolve_ref` has already recorded a
    /// diagnostic for it.
    UnresolvedRef,
    Comparison(JsonSchemaComparisonError),
}

impl OpenApiImporter<'_> {
    #[expect(
        clippy::too_many_lines,
        reason = "OpenAPI schema import is deliberately kept in one local recursive routine for the first v4 slice."
    )]
    pub(super) fn import_schema(
        &mut self,
        schema: &Value,
        suggested_id: &str,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Option<String> {
        let resolved = self.resolve_ref(schema, operation_id, diagnostics)?;
        let type_id = schema.get("$ref").and_then(Value::as_str).map_or_else(
            || normalize_identifier(suggested_id, "type"),
            type_id_from_ref,
        );
        if self.types.contains_key(&type_id) {
            return Some(type_id);
        }
        let description = resolved
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let nullable = resolved
            .get("nullable")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        self.types.insert(
            type_id.clone(),
            IrType {
                id: type_id.clone(),
                shape: IrTypeShape::Json,
                nullable,
                description: description.clone(),
            },
        );
        let shape = if resolved.get("allOf").and_then(Value::as_array).is_some() {
            let mut merged = JsonObjectShape::default();
            match self.merge_all_of_properties(&resolved, &mut merged, operation_id, 0, diagnostics)
            {
                Ok(()) => {}
                // `resolve_ref` has already reported the branch it could not
                // reach.
                Err(AllOfMergeError::UnresolvedRef) => return None,
                Err(AllOfMergeError::Comparison(JsonSchemaComparisonError::PropertyConflict(
                    property,
                ))) => {
                    diagnostics.push(Diagnostic::new(
                        format!(
                            "allOf property '{property}' conflicts in operation '{operation_id}'"
                        ),
                        Some(operation_id.to_string()),
                    ));
                    return None;
                }
                Err(AllOfMergeError::Comparison(JsonSchemaComparisonError::DepthExceeded)) => {
                    diagnostics.push(Diagnostic::new(
                        format!(
                            "allOf schema exceeds maximum comparison depth in operation '{operation_id}'"
                        ),
                        Some(operation_id.to_string()),
                    ));
                    return None;
                }
            }
            IrTypeShape::Object {
                // The merged `required` set is deliberately dropped, as it was
                // before this folded the whole tree: honouring it here would
                // flip the nullability of every field of every composed type,
                // which is a change for its own commit.
                fields: self.import_object_fields(
                    merged.properties.iter(),
                    &BTreeSet::new(),
                    &type_id,
                    operation_id,
                    diagnostics,
                ),
            }
        } else if let Some(values) = resolved.get("enum").and_then(Value::as_array) {
            IrTypeShape::Enum {
                values: values.iter().map(enum_value).collect(),
            }
        } else if let Some(scalar) = json_schema_scalar_type(&resolved) {
            IrTypeShape::Scalar(scalar)
        } else {
            match resolved
                .get("type")
                .and_then(Value::as_str)
                .unwrap_or("object")
            {
                "object" => {
                    if let Some(properties) = resolved.get("properties").and_then(Value::as_object)
                    {
                        let required = resolved
                            .as_object()
                            .map(json_schema_required_fields)
                            .unwrap_or_default();
                        IrTypeShape::Object {
                            fields: self.import_object_fields(
                                properties.iter(),
                                &required,
                                &type_id,
                                operation_id,
                                diagnostics,
                            ),
                        }
                    } else if let Some(additional) = resolved.get("additionalProperties") {
                        if additional.as_bool() == Some(false) {
                            IrTypeShape::Object { fields: Vec::new() }
                        } else {
                            let value_type_ref = self
                                .import_schema(
                                    additional,
                                    &format!("{type_id}_value"),
                                    operation_id,
                                    diagnostics,
                                )
                                .unwrap_or_else(|| "json".to_string());
                            IrTypeShape::Map { value_type_ref }
                        }
                    } else {
                        IrTypeShape::Json
                    }
                }
                "array" => {
                    let item = resolved.get("items").unwrap_or(&Value::Null);
                    let item_type_ref = self
                        .import_schema(item, &format!("{type_id}_item"), operation_id, diagnostics)
                        .unwrap_or_else(|| "json".to_string());
                    IrTypeShape::List { item_type_ref }
                }
                _ => IrTypeShape::Json,
            }
        };
        self.types.insert(
            type_id.clone(),
            IrType {
                id: type_id.clone(),
                shape,
                nullable,
                description,
            },
        );
        Some(type_id)
    }

    /// Folds a schema's own properties together with those of every `allOf`
    /// branch, recursively.
    ///
    /// Providers compose envelopes out of already-composed bases: a Graph delta
    /// collection response is an `allOf` over `BaseDeltaFunctionResponse`, which
    /// is itself an `allOf` over `BaseCollectionPaginationCountResponse`.
    /// Merging only the immediate branches drops everything the nested ones
    /// declare, which does two things. It silently loses fields from the
    /// imported type, and — because row-path inference folds the whole tree —
    /// it makes a path inference found in a nested branch look absent here, so
    /// `infer_row_path` discards it and the relation collapses to a single JSON
    /// row.
    ///
    /// A schema's own `properties` are merged alongside its branches for the
    /// same reason: `{type: object, properties: {...}, allOf: [...]}` is legal,
    /// and reading only the branches drops the properties declared in place.
    ///
    /// Deliberately not `merged_all_of_object_view`, which answers heuristic
    /// shape questions and lets the first branch win on a duplicate property.
    /// Type import has the opposite need: a genuine disagreement between
    /// branches must still be reported rather than resolved by ordering.
    ///
    /// Properties are compared on validation semantics, not raw equality. A
    /// derived type routinely re-declares an inherited property to narrow an
    /// annotation — every Graph subtype re-declares the `@odata.type`
    /// discriminator with its own `default` — and folding the full chain under
    /// exact equality would read those as conflicts and discard the type
    /// entirely. A property that genuinely disagrees about type or constraints
    /// is still a conflict.
    fn merge_all_of_properties(
        &self,
        schema: &Value,
        merged: &mut JsonObjectShape,
        operation_id: &str,
        depth: usize,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Result<(), AllOfMergeError> {
        if depth > MAX_ALL_OF_DEPTH {
            return Err(AllOfMergeError::Comparison(
                JsonSchemaComparisonError::DepthExceeded,
            ));
        }
        merge_json_object_shape_annotation_insensitive(
            merged,
            direct_json_object_shape(schema),
            0,
            MAX_PROPERTY_COMPARISON_DEPTH,
        )
        .map_err(AllOfMergeError::Comparison)?;
        for branch in schema
            .get("allOf")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let branch = self
                .resolve_ref(branch, operation_id, diagnostics)
                .ok_or(AllOfMergeError::UnresolvedRef)?;
            self.merge_all_of_properties(&branch, merged, operation_id, depth + 1, diagnostics)?;
        }
        Ok(())
    }

    fn import_object_fields<'a>(
        &mut self,
        properties: impl Iterator<Item = (&'a String, &'a Value)>,
        required: &BTreeSet<String>,
        parent_id: &str,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Vec<IrField> {
        properties
            .map(|(name, schema)| {
                let type_ref = self
                    .import_schema(
                        schema,
                        &format!("{parent_id}_{name}"),
                        operation_id,
                        diagnostics,
                    )
                    .unwrap_or_else(|| "json".to_string());
                IrField {
                    name: name.clone(),
                    type_ref,
                    required: required.contains(name),
                    nullable: true,
                    description: self.field_description(schema),
                }
            })
            .collect()
    }

    fn field_description(&self, schema: &Value) -> String {
        if let Some(description) = schema.get("description").and_then(Value::as_str) {
            return description.to_string();
        }
        let Some(reference) = schema.get("$ref").and_then(Value::as_str) else {
            return String::new();
        };
        let Some(pointer) = reference.strip_prefix('#') else {
            return String::new();
        };
        self.document
            .pointer(pointer)
            .and_then(|resolved| resolved.get("description"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string()
    }
}

fn enum_value(value: &Value) -> String {
    value
        .as_str()
        .map_or_else(|| value.to_string(), ToString::to_string)
}

fn type_id_from_ref(reference: &str) -> String {
    normalize_identifier(reference.rsplit('/').next().unwrap_or(reference), "type")
}
