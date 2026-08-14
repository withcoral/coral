use std::collections::BTreeSet;

use serde_json::Value;

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{IrField, IrType, IrTypeShape};
use crate::v4::naming::normalize_identifier;
use crate::v4::surfaces::json_schema::{
    JsonObjectShape, JsonSchemaComparisonError, JsonSchemaWalkError, RefError,
    direct_json_object_shape, json_schema_required_fields, json_schema_scalar_type,
    merge_json_object_shape_annotation_insensitive, with_resolved_json_schema,
};

use super::import::{OpenApiImporter, RefDiagnosticContext};

/// Ceiling on the `allOf` fold, matching the budget the inference passes walk
/// to. Spent on composition nesting and `$ref` hops alike, because the fold
/// resolves branches through the same shared walk — so both halves stop at the
/// same schema rather than one seeing a tree the other cannot.
///
/// Not the cycle guard: the walk tracks the refs it is resolving, so
/// `A: {allOf: [$ref A]}` is reported as a cycle rather than recursing until
/// this stops it.
const MAX_ALL_OF_DEPTH: usize = 8;

/// Ceiling on the walk that compares two declarations of the same property.
/// Separate from [`MAX_ALL_OF_DEPTH`], which bounds composition nesting: this
/// one bounds the property schema itself, and exceeding it discards the whole
/// type. Matches the MCP importer's ceiling for the same comparison.
const MAX_PROPERTY_COMPARISON_DEPTH: usize = 64;

/// Why folding a schema's `allOf` tree stopped.
enum AllOfMergeError {
    /// A branch could not be resolved. A diagnostic naming it has already been
    /// recorded.
    UnresolvedRef,
    /// A branch references itself, directly or through a chain.
    RefCycle(String),
    /// Composition nests past [`MAX_ALL_OF_DEPTH`]. Distinct from
    /// [`JsonSchemaComparisonError::DepthExceeded`], which is the much larger
    /// ceiling on one property's own schema: reporting them with the same
    /// wording sends whoever is debugging a deeply composed spec to the wrong
    /// constant.
    CompositionTooDeep,
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
        // The type is named after the ref it arrived through, and a nullable
        // `$ref` union only carries one once it is unwrapped — so the referring
        // schema is read the same way the referent is.
        let schema = self.read_schema(schema);
        let resolved = self.resolve_ref(&schema, operation_id, diagnostics)?;
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
            match self.merge_all_of_properties(
                &resolved,
                &mut merged,
                operation_id,
                &mut BTreeSet::new(),
                0,
                diagnostics,
            ) {
                Ok(()) => {}
                // The walk has already reported the branch it could not reach.
                Err(AllOfMergeError::UnresolvedRef) => return None,
                Err(AllOfMergeError::RefCycle(reference)) => {
                    diagnostics.push(Diagnostic::new(
                        format!(
                            "allOf branch '{reference}' is cyclic in operation '{operation_id}'"
                        ),
                        Some(operation_id.to_string()),
                    ));
                    return None;
                }
                Err(AllOfMergeError::CompositionTooDeep) => {
                    diagnostics.push(Diagnostic::new(
                        format!(
                            "allOf composition nests past {MAX_ALL_OF_DEPTH} levels in operation '{operation_id}'"
                        ),
                        Some(operation_id.to_string()),
                    ));
                    return None;
                }
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
    /// Folds a schema's `allOf` chain into `merged`, resolving each branch
    /// through the shared walk.
    ///
    /// `resolve_ref` follows exactly one hop, so an alias branch — `{$ref:
    /// Alias}` where `Alias` is itself `{$ref: Base}` — came back still holding
    /// a `$ref`, with no `properties` and no `allOf` to contribute. It was
    /// dropped silently. Inference resolves chains, so it could find a row path
    /// behind an alias that the imported type lacked, and `infer_row_path`
    /// would then discard the path and collapse the collection to one JSON row
    /// — the failure this whole pass exists to remove. Both halves have to fold
    /// the same tree.
    ///
    /// The walk also tracks the refs it is resolving, so a self-referential
    /// branch is reported as a cycle instead of recursing until the depth cap
    /// stops it. That cap is now a shared budget: it bounds composition nesting
    /// and `$ref` hops together, matching how the inference walk spends it.
    fn merge_all_of_properties(
        &self,
        schema: &Value,
        merged: &mut JsonObjectShape,
        operation_id: &str,
        resolving_refs: &mut BTreeSet<String>,
        depth: usize,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Result<(), AllOfMergeError> {
        let walked = with_resolved_json_schema(
            self.schema_root(),
            schema,
            resolving_refs,
            depth,
            MAX_ALL_OF_DEPTH,
            |resolved, resolving_refs, next_depth| {
                // The merge's own failures ride inside `T`: the walk fixes the
                // error type as `JsonSchemaWalkError`, and a property conflict
                // has to stay distinguishable from a ref that would not resolve.
                Ok(self.merge_resolved_all_of_properties(
                    resolved,
                    merged,
                    operation_id,
                    resolving_refs,
                    next_depth,
                    diagnostics,
                ))
            },
        );
        match walked {
            Ok(merge_result) => merge_result,
            // Converted rather than propagated: the walk reports why a branch
            // could not be read, and the merge answers in its own error type,
            // which distinguishes a property conflict from an unresolvable ref.
            Err(error) => Err(Self::all_of_walk_error(&error, operation_id, diagnostics)),
        }
    }

    /// The per-level half of [`Self::merge_all_of_properties`], after the walk
    /// has resolved this branch to a concrete schema.
    fn merge_resolved_all_of_properties(
        &self,
        resolved: &Value,
        merged: &mut JsonObjectShape,
        operation_id: &str,
        resolving_refs: &mut BTreeSet<String>,
        depth: usize,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Result<(), AllOfMergeError> {
        merge_json_object_shape_annotation_insensitive(
            merged,
            direct_json_object_shape(self.schema_root(), resolved),
            0,
            MAX_PROPERTY_COMPARISON_DEPTH,
        )
        .map_err(AllOfMergeError::Comparison)?;
        for branch in resolved
            .get("allOf")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            self.merge_all_of_properties(
                branch,
                merged,
                operation_id,
                resolving_refs,
                depth,
                diagnostics,
            )?;
        }
        Ok(())
    }

    /// Records the diagnostic a failed branch walk deserves and reduces it to
    /// the merge's own error.
    fn all_of_walk_error(
        error: &JsonSchemaWalkError,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> AllOfMergeError {
        let ref_error = match error {
            JsonSchemaWalkError::ExternalRef(reference) => RefError::External(reference.as_str()),
            JsonSchemaWalkError::RefNotFound(reference) => RefError::NotFound(reference.as_str()),
            JsonSchemaWalkError::RefCycle(reference) => {
                return AllOfMergeError::RefCycle(reference.clone());
            }
            JsonSchemaWalkError::DepthExceeded => return AllOfMergeError::CompositionTooDeep,
        };
        diagnostics.push(OpenApiImporter::ref_error_diagnostic(
            ref_error,
            &RefDiagnosticContext::OperationId(operation_id),
        ));
        AllOfMergeError::UnresolvedRef
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
                let schema = self.read_schema(schema);
                let type_ref = self
                    .import_schema(
                        &schema,
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
                    description: self.field_description(&schema),
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
