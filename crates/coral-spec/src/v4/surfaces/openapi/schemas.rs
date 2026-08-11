use std::collections::BTreeSet;

use serde_json::{Map, Value};

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::{IrField, IrType, IrTypeShape};
use crate::v4::naming::normalize_identifier;
use crate::v4::surfaces::json_schema::{
    JsonObjectShape, JsonSchemaComparisonError, JsonSchemaWalkError, RefError,
    direct_json_object_shape, json_schema_declares_only_type, json_schema_has_declared_type,
    json_schema_required_fields, json_schema_scalar_type, json_schema_type_contains,
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

/// Keywords beside a `$ref` that describe it rather than narrow it.
///
/// An annotation says nothing about which instances the schema admits, so it
/// cannot make the reference mean something the reference does not already
/// mean — a `description` written beside a `$ref` documents the field that
/// holds it, and the schema is still exactly what it resolves to.
const REF_SIBLING_ANNOTATIONS: [&str; 11] = [
    "description",
    "title",
    "summary",
    "example",
    "examples",
    "deprecated",
    "readOnly",
    "writeOnly",
    "externalDocs",
    "xml",
    "$comment",
];

/// Assertions beside a `$ref` that the `allOf` fold can compose.
///
/// The fold merges properties and answers with an object, so these are the
/// keywords whose composition it actually implements. Anything else — a
/// `maxLength` narrowing a referenced string, a `type` contradicting the
/// reference — is reported instead of being folded into a shape it does not
/// describe.
const COMPOSABLE_REF_SIBLINGS: [&str; 3] = ["properties", "required", "additionalProperties"];

/// Ceiling on how many nested `[T, null]` unions are followed before the schema
/// is left to import as written. A nullable schema is one union deep in every
/// document seen so far; this only has to stop a pathological one from walking
/// for ever.
const MAX_NULL_UNION_DEPTH: usize = 8;

/// Where a walk through a chain of `[T, null]` unions ended.
pub(super) struct NullUnionWalk {
    /// The schema carrying the shape, with the walk's last `$ref` hop already
    /// followed.
    pub(super) resolved: Value,
    /// The reference the result is interned under, when the walk crossed one.
    ///
    /// The site the walk ended on supplies this, so a chain of aliases still
    /// interns under the name it finally arrived at and every route to that
    /// name shares one type. Only when the walk ends somewhere unnamed — a
    /// union whose real branch is written inline — does the first reference it
    /// crossed stand in.
    ///
    /// That fallback is what terminates a recursive nullable reference. A
    /// self-referential type reached through a wrapper — `node: anyOf: [null,
    /// {type: object, properties: {child: {anyOf: [null, {$ref: node}]}}}]` —
    /// ends its walk on the inline branch, and without a name to intern under
    /// every nested `child` took the suggested id instead, growing a fresh
    /// `node_child_child…` that the interning check could never match until the
    /// stack ran out.
    pub(super) reference: Option<String>,
}

impl NullUnionWalk {
    fn ending_at(site: &Value, resolved: Value, first_reference: Option<String>) -> Self {
        Self {
            reference: site
                .get("$ref")
                .and_then(Value::as_str)
                .map(ToString::to_string)
                .or(first_reference),
            resolved,
        }
    }
}

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
        // Before the reference is resolved, because resolving it is what loses
        // the assertions written beside it. The rewrite carries no `$ref` of its
        // own, so this re-entry cannot repeat.
        if let Some(composed) = self.ref_siblings_composed(schema, operation_id, diagnostics) {
            return self.import_schema(&composed, suggested_id, operation_id, diagnostics);
        }
        // A union of one real branch and `null` is imported as that branch.
        //
        // It is how a nullable reference has to be written once `nullable` is
        // gone: a `$ref` carries no type of its own to list `null` in, so 3.1
        // documents spell it `anyOf: [{type: 'null'}, {$ref: ...}]`. Left alone
        // it matches no arm of the shape dispatch below and imports as opaque
        // JSON, taking every field of the referenced type with it.
        let walk = self.schema_through_null_unions(schema, operation_id, diagnostics)?;
        let resolved = walk.resolved;
        // The type id still comes from the reference this was reached by, and
        // only falls back to what the walk arrived at. That ordering is what
        // ends a cycle: `Node: anyOf: [{type: 'null'}, {$ref: Node}]` unwraps to
        // a schema that resolves back to the same union, and taking the branch's
        // identity instead would leave the interning check below — the thing
        // that terminates every other `$ref` cycle here — with a name that never
        // repeats, growing a fresh `root_child_child…` on each pass until the
        // stack ran out.
        //
        // Preferring a reference over the suggested id when this site has none
        // is what keeps a nullable reference on the type it already has, rather
        // than copying the target and every inline schema beneath it once per
        // referring site — on GitHub's 3.1 descriptor, the difference between
        // 15k imported types and 25k.
        let type_id = schema
            .get("$ref")
            .and_then(Value::as_str)
            .map(ToString::to_string)
            .or(walk.reference)
            .map_or_else(
                || normalize_identifier(suggested_id, "type"),
                |reference| type_id_from_ref(&reference),
            );
        if self.types.contains_key(&type_id) {
            return Some(type_id);
        }
        // The target's own description, never one written beside the `$ref`.
        // 3.1 does give those siblings meaning, but a type here is interned by
        // the name it is referenced under and shared by every site that
        // references it, so there is nowhere to put a site-specific description
        // except over the top of the shared one — where the first reference
        // would win and every later one would be lost. The site's wording
        // already reaches the only place it fits, `field_description` below,
        // which reads it in preference to the target's.
        let description = resolved
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let nullable = self.dialect.schema_nullable(&resolved);
        self.warn_removed_keywords(
            &resolved,
            &format!("type '{type_id}'"),
            operation_id,
            diagnostics,
        );
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
        // `const` is read before `enum`, because a schema carrying both is
        // constrained by both and `const` is the narrower of the two. That pair
        // is how a 2020-12 document pins one branch of a union: the branch
        // re-declares the shared discriminator it inherits — `enum` listing
        // every tag in the family — and adds the single `const` that says which
        // of them this branch is. Reading `enum` first published all of them,
        // leaving each branch claiming the whole family's tags.
        //
        // A structured `const` still falls through to `enum` and then to the
        // shape dispatch, because `const_enum_values` reads only scalars.
        } else if let Some(values) = self.dialect.const_enum_values(&resolved) {
            IrTypeShape::Enum { values }
        } else if let Some(values) = resolved.get("enum").and_then(Value::as_array) {
            IrTypeShape::Enum {
                values: values.iter().map(enum_value).collect(),
            }
        } else if let Some(scalar) = json_schema_scalar_type(&resolved) {
            IrTypeShape::Scalar(scalar)
        // `type` is matched through the array-aware helpers rather than read as
        // a string. A schema may list several types, and a nullable one always
        // does — `{"type": ["object", "null"]}` is how 3.1 spells what 3.0
        // spelled `nullable: true`. Reading only the string form left every such
        // schema falling through to the typeless default, so a nullable array
        // was imported as an object.
        //
        // A schema declaring no type at all still defaults to an object, and
        // `object` still wins over `array` when a schema somehow claims both.
        } else if json_schema_type_contains(&resolved, "object")
            || !json_schema_has_declared_type(&resolved)
        {
            if let Some(properties) = resolved.get("properties").and_then(Value::as_object) {
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
        // Asking for the sole declared type, where the object branch above only
        // asks whether `object` is among them. A union that merely includes
        // `array` is not a collection: `{"type": ["array", "string"]}` accepts
        // one string too, and importing `items` regardless types those instances
        // as a row of item-derived columns that are all null. Object stays the
        // permissive reading because it is already what a schema declaring no
        // type at all is imported as, so a union including it degrades to the
        // same columns rather than to a claim about the response's shape.
        } else if json_schema_declares_only_type(&resolved, "array") {
            let item = resolved.get("items").unwrap_or(&Value::Null);
            let item_type_ref = self
                .import_schema(item, &format!("{type_id}_item"), operation_id, diagnostics)
                .unwrap_or_else(|| "json".to_string());
            IrTypeShape::List { item_type_ref }
        } else {
            IrTypeShape::Json
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

    /// Rewrites a `$ref` carrying assertions beside it as the `allOf` it means.
    ///
    /// Under 3.1, `{$ref: Base, properties: {extra}, required: [extra]}`
    /// describes `Base` *and* `extra`. Resolving straight through the reference
    /// answered `Base` alone, so `extra` was gone from the IR and from every
    /// projection over it with nothing said — the schema silently narrowed to
    /// its base.
    ///
    /// Composed as `allOf` rather than merged here, so the fold already written
    /// for that keyword — with its depth ceiling, its cycle guard and its
    /// conflict reporting — is what does the work.
    pub(super) fn ref_siblings_composed(
        &self,
        schema: &Value,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Option<Value> {
        if !self.dialect.ref_siblings_apply() {
            return None;
        }
        let members = schema.as_object()?;
        let reference = members.get("$ref")?;
        let assertions: Map<String, Value> = members
            .iter()
            .filter(|(name, _)| {
                name.as_str() != "$ref" && !REF_SIBLING_ANNOTATIONS.contains(&name.as_str())
            })
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect();
        if assertions.is_empty() {
            return None;
        }
        // Only the keywords describing an object can be composed this way: the
        // `allOf` fold merges properties and answers with an object, so routing
        // `{$ref: Name, maxLength: 5}` through it would replace a string column
        // with an object that has no fields. Reporting the ones left out keeps
        // this from being the same silence one keyword along.
        if let Some(unsupported) = assertions
            .keys()
            .find(|name| !COMPOSABLE_REF_SIBLINGS.contains(&name.as_str()))
        {
            diagnostics.push(Diagnostic::new(
                format!(
                    "schema in operation '{operation_id}' narrows a '$ref' with '{unsupported}', which Coral does not compose; the reference is imported without it"
                ),
                Some(operation_id.to_string()),
            ));
            return None;
        }
        let mut reference_branch = Map::new();
        reference_branch.insert("$ref".to_string(), reference.clone());
        let mut composed = Map::new();
        composed.insert(
            "allOf".to_string(),
            Value::Array(vec![
                Value::Object(reference_branch),
                Value::Object(assertions),
            ]),
        );
        Some(Value::Object(composed))
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
            self.document,
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
            // Converted rather than propagated: the walk error borrows from the
            // schema it walked, and the top-level caller passes a local.
            Err(error) => Err(Self::all_of_walk_error(error, operation_id, diagnostics)),
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
            direct_json_object_shape(resolved),
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

    /// Records the diagnostic a failed branch walk deserves and reduces it to a
    /// lifetime-free error.
    fn all_of_walk_error(
        error: JsonSchemaWalkError<'_>,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> AllOfMergeError {
        let ref_error = match error {
            JsonSchemaWalkError::ExternalRef(reference) => RefError::External(reference),
            JsonSchemaWalkError::RefNotFound(reference) => RefError::NotFound(reference),
            JsonSchemaWalkError::RefCycle(reference) => {
                return AllOfMergeError::RefCycle(reference.to_string());
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

    /// Follows a chain of unions between a schema and `null` to the schema that
    /// carries the shape, returning the one given when it is not such a union.
    ///
    /// Both halves of what the caller needs come back together: the resolved
    /// schema, so the one `$ref` hop the walk already followed is not followed
    /// again on the hot path, and the reference to intern the result under.
    ///
    /// Bounded twice over, because `resolve_ref` follows one hop and tracks
    /// nothing: a branch leading back to a reference already followed stops the
    /// walk, and [`MAX_NULL_UNION_DEPTH`] backs that up for a chain that nests
    /// without repeating. A union stopped either way keeps its own reading and
    /// imports as opaque JSON, which is what every union did before this.
    pub(super) fn schema_through_null_unions(
        &self,
        schema: &Value,
        operation_id: &str,
        diagnostics: &mut Vec<Diagnostic>,
    ) -> Option<NullUnionWalk> {
        let mut site = schema.clone();
        let mut followed = BTreeSet::new();
        let mut first_reference = None;
        if let Some(reference) = site.get("$ref").and_then(Value::as_str) {
            followed.insert(reference.to_string());
            first_reference = Some(reference.to_string());
        }
        for _ in 0..MAX_NULL_UNION_DEPTH {
            let resolved = self.resolve_ref(&site, operation_id, diagnostics)?;
            let Some(branch) = null_union_branch(&resolved).cloned() else {
                return Some(NullUnionWalk::ending_at(&site, resolved, first_reference));
            };
            if let Some(reference) = branch.get("$ref").and_then(Value::as_str) {
                first_reference.get_or_insert_with(|| reference.to_string());
                if !followed.insert(reference.to_string()) {
                    return Some(NullUnionWalk::ending_at(&site, resolved, first_reference));
                }
            }
            site = branch;
        }
        let resolved = self.resolve_ref(&site, operation_id, diagnostics)?;
        Some(NullUnionWalk::ending_at(&site, resolved, first_reference))
    }

    fn field_description(&self, schema: &Value) -> String {
        // Iterative rather than recursive, so no chain of wrappers can put this
        // lookup on the stack. It needs no depth budget the way the import walk
        // above does: that one follows `$ref`s, which can lead anywhere and
        // back, while this only ever steps to a branch written inside the schema
        // it is already holding. Each step is strictly further into one finite
        // tree, so the loop ends because the document does.
        let mut schema = schema;
        loop {
            if let Some(description) = schema.get("description").and_then(Value::as_str) {
                return description.to_string();
            }
            // A nullable spelling wraps the schema that has the description. The
            // wrapper says nothing itself, so without this a field written
            // `anyOf: [{type: 'null'}, {$ref: ...}]` lost the column description
            // that the same field written as a bare `$ref` keeps.
            let Some(branch) = null_union_branch(schema) else {
                break;
            };
            schema = branch;
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

/// The single real branch of a union between a schema and `null`.
///
/// Only a union that reduces to exactly one real branch is unwrapped. Two or
/// more is a genuine choice between shapes, which Coral has no type for and
/// which has to keep importing as opaque JSON — reading the first branch and
/// discarding the rest would describe a column the response does not have.
fn null_union_branch(schema: &Value) -> Option<&Value> {
    for keyword in ["anyOf", "oneOf"] {
        let Some(branches) = schema.get(keyword).and_then(Value::as_array) else {
            continue;
        };
        // A union has to have lost something to `null` to be one of these; a
        // single-branch `anyOf` is just a wrapper and keeps its own reading.
        if branches.len() < 2 {
            continue;
        }
        let mut real = branches.iter().filter(|branch| !declares_only_null(branch));
        let only = real.next()?;
        if real.next().is_some() {
            return None;
        }
        return Some(only);
    }
    None
}

/// Whether a branch admits nothing but `null`.
///
/// `type` is the usual spelling, but 2020-12 gives two more ways to say the same
/// thing, and a branch written either of them is the same nullable reference —
/// missing it would leave the union looking like a choice between two real
/// shapes and send it back to opaque JSON.
fn declares_only_null(schema: &Value) -> bool {
    let declares_null_type = match schema.get("type") {
        Some(Value::String(value)) => value == "null",
        Some(Value::Array(values)) => {
            !values.is_empty() && values.iter().all(|value| value.as_str() == Some("null"))
        }
        _ => false,
    };
    let is_null_constant = schema.get("const").is_some_and(Value::is_null)
        || schema
            .get("enum")
            .and_then(Value::as_array)
            .is_some_and(|values| !values.is_empty() && values.iter().all(Value::is_null));
    declares_null_type || is_null_constant
}

pub(super) fn enum_value(value: &Value) -> String {
    value
        .as_str()
        .map_or_else(|| value.to_string(), ToString::to_string)
}

fn type_id_from_ref(reference: &str) -> String {
    normalize_identifier(reference.rsplit('/').next().unwrap_or(reference), "type")
}
