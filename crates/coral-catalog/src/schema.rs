use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use serde_json::{Map, Value};

use crate::{CatalogDiagnostic, SchemaGraph, SchemaId, SchemaSemantics};

pub(super) fn is_binary_schema<'a>(
    id: &SchemaId,
    lookup: impl Fn(&SchemaId) -> Option<&'a Value>,
) -> bool {
    fn visit<'a>(
        schema: &Value,
        lookup: &impl Fn(&SchemaId) -> Option<&'a Value>,
        visiting: &mut HashSet<SchemaId>,
    ) -> bool {
        if schema.get("format").and_then(Value::as_str) == Some("binary")
            && schema.get("type").is_some_and(|kind| {
                kind.as_str() == Some("string")
                    || kind.as_array().is_some_and(|kinds| {
                        kinds.iter().any(|kind| kind.as_str() == Some("string"))
                    })
            })
        {
            return true;
        }
        if let Some(reference) = schema.get("$ref").and_then(Value::as_str)
            && let Ok(id) = SchemaId::from_storage(reference.to_string())
            && visiting.insert(id.clone())
        {
            let binary = lookup(&id).is_some_and(|schema| visit(schema, lookup, visiting));
            visiting.remove(&id);
            if binary {
                return true;
            }
        }
        // An intersection can carry its binary declaration in one member;
        // a union is unambiguously binary only when every alternative is binary.
        if schema
            .get("allOf")
            .and_then(Value::as_array)
            .is_some_and(|branches| {
                branches
                    .iter()
                    .any(|branch| visit(branch, lookup, visiting))
            })
        {
            return true;
        }
        ["anyOf", "oneOf"].iter().any(|keyword| {
            schema
                .get(*keyword)
                .and_then(Value::as_array)
                .is_some_and(|branches| {
                    !branches.is_empty()
                        && branches
                            .iter()
                            .all(|branch| visit(branch, lookup, visiting))
                })
        })
    }
    lookup(id).is_some_and(|schema| visit(schema, &lookup, &mut HashSet::from([id.clone()])))
}

pub(super) struct SchemaGraphAccumulator {
    semantics: SchemaSemantics,
    schemas: BTreeMap<SchemaId, Value>,
    source_index: BTreeMap<String, SchemaId>,
}

impl SchemaGraphAccumulator {
    pub(super) fn new(semantics: SchemaSemantics) -> Self {
        Self {
            semantics,
            schemas: BTreeMap::new(),
            source_index: BTreeMap::new(),
        }
    }

    pub(super) fn insert_normalized(&mut self, source: &str, schema: Value) -> SchemaId {
        if let Some(id) = self.source_index.get(source) {
            return id.clone();
        }
        let id = SchemaId::for_source(source);
        self.source_index.insert(source.to_string(), id.clone());
        self.store(id.clone(), schema);
        id
    }

    pub(super) fn add_embedded_schema(
        &mut self,
        source: &str,
        schema: &Value,
    ) -> Result<SchemaId, CatalogDiagnostic> {
        validate_schema_shape(schema)?;
        let mut references = BTreeSet::new();
        collect_embedded_references(schema, schema, &mut references)?;
        let reference_ids = references
            .iter()
            .map(|reference| {
                (
                    reference.clone(),
                    SchemaId::for_source(&format!("{source}{reference}")),
                )
            })
            .collect::<BTreeMap<_, _>>();
        for reference in &references {
            let target = schema
                .pointer(reference.strip_prefix('#').unwrap_or_default())
                .ok_or_else(invalid_schema_reference)?;
            validate_schema_shape(target)?;
        }
        let root_id = SchemaId::for_source(source);
        self.source_index
            .insert(source.to_string(), root_id.clone());
        self.store(
            root_id.clone(),
            rewrite_embedded_references(schema, &reference_ids)?,
        );
        for reference in references {
            let id = reference_ids
                .get(&reference)
                .cloned()
                .ok_or_else(invalid_schema_reference)?;
            let target = schema
                .pointer(reference.strip_prefix('#').unwrap_or_default())
                .ok_or_else(invalid_schema_reference)?;
            self.source_index
                .insert(format!("{source}{reference}"), id.clone());
            self.store(id, rewrite_embedded_references(target, &reference_ids)?);
        }
        Ok(root_id)
    }

    fn store(&mut self, id: SchemaId, mut schema: Value) {
        deduplicate_required(&mut schema);
        self.schemas.insert(id, schema);
    }

    pub(super) fn contains(&self, id: &SchemaId) -> bool {
        self.schemas.contains_key(id)
    }

    pub(super) fn schema(&self, id: &SchemaId) -> Option<&Value> {
        self.schemas.get(id)
    }

    pub(super) fn finish(self) -> Result<SchemaGraph, CatalogDiagnostic> {
        SchemaGraph::new(self.semantics, self.schemas, self.source_index).map_err(|_error| {
            CatalogDiagnostic::stable(
                "invalid_schema_graph",
                "the authoritative schema graph is not package-local",
            )
        })
    }
}

pub(super) const SCHEMA_MAP_KEYWORDS: [&str; 5] = [
    "properties",
    "patternProperties",
    "$defs",
    "definitions",
    "dependentSchemas",
];
pub(super) const SCHEMA_ARRAY_KEYWORDS: [&str; 4] = ["allOf", "anyOf", "oneOf", "prefixItems"];
pub(super) const SCHEMA_VALUE_KEYWORDS: [&str; 11] = [
    "additionalProperties",
    "unevaluatedProperties",
    "propertyNames",
    "items",
    "unevaluatedItems",
    "contains",
    "not",
    "if",
    "then",
    "else",
    "contentSchema",
];

pub(crate) fn graph_references_are_package_local(
    semantics: SchemaSemantics,
    schemas: &BTreeMap<SchemaId, Value>,
) -> bool {
    for schema in schemas.values() {
        let mut pending = vec![schema];
        while let Some(schema) = pending.pop() {
            if validate_schema_node(schema).is_err() {
                return false;
            }
            let Some(object) = schema.as_object() else {
                continue;
            };
            if let Some(reference) = object.get("$ref") {
                let Some(reference) = reference.as_str() else {
                    return false;
                };
                let Ok(schema_id) = SchemaId::from_storage(reference.to_string()) else {
                    return false;
                };
                if !schemas.contains_key(&schema_id) {
                    return false;
                }
                if !semantics.ref_siblings_apply() {
                    continue;
                }
            }
            for keyword in SCHEMA_MAP_KEYWORDS {
                let Some(children) = object.get(keyword) else {
                    continue;
                };
                let Some(children) = children.as_object() else {
                    return false;
                };
                pending.extend(children.values());
            }
            for keyword in SCHEMA_ARRAY_KEYWORDS {
                let Some(children) = object.get(keyword) else {
                    continue;
                };
                let Some(children) = children.as_array() else {
                    return false;
                };
                pending.extend(children);
            }
            for keyword in SCHEMA_VALUE_KEYWORDS {
                if let Some(child) = object.get(keyword) {
                    pending.push(child);
                }
            }
        }
    }
    true
}

fn collect_embedded_references(
    root: &Value,
    value: &Value,
    references: &mut BTreeSet<String>,
) -> Result<(), CatalogDiagnostic> {
    let Some(object) = value.as_object() else {
        return value.is_boolean().then_some(()).ok_or_else(invalid_schema);
    };
    if ["$id", "$anchor", "$dynamicRef", "$dynamicAnchor"]
        .iter()
        .any(|keyword| object.contains_key(*keyword))
    {
        return Err(CatalogDiagnostic::stable(
            "unsupported_embedded_schema_scope",
            "an embedded Tool schema uses unsupported reference scope semantics",
        ));
    }
    if let Some(reference) = object.get("$ref") {
        let reference = reference.as_str().ok_or_else(invalid_schema_reference)?;
        if !reference.starts_with('#')
            || root
                .pointer(reference.strip_prefix('#').unwrap_or_default())
                .is_none()
        {
            return Err(invalid_schema_reference());
        }
        references.insert(reference.to_string());
    }
    for keyword in SCHEMA_MAP_KEYWORDS {
        if let Some(children) = object.get(keyword) {
            for child in children.as_object().ok_or_else(invalid_schema)?.values() {
                collect_embedded_references(root, child, references)?;
            }
        }
    }
    for keyword in SCHEMA_ARRAY_KEYWORDS {
        if let Some(children) = object.get(keyword) {
            for child in children.as_array().ok_or_else(invalid_schema)? {
                collect_embedded_references(root, child, references)?;
            }
        }
    }
    for keyword in SCHEMA_VALUE_KEYWORDS {
        if let Some(child) = object.get(keyword)
            && (child.is_object() || child.is_boolean())
        {
            collect_embedded_references(root, child, references)?;
        }
    }
    Ok(())
}

fn rewrite_embedded_references(
    value: &Value,
    reference_ids: &BTreeMap<String, SchemaId>,
) -> Result<Value, CatalogDiagnostic> {
    let Some(object) = value.as_object() else {
        return Ok(value.clone());
    };
    let mut rewritten = object.clone();
    if let Some(reference) = object.get("$ref") {
        let reference = reference.as_str().ok_or_else(invalid_schema_reference)?;
        rewritten.insert(
            "$ref".to_string(),
            Value::String(
                reference_ids
                    .get(reference)
                    .ok_or_else(invalid_schema_reference)?
                    .as_str()
                    .to_string(),
            ),
        );
    }
    for keyword in SCHEMA_MAP_KEYWORDS {
        if let Some(children) = object.get(keyword) {
            let children = children.as_object().ok_or_else(invalid_schema)?;
            let mut rewritten_children = children.clone();
            for (name, child) in children {
                rewritten_children.insert(
                    name.clone(),
                    rewrite_embedded_references(child, reference_ids)?,
                );
            }
            rewritten.insert(keyword.to_string(), Value::Object(rewritten_children));
        }
    }
    for keyword in SCHEMA_ARRAY_KEYWORDS {
        if let Some(children) = object.get(keyword) {
            let children = children.as_array().ok_or_else(invalid_schema)?;
            rewritten.insert(
                keyword.to_string(),
                Value::Array(
                    children
                        .iter()
                        .map(|child| rewrite_embedded_references(child, reference_ids))
                        .collect::<Result<Vec<_>, _>>()?,
                ),
            );
        }
    }
    for keyword in SCHEMA_VALUE_KEYWORDS {
        if let Some(child) = object.get(keyword)
            && (child.is_object() || child.is_boolean())
        {
            rewritten.insert(
                keyword.to_string(),
                rewrite_embedded_references(child, reference_ids)?,
            );
        }
    }
    Ok(Value::Object(rewritten))
}

/// Removes repeated `required` names. JSON Schema requires them to be unique, but
/// repetition cannot change which instances a schema accepts, so published
/// documents that repeat a name are repaired rather than rejected.
fn deduplicate_required(schema: &mut Value) {
    let mut pending = vec![schema];
    while let Some(schema) = pending.pop() {
        let Some(object) = schema.as_object_mut() else {
            continue;
        };
        for (keyword, child) in object.iter_mut() {
            let keyword = keyword.as_str();
            if keyword == "required" {
                if let Some(names) = child.as_array_mut() {
                    let mut seen = HashSet::new();
                    names.retain(|name| seen.insert(name.clone()));
                }
            } else if SCHEMA_MAP_KEYWORDS.contains(&keyword) {
                if let Some(children) = child.as_object_mut() {
                    pending.extend(children.values_mut());
                }
            } else if SCHEMA_ARRAY_KEYWORDS.contains(&keyword) {
                if let Some(children) = child.as_array_mut() {
                    pending.extend(children.iter_mut());
                }
            } else if SCHEMA_VALUE_KEYWORDS.contains(&keyword) {
                pending.push(child);
            }
        }
    }
}

pub(super) fn validate_schema_shape(schema: &Value) -> Result<(), CatalogDiagnostic> {
    let mut pending = vec![schema];
    while let Some(schema) = pending.pop() {
        validate_schema_node(schema)?;
        let Some(object) = schema.as_object() else {
            continue;
        };
        for keyword in SCHEMA_MAP_KEYWORDS {
            if let Some(children) = object.get(keyword).and_then(Value::as_object) {
                pending.extend(children.values());
            }
        }
        for keyword in SCHEMA_ARRAY_KEYWORDS {
            if let Some(children) = object.get(keyword).and_then(Value::as_array) {
                pending.extend(children);
            }
        }
        for keyword in SCHEMA_VALUE_KEYWORDS {
            if let Some(child) = object.get(keyword) {
                pending.push(child);
            }
        }
    }
    Ok(())
}

pub(super) fn validate_schema_node(schema: &Value) -> Result<(), CatalogDiagnostic> {
    let Some(schema) = schema.as_object() else {
        return schema.is_boolean().then_some(()).ok_or_else(invalid_schema);
    };
    validate_schema_type(schema.get("type"))?;
    validate_string_keywords(schema)?;
    validate_boolean_keywords(schema)?;
    validate_numeric_keywords(schema)?;
    validate_integer_keywords(schema)?;
    validate_required(schema.get("required"))?;
    if let Some(enumeration) = schema.get("enum")
        && enumeration.as_array().is_none_or(Vec::is_empty)
    {
        return Err(invalid_schema());
    }
    for keyword in SCHEMA_MAP_KEYWORDS {
        if schema
            .get(keyword)
            .is_some_and(|children| !children.is_object())
        {
            return Err(invalid_schema());
        }
    }
    for keyword in SCHEMA_ARRAY_KEYWORDS {
        if let Some(children) = schema.get(keyword)
            && children.as_array().is_none_or(Vec::is_empty)
        {
            return Err(invalid_schema());
        }
    }
    if let Some(required_by_property) = schema.get("dependentRequired") {
        let required_by_property = required_by_property
            .as_object()
            .ok_or_else(invalid_schema)?;
        for required in required_by_property.values() {
            validate_dependent_required(required)?;
        }
    }
    Ok(())
}

fn validate_schema_type(value: Option<&Value>) -> Result<(), CatalogDiagnostic> {
    let Some(value) = value else {
        return Ok(());
    };
    let valid_type = |kind: &str| {
        matches!(
            kind,
            "null" | "boolean" | "object" | "array" | "number" | "string" | "integer"
        )
    };
    match value {
        Value::String(kind) if valid_type(kind) => Ok(()),
        Value::Array(kinds) if !kinds.is_empty() => {
            let mut seen = HashSet::new();
            if kinds.iter().all(|kind| {
                kind.as_str()
                    .is_some_and(|kind| valid_type(kind) && seen.insert(kind))
            }) {
                Ok(())
            } else {
                Err(invalid_schema())
            }
        }
        _ => Err(invalid_schema()),
    }
}

fn validate_string_keywords(schema: &Map<String, Value>) -> Result<(), CatalogDiagnostic> {
    for keyword in [
        "$ref",
        "$schema",
        "$id",
        "$anchor",
        "$dynamicRef",
        "$dynamicAnchor",
        "format",
        "pattern",
        "contentEncoding",
        "contentMediaType",
    ] {
        if schema.get(keyword).is_some_and(|value| !value.is_string()) {
            return Err(invalid_schema());
        }
    }
    Ok(())
}

fn validate_boolean_keywords(schema: &Map<String, Value>) -> Result<(), CatalogDiagnostic> {
    for keyword in [
        "nullable",
        "readOnly",
        "writeOnly",
        "deprecated",
        "uniqueItems",
    ] {
        if schema.get(keyword).is_some_and(|value| !value.is_boolean()) {
            return Err(invalid_schema());
        }
    }
    Ok(())
}

fn validate_numeric_keywords(schema: &Map<String, Value>) -> Result<(), CatalogDiagnostic> {
    for keyword in ["multipleOf", "minimum", "maximum"] {
        if schema.get(keyword).is_some_and(|value| !value.is_number()) {
            return Err(invalid_schema());
        }
    }
    for keyword in ["exclusiveMinimum", "exclusiveMaximum"] {
        if schema
            .get(keyword)
            .is_some_and(|value| !value.is_number() && !value.is_boolean())
        {
            return Err(invalid_schema());
        }
    }
    Ok(())
}

fn validate_integer_keywords(schema: &Map<String, Value>) -> Result<(), CatalogDiagnostic> {
    for keyword in [
        "minLength",
        "maxLength",
        "minItems",
        "maxItems",
        "minContains",
        "maxContains",
        "minProperties",
        "maxProperties",
    ] {
        if schema
            .get(keyword)
            .is_some_and(|value| value.as_u64().is_none())
        {
            return Err(invalid_schema());
        }
    }
    Ok(())
}

/// Accepts repeated names, which [`deduplicate_required`] removes on storage.
fn validate_required(value: Option<&Value>) -> Result<(), CatalogDiagnostic> {
    if value.is_some_and(|value| {
        !value
            .as_array()
            .is_some_and(|names| names.iter().all(Value::is_string))
    }) {
        return Err(invalid_schema());
    }
    Ok(())
}

fn validate_dependent_required(value: &Value) -> Result<(), CatalogDiagnostic> {
    let values = value.as_array().ok_or_else(invalid_schema)?;
    let mut seen = HashSet::new();
    if values.is_empty()
        || !values.iter().all(|value| {
            value
                .as_str()
                .is_some_and(|value| seen.insert(value.to_string()))
        })
    {
        return Err(invalid_schema());
    }
    Ok(())
}

pub(super) struct TypeScriptProjection {
    /// The Tool's own declaration. It names the Provider's shared types
    /// rather than declaring them.
    pub(super) declaration: String,
    /// The shared types the declaration names directly.
    pub(super) type_references: Vec<String>,
    pub(super) input_declared: bool,
    pub(super) diagnostics: Vec<CatalogDiagnostic>,
}

/// One named type of a Provider: its `type Name = …;` line and the named
/// types that line names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SharedType {
    pub(crate) declaration: String,
    pub(crate) references: Vec<String>,
}

/// The named types of one Provider, each declared once.
///
/// A Tool's declaration names these types instead of declaring them.
/// Its complete declaration is its own text plus every shared type it
/// reaches; see [`TypeClosure`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct SharedTypes(BTreeMap<String, SharedType>);

impl SharedTypes {
    fn contains(&self, name: &str) -> bool {
        self.0.contains_key(name)
    }

    fn insert(&mut self, name: String, shared: SharedType) {
        debug_assert!(
            self.0.get(&name).is_none_or(|kept| *kept == shared),
            "a shared type name means one type across the Provider"
        );
        self.0.entry(name).or_insert(shared);
    }

    /// Every shared type, by name.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (&str, &SharedType)> {
        self.0.iter().map(|(name, shared)| (name.as_str(), shared))
    }

    /// The complete declaration of a Tool whose own text is
    /// `declaration` and which names `references` directly.
    pub(crate) fn compose(&self, declaration: &str, references: &[String]) -> String {
        let mut closure = TypeClosure::new(references);
        loop {
            let names = closure.pending();
            if names.is_empty() {
                break;
            }
            let found = names
                .iter()
                .filter_map(|name| Some((name.clone(), self.0.get(name)?.clone())))
                .collect();
            closure
                .reach(&names, found)
                .expect("materialization declares every shared type a Tool names");
        }
        closure.compose(declaration)
    }
}

/// The shared types one Tool reaches, found level by level from the
/// names its own declaration names. Each level looks up only names not yet
/// reached, so a cycle ends the walk.
pub(crate) struct TypeClosure {
    reached: BTreeMap<String, String>,
    pending: Vec<String>,
}

impl TypeClosure {
    pub(crate) fn new(references: &[String]) -> Self {
        Self {
            reached: BTreeMap::new(),
            pending: references.to_vec(),
        }
    }

    /// The names to look up next, each once. Empty when the walk is done.
    pub(crate) fn pending(&mut self) -> Vec<String> {
        let mut names = std::mem::take(&mut self.pending);
        names.sort();
        names.dedup();
        names.retain(|name| !self.reached.contains_key(name));
        names
    }

    /// Records the shared types found for the `requested` names and queues
    /// the names they reference.
    ///
    /// # Errors
    ///
    /// Corrupt data when a requested name has no shared type.
    pub(crate) fn reach(
        &mut self,
        requested: &[String],
        found: Vec<(String, SharedType)>,
    ) -> Result<(), crate::Error> {
        for (name, shared) in found {
            self.pending.extend(shared.references);
            self.reached.insert(name, shared.declaration);
        }
        if requested.iter().all(|name| self.reached.contains_key(name)) {
            Ok(())
        } else {
            Err(crate::Error::CorruptData(
                "a Tool names a missing shared type".to_string(),
            ))
        }
    }

    /// The complete declaration: the reached types in name order, then the
    /// Tool's own `declaration`.
    pub(crate) fn compose(self, declaration: &str) -> String {
        let mut lines = self.reached.into_values().collect::<Vec<_>>();
        lines.push(declaration.to_string());
        lines.join("\n")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ProjectionOptions {
    honor_property_access: bool,
    binary_string_type: Option<&'static str>,
    /// Names the declaration itself declares, beside the binary string
    /// type, which no Provider schema may take.
    declared_type_names: &'static [&'static str],
    disambiguation_source_prefix: Option<&'static str>,
    widen_unguarded_reference_cycles: bool,
    use_recursive_map_index_signatures: bool,
}

impl ProjectionOptions {
    pub(super) const fn openapi() -> Self {
        Self {
            honor_property_access: true,
            binary_string_type: Some("BinaryValue"),
            declared_type_names: &crate::openapi::DECLARED_TYPE_NAMES,
            disambiguation_source_prefix: None,
            widen_unguarded_reference_cycles: true,
            use_recursive_map_index_signatures: true,
        }
    }

    /// Coral names its own schemas, so their names need no disambiguation.
    pub(super) const fn builtin() -> Self {
        Self {
            honor_property_access: false,
            binary_string_type: None,
            declared_type_names: &[],
            disambiguation_source_prefix: None,
            widen_unguarded_reference_cycles: false,
            use_recursive_map_index_signatures: false,
        }
    }

    pub(super) const fn mcp() -> Self {
        Self {
            honor_property_access: false,
            binary_string_type: None,
            declared_type_names: &[],
            disambiguation_source_prefix: Some("mcp:tool:"),
            widen_unguarded_reference_cycles: false,
            use_recursive_map_index_signatures: false,
        }
    }
}

/// Declares a Tool's callable as an anonymous default export, as a
/// Saved Function's compiler-emitted declaration does. The Call Path travels
/// beside the declaration, so a segment such as `delete`, which TypeScript
/// rejects as a function or namespace name, never has to parse here.
pub(super) fn render_callable_declaration(input_parameter: &str, return_type: &str) -> String {
    format!("export default function ({input_parameter}): {return_type};")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) enum Direction {
    Input,
    Output,
}

pub(super) fn property_is_visible(
    property: &Value,
    direction: Direction,
    honor_property_access: bool,
) -> bool {
    if !honor_property_access {
        return true;
    }
    let property = property.as_object();
    !match direction {
        Direction::Input => property
            .and_then(|object| object.get("readOnly"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
        Direction::Output => property
            .and_then(|object| object.get("writeOnly"))
            .and_then(Value::as_bool)
            .unwrap_or(false),
    }
}

type ProjectionKey = (SchemaId, Direction);

/// One named schema's projection, kept for the Provider's later Tools.
///
/// A named body does not depend on the Tool that reaches it: references
/// inside it project as names, and its reference guard starts at zero. What
/// does depend on the Tool is which diagnostics are new and which named
/// types are not yet rendered, so the render keeps its effects in order and
/// each Tool replays them.
struct NamedRender {
    body: String,
    effects: Vec<RenderEffect>,
}

enum RenderEffect {
    Warn(&'static str, &'static str),
    NamedScalar(&'static str),
    Reference {
        target: ProjectionKey,
        guarded: bool,
    },
}

/// TypeScript names for every schema of one Provider, in both directions.
///
/// Every Tool of the Provider projects through the same table, so a name
/// means one type across the Provider and any Tools' declarations can
/// share one scope.
pub(crate) struct TypeScriptNames {
    names: HashMap<ProjectionKey, String>,
    options: ProjectionOptions,
    /// Named schemas already projected for an earlier Tool. Projecting
    /// each Tool from scratch repeats the whole reachable type graph,
    /// which large Providers such as Microsoft Graph cannot afford.
    renders: RefCell<HashMap<ProjectionKey, Arc<NamedRender>>>,
}

impl TypeScriptNames {
    pub(crate) fn new(graph: &SchemaGraph, options: ProjectionOptions) -> Self {
        let mut candidates = graph
            .sources()
            .flat_map(|(source, id)| {
                [Direction::Input, Direction::Output].map(|direction| {
                    let (candidate, priority) = candidate_type_name(source, id, direction, options);
                    (candidate, priority, source, (id.clone(), direction))
                })
            })
            .collect::<Vec<_>>();
        candidates.sort();
        let mut names = HashMap::with_capacity(candidates.len());
        // A schema that takes a name the declaration itself declares is
        // renamed, as for any other collision.
        let mut claimed = options
            .binary_string_type
            .into_iter()
            .chain(options.declared_type_names.iter().copied())
            .map(str::to_string)
            .collect::<HashSet<_>>();
        let mut clashing = Vec::new();
        for (candidate, _priority, _source, key) in candidates {
            if claimed.insert(candidate.clone()) {
                names.insert(key, candidate);
            } else {
                clashing.push((candidate, key));
            }
        }
        for (candidate, key) in clashing {
            let mut suffix = 2_u32;
            let mut name = format!("{candidate}{suffix}");
            while !claimed.insert(name.clone()) {
                suffix += 1;
                name = format!("{candidate}{suffix}");
            }
            names.insert(key, name);
        }
        Self {
            names,
            options,
            renders: RefCell::default(),
        }
    }

    fn get(&self, id: &SchemaId, direction: Direction) -> Option<&String> {
        self.names.get(&(id.clone(), direction))
    }

    fn render(&self, key: &ProjectionKey) -> Option<Arc<NamedRender>> {
        self.renders.borrow().get(key).cloned()
    }

    fn keep_render(&self, key: ProjectionKey, render: NamedRender) {
        self.renders.borrow_mut().insert(key, Arc::new(render));
    }
}

pub(super) struct TypeScriptProjector<'a> {
    graph: &'a SchemaGraph,
    type_names: &'a TypeScriptNames,
    options: ProjectionOptions,
    referenced: BTreeMap<ProjectionKey, String>,
    resolving: BTreeSet<ProjectionKey>,
    rendering: Vec<ProjectionKey>,
    unguarded_reference_edges: BTreeMap<ProjectionKey, Vec<ProjectionKey>>,
    reference_guard_depth: usize,
    rendered: BTreeMap<ProjectionKey, String>,
    diagnostics: Vec<CatalogDiagnostic>,
    diagnostic_codes: HashSet<&'static str>,
    used_named_scalars: BTreeSet<&'static str>,
    /// Effects of each named render in progress, innermost last.
    recording: Vec<Vec<RenderEffect>>,
    /// Named types the Tool's own projections name, outside any named
    /// type's body.
    direct_references: BTreeSet<String>,
}

impl<'a> TypeScriptProjector<'a> {
    pub(super) fn new(
        graph: &'a SchemaGraph,
        type_names: &'a TypeScriptNames,
        options: ProjectionOptions,
    ) -> Self {
        debug_assert_eq!(
            type_names.options, options,
            "kept renders are valid only for the options that named the schemas"
        );
        Self {
            graph,
            type_names,
            options,
            referenced: BTreeMap::new(),
            resolving: BTreeSet::new(),
            rendering: Vec::new(),
            unguarded_reference_edges: BTreeMap::new(),
            reference_guard_depth: 0,
            rendered: BTreeMap::new(),
            diagnostics: Vec::new(),
            diagnostic_codes: HashSet::new(),
            used_named_scalars: BTreeSet::new(),
            recording: Vec::new(),
            direct_references: BTreeSet::new(),
        }
    }

    pub(super) fn project(
        &mut self,
        id: &SchemaId,
        direction: Direction,
    ) -> Result<String, CatalogDiagnostic> {
        self.project_id(id, direction, false)
    }

    fn project_id(
        &mut self,
        id: &SchemaId,
        direction: Direction,
        named: bool,
    ) -> Result<String, CatalogDiagnostic> {
        let graph = self.graph;
        let schema = graph.schema(id).ok_or_else(|| {
            CatalogDiagnostic::stable(
                "invalid_schema_graph",
                "a Tool references a missing package-local schema",
            )
        })?;
        if named {
            let name = self.type_name(id, direction)?;
            let key = (id.clone(), direction);
            if self.rendered.contains_key(&key) || self.resolving.contains(&key) {
                return Ok(name);
            }
            self.resolving.insert(key.clone());
            self.rendering.push(key.clone());
            let caller_guard_depth = std::mem::take(&mut self.reference_guard_depth);
            self.recording.push(Vec::new());
            let rendered = match self.type_names.render(&key) {
                Some(kept) => self.replay(&key, &kept).map(|()| kept.body.clone()),
                None => self.project_schema(schema, direction),
            };
            let effects = self.recording.pop().unwrap_or_default();
            self.reference_guard_depth = caller_guard_depth;
            self.rendering.pop();
            self.resolving.remove(&key);
            let rendered = rendered?;
            if self.type_names.render(&key).is_none() {
                self.type_names.keep_render(
                    key.clone(),
                    NamedRender {
                        body: rendered.clone(),
                        effects,
                    },
                );
            }
            self.rendered.insert(key, rendered);
            Ok(name)
        } else {
            self.project_schema(schema, direction)
        }
    }

    /// Applies a kept render's effects to this Tool in their original order.
    fn replay(&mut self, key: &ProjectionKey, kept: &NamedRender) -> Result<(), CatalogDiagnostic> {
        for effect in &kept.effects {
            match effect {
                RenderEffect::Warn(code, message) => self.warn(code, message),
                RenderEffect::NamedScalar(name) => {
                    self.used_named_scalars.insert(name);
                }
                RenderEffect::Reference { target, guarded } => {
                    if !guarded {
                        self.unguarded_reference_edges
                            .entry(key.clone())
                            .or_default()
                            .push(target.clone());
                    }
                    let _name = self.project_id(&target.0, target.1, true)?;
                }
            }
        }
        Ok(())
    }

    #[expect(
        clippy::too_many_lines,
        reason = "the local recursive projection keeps JSON Schema branch precedence explicit"
    )]
    fn project_schema(
        &mut self,
        schema: &Value,
        direction: Direction,
    ) -> Result<String, CatalogDiagnostic> {
        if let Some(allowed) = schema.as_bool() {
            return Ok(if allowed { "unknown" } else { "never" }.to_string());
        }
        let object = schema.as_object().ok_or_else(invalid_schema)?;
        if !self.graph.semantics().nullable_applies()
            && declares_nullable(object)
            && !type_admits_null(object)
        {
            self.warn(
                "nullable_ignored",
                "nullable has no effect in JSON Schema; include null in type instead",
            );
        }
        if let Some(reference) = object.get("$ref") {
            let reference = reference.as_str().ok_or_else(invalid_schema_reference)?;
            let id = SchemaId::from_storage(reference.to_string())
                .map_err(|_error| invalid_schema_reference())?;
            let target = (id.clone(), direction);
            if let Some(effects) = self.recording.last_mut() {
                effects.push(RenderEffect::Reference {
                    target: target.clone(),
                    guarded: self.reference_guard_depth != 0,
                });
            }
            if self.reference_guard_depth == 0
                && let Some(source) = self.rendering.last()
            {
                self.unguarded_reference_edges
                    .entry(source.clone())
                    .or_default()
                    .push(target);
            }
            let referenced = self.project_id(&id, direction, true)?;
            if self.rendering.is_empty() {
                self.direct_references.insert(referenced.clone());
            }
            let siblings = object
                .iter()
                .filter(|(name, _value)| name.as_str() != "$ref")
                .collect::<Vec<_>>();
            if siblings.is_empty() || self.graph.semantics() == SchemaSemantics::OpenApiV3_0 {
                return Ok(referenced);
            }
            let sibling_schema = Value::Object(
                siblings
                    .into_iter()
                    .map(|(name, value)| (name.clone(), value.clone()))
                    .collect(),
            );
            let sibling = self.project_schema(&sibling_schema, direction)?;
            return Ok(format!("({referenced} & {sibling})"));
        }
        if let Some(constant) = object.get("const") {
            let projected = literal_type(constant).unwrap_or_else(|| {
                self.warn(
                    "typescript_literal_widened",
                    "a structured JSON Schema literal is widened in TypeScript",
                );
                "unknown".to_string()
            });
            return Ok(self.with_nullable(object, projected));
        }
        if let Some(enumeration) = object.get("enum") {
            let values = enumeration.as_array().ok_or_else(invalid_schema)?;
            if values.is_empty() {
                return Ok("never".to_string());
            }
            let literals = values.iter().map(literal_type).collect::<Option<Vec<_>>>();
            let projected = literals.map_or_else(
                || {
                    self.warn(
                        "typescript_literal_widened",
                        "a structured JSON Schema literal is widened in TypeScript",
                    );
                    "unknown".to_string()
                },
                |values| values.join(" | "),
            );
            return Ok(self.with_nullable(object, projected));
        }
        for keyword in ["oneOf", "anyOf"] {
            if let Some(branches) = object.get(keyword) {
                let branches = branches.as_array().ok_or_else(invalid_schema)?;
                if branches.is_empty() {
                    return Err(invalid_schema());
                }
                return branches
                    .iter()
                    .map(|branch| self.project_schema(branch, direction))
                    .collect::<Result<Vec<_>, _>>()
                    .map(|types| self.with_nullable(object, format!("({})", types.join(" | "))));
            }
        }
        if let Some(branches) = object.get("allOf") {
            let branches = branches.as_array().ok_or_else(invalid_schema)?;
            if branches.is_empty() {
                return Err(invalid_schema());
            }
            return branches
                .iter()
                .map(|branch| self.project_schema(branch, direction))
                .collect::<Result<Vec<_>, _>>()
                .map(|types| self.with_nullable(object, format!("({})", types.join(" & "))));
        }
        if object.contains_key("not")
            || object.contains_key("if")
            || object.contains_key("then")
            || object.contains_key("else")
        {
            self.warn(
                "typescript_schema_widened",
                "a JSON Schema conditional or negation is widened in TypeScript",
            );
            return Ok("unknown".to_string());
        }

        let projected = match object.get("type") {
            Some(Value::Array(types)) => {
                let mut projections = Vec::new();
                for kind in types {
                    let kind = kind.as_str().ok_or_else(invalid_schema)?;
                    projections.push(self.project_kind(kind, object, direction)?);
                }
                projections.sort();
                projections.dedup();
                projections.join(" | ")
            }
            Some(Value::String(kind)) => self.project_kind(kind, object, direction)?,
            Some(_) => return Err(invalid_schema()),
            None if object.contains_key("properties")
                || object.contains_key("additionalProperties")
                || object.contains_key("patternProperties") =>
            {
                self.project_object(object, direction)?
            }
            None => "unknown".to_string(),
        };
        let projected = self.with_nullable(object, projected);
        self.warn_unrepresented_constraints(object);
        Ok(projected)
    }

    fn project_kind(
        &mut self,
        kind: &str,
        schema: &Map<String, Value>,
        direction: Direction,
    ) -> Result<String, CatalogDiagnostic> {
        match kind {
            "null" => Ok("null".to_string()),
            "boolean" => Ok("boolean".to_string()),
            "integer" | "number" => {
                if schema.get("format").and_then(Value::as_str) == Some("int64") {
                    self.warn(
                        "typescript_integer_widened",
                        "an integer format can exceed exact TypeScript number precision",
                    );
                }
                Ok("number".to_string())
            }
            "string" => {
                if let Some(binary_type) = self.options.binary_string_type
                    && matches!(
                        schema.get("format").and_then(Value::as_str),
                        Some("binary" | "base64")
                    )
                {
                    if let Some(effects) = self.recording.last_mut() {
                        effects.push(RenderEffect::NamedScalar(binary_type));
                    }
                    self.used_named_scalars.insert(binary_type);
                    Ok(binary_type.to_string())
                } else {
                    Ok("string".to_string())
                }
            }
            "array" => self.project_array(schema, direction),
            "object" => self.project_object(schema, direction),
            _ => Err(invalid_schema()),
        }
    }

    fn project_guarded_schema(
        &mut self,
        schema: &Value,
        direction: Direction,
    ) -> Result<String, CatalogDiagnostic> {
        self.reference_guard_depth += 1;
        let projected = self.project_schema(schema, direction);
        self.reference_guard_depth -= 1;
        projected
    }

    fn project_array(
        &mut self,
        schema: &Map<String, Value>,
        direction: Direction,
    ) -> Result<String, CatalogDiagnostic> {
        if let Some(prefix_items) = schema.get("prefixItems") {
            let prefix_items = prefix_items.as_array().ok_or_else(invalid_schema)?;
            let mut items = prefix_items
                .iter()
                .map(|item| self.project_guarded_schema(item, direction))
                .collect::<Result<Vec<_>, _>>()?;
            match schema.get("items") {
                Some(Value::Bool(false)) => {}
                Some(items_schema) => {
                    let rest = self.project_guarded_schema(items_schema, direction)?;
                    items.push(format!("...Array<{rest}>"));
                }
                None => items.push("...Array<unknown>".to_string()),
            }
            return Ok(format!("[{}]", items.join(", ")));
        }
        let item = match schema.get("items") {
            Some(Value::Bool(false)) => "never".to_string(),
            Some(items) => self.project_guarded_schema(items, direction)?,
            None => "unknown".to_string(),
        };
        Ok(format!("Array<{item}>"))
    }

    fn project_object(
        &mut self,
        schema: &Map<String, Value>,
        direction: Direction,
    ) -> Result<String, CatalogDiagnostic> {
        let required = match schema.get("required") {
            None => BTreeSet::new(),
            Some(value) => value
                .as_array()
                .ok_or_else(invalid_schema)?
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(str::to_string)
                        .ok_or_else(invalid_schema)
                })
                .collect::<Result<BTreeSet<_>, _>>()?,
        };
        let properties = match schema.get("properties") {
            None => Map::new(),
            Some(value) => value.as_object().cloned().ok_or_else(invalid_schema)?,
        };
        let mut fields = Vec::new();
        for (name, property) in properties {
            if !property_is_visible(&property, direction, self.options.honor_property_access) {
                continue;
            }
            let kind = self.project_guarded_schema(&property, direction)?;
            let optional = if required.contains(&name) { "" } else { "?" };
            let quoted = serde_json::to_string(&name).map_err(|_error| invalid_schema())?;
            fields.push(format!("{quoted}{optional}: {kind}"));
        }
        fields.sort();
        let has_pattern_properties = schema.contains_key("patternProperties");
        let exact = if fields.is_empty()
            && schema.get("additionalProperties") == Some(&Value::Bool(false))
            && !has_pattern_properties
        {
            "Record<string, never>".to_string()
        } else if fields.is_empty() {
            "{}".to_string()
        } else {
            format!("{{ {} }}", fields.join("; "))
        };
        let mut intersections = vec![exact];
        match schema.get("additionalProperties") {
            Some(Value::Bool(false)) => {
                if !fields.is_empty() || has_pattern_properties {
                    self.warn(
                        "typescript_closed_object_widened",
                        "a closed JSON Schema object is wider in structural TypeScript",
                    );
                }
            }
            Some(Value::Bool(true)) => {
                intersections.push("Record<string, unknown>".to_string());
            }
            // Undeclared extra properties still arrive at runtime, but the type
            // lists only documented fields, so a misspelled read is an error.
            // A free-form object with no fields stays an open map.
            None => {
                if fields.is_empty() {
                    intersections.push("Record<string, unknown>".to_string());
                }
            }
            Some(additional) => {
                let kind = self.project_guarded_schema(additional, direction)?;
                if fields.is_empty() && self.options.use_recursive_map_index_signatures {
                    intersections.clear();
                    intersections.push(format!("{{ [key: string]: {kind} }}"));
                } else if fields.is_empty() {
                    intersections.push(format!("Record<string, {kind}>"));
                } else {
                    intersections.push("Record<string, unknown>".to_string());
                    self.warn(
                        "typescript_additional_properties_widened",
                        "typed additional properties are widened around named TypeScript fields",
                    );
                }
            }
        }
        if let Some(patterns) = schema.get("patternProperties") {
            let patterns = patterns.as_object().ok_or_else(invalid_schema)?;
            for pattern in patterns.values() {
                let _widened_pattern_type = self.project_guarded_schema(pattern, direction)?;
            }
            intersections.push("Record<string, unknown>".to_string());
            self.warn(
                "typescript_property_pattern_widened",
                "JSON Schema property-name patterns are widened to a string index in TypeScript",
            );
        }
        Ok(if intersections.len() == 1 {
            intersections.remove(0)
        } else {
            format!("({})", intersections.join(" & "))
        })
    }

    fn warn_unrepresented_constraints(&mut self, schema: &Map<String, Value>) {
        if [
            "minimum",
            "maximum",
            "exclusiveMinimum",
            "exclusiveMaximum",
            "multipleOf",
            "minLength",
            "maxLength",
            "pattern",
            "minItems",
            "maxItems",
            "uniqueItems",
            "minProperties",
            "maxProperties",
            "propertyNames",
            "dependentRequired",
            "dependentSchemas",
            "unevaluatedItems",
            "unevaluatedProperties",
        ]
        .iter()
        .any(|keyword| schema.contains_key(*keyword))
        {
            self.warn(
                "typescript_constraint_widened",
                "a JSON Schema validation constraint is wider in TypeScript",
            );
        }
    }

    fn with_nullable(&self, schema: &Map<String, Value>, projected: String) -> String {
        if self.graph.semantics().nullable_applies()
            && declares_nullable(schema)
            && !projected.split(" | ").any(|kind| kind == "null")
        {
            format!("{projected} | null")
        } else {
            projected
        }
    }

    fn warn(&mut self, code: &'static str, message: &'static str) {
        if let Some(effects) = self.recording.last_mut() {
            effects.push(RenderEffect::Warn(code, message));
        }
        if self.diagnostic_codes.insert(code) {
            self.diagnostics
                .push(CatalogDiagnostic::warning(code, message));
        }
    }

    fn type_name(
        &mut self,
        id: &SchemaId,
        direction: Direction,
    ) -> Result<String, CatalogDiagnostic> {
        let name = self.type_names.get(id, direction).cloned().ok_or_else(|| {
            CatalogDiagnostic::stable(
                "invalid_schema_graph",
                "a Tool references a missing package-local schema",
            )
        })?;
        self.referenced
            .insert((id.clone(), direction), name.clone());
        Ok(name)
    }

    fn widen_unguarded_reference_cycles(&mut self) {
        let keys = self.rendered.keys().cloned().collect::<Vec<_>>();
        let key_indices = keys
            .iter()
            .enumerate()
            .map(|(index, key)| (key.clone(), index))
            .collect::<HashMap<_, _>>();
        let edges = keys
            .iter()
            .map(|key| {
                self.unguarded_reference_edges
                    .get(key)
                    .into_iter()
                    .flatten()
                    .filter_map(|target| key_indices.get(target).copied())
                    .collect()
            })
            .collect::<Vec<_>>();
        let cyclic = cyclic_alias_indices(&edges);
        if !cyclic.is_empty() {
            self.warn(
                "typescript_schema_widened",
                "a reference-only JSON Schema cycle is widened in TypeScript",
            );
        }
        for index in cyclic {
            if let Some(key) = keys.get(index)
                && let Some(rendered) = self.rendered.get_mut(key)
            {
                *rendered = "unknown".to_string();
            }
        }
    }

    pub(super) fn uses_named_scalar(&self, name: &str) -> bool {
        self.used_named_scalars.contains(name)
    }

    pub(super) fn into_diagnostics(self) -> Vec<CatalogDiagnostic> {
        self.diagnostics
    }

    /// Adds every named type the Tool reaches to the Provider's shared
    /// types, and returns the names its own projections name directly.
    pub(super) fn share_named_declarations(
        &mut self,
        shared: &mut SharedTypes,
    ) -> Result<Vec<String>, CatalogDiagnostic> {
        loop {
            let pending = self
                .referenced
                .keys()
                .filter(|key| !self.rendered.contains_key(*key))
                .cloned()
                .collect::<Vec<_>>();
            if pending.is_empty() {
                break;
            }
            for (id, direction) in pending {
                let _name = self.project_id(&id, direction, true)?;
            }
        }
        if self.options.widen_unguarded_reference_cycles {
            self.widen_unguarded_reference_cycles();
        }
        for (key, body) in &self.rendered {
            let name = self
                .referenced
                .get(key)
                .expect("a rendered schema always has an assigned TypeScript name");
            if shared.contains(name) {
                continue;
            }
            // A body widened to `unknown` names nothing.
            let references = if body == "unknown" {
                Vec::new()
            } else {
                self.type_names
                    .render(key)
                    .into_iter()
                    .flat_map(|kept| {
                        kept.effects
                            .iter()
                            .filter_map(|effect| match effect {
                                RenderEffect::Reference { target, .. } => {
                                    self.type_names.get(&target.0, target.1).cloned()
                                }
                                RenderEffect::Warn(..) | RenderEffect::NamedScalar(_) => None,
                            })
                            .collect::<Vec<_>>()
                    })
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect()
            };
            shared.insert(
                name.clone(),
                SharedType {
                    declaration: format!("type {name} = {body};"),
                    references,
                },
            );
        }
        Ok(self.direct_references.iter().cloned().collect())
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "all node indices originate in the exact adjacency-list graph"
)]
pub(super) fn strongly_connected_components(edges: &[&[usize]]) -> Vec<usize> {
    let mut visited = vec![false; edges.len()];
    let mut finished = Vec::with_capacity(edges.len());
    for start in 0..edges.len() {
        if visited[start] {
            continue;
        }
        visited[start] = true;
        let mut stack = vec![(start, 0_usize)];
        while let Some((node, next)) = stack.last_mut() {
            if let Some(target) = edges[*node].get(*next).copied() {
                *next += 1;
                if !visited[target] {
                    visited[target] = true;
                    stack.push((target, 0));
                }
            } else {
                let (node, _next) = stack.pop().expect("alias DFS stack is nonempty");
                finished.push(node);
            }
        }
    }

    let mut reverse = vec![Vec::new(); edges.len()];
    for (node, targets) in edges.iter().enumerate() {
        for target in *targets {
            reverse[*target].push(node);
        }
    }
    let mut components = vec![usize::MAX; edges.len()];
    let mut component = 0_usize;
    for start in finished.into_iter().rev() {
        if components[start] != usize::MAX {
            continue;
        }
        components[start] = component;
        let mut stack = vec![start];
        while let Some(node) = stack.pop() {
            for incoming in reverse[node].iter().rev().copied() {
                if components[incoming] == usize::MAX {
                    components[incoming] = component;
                    stack.push(incoming);
                }
            }
        }
        component += 1;
    }
    components
}

#[expect(
    clippy::indexing_slicing,
    reason = "all alias indices originate in the exact rendered-alias graph"
)]
fn cyclic_alias_indices(edges: &[Vec<usize>]) -> Vec<usize> {
    let components =
        strongly_connected_components(&edges.iter().map(Vec::as_slice).collect::<Vec<_>>());
    let mut component_sizes = vec![0_usize; edges.len()];
    for component in &components {
        component_sizes[*component] += 1;
    }
    edges
        .iter()
        .enumerate()
        .filter_map(|(node, targets)| {
            (component_sizes[components[node]] > 1 || targets.contains(&node)).then_some(node)
        })
        .collect()
}

/// Whether a schema sets OpenAPI 3.0's `nullable` keyword.
pub(crate) fn declares_nullable(schema: &Map<String, Value>) -> bool {
    schema.get("nullable").and_then(Value::as_bool) == Some(true)
}

fn type_admits_null(schema: &Map<String, Value>) -> bool {
    match schema.get("type") {
        Some(Value::String(kind)) => kind == "null",
        Some(Value::Array(kinds)) => kinds.iter().any(|kind| kind == "null"),
        _ => false,
    }
}

fn literal_type(value: &Value) -> Option<String> {
    match value {
        Value::Null | Value::Bool(_) | Value::String(_) | Value::Number(_) => {
            serde_json::to_string(value).ok()
        }
        _ => None,
    }
}

/// Derives the preferred TypeScript name of one schema from its source, and
/// its priority when names clash: a component named exactly that wins, then
/// other reusable components, then inline schemas.
///
/// A reusable component keeps its own name. An inline OpenAPI schema is named
/// by where it sits: method, path, and role, such as
/// `GetModelsModelParameter0Input` or `PatchAdminBodyInput`.
fn candidate_type_name(
    source: &str,
    id: &SchemaId,
    direction: Direction,
    options: ProjectionOptions,
) -> (String, u8) {
    let hash = || id.as_str().chars().skip(7).take(8).collect::<String>();
    let tokens = source.strip_prefix("#/").map(|pointer| {
        pointer
            .split('/')
            .map(decode_pointer_token)
            .collect::<Vec<_>>()
    });
    let verbatim = |name: &str| {
        tokens
            .as_ref()
            .and_then(|tokens| tokens.last())
            .is_some_and(|leaf| leaf == name)
    };
    let (mut name, reusable) = if let Some(tokens) = &tokens {
        pointer_type_name(tokens)
    } else {
        let leaf = source.rsplit('/').next().unwrap_or("Schema");
        let mut name = typescript_type_name(&decode_pointer_token(leaf));
        if options
            .disambiguation_source_prefix
            .is_some_and(|prefix| source.starts_with(prefix))
        {
            name.push_str("Schema");
            name.push_str(&hash());
        }
        (name, false)
    };
    let priority = match (reusable, direction) {
        (true, Direction::Output) if verbatim(&name) => 0,
        (true, Direction::Output) => 1,
        (true, Direction::Input) => 2,
        (false, _) => 3,
    };
    if direction == Direction::Input {
        name.push_str("Input");
    }
    if name.is_empty() {
        name = format!("Schema{}", hash());
    }
    (name, priority)
}

fn decode_pointer_token(token: &str) -> String {
    token.replace("~1", "/").replace("~0", "~")
}

/// Names a schema from its OpenAPI document pointer tokens, and reports
/// whether the pointer is a reusable component.
fn pointer_type_name(tokens: &[String]) -> (String, bool) {
    let (mut words, rest) = match tokens {
        [root, map, name, rest @ ..] if root == "components" => {
            let role = pointer_role(map).unwrap_or_default();
            (vec![name.clone(), role.to_string()], rest)
        }
        [root, name, rest @ ..] if root == "$defs" || root == "definitions" => {
            (vec![name.clone()], rest)
        }
        [root, path, method, rest @ ..] if root == "paths" && is_http_method(method) => {
            (vec![method.clone(), path.clone()], rest)
        }
        [root, path, rest @ ..] if root == "paths" => (vec![path.clone()], rest),
        _ => (Vec::new(), tokens),
    };
    let reusable = matches!(tokens, [root, map, _name] if root == "components" && map == "schemas")
        || matches!(tokens, [root, _name] if root == "$defs" || root == "definitions");
    let mut rest = rest.iter();
    while let Some(token) = rest.next() {
        match token.as_str() {
            "schema" => {}
            "content" => {
                rest.next();
            }
            "properties" | "patternProperties" | "$defs" | "definitions" | "dependentSchemas" => {
                words.extend(rest.next().cloned());
            }
            "items" => words.push("Item".to_string()),
            token => words.push(pointer_role(token).unwrap_or(token).to_string()),
        }
    }
    (typescript_type_name(&words.join(" ")), reusable)
}

/// The word an OpenAPI structural token contributes to a type name.
fn pointer_role(token: &str) -> Option<&'static str> {
    match token {
        "parameters" => Some("Parameter"),
        "requestBody" | "requestBodies" => Some("Body"),
        "responses" => Some("Response"),
        "headers" => Some("Header"),
        _ => None,
    }
}

fn is_http_method(token: &str) -> bool {
    matches!(
        token,
        "get" | "put" | "post" | "delete" | "options" | "head" | "patch" | "trace"
    )
}

fn typescript_type_name(value: &str) -> String {
    let mut name = String::new();
    let mut capitalize = true;
    for character in value.chars() {
        if character.is_ascii_alphanumeric() || character == '_' {
            if capitalize {
                name.extend(character.to_uppercase());
                capitalize = false;
            } else {
                name.push(character);
            }
        } else {
            capitalize = true;
        }
    }
    if !name
        .bytes()
        .next()
        .is_some_and(|byte| byte.is_ascii_alphabetic() || matches!(byte, b'_' | b'$'))
    {
        name.insert_str(0, "Schema");
    }
    name
}

pub(super) fn canonicalize(value: &Value) -> Value {
    let mut value = value.clone();
    value.sort_all_objects();
    value
}

fn invalid_schema() -> CatalogDiagnostic {
    CatalogDiagnostic::stable(
        "invalid_tool_schema",
        "an OpenAPI Operation contains an invalid JSON Schema",
    )
}

fn invalid_schema_reference() -> CatalogDiagnostic {
    CatalogDiagnostic::stable(
        "invalid_schema_reference",
        "an OpenAPI Operation contains an unresolved schema reference",
    )
}

#[cfg(test)]
mod tests {
    #[test]
    fn canonicalization_sorts_nested_objects_without_reordering_arrays_or_mutating_input() {
        let source = r#"{"z":[{"z":0,"a":{"z":false,"a":null}},2,1],"a":"first"}"#;
        let value: serde_json::Value = serde_json::from_str(source).unwrap();
        assert_eq!(
            super::canonicalize(&value).to_string(),
            r#"{"a":"first","z":[{"a":{"a":null,"z":false},"z":0},2,1]}"#
        );
        assert_eq!(value.to_string(), source);
    }
}
