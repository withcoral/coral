use std::cell::OnceCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;

use serde_json::Value;

use crate::{
    CatalogDiagnostic, DeclarationPath, ExecutionBinding, MaterializedOperation, SchemaGraph,
    SchemaId, SearchableOperation,
};

const MAX_SEARCH_FIELDS: usize = 8_192;
const MAX_SCHEMA_DEPTH: usize = 64;

/// One typed declaration-path contributor in prepared search material.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeclarationIndexEntry {
    path: DeclarationPath,
    text: String,
}

impl DeclarationIndexEntry {
    fn new(path: DeclarationPath, text: impl Into<String>) -> Self {
        Self {
            path,
            text: text.into(),
        }
    }

    /// Returns the validated callable-rooted public path.
    #[must_use]
    pub const fn path(&self) -> &DeclarationPath {
        &self.path
    }

    /// Returns lexical material associated with this path.
    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }
}

/// A stored Generation's schema graph, ready to rebuild the declaration
/// fields of any of its Operations by the current rules: the TypeScript
/// projection materialization uses, then the breadth-first walk. Only Universal
/// Search runs the walk, after the Generation is stored (ADR 0017).
///
/// Loading and naming a large graph costs far more than one Operation's walk,
/// so a caller that needs several Operations of one Generation keeps one of
/// these. The names are built on first use.
pub struct StoredDeclarationFields {
    graph: SchemaGraph,
    openapi_names: OnceCell<crate::schema::TypeScriptNames>,
    mcp_names: OnceCell<crate::schema::TypeScriptNames>,
}

impl StoredDeclarationFields {
    /// Prepares fields from one stored Generation's schema graph.
    #[must_use]
    pub const fn new(graph: SchemaGraph) -> Self {
        Self {
            graph,
            openapi_names: OnceCell::new(),
            mcp_names: OnceCell::new(),
        }
    }

    /// Rebuilds one structurally available Operation's declaration fields and
    /// the warnings its walk left.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::CorruptData`] when the stored Operation's input
    /// declaration can no longer be read.
    pub fn operation(
        &self,
        operation: &MaterializedOperation,
    ) -> Result<PreparedDeclarationFields, crate::Error> {
        self.fields(operation.execution_binding(), operation.input_schema())
    }

    /// Rebuilds the declaration fields of an Operation as the search index
    /// reads it, as [`Self::operation`] does for a full one.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::CorruptData`] when the stored Operation's input
    /// declaration can no longer be read.
    pub fn searchable_operation(
        &self,
        operation: &SearchableOperation,
    ) -> Result<PreparedDeclarationFields, crate::Error> {
        self.fields(operation.execution_binding(), operation.input_schema())
    }

    fn fields(
        &self,
        binding: &ExecutionBinding,
        input_schema: &SchemaId,
    ) -> Result<PreparedDeclarationFields, crate::Error> {
        let graph = &self.graph;
        let input_declared = match binding {
            ExecutionBinding::Http(binding) => crate::openapi::declares_input(
                input_schema,
                binding,
                graph,
                self.openapi_names
                    .get_or_init(|| crate::openapi::typescript_names(graph)),
            ),
            ExecutionBinding::Mcp(binding) => crate::mcp::declares_input(
                input_schema,
                binding,
                graph,
                self.mcp_names
                    .get_or_init(|| crate::mcp::typescript_names(graph)),
            ),
            // Every built-in Operation takes one `input` object.
            ExecutionBinding::Builtin(_) => Ok(true),
        };
        input_declared
            .map(|input_declared| {
                prepare_operation_declaration_fields(binding, input_schema, input_declared, graph)
            })
            .map_err(|_diagnostic| {
                crate::Error::CorruptData(
                    "a stored Operation's input declaration cannot be read".to_string(),
                )
            })
    }
}

#[derive(Clone, Copy)]
struct EvidenceProjection {
    direction: crate::schema::Direction,
    honor_property_access: bool,
}

/// Declaration fields for one Operation, and the warnings the walk left:
/// at most one for a schema it could not read and one for the field bound.
/// The warnings concern search only; they never reach the Operation (ADR 0017).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreparedDeclarationFields {
    pub(crate) entries: Vec<DeclarationIndexEntry>,
    pub(crate) warnings: Vec<CatalogDiagnostic>,
}

impl PreparedDeclarationFields {
    /// Returns the readable declaration paths with the values the schema lists.
    #[must_use]
    pub fn entries(&self) -> &[DeclarationIndexEntry] {
        &self.entries
    }

    /// Returns the warnings the walk left.
    #[must_use]
    pub fn warnings(&self) -> &[CatalogDiagnostic] {
        &self.warnings
    }

    /// Returns the fields without the warnings.
    #[must_use]
    pub fn into_entries(self) -> Vec<DeclarationIndexEntry> {
        self.entries
    }
}

/// Indexes an Operation's input and output declarations breadth-first.
///
/// The walk never fails the Operation (ADR 0017). When the field bound is
/// reached it stops and keeps the shallower fields. When the schema graph
/// cannot be read it stops and keeps the fields indexed so far. Each case
/// leaves its own warning, both when both apply, and the Operation stays
/// available.
pub(crate) fn prepare_operation_declaration_fields(
    binding: &ExecutionBinding,
    input_schema: &SchemaId,
    input_declared: bool,
    graph: &SchemaGraph,
) -> PreparedDeclarationFields {
    let mut walk = DeclarationWalk::new(graph);
    let mut warnings = Vec::new();
    if let Err(unreadable) = walk_declarations(&mut walk, binding, input_schema, input_declared) {
        warnings.push(unreadable);
    }
    if walk.omitted {
        warnings.push(truncation_warning());
    }
    let entries = walk
        .fields
        .into_iter()
        .map(|(path, values)| {
            let text = values.into_iter().collect::<Vec<_>>().join("\n");
            DeclarationIndexEntry::new(path, text)
        })
        .collect();
    PreparedDeclarationFields { entries, warnings }
}

/// Walks every root of the Operation. `Err` carries the warning for a schema
/// graph the walk cannot read, never an Error diagnostic; the fields indexed
/// before it stay in `walk`.
fn walk_declarations(
    walk: &mut DeclarationWalk<'_>,
    binding: &ExecutionBinding,
    input_schema: &SchemaId,
    input_declared: bool,
) -> Result<(), CatalogDiagnostic> {
    let honor_property_access = matches!(binding, ExecutionBinding::Http(_));
    if input_declared {
        walk.index_root(
            input_schema,
            &DeclarationPath::input(),
            EvidenceProjection {
                direction: crate::schema::Direction::Input,
                honor_property_access,
            },
        )?;
    }
    let output_projection = EvidenceProjection {
        direction: crate::schema::Direction::Output,
        honor_property_access,
    };
    match binding {
        ExecutionBinding::Http(binding) => {
            let output_path = DeclarationPath::result()
                .property("data")
                .ok_or_else(invalid_index)?;
            for response in binding.responses() {
                for content in response.content() {
                    walk.index_root(content.schema(), &output_path, output_projection)?;
                }
            }
        }
        ExecutionBinding::Mcp(binding) => {
            if let Some(output_schema) = binding.output_schema() {
                let output_path = DeclarationPath::result()
                    .property("data")
                    .and_then(|path| path.property("structuredContent"))
                    .ok_or_else(invalid_index)?;
                walk.index_root(output_schema, &output_path, output_projection)?;
            }
        }
        ExecutionBinding::Builtin(binding) => {
            let output_path = DeclarationPath::result()
                .property("data")
                .ok_or_else(invalid_index)?;
            walk.index_root(binding.output_schema(), &output_path, output_projection)?;
        }
    }
    walk.drain()
}

/// The field bound stopped the walk; the shallower fields are indexed.
fn truncation_warning() -> CatalogDiagnostic {
    CatalogDiagnostic::warning(
        "operation_search_material_truncated",
        "an Operation exceeds the Universal Search bound; only its shallower fields are indexed",
    )
}

/// One schema reference being resolved, linked to the references above it.
///
/// Queued nodes each carry their own ancestry; the shared tail avoids copying
/// a set per node.
struct Ancestor {
    schema: SchemaId,
    parent: Ancestry,
}

type Ancestry = Option<Rc<Ancestor>>;

fn is_resolving(ancestry: &Ancestry, schema: &SchemaId) -> bool {
    let mut current = ancestry.as_deref();
    while let Some(ancestor) = current {
        if &ancestor.schema == schema {
            return true;
        }
        current = ancestor.parent.as_deref();
    }
    false
}

type ChildSchemas<'g> = Box<dyn Iterator<Item = (&'g Value, DeclarationPath)> + 'g>;

/// One parent's child declaration paths, waiting for the walk to reach their level.
///
/// Children are produced one at a time, so the queue holds one entry per
/// parent rather than one per child.
struct PendingChildren<'g> {
    children: ChildSchemas<'g>,
    projection: EvidenceProjection,
    depth: usize,
    ancestry: Ancestry,
}

/// Breadth-first declaration walk shared by every root of one Operation.
///
/// Work at one path (references and composition branches) runs inline;
/// child paths join the queue, so all roots advance one level at a time.
struct DeclarationWalk<'g> {
    graph: &'g SchemaGraph,
    queue: VecDeque<PendingChildren<'g>>,
    visited: BTreeSet<(SchemaId, DeclarationPath)>,
    fields: BTreeMap<DeclarationPath, BTreeSet<String>>,
    omitted: bool,
}

impl<'g> DeclarationWalk<'g> {
    fn new(graph: &'g SchemaGraph) -> Self {
        Self {
            graph,
            queue: VecDeque::new(),
            visited: BTreeSet::new(),
            fields: BTreeMap::new(),
            omitted: false,
        }
    }

    fn index_root(
        &mut self,
        schema: &SchemaId,
        path: &DeclarationPath,
        projection: EvidenceProjection,
    ) -> Result<(), CatalogDiagnostic> {
        self.index_reference(schema, path, projection, 0, &None)
    }

    fn drain(&mut self) -> Result<(), CatalogDiagnostic> {
        while let Some(mut pending) = self.queue.pop_front() {
            if self.omitted && self.fields.len() >= MAX_SEARCH_FIELDS {
                self.queue.clear();
                break;
            }
            let Some((schema, path)) = pending.children.next() else {
                continue;
            };
            self.index_value(
                schema,
                &path,
                pending.projection,
                pending.depth,
                &pending.ancestry,
            )?;
            // The parent's remaining children stay ahead of anything queued above.
            self.queue.push_front(pending);
        }
        Ok(())
    }

    fn enqueue(
        &mut self,
        children: impl Iterator<Item = (&'g Value, DeclarationPath)> + 'g,
        projection: EvidenceProjection,
        depth: usize,
        ancestry: &Ancestry,
    ) {
        self.queue.push_back(PendingChildren {
            children: Box::new(children),
            projection,
            depth,
            ancestry: ancestry.clone(),
        });
        debug_assert!(
            self.queue.len() <= MAX_SEARCH_FIELDS,
            "one queue entry per parent keeps the walk within the field bound"
        );
    }

    fn index_reference(
        &mut self,
        schema_id: &SchemaId,
        path: &DeclarationPath,
        projection: EvidenceProjection,
        depth: usize,
        ancestry: &Ancestry,
    ) -> Result<(), CatalogDiagnostic> {
        if depth > MAX_SCHEMA_DEPTH {
            self.omitted = true;
            return Ok(());
        }
        if is_resolving(ancestry, schema_id)
            || !self.visited.insert((schema_id.clone(), path.clone()))
        {
            return Ok(());
        }
        let schema = self.graph.schema(schema_id).ok_or_else(invalid_index)?;
        let ancestry = Some(Rc::new(Ancestor {
            schema: schema_id.clone(),
            parent: ancestry.clone(),
        }));
        self.index_value(schema, path, projection, depth, &ancestry)
    }

    fn index_value(
        &mut self,
        schema: &'g Value,
        path: &DeclarationPath,
        projection: EvidenceProjection,
        depth: usize,
        ancestry: &Ancestry,
    ) -> Result<(), CatalogDiagnostic> {
        if depth > MAX_SCHEMA_DEPTH {
            self.omitted = true;
            return Ok(());
        }
        let Some(object) = schema.as_object() else {
            self.add_field(path, &schema.to_string());
            return Ok(());
        };

        let mut descriptors = vec![path.as_str().to_string()];
        for name in [
            "title",
            "description",
            "format",
            "contentEncoding",
            "contentMediaType",
        ] {
            if let Some(value) = object.get(name).and_then(Value::as_str) {
                descriptors.push(value.to_string());
            }
        }
        if let Some(kind) = object.get("type") {
            append_json_descriptor(&mut descriptors, kind)?;
        }
        for name in ["const", "enum", "default", "examples"] {
            if let Some(value) = object.get(name) {
                append_json_descriptor(&mut descriptors, value)?;
            }
        }
        if let Some(reference) = object.get("$ref").and_then(Value::as_str)
            && self.graph.semantics() == crate::SchemaSemantics::OpenApiV3_0
        {
            return self.index_reference(
                &referenced_schema(self.graph, reference)?,
                path,
                projection,
                depth + 1,
                ancestry,
            );
        }
        self.add_field(path, &descriptors.join("\n"));

        if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
            self.index_reference(
                &referenced_schema(self.graph, reference)?,
                path,
                projection,
                depth + 1,
                ancestry,
            )?;
        }
        if object.contains_key("const") || object.contains_key("enum") {
            return Ok(());
        }
        for keyword in ["oneOf", "anyOf", "allOf"] {
            if let Some(branches) = object.get(keyword) {
                for branch in branches.as_array().ok_or_else(invalid_index)? {
                    self.index_value(branch, path, projection, depth + 1, ancestry)?;
                }
                return Ok(());
            }
        }
        if ["not", "if", "then", "else"]
            .iter()
            .any(|keyword| object.contains_key(*keyword))
        {
            return Ok(());
        }

        match object.get("type") {
            Some(Value::String(kind)) => {
                self.index_kind_children(object, kind, path, projection, depth, ancestry)?;
            }
            Some(Value::Array(kinds)) => {
                for kind in kinds {
                    self.index_kind_children(
                        object,
                        kind.as_str().ok_or_else(invalid_index)?,
                        path,
                        projection,
                        depth,
                        ancestry,
                    )?;
                }
            }
            Some(_) => return Err(invalid_index()),
            None if object.contains_key("properties")
                || object.contains_key("additionalProperties")
                || object.contains_key("patternProperties") =>
            {
                self.index_object_children(object, path, projection, depth, ancestry)?;
            }
            None => {}
        }
        Ok(())
    }

    fn index_kind_children(
        &mut self,
        schema: &'g serde_json::Map<String, Value>,
        kind: &str,
        path: &DeclarationPath,
        projection: EvidenceProjection,
        depth: usize,
        ancestry: &Ancestry,
    ) -> Result<(), CatalogDiagnostic> {
        match kind {
            "array" => self.index_array_children(schema, path, projection, depth, ancestry),
            "object" => self.index_object_children(schema, path, projection, depth, ancestry),
            "null" | "boolean" | "integer" | "number" | "string" => Ok(()),
            _ => Err(invalid_index()),
        }
    }

    fn index_array_children(
        &mut self,
        schema: &'g serde_json::Map<String, Value>,
        path: &DeclarationPath,
        projection: EvidenceProjection,
        depth: usize,
        ancestry: &Ancestry,
    ) -> Result<(), CatalogDiagnostic> {
        let Some(item_path) = path.array_element() else {
            return Ok(());
        };
        let prefix_items = match schema.get("prefixItems") {
            Some(prefix_items) => prefix_items
                .as_array()
                .ok_or_else(invalid_index)?
                .as_slice(),
            None => &[],
        };
        let items = match schema.get("items") {
            Some(Value::Bool(false)) | None => None,
            Some(items) => Some(items),
        };
        if prefix_items.is_empty() && items.is_none() {
            self.add_field(&item_path, item_path.as_str());
            return Ok(());
        }
        let children = prefix_items
            .iter()
            .chain(items)
            .map(move |item| (item, item_path.clone()));
        self.enqueue(children, projection, depth + 1, ancestry);
        Ok(())
    }

    fn index_object_children(
        &mut self,
        schema: &'g serde_json::Map<String, Value>,
        path: &DeclarationPath,
        projection: EvidenceProjection,
        depth: usize,
        ancestry: &Ancestry,
    ) -> Result<(), CatalogDiagnostic> {
        let properties = match schema.get("properties") {
            Some(properties) => Some(properties.as_object().ok_or_else(invalid_index)?),
            None => None,
        };
        let index_path = path.index();
        let additional = match schema.get("additionalProperties") {
            Some(Value::Bool(false)) => None,
            Some(Value::Bool(true)) | None => {
                if let Some(index_path) = &index_path {
                    self.add_field(index_path, index_path.as_str());
                }
                None
            }
            Some(additional) => Some(additional),
        };
        let patterns = match (schema.get("patternProperties"), &index_path) {
            (Some(patterns), Some(_)) => Some(patterns.as_object().ok_or_else(invalid_index)?),
            _ => None,
        };
        let parent_path = path.clone();
        let property_children = properties
            .into_iter()
            .flatten()
            .filter(move |(_, child)| {
                crate::schema::property_is_visible(
                    child,
                    projection.direction,
                    projection.honor_property_access,
                )
            })
            .filter_map(move |(name, child)| Some((child, parent_path.property(name)?)));
        let index_children = index_path.into_iter().flat_map(move |index_path| {
            additional
                .into_iter()
                .chain(patterns.into_iter().flat_map(serde_json::Map::values))
                .map(move |child| (child, index_path.clone()))
        });
        self.enqueue(
            property_children.chain(index_children),
            projection,
            depth + 1,
            ancestry,
        );
        Ok(())
    }

    /// Records one descriptor, refusing new paths once the field bound is full.
    fn add_field(&mut self, path: &DeclarationPath, text: &str) {
        if let Some(values) = self.fields.get_mut(path) {
            values.insert(text.to_string());
        } else if self.fields.len() < MAX_SEARCH_FIELDS {
            self.fields
                .insert(path.clone(), BTreeSet::from([text.to_string()]));
        } else {
            self.omitted = true;
        }
    }
}

fn referenced_schema(graph: &SchemaGraph, reference: &str) -> Result<SchemaId, CatalogDiagnostic> {
    if reference.starts_with("schema_") {
        SchemaId::from_storage(reference.to_string()).map_err(|_error| invalid_index())
    } else {
        graph
            .schema_id_for_source(reference)
            .cloned()
            .ok_or_else(invalid_index)
    }
}

fn append_json_descriptor(
    descriptors: &mut Vec<String>,
    value: &Value,
) -> Result<(), CatalogDiagnostic> {
    let encoded = serde_json::to_string(value).map_err(|_error| invalid_index())?;
    descriptors.push(encoded);
    Ok(())
}

/// The walk met a schema it cannot read. A warning, never an error: the
/// Operation stays available with the fields indexed before it (ADR 0017).
fn invalid_index() -> CatalogDiagnostic {
    CatalogDiagnostic::warning(
        "invalid_operation_search_material",
        "part of an Operation cannot be represented in Universal Search material; only its readable fields are indexed",
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn graph(schemas: &[(&str, Value)]) -> SchemaGraph {
        let entries = schemas
            .iter()
            .map(|(source, schema)| {
                (
                    source.to_string(),
                    SchemaId::for_source(source),
                    schema.clone(),
                )
            })
            .collect::<Vec<_>>();
        SchemaGraph::new(
            crate::SchemaSemantics::JsonSchema2020_12,
            entries
                .iter()
                .map(|(_, id, schema)| (id.clone(), schema.clone()))
                .collect(),
            entries
                .iter()
                .map(|(source, id, _)| (source.clone(), id.clone()))
                .collect(),
        )
        .expect("valid test graph")
    }

    fn mcp_binding(input: &str, output: Option<&str>) -> ExecutionBinding {
        ExecutionBinding::Mcp(crate::McpExecutionBinding::new(
            "2025-06-18".to_string(),
            crate::McpTransport::StreamableHttp,
            "https://mcp.example.test".to_string(),
            crate::McpInvocationMethod::CallTool,
            Some("tool".to_string()),
            None,
            SchemaId::for_source(input),
            output.map(SchemaId::for_source),
            None,
            None,
            None,
        ))
    }

    fn wide_object(count: usize) -> Value {
        let properties = (0..count)
            .map(|index| {
                (
                    format!("f{index:05}"),
                    serde_json::json!({"type": "boolean"}),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        serde_json::json!({"type": "object", "properties": properties})
    }

    fn indexed_paths(fields: &[DeclarationIndexEntry]) -> BTreeSet<&str> {
        fields.iter().map(|field| field.path().as_str()).collect()
    }

    #[test]
    fn oversized_declaration_keeps_shallow_fields_and_warns() {
        // "deep" sorts before "shallow", so a depth-first walk would spend the
        // whole bound inside "deep" before it reached "shallow".
        let graph = graph(&[(
            "input",
            serde_json::json!({
                "type": "object",
                "properties": {
                    "deep": wide_object(MAX_SEARCH_FIELDS + 100),
                    "shallow": {"type": "string"},
                },
            }),
        )]);

        let prepared = prepare_operation_declaration_fields(
            &mcp_binding("input", None),
            &SchemaId::for_source("input"),
            true,
            &graph,
        );

        let paths = indexed_paths(&prepared.entries);
        assert_eq!(paths.len(), MAX_SEARCH_FIELDS);
        assert!(paths.contains("input.deep"));
        assert!(paths.contains("input.shallow"));
        assert_eq!(prepared.warnings.len(), 1);
        let warning = prepared.warnings.first().expect("one warning");
        assert_eq!(warning.code(), "operation_search_material_truncated");
        assert_eq!(
            warning.severity(),
            crate::CatalogDiagnosticSeverity::Warning
        );
    }

    #[test]
    fn oversized_input_does_not_starve_the_output_root() {
        let graph = graph(&[
            (
                "input",
                serde_json::json!({
                    "type": "object",
                    "properties": {"wide": wide_object(MAX_SEARCH_FIELDS + 100)},
                }),
            ),
            (
                "output",
                serde_json::json!({
                    "type": "object",
                    "properties": {"name": {"type": "string"}},
                }),
            ),
        ]);

        let prepared = prepare_operation_declaration_fields(
            &mcp_binding("input", Some("output")),
            &SchemaId::for_source("input"),
            true,
            &graph,
        );

        assert!(indexed_paths(&prepared.entries).contains("result.data.structuredContent.name"));
        assert!(!prepared.warnings.is_empty());
    }

    #[test]
    fn fan_out_through_references_keeps_the_queue_bounded() {
        // Three small schemas expand to a billion paths. Queuing every child
        // path up front would hold millions of entries before the bound stops
        // the walk.
        let fan_out = |target: &str| {
            let target = SchemaId::for_source(target);
            let properties = (0..1_000)
                .map(|index| {
                    (
                        format!("f{index:04}"),
                        serde_json::json!({"$ref": target.as_str()}),
                    )
                })
                .collect::<serde_json::Map<_, _>>();
            serde_json::json!({"type": "object", "properties": properties})
        };
        let graph = graph(&[
            ("a", fan_out("b")),
            ("b", fan_out("c")),
            ("c", wide_object(1_000)),
        ]);

        let mut walk = DeclarationWalk::new(&graph);
        walk.index_root(
            &SchemaId::for_source("a"),
            &DeclarationPath::input(),
            EvidenceProjection {
                direction: crate::schema::Direction::Input,
                honor_property_access: false,
            },
        )
        .expect("root");
        walk.drain().expect("walk");

        // `enqueue` asserts the queue bound on every push in debug builds.
        assert_eq!(walk.fields.len(), MAX_SEARCH_FIELDS);
        assert!(walk.omitted);
    }

    #[test]
    fn declaration_within_the_bound_is_complete_and_silent() {
        let graph = graph(&[("input", wide_object(10))]);

        let prepared = prepare_operation_declaration_fields(
            &mcp_binding("input", None),
            &SchemaId::for_source("input"),
            true,
            &graph,
        );

        // The root, its ten properties, and its open index signature.
        assert_eq!(prepared.entries.len(), 12);
        assert!(prepared.warnings.is_empty());
    }

    #[test]
    fn oversized_field_text_is_indexed_whole_and_keeps_the_operation() {
        // Before ADR 0017 one field over 256 KiB made the whole Operation
        // unavailable. There is no byte bound: the text is indexed as it is.
        let description = "x".repeat(300 * 1024);
        let graph = graph(&[(
            "input",
            serde_json::json!({"type": "string", "description": description}),
        )]);

        let prepared = prepare_operation_declaration_fields(
            &mcp_binding("input", None),
            &SchemaId::for_source("input"),
            true,
            &graph,
        );

        let field = prepared
            .entries
            .iter()
            .find(|field| field.path().as_str() == "input")
            .expect("the root field is indexed");
        assert!(field.text().len() > 256 * 1024);
        assert!(prepared.warnings.is_empty());
    }
}
