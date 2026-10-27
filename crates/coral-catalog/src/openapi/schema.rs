use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value};

use crate::schema::{
    SCHEMA_ARRAY_KEYWORDS, SCHEMA_MAP_KEYWORDS, SCHEMA_VALUE_KEYWORDS, SchemaGraphAccumulator,
    canonicalize, strongly_connected_components, validate_schema_node, validate_schema_shape,
};
use crate::{CatalogDiagnostic, SchemaGraph, SchemaId, SchemaSemantics};

const OPENAPI_BASE_DIALECT: &str = "https://spec.openapis.org/oas/3.1/dialect/base";
const JSON_SCHEMA_2020_12_DIALECT: &str = "https://json-schema.org/draft/2020-12/schema";
/// Scope keywords and the diagnostic message that names each one.
const UNSUPPORTED_SCOPE_KEYWORDS: [(&str, &str); 6] = [
    (
        "$id",
        "an OpenAPI schema uses the unsupported keyword `$id`",
    ),
    (
        "$anchor",
        "an OpenAPI schema uses the unsupported keyword `$anchor`",
    ),
    (
        "$dynamicRef",
        "an OpenAPI schema uses the unsupported keyword `$dynamicRef`",
    ),
    (
        "$dynamicAnchor",
        "an OpenAPI schema uses the unsupported keyword `$dynamicAnchor`",
    ),
    (
        "$recursiveRef",
        "an OpenAPI schema uses the unsupported keyword `$recursiveRef`",
    ),
    (
        "$recursiveAnchor",
        "an OpenAPI schema uses the unsupported keyword `$recursiveAnchor`",
    ),
];

pub(super) struct ResolvedObject {
    pub(super) pointer: String,
    pub(super) object: Map<String, Value>,
}

#[derive(Clone, Copy)]
pub(super) enum ReferenceObjectKind {
    PathItem,
    Parameter,
    Header,
    RequestBody,
    Response,
}

impl ReferenceObjectKind {
    const fn metadata(self) -> (&'static str, &'static str) {
        match self {
            Self::PathItem => ("pathItems", "an OpenAPI path item reference"),
            Self::Parameter => ("parameters", "an OpenAPI parameter reference"),
            Self::Header => ("headers", "an OpenAPI header reference"),
            Self::RequestBody => ("requestBodies", "an OpenAPI request-body reference"),
            Self::Response => ("responses", "an OpenAPI response reference"),
        }
    }
}

pub(super) struct OpenApiSchemaGraphBuilder<'a> {
    document: &'a Value,
    semantics: SchemaSemantics,
    provider_schema_locations: HashMap<String, &'a Value>,
    schema_analysis_outcomes: HashMap<String, Result<(), CatalogDiagnostic>>,
    schemas: SchemaGraphAccumulator,
}

impl<'a> OpenApiSchemaGraphBuilder<'a> {
    pub(super) fn new(
        document: &'a Value,
        semantics: SchemaSemantics,
    ) -> Result<Self, CatalogDiagnostic> {
        validate_root_dialect(document, semantics)?;
        let discovered = discover_schema_locations(document, semantics)?;
        let analysis = analyze_schema_dependencies(semantics, &discovered);

        let retained_sources = retained_schema_sources(&discovered, &analysis);
        let mut schemas = SchemaGraphAccumulator::new(semantics);
        for source in retained_sources {
            let schema = discovered
                .locations
                .get(&source)
                .copied()
                .ok_or_else(invalid_schema_reference)?;
            let normalized =
                normalize_provider_schema(schema, &source, semantics, &analysis.reference_targets)?;
            schemas.insert_normalized(&source, normalized);
        }

        Ok(Self {
            document,
            semantics,
            provider_schema_locations: discovered.locations,
            schema_analysis_outcomes: analysis.outcomes,
            schemas,
        })
    }

    pub(super) fn retain_provider_schema(
        &mut self,
        pointer: &str,
    ) -> Result<SchemaId, CatalogDiagnostic> {
        self.provider_schema_locations
            .get(pointer)
            .ok_or_else(invalid_schema_reference)?;
        self.schema_analysis_outcomes
            .get(pointer)
            .cloned()
            .unwrap_or_else(|| Err(invalid_schema_reference()))?;
        let id = SchemaId::for_source(pointer);
        self.schemas
            .contains(&id)
            .then_some(id)
            .ok_or_else(invalid_schema_reference)
    }

    pub(super) fn add_generated_schema(
        &mut self,
        source: &str,
        value: &Value,
    ) -> Result<SchemaId, CatalogDiagnostic> {
        validate_schema_shape(value)?;
        validate_generated_references(value, self.semantics, &self.schemas)?;
        Ok(self.schemas.insert_normalized(source, canonicalize(value)))
    }

    pub(super) fn resolve_reference_object(
        &self,
        pointer: &str,
        kind: ReferenceObjectKind,
    ) -> Result<ResolvedObject, CatalogDiagnostic> {
        resolve_reference_object(self.document, pointer, kind)
    }

    pub(super) fn value(&self, pointer: &str) -> Option<&Value> {
        value_at(self.document, pointer)
    }

    pub(super) fn schema(&self, id: &SchemaId) -> Option<&Value> {
        self.schemas.schema(id)
    }

    pub(super) const fn semantics(&self) -> SchemaSemantics {
        self.semantics
    }

    pub(super) fn finish(self) -> Result<SchemaGraph, CatalogDiagnostic> {
        self.schemas.finish()
    }
}

/// Resolves structural OpenAPI references without analyzing request/response schemas.
pub(super) fn resolve_reference_object(
    document: &Value,
    pointer: &str,
    kind: ReferenceObjectKind,
) -> Result<ResolvedObject, CatalogDiagnostic> {
    let mut current = pointer.to_string();
    let (component_map, invalid_description) = kind.metadata();
    let mut resolving = HashSet::new();
    loop {
        let object = value_at(document, &current)
            .and_then(Value::as_object)
            .cloned()
            .ok_or_else(|| {
                diagnostic(
                    "invalid_openapi_operation",
                    "an OpenAPI definition is not an object",
                )
            })?;
        let Some(reference) = object.get("$ref") else {
            return Ok(ResolvedObject {
                pointer: current,
                object,
            });
        };
        validate_reference_siblings(&object)?;
        let reference = reference.as_str().ok_or_else(|| {
            diagnostic(
                "invalid_openapi_operation",
                "an OpenAPI reference is not a string",
            )
        })?;
        let target = canonical_document_pointer(reference).map_err(|error| match error {
            PointerError::External => diagnostic(
                "unsupported_external_openapi_reference",
                "a Tool depends on an external or unsupported OpenAPI reference",
            ),
            PointerError::Scope | PointerError::Invalid => {
                diagnostic("invalid_openapi_operation", invalid_description)
            }
        })?;
        let tokens = pointer_tokens(&target)
            .ok_or_else(|| diagnostic("invalid_openapi_operation", invalid_description))?;
        if !matches!(
            tokens.as_slice(),
            [root, candidate_map, _name]
                if root == "components" && candidate_map == component_map
        ) {
            return Err(diagnostic(
                "unsupported_external_openapi_reference",
                "a Tool depends on an external or unsupported OpenAPI reference",
            ));
        }
        if !resolving.insert(target.clone()) {
            return Err(diagnostic(
                "invalid_openapi_operation",
                "OpenAPI component references contain a cycle",
            ));
        }
        current = target;
    }
}

struct DiscoveredSchemas<'a> {
    locations: HashMap<String, &'a Value>,
    source_order: Vec<String>,
    retained_roots: HashSet<String>,
    containment: HashMap<String, Vec<String>>,
}

#[derive(Clone, Copy)]
enum OpenApiObjectKind {
    PathItem,
    Operation,
    Parameter,
    Header,
    MediaType,
    RequestBody,
    Response,
    Callback,
    Encoding,
}

impl OpenApiObjectKind {
    const fn allows_reference_object(self) -> bool {
        !matches!(self, Self::Operation | Self::MediaType | Self::Encoding)
    }
}

enum DiscoveryTask<'a> {
    Schema {
        pointer: String,
        value: &'a Value,
        retained_root: bool,
    },
    Object {
        pointer: String,
        value: &'a Value,
        kind: OpenApiObjectKind,
    },
}

#[expect(
    clippy::too_many_lines,
    reason = "the explicit discovery stack keeps every supported OpenAPI schema-bearing location together"
)]
fn discover_schema_locations(
    document: &Value,
    semantics: SchemaSemantics,
) -> Result<DiscoveredSchemas<'_>, CatalogDiagnostic> {
    let root = document.as_object().ok_or_else(|| {
        diagnostic(
            "invalid_openapi_document",
            "the pinned OpenAPI document root must be an object",
        )
    })?;
    let mut tasks = Vec::new();
    push_path_items(root.get("paths"), "#/paths", &mut tasks);
    if semantics == SchemaSemantics::JsonSchema2020_12 {
        push_map_objects(
            root.get("webhooks"),
            "#/webhooks",
            OpenApiObjectKind::PathItem,
            &mut tasks,
        );
    }

    if let Some(components) = root.get("components") {
        let components = components.as_object().ok_or_else(|| {
            diagnostic(
                "invalid_openapi_document",
                "the pinned OpenAPI components value is not an object",
            )
        })?;
        if let Some(schemas) = components.get("schemas") {
            let schemas = schemas.as_object().ok_or_else(|| {
                diagnostic(
                    "invalid_schema_graph",
                    "the pinned OpenAPI components.schemas value is not an object",
                )
            })?;
            for (name, schema) in schemas.iter().rev() {
                let pointer = child_pointer("#/components/schemas", name);
                tasks.push(DiscoveryTask::Schema {
                    pointer,
                    value: schema,
                    retained_root: true,
                });
            }
        }
        for (field, kind) in [
            ("parameters", OpenApiObjectKind::Parameter),
            ("headers", OpenApiObjectKind::Header),
            ("requestBodies", OpenApiObjectKind::RequestBody),
            ("responses", OpenApiObjectKind::Response),
            ("callbacks", OpenApiObjectKind::Callback),
            ("pathItems", OpenApiObjectKind::PathItem),
        ] {
            if field == "pathItems" && semantics == SchemaSemantics::OpenApiV3_0 {
                continue;
            }
            push_map_objects(
                components.get(field),
                &format!("#/components/{field}"),
                kind,
                &mut tasks,
            );
        }
    }
    let mut locations = HashMap::new();
    let mut source_order = Vec::new();
    let mut retained_roots = HashSet::new();
    let mut containment = HashMap::<String, Vec<String>>::new();
    let mut visited_objects = HashSet::new();
    while let Some(task) = tasks.pop() {
        match task {
            DiscoveryTask::Schema {
                pointer,
                value,
                retained_root,
            } => {
                if retained_root {
                    retained_roots.insert(pointer.clone());
                }
                if locations.insert(pointer.clone(), value).is_some() {
                    continue;
                }
                source_order.push(pointer.clone());
                containment.entry(pointer.clone()).or_default();
                let Some(object) = value.as_object() else {
                    continue;
                };
                if !semantics.ref_siblings_apply() && object.contains_key("$ref") {
                    continue;
                }
                for keyword in SCHEMA_MAP_KEYWORDS.into_iter().rev() {
                    let Some(children) = object.get(keyword).and_then(Value::as_object) else {
                        continue;
                    };
                    for (name, child_value) in children.iter().rev() {
                        let child = child_pointer(&child_pointer(&pointer, keyword), name);
                        containment
                            .entry(pointer.clone())
                            .or_default()
                            .push(child.clone());
                        tasks.push(DiscoveryTask::Schema {
                            pointer: child,
                            value: child_value,
                            retained_root: false,
                        });
                    }
                }
                for keyword in SCHEMA_ARRAY_KEYWORDS.into_iter().rev() {
                    let Some(children) = object.get(keyword).and_then(Value::as_array) else {
                        continue;
                    };
                    for (index, child_value) in children.iter().enumerate().rev() {
                        let child = format!("{pointer}/{keyword}/{index}");
                        containment
                            .entry(pointer.clone())
                            .or_default()
                            .push(child.clone());
                        tasks.push(DiscoveryTask::Schema {
                            pointer: child,
                            value: child_value,
                            retained_root: false,
                        });
                    }
                }
                for keyword in SCHEMA_VALUE_KEYWORDS.into_iter().rev() {
                    if let Some(child_value) = object.get(keyword) {
                        let child = child_pointer(&pointer, keyword);
                        containment
                            .entry(pointer.clone())
                            .or_default()
                            .push(child.clone());
                        tasks.push(DiscoveryTask::Schema {
                            pointer: child,
                            value: child_value,
                            retained_root: false,
                        });
                    }
                }
            }
            DiscoveryTask::Object {
                pointer,
                value,
                kind,
            } => {
                if !visited_objects.insert(pointer.clone()) {
                    continue;
                }
                let Some(object) = value.as_object() else {
                    continue;
                };
                if kind.allows_reference_object() && object.contains_key("$ref") {
                    continue;
                }
                discover_object_children(object, &pointer, kind, &mut tasks);
            }
        }
    }
    Ok(DiscoveredSchemas {
        locations,
        source_order,
        retained_roots,
        containment,
    })
}

#[expect(
    clippy::too_many_lines,
    reason = "one match makes the bounded OpenAPI visitor's legal object locations auditable"
)]
fn discover_object_children<'a>(
    object: &'a Map<String, Value>,
    pointer: &str,
    kind: OpenApiObjectKind,
    tasks: &mut Vec<DiscoveryTask<'a>>,
) {
    match kind {
        OpenApiObjectKind::PathItem => {
            push_array_objects(
                object.get("parameters"),
                &child_pointer(pointer, "parameters"),
                OpenApiObjectKind::Parameter,
                tasks,
            );
            for method in [
                "trace", "patch", "options", "head", "delete", "post", "put", "get",
            ] {
                if let Some(value) = object.get(method) {
                    tasks.push(DiscoveryTask::Object {
                        pointer: child_pointer(pointer, method),
                        value,
                        kind: OpenApiObjectKind::Operation,
                    });
                }
            }
        }
        OpenApiObjectKind::Operation => {
            push_array_objects(
                object.get("parameters"),
                &child_pointer(pointer, "parameters"),
                OpenApiObjectKind::Parameter,
                tasks,
            );
            if let Some(value) = object.get("requestBody") {
                tasks.push(DiscoveryTask::Object {
                    pointer: child_pointer(pointer, "requestBody"),
                    value,
                    kind: OpenApiObjectKind::RequestBody,
                });
            }
            push_responses(
                object.get("responses"),
                &child_pointer(pointer, "responses"),
                tasks,
            );
            push_map_objects(
                object.get("callbacks"),
                &child_pointer(pointer, "callbacks"),
                OpenApiObjectKind::Callback,
                tasks,
            );
        }
        OpenApiObjectKind::Parameter | OpenApiObjectKind::Header => {
            if let Some(value) = object.get("schema") {
                tasks.push(DiscoveryTask::Schema {
                    pointer: child_pointer(pointer, "schema"),
                    value,
                    retained_root: true,
                });
            }
            push_map_objects(
                object.get("content"),
                &child_pointer(pointer, "content"),
                OpenApiObjectKind::MediaType,
                tasks,
            );
        }
        OpenApiObjectKind::MediaType => {
            if let Some(value) = object.get("schema") {
                tasks.push(DiscoveryTask::Schema {
                    pointer: child_pointer(pointer, "schema"),
                    value,
                    retained_root: true,
                });
            }
            push_map_objects(
                object.get("encoding"),
                &child_pointer(pointer, "encoding"),
                OpenApiObjectKind::Encoding,
                tasks,
            );
        }
        OpenApiObjectKind::RequestBody => push_map_objects(
            object.get("content"),
            &child_pointer(pointer, "content"),
            OpenApiObjectKind::MediaType,
            tasks,
        ),
        OpenApiObjectKind::Response => {
            push_map_objects(
                object.get("headers"),
                &child_pointer(pointer, "headers"),
                OpenApiObjectKind::Header,
                tasks,
            );
            push_map_objects(
                object.get("content"),
                &child_pointer(pointer, "content"),
                OpenApiObjectKind::MediaType,
                tasks,
            );
        }
        OpenApiObjectKind::Callback => {
            for (expression, value) in object.iter().rev() {
                if !expression.starts_with("x-") {
                    tasks.push(DiscoveryTask::Object {
                        pointer: child_pointer(pointer, expression),
                        value,
                        kind: OpenApiObjectKind::PathItem,
                    });
                }
            }
        }
        OpenApiObjectKind::Encoding => push_map_objects(
            object.get("headers"),
            &child_pointer(pointer, "headers"),
            OpenApiObjectKind::Header,
            tasks,
        ),
    }
}

fn push_path_items<'a>(
    value: Option<&'a Value>,
    pointer: &str,
    tasks: &mut Vec<DiscoveryTask<'a>>,
) {
    let Some(paths) = value.and_then(Value::as_object) else {
        return;
    };
    for (path, value) in paths.iter().rev() {
        if path.starts_with('/') {
            tasks.push(DiscoveryTask::Object {
                pointer: child_pointer(pointer, path),
                value,
                kind: OpenApiObjectKind::PathItem,
            });
        }
    }
}

fn push_responses<'a>(value: Option<&'a Value>, pointer: &str, tasks: &mut Vec<DiscoveryTask<'a>>) {
    let Some(responses) = value.and_then(Value::as_object) else {
        return;
    };
    for (status, value) in responses.iter().rev() {
        if response_key_is_legal(status) {
            tasks.push(DiscoveryTask::Object {
                pointer: child_pointer(pointer, status),
                value,
                kind: OpenApiObjectKind::Response,
            });
        }
    }
}

pub(super) fn response_key_is_legal(value: &str) -> bool {
    if value == "default" {
        return true;
    }
    let bytes = value.as_bytes();
    bytes.len() == 3
        && bytes
            .first()
            .is_some_and(|byte| (b'1'..=b'5').contains(byte))
        && (bytes.iter().skip(1).all(u8::is_ascii_digit)
            || bytes.iter().skip(1).all(|byte| matches!(byte, b'X' | b'x')))
}

fn push_map_objects<'a>(
    value: Option<&'a Value>,
    pointer: &str,
    kind: OpenApiObjectKind,
    tasks: &mut Vec<DiscoveryTask<'a>>,
) {
    let Some(values) = value.and_then(Value::as_object) else {
        return;
    };
    for (name, value) in values.iter().rev() {
        tasks.push(DiscoveryTask::Object {
            pointer: child_pointer(pointer, name),
            value,
            kind,
        });
    }
}

fn push_array_objects<'a>(
    value: Option<&'a Value>,
    pointer: &str,
    kind: OpenApiObjectKind,
    tasks: &mut Vec<DiscoveryTask<'a>>,
) {
    let Some(values) = value.and_then(Value::as_array) else {
        return;
    };
    for (index, value) in values.iter().enumerate().rev() {
        tasks.push(DiscoveryTask::Object {
            pointer: format!("{pointer}/{index}"),
            value,
            kind,
        });
    }
}

struct NodeAnalysis {
    dependencies: Vec<usize>,
    reference_dependency: Option<usize>,
    local_failure: Option<CatalogDiagnostic>,
}

struct SchemaAnalysis {
    outcomes: HashMap<String, Result<(), CatalogDiagnostic>>,
    reference_targets: HashMap<String, String>,
}

#[expect(
    clippy::indexing_slicing,
    clippy::too_many_lines,
    reason = "indices are built from the same node and SCC vectors; keeping the linear passes together makes failure propagation auditable"
)]
fn analyze_schema_dependencies(
    semantics: SchemaSemantics,
    discovered: &DiscoveredSchemas<'_>,
) -> SchemaAnalysis {
    let sources = discovered.source_order.clone();
    let source_indices = sources
        .iter()
        .enumerate()
        .map(|(index, source)| (source.as_str(), index))
        .collect::<HashMap<_, _>>();
    let mut analyses = Vec::with_capacity(sources.len());
    for source in &sources {
        let mut local_failure = None;
        let mut reference_dependency = None;
        let mut dependencies = discovered
            .containment
            .get(source)
            .into_iter()
            .flatten()
            .filter_map(|dependency| source_indices.get(dependency.as_str()).copied())
            .collect::<Vec<_>>();
        let schema = discovered.locations.get(source).copied();
        match schema {
            None => local_failure = Some(invalid_schema_reference()),
            Some(Value::Bool(_)) => {}
            Some(Value::Object(object)) => {
                if object
                    .get("$ref")
                    .is_some_and(|reference| !reference.is_string())
                {
                    local_failure = Some(invalid_schema_reference());
                }
                let reference_only = !semantics.ref_siblings_apply() && object.contains_key("$ref");
                if local_failure.is_none() && !reference_only {
                    if let Some((_, message)) = UNSUPPORTED_SCOPE_KEYWORDS
                        .iter()
                        .find(|(keyword, _)| object.contains_key(*keyword))
                    {
                        local_failure =
                            Some(diagnostic("unsupported_schema_reference_scope", message));
                    } else if let Some(declaration) = object.get("$schema")
                        && !supported_dialect_declaration(declaration, semantics)
                    {
                        local_failure = Some(unsupported_schema_dialect());
                    }
                    if local_failure.is_none()
                        && let Err(diagnostic) =
                            validate_schema_node(schema.expect("present schema"))
                    {
                        local_failure = Some(diagnostic);
                    }
                }
                if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
                    match canonical_document_pointer(reference) {
                        Ok(target) => {
                            if let Some(index) = source_indices.get(target.as_str()) {
                                dependencies.push(*index);
                                reference_dependency = Some(*index);
                            } else if local_failure.is_none() {
                                local_failure = Some(invalid_schema_reference());
                            }
                        }
                        Err(error) if local_failure.is_none() => {
                            local_failure = Some(pointer_diagnostic(error));
                        }
                        Err(_error) => {}
                    }
                }
            }
            Some(_) => local_failure = Some(invalid_tool_schema()),
        }
        analyses.push(NodeAnalysis {
            dependencies,
            reference_dependency,
            local_failure: local_failure.map(|failure| failure.at_location(source)),
        });
    }

    let dependencies = analyses
        .iter()
        .map(|node| node.dependencies.as_slice())
        .collect::<Vec<_>>();
    let components = strongly_connected_components(&dependencies);
    let component_count = components
        .iter()
        .copied()
        .max()
        .map_or(0, |maximum| maximum + 1);
    let mut members = vec![Vec::new(); component_count];
    for (node, component) in components.iter().copied().enumerate() {
        members[component].push(node);
    }
    let mut component_dependencies = vec![Vec::new(); component_count];
    for (node, analysis) in analyses.iter().enumerate() {
        let component = components[node];
        for dependency in &analysis.dependencies {
            let dependency_component = components[*dependency];
            if dependency_component != component {
                component_dependencies[component].push(dependency_component);
            }
        }
    }
    let processing_order = dependency_first_order(&component_dependencies);
    let mut failures = vec![None; component_count];
    for component in processing_order {
        let local = members[component]
            .iter()
            .find_map(|node| analyses[*node].local_failure.clone());
        failures[component] = local.or_else(|| {
            component_dependencies[component]
                .iter()
                .find_map(|dependency| failures[*dependency].clone())
        });
    }

    let reference_targets = sources
        .iter()
        .enumerate()
        .filter_map(|(node, source)| {
            analyses[node]
                .reference_dependency
                .map(|target| (source.clone(), sources[target].clone()))
        })
        .collect();
    let outcomes = sources
        .into_iter()
        .enumerate()
        .map(|(node, source)| {
            let outcome = failures[components[node]].clone().map_or(Ok(()), Err);
            (source, outcome)
        })
        .collect();
    SchemaAnalysis {
        outcomes,
        reference_targets,
    }
}

fn retained_schema_sources(
    discovered: &DiscoveredSchemas<'_>,
    analysis: &SchemaAnalysis,
) -> Vec<String> {
    let mut retained_set = HashSet::new();
    let mut retained = Vec::new();
    for source in &discovered.source_order {
        if discovered.retained_roots.contains(source)
            && analysis.outcomes.get(source).is_some_and(Result::is_ok)
            && retained_set.insert(source.clone())
        {
            retained.push(source.clone());
        }
    }
    let mut pending_roots = retained.clone();
    let mut expanded_locations = HashSet::new();
    while let Some(root) = pending_roots.pop() {
        let mut pending_locations = vec![root];
        while let Some(source) = pending_locations.pop() {
            if !expanded_locations.insert(source.clone()) {
                continue;
            }
            if let Some(target) = analysis.reference_targets.get(&source)
                && retained_set.insert(target.clone())
            {
                retained.push(target.clone());
                pending_roots.push(target.clone());
            }
            if let Some(children) = discovered.containment.get(&source) {
                pending_locations.extend(children.iter().rev().cloned());
            }
        }
    }
    retained
}

#[expect(
    clippy::indexing_slicing,
    reason = "all component indices originate in this exact condensed graph"
)]
fn dependency_first_order(dependencies: &[Vec<usize>]) -> Vec<usize> {
    let mut visited = vec![false; dependencies.len()];
    let mut order = Vec::with_capacity(dependencies.len());
    for start in 0..dependencies.len() {
        if visited[start] {
            continue;
        }
        visited[start] = true;
        let mut stack = vec![(start, dependencies[start].iter())];
        while let Some((_component, pending)) = stack.last_mut() {
            if let Some(dependency) = pending.next().copied() {
                if !visited[dependency] {
                    visited[dependency] = true;
                    stack.push((dependency, dependencies[dependency].iter()));
                }
            } else {
                let (component, _pending) = stack.pop().expect("DAG stack is nonempty");
                order.push(component);
            }
        }
    }
    order
}

pub(super) fn validate_root_dialect(
    document: &Value,
    semantics: SchemaSemantics,
) -> Result<(), CatalogDiagnostic> {
    let declaration = document
        .as_object()
        .and_then(|root| root.get("jsonSchemaDialect"));
    if declaration.is_some_and(|value| !supported_dialect_declaration(value, semantics)) {
        return Err(unsupported_schema_dialect());
    }
    Ok(())
}

fn supported_dialect_declaration(value: &Value, semantics: SchemaSemantics) -> bool {
    if semantics == SchemaSemantics::OpenApiV3_0 {
        return false;
    }
    value.as_str().is_some_and(|value| {
        let without_empty_fragment = value.strip_suffix('#').unwrap_or(value);
        matches!(
            without_empty_fragment,
            OPENAPI_BASE_DIALECT | JSON_SCHEMA_2020_12_DIALECT
        )
    })
}

fn validate_generated_references(
    schema: &Value,
    semantics: SchemaSemantics,
    schemas: &SchemaGraphAccumulator,
) -> Result<(), CatalogDiagnostic> {
    let mut pending = vec![schema];
    while let Some(schema) = pending.pop() {
        let Some(object) = schema.as_object() else {
            continue;
        };
        if let Some(reference) = object.get("$ref") {
            let reference = reference.as_str().ok_or_else(invalid_schema_reference)?;
            let id = SchemaId::from_storage(reference.to_string())
                .map_err(|_error| invalid_schema_reference())?;
            if !schemas.contains(&id) {
                return Err(invalid_schema_reference());
            }
            if !semantics.ref_siblings_apply() {
                continue;
            }
        }
        push_schema_children(object, &mut pending)?;
    }
    Ok(())
}

fn push_schema_children<'a>(
    object: &'a Map<String, Value>,
    pending: &mut Vec<&'a Value>,
) -> Result<(), CatalogDiagnostic> {
    for keyword in SCHEMA_MAP_KEYWORDS {
        if let Some(children) = object.get(keyword) {
            pending.extend(
                children
                    .as_object()
                    .ok_or_else(invalid_tool_schema)?
                    .values(),
            );
        }
    }
    for keyword in SCHEMA_ARRAY_KEYWORDS {
        if let Some(children) = object.get(keyword) {
            pending.extend(children.as_array().ok_or_else(invalid_tool_schema)?);
        }
    }
    for keyword in SCHEMA_VALUE_KEYWORDS {
        if let Some(child) = object.get(keyword) {
            pending.push(child);
        }
    }
    Ok(())
}

enum CopyTask<'a> {
    Schema {
        value: &'a Value,
        source: String,
    },
    SchemaMap {
        object: &'a Map<String, Value>,
        source: String,
    },
    SchemaArray {
        array: &'a [Value],
        source: String,
    },
    Raw(&'a Value),
    Emit(Value),
    AssembleArray(usize),
    AssembleObject(Vec<String>),
}

#[expect(
    clippy::too_many_lines,
    reason = "one explicit post-order stack copies literals and rewrites only Schema Object references without recursion"
)]
fn normalize_provider_schema(
    schema: &Value,
    source: &str,
    semantics: SchemaSemantics,
    reference_targets: &HashMap<String, String>,
) -> Result<Value, CatalogDiagnostic> {
    let mut tasks = vec![CopyTask::Schema {
        value: schema,
        source: source.to_string(),
    }];
    let mut values = Vec::new();
    while let Some(task) = tasks.pop() {
        match task {
            CopyTask::Schema { value, source } => {
                let Some(object) = value.as_object() else {
                    values.push(value.clone());
                    continue;
                };
                if !semantics.ref_siblings_apply() && object.contains_key("$ref") {
                    values.push(Value::Object(Map::from_iter([(
                        "$ref".to_string(),
                        Value::String(package_reference(&source, reference_targets)?),
                    )])));
                    continue;
                }
                let keys = object.keys().cloned().collect::<Vec<_>>();
                tasks.push(CopyTask::AssembleObject(keys.clone()));
                for key in keys.iter().rev() {
                    let child = object.get(key).expect("key came from the same object");
                    if key == "$ref" {
                        tasks.push(CopyTask::Emit(Value::String(package_reference(
                            &source,
                            reference_targets,
                        )?)));
                    } else if SCHEMA_MAP_KEYWORDS.contains(&key.as_str()) {
                        tasks.push(CopyTask::SchemaMap {
                            object: child.as_object().ok_or_else(invalid_tool_schema)?,
                            source: child_pointer(&source, key),
                        });
                    } else if SCHEMA_ARRAY_KEYWORDS.contains(&key.as_str()) {
                        tasks.push(CopyTask::SchemaArray {
                            array: child.as_array().ok_or_else(invalid_tool_schema)?,
                            source: child_pointer(&source, key),
                        });
                    } else if SCHEMA_VALUE_KEYWORDS.contains(&key.as_str()) {
                        tasks.push(CopyTask::Schema {
                            value: child,
                            source: child_pointer(&source, key),
                        });
                    } else {
                        tasks.push(CopyTask::Raw(child));
                    }
                }
            }
            CopyTask::SchemaMap { object, source } => {
                let keys = object.keys().cloned().collect::<Vec<_>>();
                tasks.push(CopyTask::AssembleObject(keys.clone()));
                for key in keys.iter().rev() {
                    tasks.push(CopyTask::Schema {
                        value: object.get(key).expect("key came from the same object"),
                        source: child_pointer(&source, key),
                    });
                }
            }
            CopyTask::SchemaArray { array, source } => {
                tasks.push(CopyTask::AssembleArray(array.len()));
                for (index, child) in array.iter().enumerate().rev() {
                    tasks.push(CopyTask::Schema {
                        value: child,
                        source: format!("{source}/{index}"),
                    });
                }
            }
            CopyTask::Raw(value) => match value {
                Value::Array(array) => {
                    tasks.push(CopyTask::AssembleArray(array.len()));
                    for child in array.iter().rev() {
                        tasks.push(CopyTask::Raw(child));
                    }
                }
                Value::Object(object) => {
                    let keys = object.keys().cloned().collect::<Vec<_>>();
                    tasks.push(CopyTask::AssembleObject(keys.clone()));
                    for key in keys.iter().rev() {
                        tasks.push(CopyTask::Raw(
                            object.get(key).expect("key came from the same object"),
                        ));
                    }
                }
                _ => values.push(value.clone()),
            },
            CopyTask::Emit(value) => values.push(value),
            CopyTask::AssembleArray(length) => {
                let start = values
                    .len()
                    .checked_sub(length)
                    .ok_or_else(invalid_tool_schema)?;
                let children = values.split_off(start);
                values.push(Value::Array(children));
            }
            CopyTask::AssembleObject(keys) => {
                let start = values
                    .len()
                    .checked_sub(keys.len())
                    .ok_or_else(invalid_tool_schema)?;
                let children = values.split_off(start);
                values.push(Value::Object(keys.into_iter().zip(children).collect()));
            }
        }
    }
    values.pop().ok_or_else(invalid_tool_schema)
}

fn package_reference(
    source: &str,
    reference_targets: &HashMap<String, String>,
) -> Result<String, CatalogDiagnostic> {
    reference_targets
        .get(source)
        .map(|target| SchemaId::for_source(target).as_str().to_string())
        .ok_or_else(invalid_schema_reference)
}

#[derive(Clone, Copy)]
enum PointerError {
    External,
    Scope,
    Invalid,
}

fn canonical_document_pointer(reference: &str) -> Result<String, PointerError> {
    if reference.starts_with("schema_") {
        return Err(PointerError::Invalid);
    }
    let fragment = reference.strip_prefix('#').ok_or(PointerError::External)?;
    let decoded = percent_decode_utf8(fragment).ok_or(PointerError::Invalid)?;
    if decoded.is_empty() {
        return Ok("#".to_string());
    }
    if !decoded.starts_with('/') {
        return Err(PointerError::Scope);
    }
    let mut canonical = String::from("#");
    for token in decoded.split('/').skip(1) {
        let token = unescape_pointer_token(token).ok_or(PointerError::Invalid)?;
        canonical.push('/');
        canonical.push_str(&escape_pointer_token(&token));
    }
    Ok(canonical)
}

fn percent_decode_utf8(value: &str) -> Option<String> {
    let mut encoded = value.bytes();
    let mut decoded = Vec::with_capacity(value.len());
    while let Some(byte) = encoded.next() {
        if byte == b'%' {
            let high = encoded.next()?;
            let low = encoded.next()?;
            decoded.push(hex_value(high)? << 4 | hex_value(low)?);
        } else {
            decoded.push(byte);
        }
    }
    String::from_utf8(decoded).ok()
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn unescape_pointer_token(value: &str) -> Option<String> {
    let mut decoded = String::with_capacity(value.len());
    let mut characters = value.chars();
    while let Some(character) = characters.next() {
        if character != '~' {
            decoded.push(character);
            continue;
        }
        match characters.next()? {
            '0' => decoded.push('~'),
            '1' => decoded.push('/'),
            _ => return None,
        }
    }
    Some(decoded)
}

fn pointer_tokens(pointer: &str) -> Option<Vec<String>> {
    let pointer = pointer.strip_prefix('#')?;
    if pointer.is_empty() {
        return Some(Vec::new());
    }
    pointer
        .strip_prefix('/')?
        .split('/')
        .map(unescape_pointer_token)
        .collect()
}

fn value_at<'a>(document: &'a Value, pointer: &str) -> Option<&'a Value> {
    document.pointer(pointer.strip_prefix('#')?)
}

pub(super) fn child_pointer(parent: &str, child: &str) -> String {
    format!("{parent}/{}", escape_pointer_token(child))
}

fn escape_pointer_token(value: &str) -> String {
    value.replace('~', "~0").replace('/', "~1")
}

fn validate_reference_siblings(reference: &Map<String, Value>) -> Result<(), CatalogDiagnostic> {
    for (name, value) in reference {
        if name == "$ref" || name.starts_with("x-") {
            continue;
        }
        if matches!(name.as_str(), "summary" | "description") && value.is_string() {
            continue;
        }
        return Err(diagnostic(
            "invalid_openapi_operation",
            "an OpenAPI reference has an invalid sibling field",
        ));
    }
    Ok(())
}

fn pointer_diagnostic(error: PointerError) -> CatalogDiagnostic {
    match error {
        PointerError::External => diagnostic(
            "unsupported_external_schema_reference",
            "an OpenAPI Operation depends on an external schema reference",
        ),
        PointerError::Scope => diagnostic(
            "unsupported_schema_reference_scope",
            "an OpenAPI schema `$ref` targets a named anchor instead of a JSON pointer",
        ),
        PointerError::Invalid => invalid_schema_reference(),
    }
}

fn unsupported_schema_dialect() -> CatalogDiagnostic {
    diagnostic(
        "unsupported_schema_dialect",
        "an OpenAPI schema selects an unsupported JSON Schema dialect",
    )
}

fn invalid_tool_schema() -> CatalogDiagnostic {
    diagnostic(
        "invalid_tool_schema",
        "an OpenAPI Operation contains an invalid JSON Schema",
    )
}

fn invalid_schema_reference() -> CatalogDiagnostic {
    diagnostic(
        "invalid_schema_reference",
        "an OpenAPI Operation contains an unresolved schema reference",
    )
}

fn diagnostic(code: &'static str, message: &'static str) -> CatalogDiagnostic {
    CatalogDiagnostic::stable(code, message)
}
