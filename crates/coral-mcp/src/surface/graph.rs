use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use coral_engine::{GraphDeclaration, GraphDiagnostic};
use rmcp::{
    ErrorData,
    model::{Tool, ToolAnnotations},
};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::{Map, Value};

use super::arguments::{optional_string_argument, required_string_argument};
use super::schema::{tool_input_schema, tool_output_schema};
use super::tool_names::ToolName;

const DEFAULT_DESCRIBE_MAX_NODES: usize = 12;
const MAX_DESCRIBE_MAX_NODES: usize = 200;
const DEFAULT_DESCRIBE_MAX_RELATIONSHIPS: usize = 20;
const MAX_DESCRIBE_MAX_RELATIONSHIPS: usize = 300;
const DEFAULT_PATH_MAX_DEPTH: usize = 4;
const MAX_PATH_MAX_DEPTH: usize = 8;
const DEFAULT_PATH_MAX_PATHS: usize = 8;
const MAX_PATH_MAX_PATHS: usize = 50;

#[derive(Debug, JsonSchema)]
pub(crate) struct DescribeGraphArguments {
    #[schemars(
        length(min = 1),
        description = "Optional concept, label, relationship, table, or property focus phrase."
    )]
    pub(crate) focus: Option<String>,
    #[schemars(
        range(min = 1, max = MAX_DESCRIBE_MAX_NODES),
        description = "Maximum node mappings to return. Defaults to 12."
    )]
    pub(crate) max_nodes: Option<usize>,
    #[schemars(
        range(min = 1, max = MAX_DESCRIBE_MAX_RELATIONSHIPS),
        description = "Maximum relationship mappings to return. Defaults to 20."
    )]
    pub(crate) max_relationships: Option<usize>,
}

#[derive(Debug, JsonSchema)]
pub(crate) struct FindRelationshipPathsArguments {
    #[schemars(
        length(min = 1),
        description = "Starting domain concept, node label, table, or property focus phrase."
    )]
    pub(crate) from_focus: String,
    #[schemars(
        length(min = 1),
        description = "Target domain concept, node label, table, or property focus phrase."
    )]
    pub(crate) to_focus: String,
    #[schemars(
        range(min = 1, max = MAX_PATH_MAX_DEPTH),
        description = "Maximum relationship hops to search. Defaults to 4."
    )]
    pub(crate) max_depth: Option<usize>,
    #[schemars(
        range(min = 1, max = MAX_PATH_MAX_PATHS),
        description = "Maximum paths to return. Defaults to 8."
    )]
    pub(crate) max_paths: Option<usize>,
}

#[derive(Debug, JsonSchema)]
pub(crate) struct CypherArguments {
    #[schemars(
        length(min = 1),
        description = "Read-only openCypher query to execute against the configured Coral virtual graph."
    )]
    pub(crate) query: String,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct DescribeGraphValue {
    name: String,
    description: Option<String>,
    focus: Option<String>,
    node_count: usize,
    relationship_count: usize,
    returned_node_count: usize,
    returned_relationship_count: usize,
    nodes: Vec<GraphNodeValue>,
    relationships: Vec<GraphRelationshipValue>,
    usage_notes: Vec<String>,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct GraphNodeValue {
    label: String,
    table: GraphTableRefValue,
    key: String,
    properties: Vec<GraphPropertyValue>,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct GraphRelationshipValue {
    #[serde(rename = "type")]
    relationship_type: String,
    table: GraphTableRefValue,
    key: Option<String>,
    from: GraphEndpointValue,
    to: GraphEndpointValue,
    properties: Vec<GraphPropertyValue>,
}

#[derive(Clone, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct GraphTableRefValue {
    schema: String,
    name: String,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct GraphEndpointValue {
    label: String,
    key: String,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct GraphPropertyValue {
    property: String,
    column: String,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct FindRelationshipPathsValue {
    from_focus: String,
    to_focus: String,
    resolved_from_labels: Vec<String>,
    resolved_to_labels: Vec<String>,
    max_depth: usize,
    path_count: usize,
    paths: Vec<RelationshipPathValue>,
    usage_notes: Vec<String>,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct RelationshipPathValue {
    labels: Vec<String>,
    relationships: Vec<PathRelationshipStepValue>,
}

#[derive(Clone, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct PathRelationshipStepValue {
    #[serde(rename = "type")]
    relationship_type: String,
    traversal_direction: TraversalDirectionValue,
    from_label: String,
    to_label: String,
    table: GraphTableRefValue,
    from_key: String,
    to_key: String,
}

#[derive(Clone, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TraversalDirectionValue {
    Outgoing,
    Incoming,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct CypherValue {
    #[schemars(schema_with = "json_object_array_schema")]
    rows: Vec<Value>,
    translated_sql: String,
    diagnostics: Vec<GraphDiagnosticValue>,
}

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct GraphDiagnosticValue {
    code: String,
    path: String,
    message: String,
}

pub(crate) fn describe_graph_tool() -> Tool {
    Tool::new(
        ToolName::DescribeGraph.as_str(),
        "Return a focused slice of the configured Coral virtual graph: labels, backing tables, properties, relationships, and usage notes.",
        tool_input_schema::<DescribeGraphArguments>(),
    )
    .with_raw_output_schema(describe_graph_output_schema())
    .with_annotations(
        ToolAnnotations::with_title("Describe Graph")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(false),
    )
}

pub(crate) fn find_relationship_paths_tool() -> Tool {
    Tool::new(
        ToolName::FindRelationshipPaths.as_str(),
        "Find modeled virtual-graph paths between two domain concepts before choosing joins, Cypher patterns, or SQL.",
        tool_input_schema::<FindRelationshipPathsArguments>(),
    )
    .with_raw_output_schema(find_relationship_paths_output_schema())
    .with_annotations(
        ToolAnnotations::with_title("Find Relationship Paths")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(false),
    )
}

pub(crate) fn cypher_tool() -> Tool {
    Tool::new(
        ToolName::Cypher.as_str(),
        "Execute one read-only openCypher query against the configured Coral virtual graph. Returns rows plus the translated SQL used for execution.",
        tool_input_schema::<CypherArguments>(),
    )
    .with_raw_output_schema(cypher_output_schema())
    .with_annotations(
        ToolAnnotations::with_title("Run Cypher")
            .read_only(true)
            .destructive(false)
            .idempotent(true)
            .open_world(false),
    )
}

pub(crate) fn describe_graph_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<DescribeGraphArguments, ErrorData> {
    Ok(DescribeGraphArguments {
        focus: optional_string_argument(arguments, "focus")?
            .map(|focus| focus.trim().to_string())
            .filter(|focus| !focus.is_empty()),
        max_nodes: Some(optional_usize_argument(
            arguments,
            "max_nodes",
            DEFAULT_DESCRIBE_MAX_NODES,
            1,
            MAX_DESCRIBE_MAX_NODES,
        )?),
        max_relationships: Some(optional_usize_argument(
            arguments,
            "max_relationships",
            DEFAULT_DESCRIBE_MAX_RELATIONSHIPS,
            1,
            MAX_DESCRIBE_MAX_RELATIONSHIPS,
        )?),
    })
}

pub(crate) fn find_relationship_paths_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<FindRelationshipPathsArguments, ErrorData> {
    Ok(FindRelationshipPathsArguments {
        from_focus: required_string_argument(arguments, "from_focus")?,
        to_focus: required_string_argument(arguments, "to_focus")?,
        max_depth: Some(optional_usize_argument(
            arguments,
            "max_depth",
            DEFAULT_PATH_MAX_DEPTH,
            1,
            MAX_PATH_MAX_DEPTH,
        )?),
        max_paths: Some(optional_usize_argument(
            arguments,
            "max_paths",
            DEFAULT_PATH_MAX_PATHS,
            1,
            MAX_PATH_MAX_PATHS,
        )?),
    })
}

pub(crate) fn cypher_arguments(
    arguments: Option<&Map<String, Value>>,
) -> Result<CypherArguments, ErrorData> {
    let query = required_string_argument(arguments, "query").or_else(|_| {
        required_string_argument(arguments, "cypher").map_err(|_| {
            ErrorData::invalid_params("missing string argument 'query' for cypher tool", None)
        })
    })?;
    Ok(CypherArguments { query })
}

pub(crate) fn describe_graph_value(
    graph: &GraphDeclaration,
    arguments: DescribeGraphArguments,
) -> DescribeGraphValue {
    let focus = arguments.focus.as_deref().map(FocusQuery::new);
    let max_nodes = arguments.max_nodes.unwrap_or(DEFAULT_DESCRIBE_MAX_NODES);
    let max_relationships = arguments
        .max_relationships
        .unwrap_or(DEFAULT_DESCRIBE_MAX_RELATIONSHIPS);

    let selected_nodes = selected_node_indexes(graph, focus.as_ref(), max_nodes);
    let selected_labels = selected_nodes
        .iter()
        .map(|index| graph.nodes[*index].label.as_str())
        .collect::<BTreeSet<_>>();
    let selected_relationships =
        selected_relationship_indexes(graph, focus.as_ref(), &selected_labels, max_relationships);
    let selected_relationship_types = selected_relationships
        .iter()
        .map(|index| graph.relationships[*index].relationship_type.as_str())
        .collect::<BTreeSet<_>>();
    let usage_notes = graph_usage_notes(graph, &selected_labels, &selected_relationship_types);

    let nodes = selected_nodes
        .into_iter()
        .map(|index| GraphNodeValue::from(&graph.nodes[index]))
        .collect::<Vec<_>>();
    let relationships = selected_relationships
        .into_iter()
        .map(|index| GraphRelationshipValue::from(&graph.relationships[index]))
        .collect::<Vec<_>>();

    DescribeGraphValue {
        name: graph.name.clone(),
        description: graph.description.clone(),
        focus: arguments.focus,
        node_count: graph.nodes.len(),
        relationship_count: graph.relationships.len(),
        returned_node_count: nodes.len(),
        returned_relationship_count: relationships.len(),
        nodes,
        relationships,
        usage_notes,
    }
}

pub(crate) fn find_relationship_paths_value(
    graph: &GraphDeclaration,
    arguments: FindRelationshipPathsArguments,
) -> FindRelationshipPathsValue {
    let max_depth = arguments.max_depth.unwrap_or(DEFAULT_PATH_MAX_DEPTH);
    let max_paths = arguments.max_paths.unwrap_or(DEFAULT_PATH_MAX_PATHS);
    let from_labels = matching_node_labels(graph, &arguments.from_focus, 6);
    let to_labels = matching_node_labels(graph, &arguments.to_focus, 6);
    let paths = relationship_paths(graph, &from_labels, &to_labels, max_depth, max_paths);

    FindRelationshipPathsValue {
        from_focus: arguments.from_focus,
        to_focus: arguments.to_focus,
        resolved_from_labels: from_labels,
        resolved_to_labels: to_labels,
        max_depth,
        path_count: paths.len(),
        paths,
        usage_notes: vec![
            "Paths are graph-model guidance for selecting joins or Cypher patterns; verify row-level filters and aggregates with cypher or sql.".to_string(),
            "Incoming traversal means the path uses a declared relationship in reverse.".to_string(),
        ],
    }
}

pub(crate) fn cypher_value(
    rows: Vec<Value>,
    translated_sql: String,
    diagnostics: &[GraphDiagnostic],
) -> CypherValue {
    CypherValue {
        rows,
        translated_sql,
        diagnostics: diagnostics.iter().map(GraphDiagnosticValue::from).collect(),
    }
}

pub(crate) fn describe_graph_output_schema() -> Arc<Map<String, Value>> {
    tool_output_schema::<DescribeGraphValue>()
}

pub(crate) fn find_relationship_paths_output_schema() -> Arc<Map<String, Value>> {
    tool_output_schema::<FindRelationshipPathsValue>()
}

pub(crate) fn cypher_output_schema() -> Arc<Map<String, Value>> {
    tool_output_schema::<CypherValue>()
}

fn optional_usize_argument(
    arguments: Option<&Map<String, Value>>,
    key: &str,
    default: usize,
    min: usize,
    max: usize,
) -> Result<usize, ErrorData> {
    let Some(value) = arguments.and_then(|arguments| arguments.get(key)) else {
        return Ok(default);
    };
    if value.is_null() {
        return Ok(default);
    }
    let Some(value) = value.as_u64() else {
        return Err(ErrorData::invalid_params(
            format!("argument '{key}' must be an integer"),
            None,
        ));
    };
    let value = usize::try_from(value)
        .map_err(|_| ErrorData::invalid_params(format!("argument '{key}' is too large"), None))?;
    if !(min..=max).contains(&value) {
        return Err(ErrorData::invalid_params(
            format!("argument '{key}' must be between {min} and {max}"),
            None,
        ));
    }
    Ok(value)
}

fn selected_node_indexes(
    graph: &GraphDeclaration,
    focus: Option<&FocusQuery>,
    max_nodes: usize,
) -> Vec<usize> {
    let schema_focuses = focus.map_or_else(BTreeSet::new, |focus| schema_focuses(graph, focus));
    let mut scored = graph
        .nodes
        .iter()
        .enumerate()
        .map(|(index, node)| {
            (
                index,
                focus.map_or(1, |focus| {
                    node_score_for_focus(node, focus, &schema_focuses)
                }),
            )
        })
        .filter(|(_, score)| focus.is_none() || *score > 0)
        .collect::<Vec<_>>();
    constrain_to_schema_focuses_if_possible(graph, &mut scored, &schema_focuses);

    if focus.is_some() {
        scored.sort_by(|(left_index, left_score), (right_index, right_score)| {
            right_score
                .cmp(left_score)
                .then_with(|| left_index.cmp(right_index))
        });
    }
    scored
        .into_iter()
        .take(max_nodes)
        .map(|(index, _)| index)
        .collect()
}

fn selected_relationship_indexes(
    graph: &GraphDeclaration,
    focus: Option<&FocusQuery>,
    selected_labels: &BTreeSet<&str>,
    max_relationships: usize,
) -> Vec<usize> {
    let schema_focuses = focus.map_or_else(BTreeSet::new, |focus| schema_focuses(graph, focus));
    let mut scored = graph
        .relationships
        .iter()
        .enumerate()
        .filter_map(|(index, relationship)| {
            let endpoint_selected = selected_labels.contains(relationship.from.label.as_str())
                || selected_labels.contains(relationship.to.label.as_str());
            let score = focus.map_or(1, |focus| {
                relationship_score_for_focus(relationship, focus, &schema_focuses)
            });
            (focus.is_none() || endpoint_selected || score > 0)
                .then_some((index, score + usize::from(endpoint_selected) * 5))
        })
        .collect::<Vec<_>>();
    constrain_relationships_to_schema_focuses_if_possible(graph, &mut scored, &schema_focuses);

    if focus.is_some() {
        scored.sort_by(|(left_index, left_score), (right_index, right_score)| {
            right_score
                .cmp(left_score)
                .then_with(|| left_index.cmp(right_index))
        });
    }
    scored
        .into_iter()
        .take(max_relationships)
        .map(|(index, _)| index)
        .collect()
}

fn matching_node_labels(graph: &GraphDeclaration, focus: &str, limit: usize) -> Vec<String> {
    let focus = FocusQuery::new(focus);
    let schema_focuses = schema_focuses(graph, &focus);
    let mut scored = graph
        .nodes
        .iter()
        .enumerate()
        .map(|(index, node)| (index, node_score_for_focus(node, &focus, &schema_focuses)))
        .filter(|(_, score)| *score > 0)
        .collect::<Vec<_>>();
    constrain_to_schema_focuses_if_possible(graph, &mut scored, &schema_focuses);
    scored.sort_by(|(left_index, left_score), (right_index, right_score)| {
        right_score
            .cmp(left_score)
            .then_with(|| left_index.cmp(right_index))
    });
    scored
        .into_iter()
        .take(limit)
        .map(|(index, _)| graph.nodes[index].label.clone())
        .collect()
}

fn constrain_to_schema_focuses_if_possible(
    graph: &GraphDeclaration,
    scored: &mut Vec<(usize, usize)>,
    schema_focuses: &BTreeSet<String>,
) {
    if schema_focuses.is_empty()
        || !scored.iter().any(|(index, _)| {
            schema_focuses.contains(&graph.nodes[*index].table.schema.to_ascii_lowercase())
        })
    {
        return;
    }
    scored.retain(|(index, _)| {
        schema_focuses.contains(&graph.nodes[*index].table.schema.to_ascii_lowercase())
    });
}

fn constrain_relationships_to_schema_focuses_if_possible(
    graph: &GraphDeclaration,
    scored: &mut Vec<(usize, usize)>,
    schema_focuses: &BTreeSet<String>,
) {
    if schema_focuses.is_empty()
        || !scored.iter().any(|(index, _)| {
            schema_focuses.contains(
                &graph.relationships[*index]
                    .table
                    .schema
                    .to_ascii_lowercase(),
            )
        })
    {
        return;
    }
    scored.retain(|(index, _)| {
        schema_focuses.contains(
            &graph.relationships[*index]
                .table
                .schema
                .to_ascii_lowercase(),
        )
    });
}

fn schema_focuses(graph: &GraphDeclaration, focus: &FocusQuery) -> BTreeSet<String> {
    graph
        .nodes
        .iter()
        .map(|node| node.table.schema.as_str())
        .chain(
            graph
                .relationships
                .iter()
                .map(|relationship| relationship.table.schema.as_str()),
        )
        .filter(|schema| focus_mentions_schema(focus, schema))
        .map(str::to_ascii_lowercase)
        .collect()
}

fn focus_mentions_schema(focus: &FocusQuery, schema: &str) -> bool {
    let schema_lower = schema.to_ascii_lowercase();
    if schema_lower.is_empty() {
        return false;
    }
    if contains_schema_literal(&focus.raw_lower, &schema_lower) {
        return true;
    }
    let schema_tokens = tokens(schema);
    !schema_tokens.is_empty() && schema_tokens.is_subset(&focus.tokens)
}

fn contains_schema_literal(focus: &str, schema: &str) -> bool {
    focus.match_indices(schema).any(|(start, match_text)| {
        let before = focus[..start].chars().next_back();
        let after = focus[start + match_text.len()..].chars().next();
        is_schema_boundary(before) && is_schema_boundary(after)
    })
}

fn is_schema_boundary(character: Option<char>) -> bool {
    character.is_none_or(|character| !character.is_ascii_alphanumeric() && character != '_')
}

fn node_score_for_focus(
    node: &coral_engine::virtual_graph::Node,
    focus: &FocusQuery,
    schema_focuses: &BTreeSet<String>,
) -> usize {
    if focus_requires_non_schema_match(focus, schema_focuses) {
        node_score_without_schema(node, focus)
    } else {
        node_score(node, focus)
    }
}

fn relationship_score_for_focus(
    relationship: &coral_engine::virtual_graph::Relationship,
    focus: &FocusQuery,
    schema_focuses: &BTreeSet<String>,
) -> usize {
    if focus_requires_non_schema_match(focus, schema_focuses) {
        relationship_score_without_schema(relationship, focus)
    } else {
        relationship_score(relationship, focus)
    }
}

fn focus_requires_non_schema_match(focus: &FocusQuery, schema_focuses: &BTreeSet<String>) -> bool {
    if schema_focuses.is_empty() {
        return false;
    }
    let schema_tokens = schema_focuses
        .iter()
        .flat_map(|schema| tokens(schema))
        .collect::<BTreeSet<_>>();
    focus
        .tokens
        .iter()
        .any(|token| !schema_tokens.contains(token))
}

fn relationship_paths(
    graph: &GraphDeclaration,
    from_labels: &[String],
    to_labels: &[String],
    max_depth: usize,
    max_paths: usize,
) -> Vec<RelationshipPathValue> {
    if from_labels.is_empty() || to_labels.is_empty() {
        return Vec::new();
    }

    let target_labels = to_labels
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    let edges = graph_edges(graph);
    let mut queue = VecDeque::new();
    for label in from_labels {
        queue.push_back(SearchPath {
            current_label: label.clone(),
            labels: vec![label.clone()],
            steps: Vec::new(),
            seen_labels: BTreeSet::from([label.clone()]),
        });
    }

    let mut paths = Vec::new();
    let mut seen_path_keys = BTreeSet::new();
    while let Some(path) = queue.pop_front() {
        if path.steps.len() >= max_depth {
            continue;
        }
        for edge in edges
            .iter()
            .filter(|edge| edge.from_label == path.current_label)
        {
            if path.seen_labels.contains(&edge.to_label) {
                continue;
            }
            let relationship = &graph.relationships[edge.relationship_index];
            let mut next = path.clone();
            next.current_label = edge.to_label.clone();
            next.labels.push(edge.to_label.clone());
            next.steps.push(PathRelationshipStepValue::from_edge(
                relationship,
                edge.reversed,
                &edge.from_label,
                &edge.to_label,
            ));
            next.seen_labels.insert(edge.to_label.clone());

            if target_labels.contains(next.current_label.as_str()) {
                let key = next
                    .steps
                    .iter()
                    .map(|step| {
                        format!(
                            "{}:{}>{}",
                            step.relationship_type, step.from_label, step.to_label
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("|");
                if seen_path_keys.insert(key) {
                    paths.push(RelationshipPathValue {
                        labels: next.labels.clone(),
                        relationships: next.steps.clone(),
                    });
                    if paths.len() >= max_paths {
                        return paths;
                    }
                }
            }
            queue.push_back(next);
        }
    }
    paths
}

fn graph_edges(graph: &GraphDeclaration) -> Vec<GraphEdge> {
    let mut edges = Vec::with_capacity(graph.relationships.len() * 2);
    for (index, relationship) in graph.relationships.iter().enumerate() {
        edges.push(GraphEdge {
            relationship_index: index,
            from_label: relationship.from.label.clone(),
            to_label: relationship.to.label.clone(),
            reversed: false,
        });
        edges.push(GraphEdge {
            relationship_index: index,
            from_label: relationship.to.label.clone(),
            to_label: relationship.from.label.clone(),
            reversed: true,
        });
    }
    edges
}

#[derive(Clone)]
struct SearchPath {
    current_label: String,
    labels: Vec<String>,
    steps: Vec<PathRelationshipStepValue>,
    seen_labels: BTreeSet<String>,
}

struct GraphEdge {
    relationship_index: usize,
    from_label: String,
    to_label: String,
    reversed: bool,
}

struct FocusQuery {
    raw_lower: String,
    tokens: BTreeSet<String>,
}

impl FocusQuery {
    fn new(value: &str) -> Self {
        Self {
            raw_lower: value.trim().to_ascii_lowercase(),
            tokens: tokens(value),
        }
    }
}

fn node_score(node: &coral_engine::virtual_graph::Node, focus: &FocusQuery) -> usize {
    let mut score = score_text(&node.label, focus) * 4;
    score += score_text(&node.table.schema, focus);
    score += score_text(&node.table.name, focus) * 2;
    score += score_text(&node.key, focus);
    for (property, column) in &node.properties {
        score += score_text(property, focus) * 2;
        score += score_text(column, focus);
    }
    score
}

fn node_score_without_schema(
    node: &coral_engine::virtual_graph::Node,
    focus: &FocusQuery,
) -> usize {
    let mut score = score_text(&node.label, focus) * 4;
    score += score_text(&node.table.name, focus) * 2;
    score += score_text(&node.key, focus);
    for (property, column) in &node.properties {
        score += score_text(property, focus) * 2;
        score += score_text(column, focus);
    }
    score
}

fn relationship_score(
    relationship: &coral_engine::virtual_graph::Relationship,
    focus: &FocusQuery,
) -> usize {
    let mut score = score_text(&relationship.relationship_type, focus) * 4;
    score += score_text(&relationship.table.schema, focus);
    score += score_text(&relationship.table.name, focus) * 2;
    score += relationship
        .key
        .as_ref()
        .map_or(0, |key| score_text(key, focus));
    score += score_text(&relationship.from.label, focus) * 2;
    score += score_text(&relationship.from.key, focus);
    score += score_text(&relationship.to.label, focus) * 2;
    score += score_text(&relationship.to.key, focus);
    for (property, column) in &relationship.properties {
        score += score_text(property, focus) * 2;
        score += score_text(column, focus);
    }
    score
}

fn relationship_score_without_schema(
    relationship: &coral_engine::virtual_graph::Relationship,
    focus: &FocusQuery,
) -> usize {
    let mut score = score_text(&relationship.relationship_type, focus) * 4;
    score += score_text(&relationship.table.name, focus) * 2;
    score += relationship
        .key
        .as_ref()
        .map_or(0, |key| score_text(key, focus));
    score += score_text(&relationship.from.label, focus) * 2;
    score += score_text(&relationship.from.key, focus);
    score += score_text(&relationship.to.label, focus) * 2;
    score += score_text(&relationship.to.key, focus);
    for (property, column) in &relationship.properties {
        score += score_text(property, focus) * 2;
        score += score_text(column, focus);
    }
    score
}

fn score_text(value: &str, focus: &FocusQuery) -> usize {
    let value_lower = value.to_ascii_lowercase();
    let mut score = 0;
    if value_lower == focus.raw_lower {
        score += 100;
    } else if !focus.raw_lower.is_empty()
        && (value_lower.contains(&focus.raw_lower) || focus.raw_lower.contains(&value_lower))
    {
        score += 30;
    }
    let value_tokens = tokens(value);
    let overlap = value_tokens.intersection(&focus.tokens).count();
    score + (overlap * 10)
}

fn tokens(value: &str) -> BTreeSet<String> {
    let mut tokens = BTreeSet::new();
    for raw in value
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|token| !token.is_empty())
    {
        let token = raw.to_ascii_lowercase();
        insert_token_variants(&mut tokens, token);
    }
    tokens
}

fn insert_token_variants(tokens: &mut BTreeSet<String>, token: String) {
    if token.is_empty() {
        return;
    }
    tokens.insert(token.clone());
    if let Some(stem) = token.strip_suffix("ies") {
        tokens.insert(format!("{stem}y"));
    }
    if token.ends_with('s') && token.len() > 3 {
        tokens.insert(token.trim_end_matches('s').to_string());
    }
}

fn graph_usage_notes(
    graph: &GraphDeclaration,
    selected_labels: &BTreeSet<&str>,
    selected_relationship_types: &BTreeSet<&str>,
) -> Vec<String> {
    let mut notes = vec![
        "The graph is schema guidance: it maps domain labels and relationships to backing tables and columns, not row values.".to_string(),
        "Use cypher when the modeled labels, relationships, and properties cover the requested grain; use sql when exact warehouse aggregation or unmapped columns are needed.".to_string(),
        "Validate values, filters, grouping grain, and additive measures with cypher or sql before answering.".to_string(),
    ];
    notes.extend(
        graph
            .usage_notes
            .iter()
            .filter(|note| note.applies_to(selected_labels, selected_relationship_types))
            .map(|note| note.text().to_string()),
    );
    notes
}

fn json_object_array_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "array",
        "items": { "type": "object" }
    })
}

impl From<&coral_engine::virtual_graph::Node> for GraphNodeValue {
    fn from(node: &coral_engine::virtual_graph::Node) -> Self {
        Self {
            label: node.label.clone(),
            table: GraphTableRefValue::from(&node.table),
            key: node.key.clone(),
            properties: properties_value(&node.properties),
        }
    }
}

impl From<&coral_engine::virtual_graph::Relationship> for GraphRelationshipValue {
    fn from(relationship: &coral_engine::virtual_graph::Relationship) -> Self {
        Self {
            relationship_type: relationship.relationship_type.clone(),
            table: GraphTableRefValue::from(&relationship.table),
            key: relationship.key.clone(),
            from: GraphEndpointValue::from(&relationship.from),
            to: GraphEndpointValue::from(&relationship.to),
            properties: properties_value(&relationship.properties),
        }
    }
}

impl From<&coral_engine::virtual_graph::TableRef> for GraphTableRefValue {
    fn from(table: &coral_engine::virtual_graph::TableRef) -> Self {
        Self {
            schema: table.schema.clone(),
            name: table.name.clone(),
        }
    }
}

impl From<&coral_engine::virtual_graph::Endpoint> for GraphEndpointValue {
    fn from(endpoint: &coral_engine::virtual_graph::Endpoint) -> Self {
        Self {
            label: endpoint.label.clone(),
            key: endpoint.key.clone(),
        }
    }
}

impl PathRelationshipStepValue {
    fn from_edge(
        relationship: &coral_engine::virtual_graph::Relationship,
        reversed: bool,
        from_label: &str,
        to_label: &str,
    ) -> Self {
        let traversal_direction = if reversed {
            TraversalDirectionValue::Incoming
        } else {
            TraversalDirectionValue::Outgoing
        };
        let (from_key, to_key) = if reversed {
            (&relationship.to.key, &relationship.from.key)
        } else {
            (&relationship.from.key, &relationship.to.key)
        };
        Self {
            relationship_type: relationship.relationship_type.clone(),
            traversal_direction,
            from_label: from_label.to_string(),
            to_label: to_label.to_string(),
            table: GraphTableRefValue::from(&relationship.table),
            from_key: from_key.clone(),
            to_key: to_key.clone(),
        }
    }
}

impl From<&GraphDiagnostic> for GraphDiagnosticValue {
    fn from(diagnostic: &GraphDiagnostic) -> Self {
        Self {
            code: diagnostic.code().to_string(),
            path: diagnostic.path().to_string(),
            message: diagnostic.message().to_string(),
        }
    }
}

fn properties_value(properties: &BTreeMap<String, String>) -> Vec<GraphPropertyValue> {
    properties
        .iter()
        .map(|(property, column)| GraphPropertyValue {
            property: property.clone(),
            column: column.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use coral_engine::GraphDeclaration;
    use serde_json::{Value, json};

    use super::{
        describe_graph_arguments, describe_graph_value, find_relationship_paths_arguments,
        find_relationship_paths_value,
    };

    const GRAPH: &str = r#"
version: 1
name: enterprise
description: Enterprise sales graph
nodes:
  - label: Account
    table: { schema: crm, name: accounts }
    key: account_id
    properties:
      name: account_name
      industry: industry
  - label: Opportunity
    table: { schema: crm, name: opportunities }
    key: opportunity_id
    properties:
      amount: amount
      stage: stage_name
  - label: Employee
    table: { schema: hr, name: employees }
    key: employee_id
    properties:
      name: employee_name
relationships:
  - type: HAS_OPPORTUNITY
    table: { schema: crm, name: opportunities }
    key: opportunity_id
    from: { label: Account, key: account_id }
    to: { label: Opportunity, key: opportunity_id }
    properties:
      amount: amount
  - type: OWNS_ACCOUNT
    table: { schema: crm, name: account_owners }
    from: { label: Employee, key: employee_id }
    to: { label: Account, key: account_id }
usage_notes:
  - Validate all final aggregates against source rows.
  - text: Opportunity amount is additive at opportunity row grain.
    labels: [Opportunity]
  - text: Account-only note should not appear in employee opportunity focus.
    labels: [Account]
"#;

    const MULTI_SCHEMA_GRAPH: &str = r#"
version: 1
name: multi_org_crm
nodes:
  - label: B2BTask
    table: { schema: crm_b2b, name: tasks }
    key: id
    properties:
      status: status
      what_id: what_id
  - label: B2BOpportunity
    table: { schema: crm_b2b, name: opportunities }
    key: id
    properties:
      stage: stage_name
  - label: B2CTask
    table: { schema: crm_b2c, name: tasks }
    key: id
    properties:
      status: status
      what_id: what_id
  - label: B2COpportunity
    table: { schema: crm_b2c, name: opportunities }
    key: id
    properties:
      stage: stage_name
relationships:
  - type: TASK_FOR_OPPORTUNITY
    table: { schema: crm_b2b, name: tasks }
    from: { label: B2BTask, key: what_id }
    to: { label: B2BOpportunity, key: id }
  - type: TASK_FOR_OPPORTUNITY
    table: { schema: crm_b2c, name: tasks }
    from: { label: B2CTask, key: what_id }
    to: { label: B2COpportunity, key: id }
"#;

    const PREFIX_SCHEMA_GRAPH: &str = r#"
version: 1
name: prefixed_crm
nodes:
  - label: CrmarenaUser
    table: { schema: crmarena, name: User }
    key: Id
  - label: CrmarenaCase
    table: { schema: crmarena, name: Case }
    key: Id
    properties:
      owner_id: OwnerId
  - label: B2COpportunity
    table: { schema: crmarenapro_b2c, name: Opportunity }
    key: Id
    properties:
      owner_id: OwnerId
  - label: B2CUser
    table: { schema: crmarenapro_b2c, name: User }
    key: Id
relationships:
  - type: CASE_OWNED_BY
    table: { schema: crmarena, name: Case }
    from: { label: CrmarenaCase, key: OwnerId }
    to: { label: CrmarenaUser, key: Id }
  - type: OPPORTUNITY_OWNED_BY
    table: { schema: crmarenapro_b2c, name: Opportunity }
    from: { label: B2COpportunity, key: OwnerId }
    to: { label: B2CUser, key: Id }
"#;

    #[test]
    fn describe_graph_focuses_arbitrary_enterprise_concepts() {
        let graph = GraphDeclaration::from_yaml(GRAPH).expect("graph should parse");
        let arguments = describe_graph_arguments(Some(
            json!({"focus": "employee opportunities", "max_nodes": 2, "max_relationships": 3})
                .as_object()
                .expect("object"),
        ))
        .expect("arguments should parse");

        let value = describe_graph_value(&graph, arguments);

        assert_eq!(value.returned_node_count, 2);
        let labels = value
            .nodes
            .iter()
            .map(|node| node.label.as_str())
            .collect::<Vec<_>>();
        assert!(labels.contains(&"Employee"));
        assert!(labels.contains(&"Opportunity"));
        assert_eq!(
            value.returned_relationship_count,
            3.min(value.relationship_count)
        );
        assert!(
            value
                .usage_notes
                .iter()
                .any(|note| note == "Validate all final aggregates against source rows.")
        );
        assert!(
            value
                .usage_notes
                .iter()
                .any(|note| note == "Opportunity amount is additive at opportunity row grain.")
        );
        assert!(!value.usage_notes.iter().any(
            |note| note == "Account-only note should not appear in employee opportunity focus."
        ));
    }

    #[test]
    fn find_relationship_paths_can_traverse_declared_edges_in_reverse() {
        let graph = GraphDeclaration::from_yaml(GRAPH).expect("graph should parse");
        let object = json!({
            "from_focus": "opportunities",
            "to_focus": "employees",
            "max_depth": 3,
            "max_paths": 2
        });
        let arguments =
            find_relationship_paths_arguments(Some(object.as_object().expect("object")))
                .expect("arguments should parse");

        let value = find_relationship_paths_value(&graph, arguments);

        assert_eq!(value.path_count, 1);
        assert_eq!(
            value.paths[0].labels,
            vec![
                "Opportunity".to_string(),
                "Account".to_string(),
                "Employee".to_string()
            ]
        );
        assert!(matches!(
            value.paths[0].relationships[0].traversal_direction,
            super::TraversalDirectionValue::Incoming
        ));
    }

    #[test]
    fn describe_graph_schema_focus_filters_parallel_enterprise_schemas() {
        let graph = GraphDeclaration::from_yaml(MULTI_SCHEMA_GRAPH).expect("graph should parse");
        let arguments = describe_graph_arguments(Some(
            json!({"focus": "crm_b2b opportunity tasks", "max_nodes": 4, "max_relationships": 4})
                .as_object()
                .expect("object"),
        ))
        .expect("arguments should parse");

        let value = describe_graph_value(&graph, arguments);

        let schemas = value
            .nodes
            .iter()
            .map(|node| node.table.schema.as_str())
            .collect::<BTreeSet<_>>();
        assert_eq!(schemas, BTreeSet::from(["crm_b2b"]));
        assert!(value.nodes.iter().any(|node| node.label == "B2BTask"));
        assert!(
            value
                .nodes
                .iter()
                .any(|node| node.label == "B2BOpportunity")
        );
        assert!(
            value
                .relationships
                .iter()
                .all(|relationship| relationship.table.schema == "crm_b2b")
        );
    }

    #[test]
    fn find_relationship_paths_schema_focus_resolves_parallel_enterprise_schema() {
        let graph = GraphDeclaration::from_yaml(MULTI_SCHEMA_GRAPH).expect("graph should parse");
        let object = json!({
            "from_focus": "crm_b2c Task",
            "to_focus": "crm_b2c Opportunity",
            "max_depth": 2,
            "max_paths": 4
        });
        let arguments =
            find_relationship_paths_arguments(Some(object.as_object().expect("object")))
                .expect("arguments should parse");

        let value = find_relationship_paths_value(&graph, arguments);

        assert_eq!(value.resolved_from_labels, vec!["B2CTask"]);
        assert_eq!(value.resolved_to_labels, vec!["B2COpportunity"]);
        assert_eq!(value.path_count, 1);
        assert_eq!(
            value.paths[0].labels,
            vec!["B2CTask".to_string(), "B2COpportunity".to_string()]
        );
    }

    #[test]
    fn schema_focus_does_not_match_prefix_schema_names() {
        let graph = GraphDeclaration::from_yaml(PREFIX_SCHEMA_GRAPH).expect("graph should parse");
        let arguments = describe_graph_arguments(Some(
            json!({
                "focus": "crmarenapro_b2c opportunity user",
                "max_nodes": 4,
                "max_relationships": 4
            })
            .as_object()
            .expect("object"),
        ))
        .expect("arguments should parse");

        let value = describe_graph_value(&graph, arguments);

        assert_eq!(
            value
                .nodes
                .iter()
                .map(|node| node.table.schema.as_str())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["crmarenapro_b2c"])
        );
        assert!(
            value
                .relationships
                .iter()
                .all(|relationship| relationship.table.schema == "crmarenapro_b2c")
        );
    }

    #[test]
    fn path_focus_does_not_match_prefix_schema_names() {
        let graph = GraphDeclaration::from_yaml(PREFIX_SCHEMA_GRAPH).expect("graph should parse");
        let object = json!({
            "from_focus": "crmarenapro_b2c Opportunity",
            "to_focus": "crmarenapro_b2c User",
            "max_depth": 2,
            "max_paths": 4
        });
        let arguments =
            find_relationship_paths_arguments(Some(object.as_object().expect("object")))
                .expect("arguments should parse");

        let value = find_relationship_paths_value(&graph, arguments);

        assert_eq!(value.resolved_from_labels, vec!["B2COpportunity"]);
        assert_eq!(value.resolved_to_labels, vec!["B2CUser"]);
        assert_eq!(value.path_count, 1);
        assert_eq!(
            value.paths[0].labels,
            vec!["B2COpportunity".to_string(), "B2CUser".to_string()]
        );
    }

    #[test]
    fn cypher_argument_accepts_query_or_cypher_key() {
        let query_object = json!({"query": "MATCH (n) RETURN n LIMIT 1"});
        let query = super::cypher_arguments(Some(query_object.as_object().expect("object")))
            .expect("query argument should parse");
        assert_eq!(query.query, "MATCH (n) RETURN n LIMIT 1");

        let cypher_object = json!({"cypher": "MATCH (n) RETURN n LIMIT 2"});
        let cypher = super::cypher_arguments(Some(cypher_object.as_object().expect("object")))
            .expect("cypher argument should parse");
        assert_eq!(cypher.query, "MATCH (n) RETURN n LIMIT 2");
    }

    #[test]
    fn describe_graph_rejects_out_of_range_limits() {
        let arguments = json!({"max_nodes": 0});

        let error = describe_graph_arguments(Some(arguments.as_object().expect("object")))
            .expect_err("zero limit should fail");

        assert!(format!("{error:?}").contains("max_nodes"));
    }

    #[test]
    fn cypher_output_rows_schema_accepts_objects() {
        let schema = super::cypher_output_schema();
        assert_eq!(
            schema
                .get("properties")
                .and_then(Value::as_object)
                .and_then(|properties| properties.get("rows"))
                .and_then(Value::as_object)
                .and_then(|rows| rows.get("type")),
            Some(&Value::String("array".to_string()))
        );
    }
}
