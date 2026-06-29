use std::collections::{BTreeMap, BTreeSet};

use graphql_parser::query::{
    Definition, Directive, Field, FragmentDefinition, FragmentSpread, InlineFragment,
    OperationDefinition, Selection, SelectionSet, TypeCondition, Value, VariableDefinition,
    parse_query,
};
use ordered_float::OrderedFloat;
use regex::Regex;

use super::declaration::Declaration;
use super::diagnostic::Diagnostic;
use super::graphql_aggregate::graphql_property_aggregate_field;
use super::ir::{
    AggregateFunction, AggregateTarget, ComparisonOperator, Direction, ElementIdPredicate,
    ExistsPatternPredicate, GraphPlan, KeyPredicate, Literal, NodePattern, NullOrder,
    OrderDirection, OrderExpression, OrderKey, PredicateExpression, PredicateRhs, Projection,
    PropertyPredicate, PropertyRef, RelationshipPattern,
};
use crate::CoreError;

/// Runtime value that can be bound to a GraphQL variable in the supported subset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphqlVariableValue {
    /// Scalar literal variable, usable where a literal value is accepted.
    Literal(Literal),
    /// Scalar-list variable, usable where a literal list is accepted.
    List(Vec<Literal>),
    /// Object variable, usable where supported GraphQL input objects are accepted.
    Object(BTreeMap<String, GraphqlVariableValue>),
    /// List of object variables, usable where supported object-list input is accepted.
    ObjectList(Vec<BTreeMap<String, GraphqlVariableValue>>),
}

struct GraphqlCompileContext<'variables, 'query> {
    variables: &'variables BTreeMap<String, GraphqlVariableValue>,
    variable_defaults: BTreeMap<String, GraphqlVariableValue>,
    declared_variables: BTreeSet<String>,
    fragments: BTreeMap<String, FragmentDefinition<'query, String>>,
}

impl<'variables, 'query> GraphqlCompileContext<'variables, 'query> {
    fn new(
        variables: &'variables BTreeMap<String, GraphqlVariableValue>,
        declarations: GraphqlVariableDeclarations,
        fragments: BTreeMap<String, FragmentDefinition<'query, String>>,
    ) -> Self {
        Self {
            variables,
            variable_defaults: declarations.defaults,
            declared_variables: declarations.names,
            fragments,
        }
    }

    fn parameter_value(
        &self,
        variable: &str,
        path: impl Into<String>,
    ) -> Result<&GraphqlVariableValue, CoreError> {
        let path = path.into();
        if !self.declared_variables.contains(variable) {
            return Err(unsupported(
                path,
                format!("GraphQL variable '${variable}' is not declared by the operation"),
            ));
        }
        self.variables
            .get(variable)
            .or_else(|| self.variable_defaults.get(variable))
            .ok_or_else(|| {
                Diagnostic::new(
                    "MISSING_GRAPHQL_VARIABLE",
                    path,
                    format!("GraphQL variable '${variable}' was not provided"),
                )
                .into_core_error()
            })
    }
}

#[derive(Debug, Default)]
struct GraphqlVariableDeclarations {
    names: BTreeSet<String>,
    defaults: BTreeMap<String, GraphqlVariableValue>,
}

/// Parses and compiles the Coral-supported read-only GraphQL virtual graph subset.
///
/// The first GraphQL slice is intentionally root-node oriented: one query
/// operation with one root field whose name is the graph node label, or a
/// declaration-aware generated-client alias when a graph declaration is
/// available. Selected scalar fields become property projections, and supported
/// root arguments compile into predicates, ordering, offsets, limits, and
/// distinct row selection. Relationship fields use the explicit `out_TYPE`,
/// `in_TYPE`, and `any_TYPE` traversal convention when a graph declaration is
/// available.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the query cannot be parsed or uses
/// GraphQL features outside Coral's current read-only virtual graph subset.
pub fn compile_graphql(graphql: &str) -> Result<GraphPlan, CoreError> {
    compile_graphql_with_variables(graphql, &BTreeMap::new())
}

/// Parses and compiles the named operation from a GraphQL document.
///
/// Use this when a generated client sends a document containing multiple
/// operations and selects one with `operationName`.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the query cannot be parsed, the
/// operation name is not present, the selected operation is not a query, or the
/// selected operation uses unsupported GraphQL features.
pub fn compile_graphql_with_operation_name(
    graphql: &str,
    operation_name: &str,
) -> Result<GraphPlan, CoreError> {
    compile_graphql_with_variables_and_operation_name(graphql, &BTreeMap::new(), operation_name)
}

/// Parses and compiles GraphQL with typed variable values into a shared graph plan.
///
/// Variables are bound before SQL lowering and only in positions where the
/// same literal or literal-list value is already supported by the read-only
/// GraphQL subset.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the query cannot be parsed, uses
/// unsupported GraphQL features, references a missing variable, or binds a list
/// variable where a scalar literal is required.
pub fn compile_graphql_with_variables(
    graphql: &str,
    variables: &BTreeMap<String, GraphqlVariableValue>,
) -> Result<GraphPlan, CoreError> {
    compile_graphql_document(graphql, None, variables, None)
}

/// Parses and compiles a named GraphQL operation with typed variable values.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the query cannot be parsed, uses
/// unsupported GraphQL features, references a missing variable in the selected
/// operation, or binds a variable value in an unsupported position.
pub fn compile_graphql_with_variables_and_operation_name(
    graphql: &str,
    variables: &BTreeMap<String, GraphqlVariableValue>,
    operation_name: &str,
) -> Result<GraphPlan, CoreError> {
    compile_graphql_document(graphql, None, variables, Some(operation_name))
}

fn compile_graphql_document(
    graphql: &str,
    graph: Option<&Declaration>,
    variables: &BTreeMap<String, GraphqlVariableValue>,
    operation_name: Option<&str>,
) -> Result<GraphPlan, CoreError> {
    let document = parse_query::<String>(graphql).map_err(|error| {
        Diagnostic::new("GRAPHQL_PARSE_ERROR", "query", error.to_string()).into_core_error()
    })?;
    let plan = compile_document(&document, graph, variables, operation_name)?;
    if let Some(graph) = graph {
        graph.validate_graph_plan(&plan)?;
    }
    Ok(plan)
}

/// Parses and compiles the Coral-supported read-only GraphQL virtual graph
/// subset using a graph declaration for relationship nesting.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the query cannot be parsed or uses
/// GraphQL features outside Coral's current read-only virtual graph subset.
pub fn compile_graphql_for_graph(
    graph: &Declaration,
    graphql: &str,
) -> Result<GraphPlan, CoreError> {
    compile_graphql_for_graph_with_variables(graph, graphql, &BTreeMap::new())
}

/// Parses and compiles a named operation using a graph declaration for
/// relationship nesting.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the query cannot be parsed, the
/// operation name is not present, the selected operation is not a query, or the
/// selected operation uses unsupported GraphQL features.
pub fn compile_graphql_for_graph_with_operation_name(
    graph: &Declaration,
    graphql: &str,
    operation_name: &str,
) -> Result<GraphPlan, CoreError> {
    compile_graphql_for_graph_with_variables_and_operation_name(
        graph,
        graphql,
        &BTreeMap::new(),
        operation_name,
    )
}

/// Parses and compiles GraphQL with typed variable values and a graph declaration.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the query cannot be parsed or uses
/// GraphQL features outside Coral's current read-only virtual graph subset.
pub fn compile_graphql_for_graph_with_variables(
    graph: &Declaration,
    graphql: &str,
    variables: &BTreeMap<String, GraphqlVariableValue>,
) -> Result<GraphPlan, CoreError> {
    compile_graphql_document(graphql, Some(graph), variables, None)
}

/// Parses and compiles a named GraphQL operation with typed variables and a
/// graph declaration.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the query cannot be parsed, uses
/// unsupported GraphQL features, references a missing variable in the selected
/// operation, or binds a variable value in an unsupported position.
pub fn compile_graphql_for_graph_with_variables_and_operation_name(
    graph: &Declaration,
    graphql: &str,
    variables: &BTreeMap<String, GraphqlVariableValue>,
    operation_name: &str,
) -> Result<GraphPlan, CoreError> {
    compile_graphql_document(graphql, Some(graph), variables, Some(operation_name))
}

fn compile_document<'query>(
    document: &'query graphql_parser::query::Document<'query, String>,
    graph: Option<&Declaration>,
    variables: &BTreeMap<String, GraphqlVariableValue>,
    operation_name: Option<&str>,
) -> Result<GraphPlan, CoreError> {
    let mut operations = Vec::new();
    let mut operation_names = BTreeMap::new();
    let mut fragments = BTreeMap::new();
    for (index, definition) in document.definitions.iter().enumerate() {
        match definition {
            Definition::Operation(next_operation) => {
                if let Some(name) = graphql_operation_name(next_operation)
                    && let Some(previous_index) = operation_names.insert(name.to_string(), index)
                {
                    return Err(unsupported(
                        format!("query.definitions[{index}].name"),
                        format!(
                            "GraphQL operation '{name}' is defined more than once; first definition was at query.definitions[{previous_index}]",
                        ),
                    ));
                }
                operations.push((index, next_operation));
            }
            Definition::Fragment(fragment) => {
                if !fragment.directives.is_empty() {
                    return Err(unsupported(
                        format!("query.definitions[{index}].directives"),
                        "GraphQL fragment definition directives are not supported yet",
                    ));
                }
                if fragments
                    .insert(fragment.name.clone(), fragment.clone())
                    .is_some()
                {
                    return Err(unsupported(
                        format!("query.definitions[{index}].name"),
                        format!(
                            "GraphQL fragment '{}' is defined more than once",
                            fragment.name
                        ),
                    ));
                }
            }
        }
    }
    let operation = select_graphql_operation(&operations, operation_name)?;
    compile_operation(operation, graph, variables, fragments)
}

fn select_graphql_operation<'query>(
    operations: &[(usize, &'query OperationDefinition<'query, String>)],
    operation_name: Option<&str>,
) -> Result<&'query OperationDefinition<'query, String>, CoreError> {
    if let Some(operation_name) = operation_name {
        return operations
            .iter()
            .find_map(|(_, operation)| {
                (graphql_operation_name(operation) == Some(operation_name)).then_some(*operation)
            })
            .ok_or_else(|| {
                unsupported(
                    "query.operationName",
                    format!("GraphQL operation '{operation_name}' was not found"),
                )
            });
    }

    let [(_, operation)] = operations else {
        let message = if operations.is_empty() {
            "GraphQL virtual graph queries must contain one query operation"
        } else {
            "GraphQL virtual graph documents with multiple operations require an operationName"
        };
        return Err(unsupported("query", message));
    };
    Ok(*operation)
}

fn graphql_operation_name<'query>(
    operation: &'query OperationDefinition<'query, String>,
) -> Option<&'query str> {
    match operation {
        OperationDefinition::Query(query) => query.name.as_deref(),
        OperationDefinition::Mutation(mutation) => mutation.name.as_deref(),
        OperationDefinition::Subscription(subscription) => subscription.name.as_deref(),
        OperationDefinition::SelectionSet(_) => None,
    }
}

fn compile_operation<'query>(
    operation: &OperationDefinition<'query, String>,
    graph: Option<&Declaration>,
    variables: &BTreeMap<String, GraphqlVariableValue>,
    fragments: BTreeMap<String, FragmentDefinition<'query, String>>,
) -> Result<GraphPlan, CoreError> {
    match operation {
        OperationDefinition::SelectionSet(selection_set) => {
            let context = GraphqlCompileContext::new(
                variables,
                GraphqlVariableDeclarations::default(),
                fragments,
            );
            compile_root_selection_set(selection_set, graph, "query.selectionSet", &context)
        }
        OperationDefinition::Query(query) => {
            let context = GraphqlCompileContext::new(
                variables,
                compile_variable_definitions(&query.variable_definitions)?,
                fragments,
            );
            if !query.directives.is_empty() {
                return Err(unsupported(
                    "query.directives",
                    "GraphQL query directives are not supported yet",
                ));
            }
            compile_root_selection_set(&query.selection_set, graph, "query.selectionSet", &context)
        }
        OperationDefinition::Mutation(_) | OperationDefinition::Subscription(_) => {
            Err(unsupported(
                "query",
                "GraphQL mutations and subscriptions are not supported",
            ))
        }
    }
}

fn compile_variable_definitions(
    definitions: &[VariableDefinition<'_, String>],
) -> Result<GraphqlVariableDeclarations, CoreError> {
    let mut declarations = GraphqlVariableDeclarations::default();
    for (index, definition) in definitions.iter().enumerate() {
        let path = format!("query.variables[{index}]");
        if !declarations.names.insert(definition.name.clone()) {
            return Err(unsupported(
                format!("{path}.name"),
                format!(
                    "GraphQL variable '${}' is declared more than once",
                    definition.name
                ),
            ));
        }
        if let Some(default_value) = &definition.default_value {
            declarations.defaults.insert(
                definition.name.clone(),
                compile_variable_default_value(default_value, format!("{path}.default"))?,
            );
        }
    }
    Ok(declarations)
}

fn compile_root_selection_set<'query>(
    selection_set: &SelectionSet<'query, String>,
    graph: Option<&Declaration>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, 'query>,
) -> Result<GraphPlan, CoreError> {
    let path = path.into();
    let mut root_fields = Vec::new();
    collect_root_fields(
        selection_set,
        &path,
        context,
        &mut Vec::new(),
        &mut root_fields,
    )?;
    let [root] = root_fields.as_slice() else {
        return Err(unsupported(
            path,
            "GraphQL virtual graph queries must select exactly one included root node field",
        ));
    };
    compile_root_field(&root.field, graph, &root.path, context)
}

fn collect_root_fields<'query>(
    selection_set: &SelectionSet<'query, String>,
    path: &str,
    context: &GraphqlCompileContext<'_, 'query>,
    fragment_stack: &mut Vec<String>,
    root_fields: &mut Vec<GraphqlRootFieldSelection<'query>>,
) -> Result<(), CoreError> {
    for (index, selection) in selection_set.items.iter().enumerate() {
        let item_path = format!("{path}.items[{index}]");
        match selection {
            Selection::Field(field) => {
                if selection_is_included(
                    &field.directives,
                    format!("{item_path}.directives"),
                    context,
                )? {
                    push_or_merge_root_field(root_fields, item_path, field)?;
                }
            }
            Selection::FragmentSpread(spread) => {
                if !selection_is_included(
                    &spread.directives,
                    format!("{item_path}.directives"),
                    context,
                )? {
                    continue;
                }
                let fragment = context
                    .fragments
                    .get(&spread.fragment_name)
                    .ok_or_else(|| {
                        unsupported(
                            format!("{item_path}.name"),
                            format!("unknown GraphQL fragment '{}'", spread.fragment_name),
                        )
                    })?;
                ensure_root_fragment_type_condition(
                    Some(&fragment.type_condition),
                    format!("{item_path}.typeCondition"),
                )?;
                if fragment_stack.contains(&spread.fragment_name) {
                    return Err(unsupported(
                        format!("{item_path}.name"),
                        format!("GraphQL fragment '{}' forms a cycle", spread.fragment_name),
                    ));
                }
                fragment_stack.push(spread.fragment_name.clone());
                let result = collect_root_fields(
                    &fragment.selection_set,
                    &format!("fragment.{}.selectionSet", fragment.name),
                    context,
                    fragment_stack,
                    root_fields,
                );
                fragment_stack.pop();
                result?;
            }
            Selection::InlineFragment(fragment) => {
                if !selection_is_included(
                    &fragment.directives,
                    format!("{item_path}.directives"),
                    context,
                )? {
                    continue;
                }
                ensure_root_fragment_type_condition(
                    fragment.type_condition.as_ref(),
                    format!("{item_path}.typeCondition"),
                )?;
                collect_root_fields(
                    &fragment.selection_set,
                    &item_path,
                    context,
                    fragment_stack,
                    root_fields,
                )?;
            }
        }
    }
    Ok(())
}

fn push_or_merge_root_field<'query>(
    root_fields: &mut Vec<GraphqlRootFieldSelection<'query>>,
    path: String,
    field: &Field<'query, String>,
) -> Result<(), CoreError> {
    let response_name = graphql_response_name(field);
    let signature = graphql_root_selection_signature(field);
    if let Some(existing) = root_fields
        .iter_mut()
        .find(|selection| graphql_response_name(&selection.field) == response_name)
    {
        if existing.signature != signature {
            return Err(unsupported(
                format!("{path}.alias"),
                format!(
                    "GraphQL root response field '{response_name}' selects conflicting root fields"
                ),
            ));
        }
        existing
            .field
            .selection_set
            .items
            .extend(field.selection_set.items.clone());
        return Ok(());
    }

    root_fields.push(GraphqlRootFieldSelection {
        path,
        field: field.clone(),
        signature,
    });
    Ok(())
}

fn ensure_root_fragment_type_condition(
    type_condition: Option<&TypeCondition<'_, String>>,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let Some(TypeCondition::On(label)) = type_condition else {
        return Ok(());
    };
    if label == "Query" {
        return Ok(());
    }
    Err(unsupported(
        path,
        format!("GraphQL root fragment type condition '{label}' must be Query"),
    ))
}

fn compile_root_field(
    root: &Field<'_, String>,
    graph: Option<&Declaration>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<GraphPlan, CoreError> {
    let path = path.into();
    if root.selection_set.items.is_empty() {
        return Err(unsupported(
            format!("{path}.selectionSet"),
            "GraphQL root node fields must select at least one property",
        ));
    }

    let label = resolve_root_label(&root.name, graph, format!("{path}.name"))?;
    validate_root_arguments(root, &path)?;
    let variable = variable_for_label(&label);
    let mut plan = GraphPlan {
        nodes: vec![NodePattern {
            variable: variable.clone(),
            label: label.clone(),
        }],
        relationships: Vec::new(),
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: Vec::new(),
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let root_context = NodeContext {
        variable: variable.clone(),
        label,
        is_root: true,
        edge_variable: None,
        edge_relationship_type: None,
    };

    compile_selection_set_into_plan(
        &mut plan,
        graph,
        &root.selection_set,
        &root_context,
        format!("{path}.selectionSet"),
        context,
        &mut Vec::new(),
    )?;

    for (index, (name, value)) in root.arguments.iter().enumerate() {
        compile_root_argument(
            &mut plan,
            graph,
            &root_context,
            name,
            value,
            format!("{path}.arguments[{index}]"),
            context,
        )?;
    }

    if plan.projections.is_empty() {
        return Err(unsupported(
            format!("{path}.selectionSet"),
            "GraphQL root node fields must select at least one included property",
        ));
    }

    Ok(plan)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum GraphqlRootArgumentSlot {
    Where,
    OrderBy,
    Limit,
    Offset,
    Distinct,
}

fn validate_root_arguments(root: &Field<'_, String>, path: &str) -> Result<(), CoreError> {
    let mut seen = BTreeMap::new();
    for (index, (name, _)) in root.arguments.iter().enumerate() {
        let Some(slot) = graphql_root_argument_slot(name) else {
            continue;
        };
        if let Some(first_name) = seen.get(&slot) {
            let message = if first_name == name {
                format!("GraphQL root argument '{name}' is specified more than once")
            } else {
                format!(
                    "GraphQL root argument '{name}' conflicts with earlier '{first_name}' argument"
                )
            };
            return Err(unsupported(format!("{path}.arguments[{index}]"), message));
        }
        seen.insert(slot, name.clone());
    }
    Ok(())
}

fn graphql_root_argument_slot(name: &str) -> Option<GraphqlRootArgumentSlot> {
    match name {
        "where" => Some(GraphqlRootArgumentSlot::Where),
        "orderBy" => Some(GraphqlRootArgumentSlot::OrderBy),
        "limit" | "first" => Some(GraphqlRootArgumentSlot::Limit),
        "offset" | "skip" => Some(GraphqlRootArgumentSlot::Offset),
        "distinct" => Some(GraphqlRootArgumentSlot::Distinct),
        _ => None,
    }
}

fn resolve_root_label(
    root_name: &str,
    graph: Option<&Declaration>,
    path: impl Into<String>,
) -> Result<String, CoreError> {
    let Some(graph) = graph else {
        return Ok(root_name.to_string());
    };
    if graph.node(root_name).is_some() {
        return Ok(root_name.to_string());
    }

    let matching_labels = graph
        .nodes
        .iter()
        .filter(|node| root_field_aliases_for_label(&node.label).contains(root_name))
        .map(|node| node.label.as_str())
        .collect::<Vec<_>>();
    match matching_labels.as_slice() {
        [label] => Ok((*label).to_string()),
        [] => Err(unsupported(
            path,
            format!(
                "unknown GraphQL root node field '{root_name}'; expected a graph node label or unambiguous generated root alias"
            ),
        )),
        labels => Err(unsupported(
            path,
            format!(
                "GraphQL root field '{root_name}' is ambiguous across node labels {}; use the exact graph label instead",
                labels.join(", ")
            ),
        )),
    }
}

fn root_field_aliases_for_label(label: &str) -> BTreeSet<String> {
    let lower_first = variable_for_label(label);
    let mut aliases = BTreeSet::from([lower_first.clone()]);
    if !label.ends_with('s') {
        aliases.insert(format!("{label}s"));
        aliases.insert(format!("{lower_first}s"));
    }
    aliases
}

#[derive(Debug, Clone)]
struct NodeContext {
    variable: String,
    label: String,
    is_root: bool,
    edge_variable: Option<String>,
    edge_relationship_type: Option<String>,
}

#[derive(Debug, Default)]
struct GraphqlSelectionScope {
    relationship_fields:
        BTreeMap<GraphqlRelationshipResponseKey, GraphqlRelationshipSelectionBinding>,
}

struct GraphqlSelectionCompileContext<'a, 'variables, 'query> {
    graph: Option<&'a Declaration>,
    compile_context: &'a GraphqlCompileContext<'variables, 'query>,
    fragment_stack: &'a mut Vec<String>,
}

struct GraphqlRootFieldSelection<'query> {
    path: String,
    field: Field<'query, String>,
    signature: GraphqlRootSelectionSignature,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GraphqlRootSelectionSignature {
    field_name: String,
    arguments: Vec<(String, GraphqlValueSignature)>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct GraphqlRelationshipResponseKey {
    source_variable: String,
    response_name: String,
}

#[derive(Debug)]
struct GraphqlRelationshipSelectionBinding {
    signature: GraphqlRelationshipSelectionSignature,
    relationship_index: usize,
    target_variable: String,
    target_label: String,
    relationship_variable: Option<String>,
    edge_relationship_type: String,
    nested_scope: GraphqlSelectionScope,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GraphqlRelationshipSelectionSignature {
    field_name: String,
    arguments: Vec<(String, GraphqlValueSignature)>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum GraphqlValueSignature {
    Variable(String),
    Integer(i64),
    Float(OrderedFloat<f64>),
    String(String),
    Boolean(bool),
    Null,
    Enum(String),
    List(Vec<GraphqlValueSignature>),
    Object(Vec<(String, GraphqlValueSignature)>),
}

struct GraphqlRelationshipFieldSpec {
    direction: Direction,
    relationship_type: String,
    endpoint_argument: &'static str,
    target_label: String,
    needs_relationship_variable: bool,
    response_key: GraphqlRelationshipResponseKey,
    signature: GraphqlRelationshipSelectionSignature,
}

#[derive(Debug, Clone, Copy)]
struct GraphqlWhereScope<'a> {
    graph: Option<&'a Declaration>,
    variable: &'a str,
    label: Option<&'a str>,
}

impl<'a> GraphqlWhereScope<'a> {
    fn node(graph: Option<&'a Declaration>, context: &'a NodeContext) -> Self {
        Self {
            graph,
            variable: &context.variable,
            label: Some(&context.label),
        }
    }

    fn graph_variable(variable: &'a str) -> Self {
        Self {
            graph: None,
            variable,
            label: None,
        }
    }
}

fn compile_selection_set_into_plan(
    plan: &mut GraphPlan,
    graph: Option<&Declaration>,
    selection_set: &SelectionSet<'_, String>,
    context: &NodeContext,
    path: impl Into<String>,
    compile_context: &GraphqlCompileContext<'_, '_>,
    fragment_stack: &mut Vec<String>,
) -> Result<(), CoreError> {
    let mut scope = GraphqlSelectionScope::default();
    let mut selection_context = GraphqlSelectionCompileContext {
        graph,
        compile_context,
        fragment_stack,
    };
    compile_selection_set_items_into_plan(
        plan,
        selection_set,
        context,
        path,
        &mut selection_context,
        &mut scope,
    )
}

fn compile_selection_set_items_into_plan(
    plan: &mut GraphPlan,
    selection_set: &SelectionSet<'_, String>,
    context: &NodeContext,
    path: impl Into<String>,
    selection_context: &mut GraphqlSelectionCompileContext<'_, '_, '_>,
    scope: &mut GraphqlSelectionScope,
) -> Result<(), CoreError> {
    let path = path.into();
    for (index, selection) in selection_set.items.iter().enumerate() {
        let item_path = format!("{path}.items[{index}]");
        match selection {
            Selection::Field(field) => {
                if !selection_is_included(
                    &field.directives,
                    format!("{item_path}.directives"),
                    selection_context.compile_context,
                )? {
                    continue;
                }
                if field.name == "_edge" {
                    compile_edge_field(
                        plan,
                        field,
                        context,
                        &item_path,
                        selection_context.compile_context,
                        selection_context.fragment_stack,
                    )?;
                } else if is_node_aggregate_field(&field.name)
                    || field.selection_set.items.is_empty()
                {
                    compile_property_field(
                        plan,
                        field,
                        context,
                        &item_path,
                        selection_context.compile_context,
                    )?;
                } else {
                    selection_context.graph.ok_or_else(|| {
                        unsupported(
                            format!("{item_path}.selectionSet"),
                            "GraphQL relationship nesting requires a graph declaration",
                        )
                    })?;
                    compile_relationship_field(
                        plan,
                        context,
                        field,
                        &item_path,
                        selection_context,
                        scope,
                    )?;
                }
            }
            Selection::FragmentSpread(spread) => compile_fragment_spread(
                plan,
                context,
                spread,
                &item_path,
                selection_context,
                scope,
            )?,
            Selection::InlineFragment(fragment) => compile_inline_fragment(
                plan,
                context,
                fragment,
                &item_path,
                selection_context,
                scope,
            )?,
        }
    }
    Ok(())
}

fn compile_property_field(
    plan: &mut GraphPlan,
    field: &Field<'_, String>,
    context: &NodeContext,
    path: &str,
    compile_context: &GraphqlCompileContext<'_, '_>,
) -> Result<(), CoreError> {
    if let Some(projection) = compile_node_aggregate_field(field, context, path, compile_context)? {
        push_graphql_projection(plan, projection, path)?;
        return Ok(());
    }
    if !field.arguments.is_empty() {
        return Err(unsupported(
            format!("{path}.arguments"),
            "GraphQL property field arguments are not supported",
        ));
    }
    if field.name == "__typename" {
        push_graphql_projection(
            plan,
            Projection::Literal {
                literal: Literal::String(context.label.clone()),
                alias: projection_alias(field, context),
            },
            path,
        )?;
        return Ok(());
    }
    if field.name == "_id" {
        push_graphql_projection(
            plan,
            Projection::Key {
                variable: context.variable.clone(),
                alias: projection_alias(field, context),
            },
            path,
        )?;
        return Ok(());
    }
    if field.name == "_elementId" {
        push_graphql_projection(
            plan,
            Projection::ElementId {
                variable: context.variable.clone(),
                alias: projection_alias(field, context),
            },
            path,
        )?;
        return Ok(());
    }
    push_graphql_projection(
        plan,
        Projection::Property {
            property: PropertyRef {
                variable: context.variable.clone(),
                property: field.name.clone(),
            },
            alias: Some(projection_alias(field, context)),
        },
        path,
    )
}

fn is_node_aggregate_field(name: &str) -> bool {
    name == "_count" || graphql_property_aggregate_field(name).is_some()
}

fn compile_node_aggregate_field(
    field: &Field<'_, String>,
    context: &NodeContext,
    path: &str,
    compile_context: &GraphqlCompileContext<'_, '_>,
) -> Result<Option<Projection>, CoreError> {
    if !is_node_aggregate_field(&field.name) {
        return Ok(None);
    }
    if !field.selection_set.items.is_empty() {
        return Err(unsupported(
            format!("{path}.selectionSet"),
            "GraphQL aggregate fields must not select nested fields",
        ));
    }

    let alias = projection_alias(field, context);
    if field.name == "_count" {
        return Ok(Some(compile_count_aggregate_field(
            field,
            context,
            path,
            compile_context,
            alias,
        )?));
    }
    let aggregate = graphql_property_aggregate_field(&field.name)
        .ok_or_else(|| CoreError::internal("aggregate field name was checked"))?;
    Ok(Some(compile_property_aggregate_field(
        field,
        context,
        path,
        compile_context,
        aggregate.function,
        aggregate.distinct,
        alias,
    )?))
}

fn compile_count_aggregate_field(
    field: &Field<'_, String>,
    context: &NodeContext,
    path: &str,
    compile_context: &GraphqlCompileContext<'_, '_>,
    alias: String,
) -> Result<Projection, CoreError> {
    if field.arguments.is_empty() {
        return Ok(Projection::CountAll { alias });
    }
    compile_property_aggregate_field(
        field,
        context,
        path,
        compile_context,
        AggregateFunction::Count,
        false,
        alias,
    )
}

fn compile_property_aggregate_field(
    field: &Field<'_, String>,
    context: &NodeContext,
    path: &str,
    compile_context: &GraphqlCompileContext<'_, '_>,
    function: AggregateFunction,
    distinct: bool,
    alias: String,
) -> Result<Projection, CoreError> {
    let property = compile_single_aggregate_field_argument(field, path, compile_context)?;
    Ok(Projection::Aggregate {
        function,
        target: AggregateTarget::Property(PropertyRef {
            variable: context.variable.clone(),
            property,
        }),
        distinct,
        alias,
    })
}

fn compile_single_aggregate_field_argument(
    field: &Field<'_, String>,
    path: &str,
    compile_context: &GraphqlCompileContext<'_, '_>,
) -> Result<String, CoreError> {
    let [(name, value)] = field.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            format!(
                "GraphQL aggregate field '{}' requires exactly one 'field' argument",
                field.name
            ),
        ));
    };
    if name != "field" {
        return Err(unsupported(
            format!("{path}.arguments[0].{name}"),
            format!("unsupported GraphQL aggregate argument '{name}'"),
        ));
    }
    compile_name_value(value, format!("{path}.arguments[0].field"), compile_context)
}

fn compile_fragment_spread(
    plan: &mut GraphPlan,
    context: &NodeContext,
    spread: &FragmentSpread<'_, String>,
    path: &str,
    selection_context: &mut GraphqlSelectionCompileContext<'_, '_, '_>,
    scope: &mut GraphqlSelectionScope,
) -> Result<(), CoreError> {
    if !selection_is_included(
        &spread.directives,
        format!("{path}.directives"),
        selection_context.compile_context,
    )? {
        return Ok(());
    }
    let fragment = selection_context
        .compile_context
        .fragments
        .get(&spread.fragment_name)
        .ok_or_else(|| {
            unsupported(
                format!("{path}.name"),
                format!("unknown GraphQL fragment '{}'", spread.fragment_name),
            )
        })?;
    ensure_fragment_type_condition(
        Some(&fragment.type_condition),
        context,
        format!("{path}.typeCondition"),
    )?;
    if selection_context
        .fragment_stack
        .contains(&spread.fragment_name)
    {
        return Err(unsupported(
            format!("{path}.name"),
            format!("GraphQL fragment '{}' forms a cycle", spread.fragment_name),
        ));
    }
    selection_context
        .fragment_stack
        .push(spread.fragment_name.clone());
    let result = compile_selection_set_items_into_plan(
        plan,
        &fragment.selection_set,
        context,
        format!("fragment.{}.selectionSet", fragment.name),
        selection_context,
        scope,
    );
    selection_context.fragment_stack.pop();
    result
}

fn compile_inline_fragment(
    plan: &mut GraphPlan,
    context: &NodeContext,
    fragment: &InlineFragment<'_, String>,
    path: &str,
    selection_context: &mut GraphqlSelectionCompileContext<'_, '_, '_>,
    scope: &mut GraphqlSelectionScope,
) -> Result<(), CoreError> {
    if !selection_is_included(
        &fragment.directives,
        format!("{path}.directives"),
        selection_context.compile_context,
    )? {
        return Ok(());
    }
    ensure_fragment_type_condition(
        fragment.type_condition.as_ref(),
        context,
        format!("{path}.typeCondition"),
    )?;
    compile_selection_set_items_into_plan(
        plan,
        &fragment.selection_set,
        context,
        format!("{path}.selectionSet"),
        selection_context,
        scope,
    )
}

fn ensure_fragment_type_condition(
    type_condition: Option<&TypeCondition<'_, String>>,
    context: &NodeContext,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let Some(TypeCondition::On(label)) = type_condition else {
        return Ok(());
    };
    if label == &context.label {
        return Ok(());
    }
    Err(unsupported(
        path,
        format!(
            "GraphQL fragment type condition '{label}' must match graph label '{}'",
            context.label
        ),
    ))
}

fn compile_relationship_field(
    plan: &mut GraphPlan,
    source: &NodeContext,
    field: &Field<'_, String>,
    path: impl Into<String>,
    selection_context: &mut GraphqlSelectionCompileContext<'_, '_, '_>,
    scope: &mut GraphqlSelectionScope,
) -> Result<(), CoreError> {
    let path = path.into();
    let graph = selection_context.graph.ok_or_else(|| {
        unsupported(
            format!("{path}.selectionSet"),
            "GraphQL relationship nesting requires a graph declaration",
        )
    })?;
    let (direction, relationship_type, endpoint_argument) =
        compile_relationship_field_name(&field.name, format!("{path}.name"))?;
    let endpoint = RelationshipEndpointContext {
        graph,
        source_label: &source.label,
        relationship_type: &relationship_type,
        direction,
        endpoint_argument,
    };
    validate_relationship_field_arguments(field, &endpoint, &path)?;
    let target_label = compile_relationship_target_label(
        &endpoint,
        field,
        format!("{path}.arguments"),
        selection_context.compile_context,
    )?;

    ensure_node_label(
        graph,
        &target_label,
        format!("{path}.arguments.{endpoint_argument}"),
    )?;
    ensure_relationship_mapping(
        graph,
        &relationship_type,
        direction,
        &source.label,
        &target_label,
        &path,
    )?;

    let needs_relationship_variable = field
        .arguments
        .iter()
        .any(|(name, _)| name == "relationshipWhere")
        || relationship_selection_needs_edge_variable(
            &field.selection_set,
            &target_label,
            selection_context.compile_context,
            format!("{path}.selectionSet"),
            &mut Vec::new(),
        )?;

    let spec = GraphqlRelationshipFieldSpec {
        direction,
        relationship_type,
        endpoint_argument,
        target_label,
        needs_relationship_variable,
        response_key: GraphqlRelationshipResponseKey {
            source_variable: source.variable.clone(),
            response_name: graphql_response_name(field),
        },
        signature: graphql_relationship_selection_signature(field),
    };
    if let Some(binding) = scope.relationship_fields.get_mut(&spec.response_key) {
        if binding.signature != spec.signature {
            return Err(unsupported(
                format!("{path}.alias"),
                format!(
                    "GraphQL relationship response field '{}' selects conflicting traversals",
                    spec.response_key.response_name
                ),
            ));
        }
        return compile_existing_relationship_field(
            plan,
            field,
            &path,
            selection_context,
            binding,
            spec.needs_relationship_variable,
        );
    }

    compile_new_relationship_field(plan, source, field, &path, selection_context, scope, spec)
}

fn compile_existing_relationship_field(
    plan: &mut GraphPlan,
    field: &Field<'_, String>,
    path: &str,
    selection_context: &mut GraphqlSelectionCompileContext<'_, '_, '_>,
    binding: &mut GraphqlRelationshipSelectionBinding,
    needs_relationship_variable: bool,
) -> Result<(), CoreError> {
    if needs_relationship_variable && binding.relationship_variable.is_none() {
        let relationship_variable =
            relationship_variable_for_field(field, binding.relationship_index);
        plan.relationships
            .get_mut(binding.relationship_index)
            .ok_or_else(|| CoreError::internal("merged GraphQL relationship index missing"))?
            .variable = Some(relationship_variable.clone());
        binding.relationship_variable = Some(relationship_variable);
    }
    compile_selection_set_items_into_plan(
        plan,
        &field.selection_set,
        &NodeContext {
            variable: binding.target_variable.clone(),
            label: binding.target_label.clone(),
            is_root: false,
            edge_variable: binding.relationship_variable.clone(),
            edge_relationship_type: Some(binding.edge_relationship_type.clone()),
        },
        format!("{path}.selectionSet"),
        selection_context,
        &mut binding.nested_scope,
    )
}

fn compile_new_relationship_field(
    plan: &mut GraphPlan,
    source: &NodeContext,
    field: &Field<'_, String>,
    path: &str,
    selection_context: &mut GraphqlSelectionCompileContext<'_, '_, '_>,
    scope: &mut GraphqlSelectionScope,
    spec: GraphqlRelationshipFieldSpec,
) -> Result<(), CoreError> {
    let graph = selection_context.graph.ok_or_else(|| {
        unsupported(
            format!("{path}.selectionSet"),
            "GraphQL relationship nesting requires a graph declaration",
        )
    })?;
    let relationship_index = plan.relationships.len();
    let relationship_variable = spec
        .needs_relationship_variable
        .then(|| relationship_variable_for_field(field, relationship_index));
    let target_variable = nested_variable_for_field(field, &spec.target_label, plan.nodes.len());
    let edge_relationship_type = spec.relationship_type.clone();

    let relationship_argument_context = RelationshipFieldArgumentContext {
        graph,
        endpoint_argument: spec.endpoint_argument,
        target_variable: &target_variable,
        target_label: &spec.target_label,
        relationship_variable: relationship_variable.as_deref(),
        path,
        compile_context: selection_context.compile_context,
    };
    compile_relationship_field_arguments(plan, field, &relationship_argument_context)?;

    plan.nodes.push(NodePattern {
        variable: target_variable.clone(),
        label: spec.target_label.clone(),
    });
    plan.relationships.push(RelationshipPattern {
        variable: relationship_variable.clone(),
        relationship_type: spec.relationship_type,
        left: source.variable.clone(),
        direction: spec.direction,
        right: target_variable.clone(),
    });

    scope.relationship_fields.insert(
        spec.response_key.clone(),
        GraphqlRelationshipSelectionBinding {
            signature: spec.signature,
            relationship_index,
            target_variable: target_variable.clone(),
            target_label: spec.target_label.clone(),
            relationship_variable: relationship_variable.clone(),
            edge_relationship_type: edge_relationship_type.clone(),
            nested_scope: GraphqlSelectionScope::default(),
        },
    );
    let binding = scope
        .relationship_fields
        .get_mut(&spec.response_key)
        .ok_or_else(|| CoreError::internal("GraphQL relationship merge binding missing"))?;

    compile_selection_set_items_into_plan(
        plan,
        &field.selection_set,
        &NodeContext {
            variable: target_variable,
            label: spec.target_label,
            is_root: false,
            edge_variable: relationship_variable,
            edge_relationship_type: Some(edge_relationship_type),
        },
        format!("{path}.selectionSet"),
        selection_context,
        &mut binding.nested_scope,
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum GraphqlRelationshipArgumentSlot {
    Endpoint,
    Where,
    RelationshipWhere,
}

fn validate_relationship_field_arguments(
    field: &Field<'_, String>,
    endpoint: &RelationshipEndpointContext<'_>,
    path: &str,
) -> Result<(), CoreError> {
    let mut seen = BTreeMap::new();
    for (index, (name, _)) in field.arguments.iter().enumerate() {
        let slot = match name.as_str() {
            "to" | "from" | "label" => {
                if name != endpoint.endpoint_argument {
                    return Err(unsupported(
                        format!("{path}.arguments[{index}]"),
                        format!(
                            "GraphQL relationship field '{}' requires '{}' instead of '{}'",
                            field.name, endpoint.endpoint_argument, name
                        ),
                    ));
                }
                GraphqlRelationshipArgumentSlot::Endpoint
            }
            "where" => GraphqlRelationshipArgumentSlot::Where,
            "relationshipWhere" => GraphqlRelationshipArgumentSlot::RelationshipWhere,
            _ => continue,
        };
        if let Some(first_name) = seen.get(&slot) {
            let message = if first_name == name {
                format!("GraphQL relationship argument '{name}' is specified more than once")
            } else {
                format!(
                    "GraphQL relationship argument '{name}' conflicts with earlier '{first_name}' argument"
                )
            };
            return Err(unsupported(format!("{path}.arguments[{index}]"), message));
        }
        seen.insert(slot, name.clone());
    }
    Ok(())
}

fn compile_relationship_field_arguments(
    plan: &mut GraphPlan,
    field: &Field<'_, String>,
    context: &RelationshipFieldArgumentContext<'_, '_, '_>,
) -> Result<(), CoreError> {
    for (index, (name, value)) in field.arguments.iter().enumerate() {
        let argument_path = format!("{}.arguments[{index}]", context.path);
        match name.as_str() {
            "to" | "from" | "label" => {
                if name != context.endpoint_argument {
                    return Err(unsupported(
                        argument_path,
                        format!(
                            "GraphQL relationship field '{}' requires '{}' instead of '{}'",
                            field.name, context.endpoint_argument, name
                        ),
                    ));
                }
            }
            "where" => append_where_predicate(
                plan,
                compile_where_argument(
                    GraphqlWhereScope {
                        graph: Some(context.graph),
                        variable: context.target_variable,
                        label: Some(context.target_label),
                    },
                    value,
                    argument_path,
                    context.compile_context,
                )?,
            ),
            "relationshipWhere" => {
                let relationship_variable = context
                    .relationship_variable
                    .ok_or_else(|| CoreError::internal("relationshipWhere variable missing"))?;
                append_where_predicate(
                    plan,
                    compile_where_argument(
                        GraphqlWhereScope::graph_variable(relationship_variable),
                        value,
                        argument_path,
                        context.compile_context,
                    )?,
                );
            }
            "orderBy" | "limit" | "first" | "offset" | "skip" | "distinct" => {
                return Err(unsupported(
                    argument_path,
                    "GraphQL nested relationship fields do not support row modifiers yet",
                ));
            }
            _ => {
                return Err(unsupported(
                    argument_path,
                    format!("unsupported GraphQL relationship argument '{name}'"),
                ));
            }
        }
    }
    Ok(())
}

struct RelationshipFieldArgumentContext<'a, 'variables, 'query> {
    graph: &'a Declaration,
    endpoint_argument: &'a str,
    target_variable: &'a str,
    target_label: &'a str,
    relationship_variable: Option<&'a str>,
    path: &'a str,
    compile_context: &'a GraphqlCompileContext<'variables, 'query>,
}

fn compile_edge_field(
    plan: &mut GraphPlan,
    field: &Field<'_, String>,
    context: &NodeContext,
    path: &str,
    compile_context: &GraphqlCompileContext<'_, '_>,
    fragment_stack: &mut Vec<String>,
) -> Result<(), CoreError> {
    if field.selection_set.items.is_empty() {
        return Err(unsupported(
            format!("{path}.selectionSet"),
            "GraphQL _edge fields must select relationship properties",
        ));
    }
    if !field.arguments.is_empty() {
        return Err(unsupported(
            format!("{path}.arguments"),
            "GraphQL _edge fields do not support arguments",
        ));
    }
    let edge_variable = context.edge_variable.as_deref().ok_or_else(|| {
        unsupported(
            path,
            "GraphQL _edge selections are only valid inside relationship fields",
        )
    })?;
    let edge_relationship_type = context.edge_relationship_type.as_deref().ok_or_else(|| {
        CoreError::internal("GraphQL edge relationship type missing for _edge selection")
    })?;
    compile_edge_selection_set(
        plan,
        &field.selection_set,
        edge_variable,
        edge_relationship_type,
        format!("{path}.selectionSet"),
        compile_context,
        fragment_stack,
    )
}

fn compile_edge_selection_set(
    plan: &mut GraphPlan,
    selection_set: &SelectionSet<'_, String>,
    edge_variable: &str,
    edge_relationship_type: &str,
    path: impl Into<String>,
    compile_context: &GraphqlCompileContext<'_, '_>,
    fragment_stack: &mut Vec<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    for (index, selection) in selection_set.items.iter().enumerate() {
        let item_path = format!("{path}.items[{index}]");
        match selection {
            Selection::Field(property) => compile_edge_projection_field(
                plan,
                property,
                edge_variable,
                edge_relationship_type,
                &item_path,
                compile_context,
            )?,
            Selection::FragmentSpread(spread) => compile_edge_fragment_spread(
                plan,
                spread,
                edge_variable,
                edge_relationship_type,
                &item_path,
                compile_context,
                fragment_stack,
            )?,
            Selection::InlineFragment(fragment) => compile_edge_inline_fragment(
                plan,
                fragment,
                edge_variable,
                edge_relationship_type,
                &item_path,
                compile_context,
                fragment_stack,
            )?,
        }
    }
    Ok(())
}

fn compile_edge_projection_field(
    plan: &mut GraphPlan,
    property: &Field<'_, String>,
    edge_variable: &str,
    edge_relationship_type: &str,
    path: &str,
    compile_context: &GraphqlCompileContext<'_, '_>,
) -> Result<(), CoreError> {
    if !selection_is_included(
        &property.directives,
        format!("{path}.directives"),
        compile_context,
    )? {
        return Ok(());
    }
    if !property.arguments.is_empty() {
        return Err(unsupported(
            format!("{path}.arguments"),
            "GraphQL _edge property arguments are not supported",
        ));
    }
    if !property.selection_set.items.is_empty() {
        return Err(unsupported(
            format!("{path}.selectionSet"),
            "GraphQL _edge properties must be scalar fields",
        ));
    }
    if property.name == "__typename" {
        push_graphql_projection(
            plan,
            Projection::Literal {
                literal: Literal::String(edge_relationship_type.to_string()),
                alias: edge_projection_alias(property, edge_variable),
            },
            path,
        )?;
        return Ok(());
    }
    if property.name == "_id" {
        push_graphql_projection(
            plan,
            Projection::Key {
                variable: edge_variable.to_string(),
                alias: edge_projection_alias(property, edge_variable),
            },
            path,
        )?;
        return Ok(());
    }
    if property.name == "_elementId" {
        push_graphql_projection(
            plan,
            Projection::ElementId {
                variable: edge_variable.to_string(),
                alias: edge_projection_alias(property, edge_variable),
            },
            path,
        )?;
        return Ok(());
    }
    push_graphql_projection(
        plan,
        Projection::Property {
            property: PropertyRef {
                variable: edge_variable.to_string(),
                property: property.name.clone(),
            },
            alias: Some(edge_projection_alias(property, edge_variable)),
        },
        path,
    )
}

fn compile_edge_fragment_spread(
    plan: &mut GraphPlan,
    spread: &FragmentSpread<'_, String>,
    edge_variable: &str,
    edge_relationship_type: &str,
    path: &str,
    compile_context: &GraphqlCompileContext<'_, '_>,
    fragment_stack: &mut Vec<String>,
) -> Result<(), CoreError> {
    if !selection_is_included(
        &spread.directives,
        format!("{path}.directives"),
        compile_context,
    )? {
        return Ok(());
    }
    let fragment = compile_context
        .fragments
        .get(&spread.fragment_name)
        .ok_or_else(|| {
            unsupported(
                format!("{path}.name"),
                format!("unknown GraphQL fragment '{}'", spread.fragment_name),
            )
        })?;
    ensure_edge_fragment_type_condition(
        Some(&fragment.type_condition),
        edge_relationship_type,
        format!("{path}.typeCondition"),
    )?;
    if fragment_stack.contains(&spread.fragment_name) {
        return Err(unsupported(
            format!("{path}.name"),
            format!("GraphQL fragment '{}' forms a cycle", spread.fragment_name),
        ));
    }
    fragment_stack.push(spread.fragment_name.clone());
    let result = compile_edge_selection_set(
        plan,
        &fragment.selection_set,
        edge_variable,
        edge_relationship_type,
        format!("fragment.{}.selectionSet", fragment.name),
        compile_context,
        fragment_stack,
    );
    fragment_stack.pop();
    result
}

fn compile_edge_inline_fragment(
    plan: &mut GraphPlan,
    fragment: &InlineFragment<'_, String>,
    edge_variable: &str,
    edge_relationship_type: &str,
    path: &str,
    compile_context: &GraphqlCompileContext<'_, '_>,
    fragment_stack: &mut Vec<String>,
) -> Result<(), CoreError> {
    if !selection_is_included(
        &fragment.directives,
        format!("{path}.directives"),
        compile_context,
    )? {
        return Ok(());
    }
    ensure_edge_fragment_type_condition(
        fragment.type_condition.as_ref(),
        edge_relationship_type,
        format!("{path}.typeCondition"),
    )?;
    compile_edge_selection_set(
        plan,
        &fragment.selection_set,
        edge_variable,
        edge_relationship_type,
        format!("{path}.selectionSet"),
        compile_context,
        fragment_stack,
    )
}

fn ensure_edge_fragment_type_condition(
    type_condition: Option<&TypeCondition<'_, String>>,
    edge_relationship_type: &str,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let Some(TypeCondition::On(type_name)) = type_condition else {
        return Ok(());
    };
    if type_name == edge_relationship_type {
        return Ok(());
    }
    Err(unsupported(
        path,
        format!(
            "GraphQL edge fragment type condition '{type_name}' must match relationship type '{edge_relationship_type}'"
        ),
    ))
}

fn compile_relationship_field_name(
    name: &str,
    path: impl Into<String>,
) -> Result<(Direction, String, &'static str), CoreError> {
    let path = path.into();
    if let Some(relationship_type) = name.strip_prefix("out_") {
        return non_empty_relationship_type(relationship_type, path, Direction::Outgoing, "to");
    }
    if let Some(relationship_type) = name.strip_prefix("in_") {
        return non_empty_relationship_type(relationship_type, path, Direction::Incoming, "from");
    }
    if let Some(relationship_type) = name.strip_prefix("any_") {
        return non_empty_relationship_type(
            relationship_type,
            path,
            Direction::Undirected,
            "label",
        );
    }
    Err(unsupported(
        path,
        "GraphQL relationship fields must be named out_TYPE, in_TYPE, or any_TYPE",
    ))
}

fn non_empty_relationship_type(
    relationship_type: &str,
    path: String,
    direction: Direction,
    endpoint_argument: &'static str,
) -> Result<(Direction, String, &'static str), CoreError> {
    if relationship_type.is_empty() {
        return Err(unsupported(
            path,
            "GraphQL relationship field is missing a relationship type",
        ));
    }
    Ok((direction, relationship_type.to_string(), endpoint_argument))
}

struct RelationshipEndpointContext<'a> {
    graph: &'a Declaration,
    source_label: &'a str,
    relationship_type: &'a str,
    direction: Direction,
    endpoint_argument: &'static str,
}

fn compile_relationship_target_label(
    endpoint: &RelationshipEndpointContext<'_>,
    field: &Field<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<String, CoreError> {
    let path = path.into();
    let mut target_label = None;
    for (index, (name, value)) in field.arguments.iter().enumerate() {
        if name == endpoint.endpoint_argument {
            if target_label.is_some() {
                return Err(unsupported(
                    format!("{path}[{index}]"),
                    format!(
                        "GraphQL relationship argument '{}' is duplicated",
                        endpoint.endpoint_argument
                    ),
                ));
            }
            target_label = Some(compile_name_value(
                value,
                format!("{path}.{}", endpoint.endpoint_argument),
                context,
            )?);
        }
    }
    match target_label {
        Some(target_label) => Ok(target_label),
        None => infer_relationship_target_label(endpoint, path),
    }
}

fn infer_relationship_target_label(
    endpoint: &RelationshipEndpointContext<'_>,
    path: impl Into<String>,
) -> Result<String, CoreError> {
    let path = path.into();
    if endpoint
        .graph
        .relationships_for_type(endpoint.relationship_type)
        .next()
        .is_none()
    {
        return Err(unsupported(
            path,
            format!(
                "unknown GraphQL relationship type '{}'",
                endpoint.relationship_type
            ),
        ));
    }
    let labels = endpoint
        .graph
        .relationships_for_type(endpoint.relationship_type)
        .filter_map(|relationship| match endpoint.direction {
            Direction::Outgoing if relationship.from.label == endpoint.source_label => {
                Some(relationship.to.label.clone())
            }
            Direction::Incoming if relationship.to.label == endpoint.source_label => {
                Some(relationship.from.label.clone())
            }
            Direction::Undirected => match (
                relationship.from.label == endpoint.source_label,
                relationship.to.label == endpoint.source_label,
            ) {
                (true, _) => Some(relationship.to.label.clone()),
                (false, true) => Some(relationship.from.label.clone()),
                (false, false) => None,
            },
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    match labels.len() {
        1 => Ok(labels
            .into_iter()
            .next()
            .expect("relationship target label set length was checked")),
        0 => Err(unsupported(
            path,
            format!(
                "GraphQL relationship field requires '{endpoint_argument}' because no {relationship_type} mapping starts at graph label '{source_label}'",
                endpoint_argument = endpoint.endpoint_argument,
                relationship_type = endpoint.relationship_type,
                source_label = endpoint.source_label,
            ),
        )),
        _ => Err(unsupported(
            path,
            format!(
                "GraphQL relationship field requires '{endpoint_argument}' because relationship type '{relationship_type}' maps graph label '{source_label}' to multiple endpoint labels: {}",
                labels.into_iter().collect::<Vec<_>>().join(", "),
                endpoint_argument = endpoint.endpoint_argument,
                relationship_type = endpoint.relationship_type,
                source_label = endpoint.source_label,
            ),
        )),
    }
}

fn ensure_node_label(
    graph: &Declaration,
    label: &str,
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    if graph.node(label).is_some() {
        return Ok(());
    }
    Err(unsupported(
        path,
        format!("unknown GraphQL target node label '{label}'"),
    ))
}

fn ensure_relationship_mapping(
    graph: &Declaration,
    relationship_type: &str,
    direction: Direction,
    left_label: &str,
    right_label: &str,
    path: &str,
) -> Result<(), CoreError> {
    let candidates = graph
        .relationships_for_type(relationship_type)
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return Err(unsupported(
            format!("{path}.name"),
            format!("unknown GraphQL relationship type '{relationship_type}'"),
        ));
    }
    let matches = candidates
        .iter()
        .copied()
        .filter(|relationship| {
            let matches_forward =
                left_label == relationship.from.label && right_label == relationship.to.label;
            let matches_reverse =
                left_label == relationship.to.label && right_label == relationship.from.label;
            match direction {
                Direction::Outgoing => matches_forward,
                Direction::Incoming => matches_reverse,
                Direction::Undirected => matches_forward || matches_reverse,
            }
        })
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [_] => Ok(()),
        [] => {
            let available = candidates
                .iter()
                .map(|relationship| {
                    format!("{} -> {}", relationship.from.label, relationship.to.label)
                })
                .collect::<Vec<_>>()
                .join(", ");
            Err(unsupported(
                path.to_string(),
                format!(
                    "GraphQL relationship type '{relationship_type}' has no mapping for {left_label} -> {right_label}; available endpoint mappings: {available}"
                ),
            ))
        }
        _ => Err(unsupported(
            path.to_string(),
            format!(
                "GraphQL relationship type '{relationship_type}' with endpoints {left_label} -> {right_label} is ambiguous"
            ),
        )),
    }
}

fn projection_alias(field: &Field<'_, String>, context: &NodeContext) -> String {
    field.alias.clone().unwrap_or_else(|| {
        if context.is_root {
            field.name.clone()
        } else {
            format!("{}_{}", context.variable, field.name)
        }
    })
}

fn graphql_response_name(field: &Field<'_, String>) -> String {
    field.alias.clone().unwrap_or_else(|| field.name.clone())
}

fn graphql_root_selection_signature(field: &Field<'_, String>) -> GraphqlRootSelectionSignature {
    GraphqlRootSelectionSignature {
        field_name: field.name.clone(),
        arguments: graphql_field_argument_signature(field),
    }
}

fn graphql_relationship_selection_signature(
    field: &Field<'_, String>,
) -> GraphqlRelationshipSelectionSignature {
    GraphqlRelationshipSelectionSignature {
        field_name: field.name.clone(),
        arguments: graphql_field_argument_signature(field),
    }
}

fn graphql_field_argument_signature(
    field: &Field<'_, String>,
) -> Vec<(String, GraphqlValueSignature)> {
    let mut arguments = field
        .arguments
        .iter()
        .map(|(name, value)| (name.clone(), graphql_value_signature(value)))
        .collect::<Vec<_>>();
    arguments.sort_by(|(left, _), (right, _)| left.cmp(right));
    arguments
}

fn graphql_value_signature(value: &Value<'_, String>) -> GraphqlValueSignature {
    match value {
        Value::Variable(variable) => GraphqlValueSignature::Variable(variable.clone()),
        Value::Int(number) => GraphqlValueSignature::Integer(
            number.as_i64().expect("GraphQL parser stores Int as i64"),
        ),
        Value::Float(value) => GraphqlValueSignature::Float(OrderedFloat(*value)),
        Value::String(value) => GraphqlValueSignature::String(value.clone()),
        Value::Boolean(value) => GraphqlValueSignature::Boolean(*value),
        Value::Null => GraphqlValueSignature::Null,
        Value::Enum(value) => GraphqlValueSignature::Enum(value.clone()),
        Value::List(values) => GraphqlValueSignature::List(
            values
                .iter()
                .map(graphql_value_signature)
                .collect::<Vec<_>>(),
        ),
        Value::Object(values) => GraphqlValueSignature::Object(
            values
                .iter()
                .map(|(name, value)| (name.clone(), graphql_value_signature(value)))
                .collect::<Vec<_>>(),
        ),
    }
}

fn edge_projection_alias(field: &Field<'_, String>, edge_variable: &str) -> String {
    field
        .alias
        .clone()
        .unwrap_or_else(|| format!("{edge_variable}_{}", field.name))
}

fn selection_is_included(
    directives: &[Directive<'_, String>],
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<bool, CoreError> {
    let path = path.into();
    let mut included = true;
    let mut seen_directives = BTreeSet::new();
    for (index, directive) in directives.iter().enumerate() {
        let directive_path = format!("{path}[{index}]");
        if !seen_directives.insert(directive.name.clone()) {
            return Err(unsupported(
                format!("{directive_path}.name"),
                format!("GraphQL directive '@{}' is repeated", directive.name),
            ));
        }
        match directive.name.as_str() {
            "include" => {
                if !compile_directive_if_argument(directive, &directive_path, context)? {
                    included = false;
                }
            }
            "skip" => {
                if compile_directive_if_argument(directive, &directive_path, context)? {
                    included = false;
                }
            }
            _ => {
                return Err(unsupported(
                    format!("{directive_path}.name"),
                    format!("unsupported GraphQL directive '@{}'", directive.name),
                ));
            }
        }
    }
    Ok(included)
}

fn compile_directive_if_argument(
    directive: &Directive<'_, String>,
    path: &str,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<bool, CoreError> {
    let [(name, value)] = directive.arguments.as_slice() else {
        return Err(unsupported(
            format!("{path}.arguments"),
            format!(
                "GraphQL @{} directive requires exactly one 'if' argument",
                directive.name
            ),
        ));
    };
    if name != "if" {
        return Err(unsupported(
            format!("{path}.arguments[0]"),
            format!(
                "GraphQL @{} directive requires an 'if' argument",
                directive.name
            ),
        ));
    }
    compile_boolean(
        value,
        format!("{path}.arguments.if"),
        &format!("@{} if argument", directive.name),
        context,
    )
}

fn relationship_selection_needs_edge_variable(
    selection_set: &SelectionSet<'_, String>,
    target_label: &str,
    context: &GraphqlCompileContext<'_, '_>,
    path: impl Into<String>,
    fragment_stack: &mut Vec<String>,
) -> Result<bool, CoreError> {
    let path = path.into();
    for (index, selection) in selection_set.items.iter().enumerate() {
        let item_path = format!("{path}.items[{index}]");
        match selection {
            Selection::Field(field) => {
                if !selection_is_included(
                    &field.directives,
                    format!("{item_path}.directives"),
                    context,
                )? {
                    continue;
                }
                if field.name == "_edge" {
                    return Ok(true);
                }
            }
            Selection::FragmentSpread(spread) => {
                if !selection_is_included(
                    &spread.directives,
                    format!("{item_path}.directives"),
                    context,
                )? {
                    continue;
                }
                let fragment = context
                    .fragments
                    .get(&spread.fragment_name)
                    .ok_or_else(|| {
                        unsupported(
                            format!("{item_path}.name"),
                            format!("unknown GraphQL fragment '{}'", spread.fragment_name),
                        )
                    })?;
                ensure_fragment_type_condition(
                    Some(&fragment.type_condition),
                    &NodeContext {
                        variable: String::new(),
                        label: target_label.to_string(),
                        is_root: false,
                        edge_variable: None,
                        edge_relationship_type: None,
                    },
                    format!("{item_path}.typeCondition"),
                )?;
                if fragment_stack.contains(&spread.fragment_name) {
                    return Err(unsupported(
                        format!("{item_path}.name"),
                        format!("GraphQL fragment '{}' forms a cycle", spread.fragment_name),
                    ));
                }
                fragment_stack.push(spread.fragment_name.clone());
                let needs_edge = relationship_selection_needs_edge_variable(
                    &fragment.selection_set,
                    target_label,
                    context,
                    format!("fragment.{}.selectionSet", fragment.name),
                    fragment_stack,
                );
                fragment_stack.pop();
                if needs_edge? {
                    return Ok(true);
                }
            }
            Selection::InlineFragment(fragment) => {
                if !selection_is_included(
                    &fragment.directives,
                    format!("{item_path}.directives"),
                    context,
                )? {
                    continue;
                }
                ensure_fragment_type_condition(
                    fragment.type_condition.as_ref(),
                    &NodeContext {
                        variable: String::new(),
                        label: target_label.to_string(),
                        is_root: false,
                        edge_variable: None,
                        edge_relationship_type: None,
                    },
                    format!("{item_path}.typeCondition"),
                )?;
                if relationship_selection_needs_edge_variable(
                    &fragment.selection_set,
                    target_label,
                    context,
                    format!("{item_path}.selectionSet"),
                    fragment_stack,
                )? {
                    return Ok(true);
                }
            }
        }
    }
    Ok(false)
}

fn push_graphql_projection(
    plan: &mut GraphPlan,
    projection: Projection,
    path: &str,
) -> Result<(), CoreError> {
    let alias = graphql_projection_alias(&projection).ok_or_else(|| {
        CoreError::internal("GraphQL projection without an output alias reached frontend")
    })?;
    if let Some(existing) = plan
        .projections
        .iter()
        .find(|existing| graphql_projection_alias(existing).as_deref() == Some(alias.as_str()))
    {
        if existing == &projection {
            return Ok(());
        }
        return Err(unsupported(
            format!("{path}.alias"),
            format!("GraphQL response alias '{alias}' selects conflicting fields"),
        ));
    }
    plan.projections.push(projection);
    Ok(())
}

fn graphql_projection_alias(projection: &Projection) -> Option<String> {
    match projection {
        Projection::Property { alias, .. } => alias.clone(),
        Projection::Key { alias, .. }
        | Projection::ElementId { alias, .. }
        | Projection::RelationshipType { alias, .. }
        | Projection::NodeLabels { alias, .. }
        | Projection::PropertyKeys { alias, .. }
        | Projection::Literal { alias, .. }
        | Projection::LiteralList { alias, .. }
        | Projection::Expression { alias, .. }
        | Projection::CountAll { alias }
        | Projection::Aggregate { alias, .. } => Some(alias.clone()),
    }
}

fn relationship_variable_for_field(field: &Field<'_, String>, index: usize) -> String {
    field.alias.as_ref().map_or_else(
        || format!("relationship{index}"),
        |alias| format!("{alias}_edge"),
    )
}

fn compile_root_argument(
    plan: &mut GraphPlan,
    graph: Option<&Declaration>,
    node: &NodeContext,
    name: &str,
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<(), CoreError> {
    let path = path.into();
    match name {
        "where" => {
            append_where_predicate(
                plan,
                compile_where_argument(GraphqlWhereScope::node(graph, node), value, path, context)?,
            );
            Ok(())
        }
        "orderBy" => {
            plan.order_by.extend(compile_order_by_argument(
                &node.variable,
                value,
                path,
                context,
            )?);
            Ok(())
        }
        "limit" | "first" => {
            plan.limit = Some(compile_non_negative_u64(value, path, name, context)?);
            Ok(())
        }
        "offset" | "skip" => {
            plan.skip = Some(compile_non_negative_u64(value, path, name, context)?);
            Ok(())
        }
        "distinct" => {
            plan.distinct = compile_boolean(value, path, "distinct", context)?;
            Ok(())
        }
        _ => Err(unsupported(
            path,
            format!("unsupported GraphQL root argument '{name}'"),
        )),
    }
}

fn compile_where_argument(
    scope: GraphqlWhereScope<'_>,
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    if let Value::Variable(variable) = value {
        let GraphqlVariableValue::Object(object) =
            context.parameter_value(variable, path.clone())?
        else {
            return Err(unsupported(
                path,
                format!("GraphQL variable '${variable}' must be an object"),
            ));
        };
        return compile_where_variable_object(scope, object, path, context);
    }
    let Value::Object(properties) = value else {
        return Err(unsupported(path, "GraphQL where must be an object"));
    };
    let mut expression = None;
    for (property, condition) in properties {
        let next = if let Some(operator) = graphql_boolean_operator(property) {
            compile_where_boolean_operator(
                scope,
                operator,
                condition,
                format!("{path}.{property}"),
                context,
            )?
        } else if is_graphql_relationship_filter_key(scope, property) {
            compile_relationship_existence_filter(
                scope,
                property,
                condition,
                format!("{path}.{property}"),
                context,
            )?
        } else {
            compile_where_property_conditions(
                scope.variable,
                property,
                condition,
                format!("{path}.{property}"),
                context,
            )?
        };
        expression = append_optional_and(expression, next);
    }
    Ok(expression)
}

fn compile_where_variable_object(
    scope: GraphqlWhereScope<'_>,
    object: &BTreeMap<String, GraphqlVariableValue>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    let mut expression = None;
    for (property, condition) in object {
        let next = if let Some(operator) = graphql_boolean_operator(property) {
            compile_where_variable_boolean_operator(
                scope,
                operator,
                condition,
                format!("{path}.{property}"),
                context,
            )?
        } else if is_graphql_relationship_filter_key(scope, property) {
            compile_relationship_existence_filter_variable(
                scope,
                property,
                condition,
                format!("{path}.{property}"),
                context,
            )?
        } else {
            let next_path = format!("{path}.{property}");
            match condition {
                GraphqlVariableValue::Object(condition_object) => {
                    compile_where_variable_property_conditions(
                        scope.variable,
                        property,
                        condition_object,
                        next_path,
                    )?
                }
                GraphqlVariableValue::Literal(literal) => Some(compile_where_shorthand_expression(
                    scope.variable,
                    property,
                    PredicateRhs::Literal(literal.clone()),
                    next_path,
                )?),
                GraphqlVariableValue::List(_) | GraphqlVariableValue::ObjectList(_) => {
                    return Err(unsupported(
                        next_path,
                        "GraphQL where property shorthand variables must be scalar literals or property condition objects",
                    ));
                }
            }
        };
        expression = append_optional_and(expression, next);
    }
    Ok(expression)
}

fn is_graphql_relationship_filter_key(scope: GraphqlWhereScope<'_>, name: &str) -> bool {
    let Some(graph) = scope.graph else {
        return false;
    };
    let Some(source_label) = scope.label else {
        return false;
    };
    let (direction, relationship_type) = if let Some(relationship_type) = name.strip_prefix("out_")
    {
        (Direction::Outgoing, relationship_type)
    } else if let Some(relationship_type) = name.strip_prefix("in_") {
        (Direction::Incoming, relationship_type)
    } else if let Some(relationship_type) = name.strip_prefix("any_") {
        (Direction::Undirected, relationship_type)
    } else {
        return false;
    };
    if relationship_type.is_empty() {
        return false;
    }
    graph
        .relationships_for_type(relationship_type)
        .any(|relationship| match direction {
            Direction::Outgoing => relationship.from.label == source_label,
            Direction::Incoming => relationship.to.label == source_label,
            Direction::Undirected => {
                relationship.from.label == source_label || relationship.to.label == source_label
            }
        })
}

fn compile_relationship_existence_filter(
    scope: GraphqlWhereScope<'_>,
    field_name: &str,
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    if let Value::Variable(variable) = value {
        return compile_relationship_existence_filter_variable(
            scope,
            field_name,
            context.parameter_value(variable, path.clone())?,
            path,
            context,
        );
    }
    let Value::Object(arguments) = value else {
        return Err(unsupported(
            path,
            "GraphQL relationship existence filters must be objects",
        ));
    };
    let graph = scope
        .graph
        .ok_or_else(|| CoreError::internal("relationship filter graph missing"))?;
    let source_label = scope
        .label
        .ok_or_else(|| CoreError::internal("relationship filter source label missing"))?;
    let (direction, relationship_type, endpoint_argument) =
        compile_relationship_field_name(field_name, format!("{path}.name"))?;
    let endpoint = RelationshipEndpointContext {
        graph,
        source_label,
        relationship_type: &relationship_type,
        direction,
        endpoint_argument,
    };

    let target_label =
        compile_relationship_filter_target_label(&endpoint, arguments, &path, context)?;
    let filter_context = RelationshipFilterContext {
        endpoint,
        target_label: &target_label,
        path: &path,
    };
    let (target_where, relationship_where) =
        compile_relationship_filter_predicates(&filter_context, arguments, context)?;
    Ok(Some(build_relationship_exists_predicate(
        scope.variable,
        direction,
        relationship_type,
        target_label,
        target_where,
        relationship_where,
    )))
}

fn compile_relationship_existence_filter_variable(
    scope: GraphqlWhereScope<'_>,
    field_name: &str,
    value: &GraphqlVariableValue,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    let GraphqlVariableValue::Object(arguments) = value else {
        return Err(unsupported(
            path,
            "GraphQL relationship existence filter variables must be objects",
        ));
    };
    let graph = scope
        .graph
        .ok_or_else(|| CoreError::internal("relationship filter graph missing"))?;
    let source_label = scope
        .label
        .ok_or_else(|| CoreError::internal("relationship filter source label missing"))?;
    let (direction, relationship_type, endpoint_argument) =
        compile_relationship_field_name(field_name, format!("{path}.name"))?;
    let endpoint = RelationshipEndpointContext {
        graph,
        source_label,
        relationship_type: &relationship_type,
        direction,
        endpoint_argument,
    };

    let target_label =
        compile_relationship_filter_variable_target_label(&endpoint, arguments, &path)?;
    let filter_context = RelationshipFilterContext {
        endpoint,
        target_label: &target_label,
        path: &path,
    };
    let (target_where, relationship_where) =
        compile_relationship_filter_variable_predicates(&filter_context, arguments, context)?;
    Ok(Some(build_relationship_exists_predicate(
        scope.variable,
        direction,
        relationship_type,
        target_label,
        target_where,
        relationship_where,
    )))
}

fn compile_relationship_filter_target_label(
    endpoint: &RelationshipEndpointContext<'_>,
    arguments: &BTreeMap<String, Value<'_, String>>,
    path: &str,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<String, CoreError> {
    let mut target_label = None;
    for (name, value) in arguments {
        let argument_path = format!("{path}.{name}");
        match name.as_str() {
            "to" | "from" | "label" => {
                if name != endpoint.endpoint_argument {
                    return Err(unsupported(
                        argument_path,
                        format!(
                            "GraphQL relationship filter '{}' requires '{}' instead of '{}'",
                            endpoint.relationship_type, endpoint.endpoint_argument, name
                        ),
                    ));
                }
                if target_label.is_some() {
                    return Err(unsupported(
                        argument_path,
                        format!(
                            "GraphQL relationship filter argument '{}' is duplicated",
                            endpoint.endpoint_argument
                        ),
                    ));
                }
                target_label = Some(compile_name_value(value, argument_path, context)?);
            }
            "where" | "relationshipWhere" => {}
            _ => {
                return Err(unsupported(
                    argument_path,
                    format!("unsupported GraphQL relationship filter argument '{name}'"),
                ));
            }
        }
    }
    match target_label {
        Some(target_label) => Ok(target_label),
        None => infer_relationship_target_label(
            endpoint,
            format!("{path}.{}", endpoint.endpoint_argument),
        ),
    }
}

fn compile_relationship_filter_variable_target_label(
    endpoint: &RelationshipEndpointContext<'_>,
    arguments: &BTreeMap<String, GraphqlVariableValue>,
    path: &str,
) -> Result<String, CoreError> {
    let mut target_label = None;
    for (name, value) in arguments {
        let argument_path = format!("{path}.{name}");
        match name.as_str() {
            "to" | "from" | "label" => {
                if name != endpoint.endpoint_argument {
                    return Err(unsupported(
                        argument_path,
                        format!(
                            "GraphQL relationship filter '{}' requires '{}' instead of '{}'",
                            endpoint.relationship_type, endpoint.endpoint_argument, name
                        ),
                    ));
                }
                if target_label.is_some() {
                    return Err(unsupported(
                        argument_path,
                        format!(
                            "GraphQL relationship filter argument '{}' is duplicated",
                            endpoint.endpoint_argument
                        ),
                    ));
                }
                target_label = Some(compile_variable_name_value(value, argument_path)?);
            }
            "where" | "relationshipWhere" => {}
            _ => {
                return Err(unsupported(
                    argument_path,
                    format!("unsupported GraphQL relationship filter argument '{name}'"),
                ));
            }
        }
    }
    match target_label {
        Some(target_label) => Ok(target_label),
        None => infer_relationship_target_label(
            endpoint,
            format!("{path}.{}", endpoint.endpoint_argument),
        ),
    }
}

fn compile_relationship_filter_predicates(
    filter: &RelationshipFilterContext<'_>,
    arguments: &BTreeMap<String, Value<'_, String>>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<(Option<PredicateExpression>, Option<PredicateExpression>), CoreError> {
    ensure_node_label(
        filter.endpoint.graph,
        filter.target_label,
        format!("{}.{}", filter.path, filter.endpoint.endpoint_argument),
    )?;
    ensure_relationship_mapping(
        filter.endpoint.graph,
        filter.endpoint.relationship_type,
        filter.endpoint.direction,
        filter.endpoint.source_label,
        filter.target_label,
        filter.path,
    )?;
    let target_variable = relationship_filter_target_variable(filter.target_label);
    let relationship_variable = relationship_filter_relationship_variable();
    let mut target_where = None;
    let mut relationship_where = None;
    for (name, value) in arguments {
        match name.as_str() {
            "where" => {
                target_where = compile_where_argument(
                    GraphqlWhereScope {
                        graph: Some(filter.endpoint.graph),
                        variable: &target_variable,
                        label: Some(filter.target_label),
                    },
                    value,
                    format!("{}.{}", filter.path, name),
                    context,
                )?;
            }
            "relationshipWhere" => {
                relationship_where = compile_where_argument(
                    GraphqlWhereScope::graph_variable(&relationship_variable),
                    value,
                    format!("{}.{}", filter.path, name),
                    context,
                )?;
            }
            "to" | "from" | "label" => {}
            _ => {
                unreachable!("relationship filter arguments validated before predicate compilation")
            }
        }
    }
    Ok((target_where, relationship_where))
}

fn compile_relationship_filter_variable_predicates(
    filter: &RelationshipFilterContext<'_>,
    arguments: &BTreeMap<String, GraphqlVariableValue>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<(Option<PredicateExpression>, Option<PredicateExpression>), CoreError> {
    ensure_node_label(
        filter.endpoint.graph,
        filter.target_label,
        format!("{}.{}", filter.path, filter.endpoint.endpoint_argument),
    )?;
    ensure_relationship_mapping(
        filter.endpoint.graph,
        filter.endpoint.relationship_type,
        filter.endpoint.direction,
        filter.endpoint.source_label,
        filter.target_label,
        filter.path,
    )?;
    let target_variable = relationship_filter_target_variable(filter.target_label);
    let relationship_variable = relationship_filter_relationship_variable();
    let mut target_where = None;
    let mut relationship_where = None;
    for (name, value) in arguments {
        match name.as_str() {
            "where" => {
                let GraphqlVariableValue::Object(object) = value else {
                    return Err(unsupported(
                        format!("{}.{}", filter.path, name),
                        "GraphQL relationship filter where must be an object",
                    ));
                };
                target_where = compile_where_variable_object(
                    GraphqlWhereScope {
                        graph: Some(filter.endpoint.graph),
                        variable: &target_variable,
                        label: Some(filter.target_label),
                    },
                    object,
                    format!("{}.{}", filter.path, name),
                    context,
                )?;
            }
            "relationshipWhere" => {
                let GraphqlVariableValue::Object(object) = value else {
                    return Err(unsupported(
                        format!("{}.{}", filter.path, name),
                        "GraphQL relationship filter relationshipWhere must be an object",
                    ));
                };
                relationship_where = compile_where_variable_object(
                    GraphqlWhereScope::graph_variable(&relationship_variable),
                    object,
                    format!("{}.{}", filter.path, name),
                    context,
                )?;
            }
            "to" | "from" | "label" => {}
            _ => {
                unreachable!("relationship filter arguments validated before predicate compilation")
            }
        }
    }
    Ok((target_where, relationship_where))
}

struct RelationshipFilterContext<'a> {
    endpoint: RelationshipEndpointContext<'a>,
    target_label: &'a str,
    path: &'a str,
}

fn build_relationship_exists_predicate(
    source_variable: &str,
    direction: Direction,
    relationship_type: String,
    target_label: String,
    target_where: Option<PredicateExpression>,
    relationship_where: Option<PredicateExpression>,
) -> PredicateExpression {
    let target_variable = relationship_filter_target_variable(&target_label);
    let relationship_variable = relationship_where
        .as_ref()
        .map(|_| relationship_filter_relationship_variable());
    let predicate = append_optional_and(target_where, relationship_where);
    PredicateExpression::ExistsPattern(ExistsPatternPredicate {
        nodes: vec![NodePattern {
            variable: target_variable.clone(),
            label: target_label,
        }],
        relationships: vec![RelationshipPattern {
            variable: relationship_variable,
            relationship_type,
            left: source_variable.to_string(),
            direction,
            right: target_variable,
        }],
        predicates: Vec::new(),
        predicate: predicate.map(Box::new),
    })
}

fn relationship_filter_target_variable(label: &str) -> String {
    format!("graphql_exists_{}", variable_for_label(label))
}

fn relationship_filter_relationship_variable() -> String {
    "graphql_exists_relationship".to_string()
}

#[derive(Debug, Clone, Copy)]
enum GraphqlBooleanOperator {
    And,
    Or,
    Xor,
    Not,
}

fn graphql_boolean_operator(name: &str) -> Option<GraphqlBooleanOperator> {
    match name {
        "and" | "AND" | "_and" => Some(GraphqlBooleanOperator::And),
        "or" | "OR" | "_or" => Some(GraphqlBooleanOperator::Or),
        "xor" | "XOR" | "_xor" => Some(GraphqlBooleanOperator::Xor),
        "not" | "NOT" | "_not" => Some(GraphqlBooleanOperator::Not),
        _ => None,
    }
}

fn compile_where_boolean_operator(
    scope: GraphqlWhereScope<'_>,
    operator: GraphqlBooleanOperator,
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    if let Value::Variable(variable) = value {
        return compile_where_variable_boolean_operator(
            scope,
            operator,
            context.parameter_value(variable, path.clone())?,
            path,
            context,
        );
    }
    match operator {
        GraphqlBooleanOperator::And | GraphqlBooleanOperator::Or => {
            let Value::List(items) = value else {
                return Err(unsupported(
                    path,
                    "GraphQL where and/or/xor operators must contain a list of objects",
                ));
            };
            if items.is_empty() {
                return Err(unsupported(
                    path,
                    "GraphQL where and/or operators require at least one object",
                ));
            }
            let mut expression = None;
            for (index, item) in items.iter().enumerate() {
                let next =
                    compile_where_argument(scope, item, format!("{path}[{index}]"), context)?;
                expression = match operator {
                    GraphqlBooleanOperator::And => append_optional_and(expression, next),
                    GraphqlBooleanOperator::Or => append_optional_or(expression, next),
                    GraphqlBooleanOperator::Xor => unreachable!("XOR is handled separately"),
                    GraphqlBooleanOperator::Not => unreachable!("NOT is handled separately"),
                };
            }
            expression
                .map(Some)
                .ok_or_else(|| unsupported(path, "GraphQL where boolean list was empty"))
        }
        GraphqlBooleanOperator::Xor => {
            let Value::List(items) = value else {
                return Err(unsupported(
                    path,
                    "GraphQL where and/or/xor operators must contain a list of objects",
                ));
            };
            let [left_item, right_item] = items.as_slice() else {
                return Err(unsupported(
                    path,
                    "GraphQL where xor operator requires exactly two objects",
                ));
            };
            let left = compile_where_argument(scope, left_item, format!("{path}[0]"), context)?
                .ok_or_else(|| {
                    unsupported(
                        format!("{path}[0]"),
                        "GraphQL where xor operands must not be empty",
                    )
                })?;
            let right = compile_where_argument(scope, right_item, format!("{path}[1]"), context)?
                .ok_or_else(|| {
                unsupported(
                    format!("{path}[1]"),
                    "GraphQL where xor operands must not be empty",
                )
            })?;
            Ok(Some(PredicateExpression::Xor {
                left: Box::new(left),
                right: Box::new(right),
            }))
        }
        GraphqlBooleanOperator::Not => {
            let expression = compile_where_argument(scope, value, path.clone(), context)?
                .ok_or_else(|| unsupported(path, "GraphQL where not requires an object"))?;
            Ok(Some(PredicateExpression::Not {
                expression: Box::new(expression),
            }))
        }
    }
}

fn compile_where_variable_boolean_operator(
    scope: GraphqlWhereScope<'_>,
    operator: GraphqlBooleanOperator,
    value: &GraphqlVariableValue,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    match operator {
        GraphqlBooleanOperator::And | GraphqlBooleanOperator::Or => {
            let GraphqlVariableValue::ObjectList(items) = value else {
                return Err(unsupported(
                    path,
                    "GraphQL where and/or/xor operators must contain a list of objects",
                ));
            };
            if items.is_empty() {
                return Err(unsupported(
                    path,
                    "GraphQL where and/or operators require at least one object",
                ));
            }
            let mut expression = None;
            for (index, item) in items.iter().enumerate() {
                let next = compile_where_variable_object(
                    scope,
                    item,
                    format!("{path}[{index}]"),
                    context,
                )?;
                expression = match operator {
                    GraphqlBooleanOperator::And => append_optional_and(expression, next),
                    GraphqlBooleanOperator::Or => append_optional_or(expression, next),
                    GraphqlBooleanOperator::Xor => unreachable!("XOR is handled separately"),
                    GraphqlBooleanOperator::Not => unreachable!("NOT is handled separately"),
                };
            }
            expression
                .map(Some)
                .ok_or_else(|| unsupported(path, "GraphQL where boolean list was empty"))
        }
        GraphqlBooleanOperator::Xor => {
            let GraphqlVariableValue::ObjectList(items) = value else {
                return Err(unsupported(
                    path,
                    "GraphQL where and/or/xor operators must contain a list of objects",
                ));
            };
            let [left_item, right_item] = items.as_slice() else {
                return Err(unsupported(
                    path,
                    "GraphQL where xor operator requires exactly two objects",
                ));
            };
            let left =
                compile_where_variable_object(scope, left_item, format!("{path}[0]"), context)?
                    .ok_or_else(|| {
                        unsupported(
                            format!("{path}[0]"),
                            "GraphQL where xor operands must not be empty",
                        )
                    })?;
            let right =
                compile_where_variable_object(scope, right_item, format!("{path}[1]"), context)?
                    .ok_or_else(|| {
                        unsupported(
                            format!("{path}[1]"),
                            "GraphQL where xor operands must not be empty",
                        )
                    })?;
            Ok(Some(PredicateExpression::Xor {
                left: Box::new(left),
                right: Box::new(right),
            }))
        }
        GraphqlBooleanOperator::Not => {
            let GraphqlVariableValue::Object(object) = value else {
                return Err(unsupported(path, "GraphQL where not requires an object"));
            };
            let expression =
                compile_where_variable_object(scope, object, path.clone(), context)?
                    .ok_or_else(|| unsupported(path, "GraphQL where not requires an object"))?;
            Ok(Some(PredicateExpression::Not {
                expression: Box::new(expression),
            }))
        }
    }
}

fn compile_where_property_conditions(
    graph_variable: &str,
    property: &str,
    condition: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    if let Value::Variable(variable) = condition {
        return match context.parameter_value(variable, path.clone())? {
            GraphqlVariableValue::Object(object) => {
                compile_where_variable_property_conditions(graph_variable, property, object, path)
            }
            GraphqlVariableValue::Literal(literal) => Ok(Some(compile_where_shorthand_expression(
                graph_variable,
                property,
                PredicateRhs::Literal(literal.clone()),
                path,
            )?)),
            GraphqlVariableValue::List(_) | GraphqlVariableValue::ObjectList(_) => {
                Err(unsupported(
                    path,
                    format!(
                        "GraphQL variable '${variable}' must be a scalar literal or property condition object"
                    ),
                ))
            }
        };
    }
    let Value::Object(operators) = condition else {
        return Ok(Some(compile_where_shorthand_expression(
            graph_variable,
            property,
            PredicateRhs::Literal(compile_literal(condition, path.clone(), context)?),
            path,
        )?));
    };
    let mut expression = None;
    for (operator, value) in operators {
        let predicate = compile_where_operator_expression(
            graph_variable,
            property,
            operator,
            value,
            format!("{path}.{operator}"),
            context,
        )?;
        expression = append_optional_and(expression, Some(predicate));
    }
    Ok(expression)
}

fn compile_where_variable_property_conditions(
    graph_variable: &str,
    property: &str,
    operators: &BTreeMap<String, GraphqlVariableValue>,
    path: impl Into<String>,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    let mut expression = None;
    for (operator, value) in operators {
        let predicate = compile_where_variable_operator_expression(
            graph_variable,
            property,
            operator,
            value,
            format!("{path}.{operator}"),
        )?;
        expression = append_optional_and(expression, Some(predicate));
    }
    Ok(expression)
}

fn append_where_predicate(plan: &mut GraphPlan, expression: Option<PredicateExpression>) {
    let Some(expression) = expression else {
        return;
    };
    if is_conjunctive_property_expression(&expression) {
        append_conjunctive_property_expression(expression, &mut plan.predicates);
    } else {
        plan.predicate = Some(match plan.predicate.take() {
            Some(existing) => PredicateExpression::And {
                left: Box::new(existing),
                right: Box::new(expression),
            },
            None => expression,
        });
    }
}

fn append_optional_and(
    expression: Option<PredicateExpression>,
    next: Option<PredicateExpression>,
) -> Option<PredicateExpression> {
    match (expression, next) {
        (Some(left), Some(right)) => Some(PredicateExpression::And {
            left: Box::new(left),
            right: Box::new(right),
        }),
        (Some(expression), None) | (None, Some(expression)) => Some(expression),
        (None, None) => None,
    }
}

fn append_optional_or(
    expression: Option<PredicateExpression>,
    next: Option<PredicateExpression>,
) -> Option<PredicateExpression> {
    match (expression, next) {
        (Some(left), Some(right)) => Some(PredicateExpression::Or {
            left: Box::new(left),
            right: Box::new(right),
        }),
        (Some(expression), None) | (None, Some(expression)) => Some(expression),
        (None, None) => None,
    }
}

fn is_conjunctive_property_expression(expression: &PredicateExpression) -> bool {
    match expression {
        PredicateExpression::Comparison(_) => true,
        PredicateExpression::And { left, right } => {
            is_conjunctive_property_expression(left) && is_conjunctive_property_expression(right)
        }
        PredicateExpression::Boolean(_)
        | PredicateExpression::KeyComparison(_)
        | PredicateExpression::ElementIdComparison(_)
        | PredicateExpression::Presence(_)
        | PredicateExpression::PropertyKeyMembership(_)
        | PredicateExpression::ExistsPattern(_)
        | PredicateExpression::ScalarComparison(_)
        | PredicateExpression::Or { .. }
        | PredicateExpression::Xor { .. }
        | PredicateExpression::Not { .. } => false,
    }
}

fn append_conjunctive_property_expression(
    expression: PredicateExpression,
    predicates: &mut Vec<PropertyPredicate>,
) {
    match expression {
        PredicateExpression::Comparison(predicate) => predicates.push(predicate),
        PredicateExpression::And { left, right } => {
            append_conjunctive_property_expression(*left, predicates);
            append_conjunctive_property_expression(*right, predicates);
        }
        PredicateExpression::Boolean(_)
        | PredicateExpression::KeyComparison(_)
        | PredicateExpression::ElementIdComparison(_)
        | PredicateExpression::Presence(_)
        | PredicateExpression::PropertyKeyMembership(_)
        | PredicateExpression::ExistsPattern(_)
        | PredicateExpression::ScalarComparison(_)
        | PredicateExpression::Or { .. }
        | PredicateExpression::Xor { .. }
        | PredicateExpression::Not { .. } => {
            unreachable!("non-conjunctive GraphQL predicate reached conjunctive appender")
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GraphqlWhereOperator {
    Comparison(ComparisonOperator),
    RegexMatch,
    In,
    IsNull,
    IsNotNull,
    NegatedComparison(ComparisonOperator),
    NegatedRegexMatch,
    NotIn,
}

fn classify_graphql_where_operator(operator: &str) -> Option<GraphqlWhereOperator> {
    match operator {
        "eq" | "equals" => Some(GraphqlWhereOperator::Comparison(ComparisonOperator::Equal)),
        "ne" | "neq" | "notEq" | "notEqual" | "notEquals" => Some(
            GraphqlWhereOperator::Comparison(ComparisonOperator::NotEqual),
        ),
        "gt" | "greaterThan" => Some(GraphqlWhereOperator::Comparison(
            ComparisonOperator::GreaterThan,
        )),
        "gte" | "ge" | "greaterThanEqual" | "greaterThanOrEqual" => Some(
            GraphqlWhereOperator::Comparison(ComparisonOperator::GreaterThanOrEqual),
        ),
        "lt" | "lessThan" => Some(GraphqlWhereOperator::Comparison(
            ComparisonOperator::LessThan,
        )),
        "lte" | "le" | "lessThanEqual" | "lessThanOrEqual" => Some(
            GraphqlWhereOperator::Comparison(ComparisonOperator::LessThanOrEqual),
        ),
        "startsWith" | "starts_with" => Some(GraphqlWhereOperator::Comparison(
            ComparisonOperator::StartsWith,
        )),
        "endsWith" | "ends_with" => Some(GraphqlWhereOperator::Comparison(
            ComparisonOperator::EndsWith,
        )),
        "contains" => Some(GraphqlWhereOperator::Comparison(
            ComparisonOperator::Contains,
        )),
        "notStartsWith" | "not_starts_with" => Some(GraphqlWhereOperator::NegatedComparison(
            ComparisonOperator::StartsWith,
        )),
        "notEndsWith" | "not_ends_with" => Some(GraphqlWhereOperator::NegatedComparison(
            ComparisonOperator::EndsWith,
        )),
        "notContains" | "not_contains" => Some(GraphqlWhereOperator::NegatedComparison(
            ComparisonOperator::Contains,
        )),
        "matches" | "regex" => Some(GraphqlWhereOperator::RegexMatch),
        "notMatches" | "notRegex" | "not_regex" => Some(GraphqlWhereOperator::NegatedRegexMatch),
        "in" => Some(GraphqlWhereOperator::In),
        "notIn" | "not_in" => Some(GraphqlWhereOperator::NotIn),
        "isNull" | "is_null" => Some(GraphqlWhereOperator::IsNull),
        "isNotNull" | "is_not_null" => Some(GraphqlWhereOperator::IsNotNull),
        _ => None,
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum GraphqlWhereTarget {
    Property(PropertyRef),
    Key { variable: String },
    ElementId { variable: String },
}

fn graphql_where_target(variable: &str, property: &str) -> GraphqlWhereTarget {
    match property {
        "_id" => GraphqlWhereTarget::Key {
            variable: variable.to_string(),
        },
        "_elementId" => GraphqlWhereTarget::ElementId {
            variable: variable.to_string(),
        },
        _ => GraphqlWhereTarget::Property(PropertyRef {
            variable: variable.to_string(),
            property: property.to_string(),
        }),
    }
}

fn comparison_expression(
    target: GraphqlWhereTarget,
    operator: ComparisonOperator,
    rhs: PredicateRhs,
    path: &str,
) -> Result<PredicateExpression, CoreError> {
    if matches!(target, GraphqlWhereTarget::Key { .. })
        && matches!(
            operator,
            ComparisonOperator::StartsWith
                | ComparisonOperator::EndsWith
                | ComparisonOperator::Contains
                | ComparisonOperator::RegexMatch
        )
    {
        return Err(unsupported(
            path,
            "GraphQL _id filters do not support string predicates; use _elementId for string identity filters",
        ));
    }
    Ok(match target {
        GraphqlWhereTarget::Property(property) => {
            PredicateExpression::Comparison(PropertyPredicate {
                property,
                operator,
                rhs,
            })
        }
        GraphqlWhereTarget::Key { variable } => PredicateExpression::KeyComparison(KeyPredicate {
            variable,
            operator,
            rhs,
        }),
        GraphqlWhereTarget::ElementId { variable } => {
            PredicateExpression::ElementIdComparison(ElementIdPredicate {
                variable,
                operator,
                rhs,
            })
        }
    })
}

fn negated_comparison_expression(
    target: GraphqlWhereTarget,
    operator: ComparisonOperator,
    rhs: PredicateRhs,
    path: &str,
) -> Result<PredicateExpression, CoreError> {
    Ok(PredicateExpression::Not {
        expression: Box::new(comparison_expression(target, operator, rhs, path)?),
    })
}

fn compile_where_shorthand_expression(
    variable: &str,
    property: &str,
    rhs: PredicateRhs,
    path: impl Into<String>,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    comparison_expression(
        graphql_where_target(variable, property),
        ComparisonOperator::Equal,
        rhs,
        &path,
    )
}

fn compile_where_operator_expression(
    variable: &str,
    property: &str,
    operator: &str,
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    let target = graphql_where_target(variable, property);
    match classify_graphql_where_operator(operator) {
        Some(GraphqlWhereOperator::Comparison(operator)) => Ok(comparison_expression(
            target,
            operator,
            PredicateRhs::Literal(compile_literal(value, path.clone(), context)?),
            &path,
        )?),
        Some(GraphqlWhereOperator::RegexMatch) => Ok(comparison_expression(
            target,
            ComparisonOperator::RegexMatch,
            PredicateRhs::Literal(compile_regex_literal(value, path.clone(), context)?),
            &path,
        )?),
        Some(GraphqlWhereOperator::In) => Ok(comparison_expression(
            target,
            ComparisonOperator::In,
            PredicateRhs::List(compile_literal_list(value, path.clone(), context)?),
            &path,
        )?),
        Some(GraphqlWhereOperator::IsNull) => {
            let is_null = compile_boolean(value, path.clone(), "isNull", context)?;
            Ok(comparison_expression(
                target,
                if is_null {
                    ComparisonOperator::Equal
                } else {
                    ComparisonOperator::NotEqual
                },
                PredicateRhs::Literal(Literal::Null),
                &path,
            )?)
        }
        Some(GraphqlWhereOperator::IsNotNull) => {
            let is_not_null = compile_boolean(value, path.clone(), "isNotNull", context)?;
            Ok(comparison_expression(
                target,
                if is_not_null {
                    ComparisonOperator::NotEqual
                } else {
                    ComparisonOperator::Equal
                },
                PredicateRhs::Literal(Literal::Null),
                &path,
            )?)
        }
        Some(GraphqlWhereOperator::NegatedComparison(operator)) => {
            Ok(negated_comparison_expression(
                target,
                operator,
                PredicateRhs::Literal(compile_literal(value, path.clone(), context)?),
                &path,
            )?)
        }
        Some(GraphqlWhereOperator::NegatedRegexMatch) => Ok(negated_comparison_expression(
            target,
            ComparisonOperator::RegexMatch,
            PredicateRhs::Literal(compile_regex_literal(value, path.clone(), context)?),
            &path,
        )?),
        Some(GraphqlWhereOperator::NotIn) => Ok(negated_comparison_expression(
            target,
            ComparisonOperator::In,
            PredicateRhs::List(compile_literal_list(value, path.clone(), context)?),
            &path,
        )?),
        None => Err(unsupported(
            path,
            format!("unsupported GraphQL where operator '{operator}'"),
        )),
    }
}

fn compile_where_variable_operator_expression(
    variable: &str,
    property: &str,
    operator: &str,
    value: &GraphqlVariableValue,
    path: impl Into<String>,
) -> Result<PredicateExpression, CoreError> {
    let path = path.into();
    let target = graphql_where_target(variable, property);
    match classify_graphql_where_operator(operator) {
        Some(GraphqlWhereOperator::Comparison(operator)) => Ok(comparison_expression(
            target,
            operator,
            PredicateRhs::Literal(compile_variable_literal(value, path.clone())?),
            &path,
        )?),
        Some(GraphqlWhereOperator::RegexMatch) => Ok(comparison_expression(
            target,
            ComparisonOperator::RegexMatch,
            PredicateRhs::Literal(compile_variable_regex_literal(value, path.clone())?),
            &path,
        )?),
        Some(GraphqlWhereOperator::In) => Ok(comparison_expression(
            target,
            ComparisonOperator::In,
            PredicateRhs::List(compile_variable_literal_list(value, path.clone())?),
            &path,
        )?),
        Some(GraphqlWhereOperator::IsNull) => {
            let is_null = compile_variable_boolean(value, path.clone(), "isNull")?;
            Ok(comparison_expression(
                target,
                if is_null {
                    ComparisonOperator::Equal
                } else {
                    ComparisonOperator::NotEqual
                },
                PredicateRhs::Literal(Literal::Null),
                &path,
            )?)
        }
        Some(GraphqlWhereOperator::IsNotNull) => {
            let is_not_null = compile_variable_boolean(value, path.clone(), "isNotNull")?;
            Ok(comparison_expression(
                target,
                if is_not_null {
                    ComparisonOperator::NotEqual
                } else {
                    ComparisonOperator::Equal
                },
                PredicateRhs::Literal(Literal::Null),
                &path,
            )?)
        }
        Some(GraphqlWhereOperator::NegatedComparison(operator)) => {
            Ok(negated_comparison_expression(
                target,
                operator,
                PredicateRhs::Literal(compile_variable_literal(value, path.clone())?),
                &path,
            )?)
        }
        Some(GraphqlWhereOperator::NegatedRegexMatch) => Ok(negated_comparison_expression(
            target,
            ComparisonOperator::RegexMatch,
            PredicateRhs::Literal(compile_variable_regex_literal(value, path.clone())?),
            &path,
        )?),
        Some(GraphqlWhereOperator::NotIn) => Ok(negated_comparison_expression(
            target,
            ComparisonOperator::In,
            PredicateRhs::List(compile_variable_literal_list(value, path.clone())?),
            &path,
        )?),
        None => Err(unsupported(
            path,
            format!("unsupported GraphQL where operator '{operator}'"),
        )),
    }
}

fn compile_order_by_argument(
    variable: &str,
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Vec<OrderKey>, CoreError> {
    let path = path.into();
    match value {
        Value::Variable(graphql_variable) => match context
            .parameter_value(graphql_variable, path.clone())?
        {
            GraphqlVariableValue::Object(object) => {
                compile_order_by_variable_object(variable, object, path)
            }
            GraphqlVariableValue::ObjectList(items) => {
                let mut order_keys = Vec::with_capacity(items.len());
                for (index, object) in items.iter().enumerate() {
                    order_keys.extend(compile_order_by_variable_object(
                        variable,
                        object,
                        format!("{path}[{index}]"),
                    )?);
                }
                Ok(order_keys)
            }
            GraphqlVariableValue::List(values) if values.is_empty() => Ok(Vec::new()),
            GraphqlVariableValue::Literal(_) | GraphqlVariableValue::List(_) => Err(unsupported(
                path,
                format!(
                    "GraphQL variable '${graphql_variable}' must be an orderBy object or list of objects"
                ),
            )),
        },
        Value::Object(_) => compile_order_by_object(variable, value, path, context),
        Value::List(items) => {
            let mut order_keys = Vec::with_capacity(items.len());
            for (index, value) in items.iter().enumerate() {
                order_keys.extend(compile_order_by_object(
                    variable,
                    value,
                    format!("{path}[{index}]"),
                    context,
                )?);
            }
            Ok(order_keys)
        }
        _ => Err(unsupported(
            path,
            "GraphQL orderBy must be an object or list of objects",
        )),
    }
}

fn compile_order_by_object(
    variable: &str,
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Vec<OrderKey>, CoreError> {
    let path = path.into();
    let Value::Object(object) = value else {
        return Err(unsupported(path, "GraphQL orderBy entries must be objects"));
    };
    if !object.contains_key("field")
        && !object.contains_key("direction")
        && !object.contains_key("nulls")
    {
        return compile_order_by_shorthand_object(variable, object, path, context);
    }
    for name in object.keys() {
        if name != "field" && name != "direction" && name != "nulls" {
            return Err(unsupported(
                format!("{path}.{name}"),
                format!("unsupported GraphQL orderBy key '{name}'"),
            ));
        }
    }
    let field_value = object
        .get("field")
        .ok_or_else(|| unsupported(format!("{path}.field"), "GraphQL orderBy requires field"))?;
    let field = compile_name_value(field_value, format!("{path}.field"), context)?;
    let direction = object
        .get("direction")
        .map_or(Ok(OrderDirection::Ascending), |value| {
            compile_order_direction(value, format!("{path}.direction"), context)
        })?;
    let nulls = object
        .get("nulls")
        .map(|value| compile_null_order(value, format!("{path}.nulls"), context))
        .transpose()?;
    Ok(vec![OrderKey {
        expression: graphql_order_expression(variable, &field),
        direction,
        nulls,
    }])
}

fn compile_order_by_shorthand_object(
    variable: &str,
    object: &BTreeMap<String, Value<'_, String>>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Vec<OrderKey>, CoreError> {
    let path = path.into();
    if object.len() != 1 {
        return Err(unsupported(
            path,
            "GraphQL shorthand orderBy entries must contain exactly one field",
        ));
    }
    let (field, direction_value) = object
        .iter()
        .next()
        .expect("shorthand orderBy object length was checked");
    Ok(vec![OrderKey {
        expression: graphql_order_expression(variable, field),
        direction: compile_order_direction(direction_value, format!("{path}.{field}"), context)?,
        nulls: None,
    }])
}

fn compile_order_by_variable_object(
    variable: &str,
    object: &BTreeMap<String, GraphqlVariableValue>,
    path: impl Into<String>,
) -> Result<Vec<OrderKey>, CoreError> {
    let path = path.into();
    if !object.contains_key("field")
        && !object.contains_key("direction")
        && !object.contains_key("nulls")
    {
        return compile_order_by_variable_shorthand_object(variable, object, path);
    }
    for name in object.keys() {
        if name != "field" && name != "direction" && name != "nulls" {
            return Err(unsupported(
                format!("{path}.{name}"),
                format!("unsupported GraphQL orderBy key '{name}'"),
            ));
        }
    }
    let field_value = object
        .get("field")
        .ok_or_else(|| unsupported(format!("{path}.field"), "GraphQL orderBy requires field"))?;
    let field = compile_variable_name_value(field_value, format!("{path}.field"))?;
    let direction = object
        .get("direction")
        .map_or(Ok(OrderDirection::Ascending), |value| {
            compile_variable_order_direction(value, format!("{path}.direction"))
        })?;
    let nulls = object
        .get("nulls")
        .map(|value| compile_variable_null_order(value, format!("{path}.nulls")))
        .transpose()?;
    Ok(vec![OrderKey {
        expression: graphql_order_expression(variable, &field),
        direction,
        nulls,
    }])
}

fn compile_order_by_variable_shorthand_object(
    variable: &str,
    object: &BTreeMap<String, GraphqlVariableValue>,
    path: impl Into<String>,
) -> Result<Vec<OrderKey>, CoreError> {
    let path = path.into();
    if object.len() != 1 {
        return Err(unsupported(
            path,
            "GraphQL shorthand orderBy variable entries must contain exactly one field",
        ));
    }
    let (field, direction_value) = object
        .iter()
        .next()
        .expect("shorthand orderBy variable object length was checked");
    Ok(vec![OrderKey {
        expression: graphql_order_expression(variable, field),
        direction: compile_variable_order_direction(direction_value, format!("{path}.{field}"))?,
        nulls: None,
    }])
}

fn graphql_order_expression(variable: &str, field: &str) -> OrderExpression {
    match field {
        "_id" => OrderExpression::Key {
            variable: variable.to_string(),
        },
        "_elementId" => OrderExpression::ElementId {
            variable: variable.to_string(),
        },
        _ => OrderExpression::Property(PropertyRef {
            variable: variable.to_string(),
            property: field.to_string(),
        }),
    }
}

fn compile_order_direction(
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<OrderDirection, CoreError> {
    let path = path.into();
    let direction = compile_name_value(value, path.clone(), context)?;
    compile_order_direction_name(&direction, path)
}

fn compile_null_order(
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<NullOrder, CoreError> {
    let path = path.into();
    let nulls = compile_name_value(value, path.clone(), context)?;
    compile_null_order_name(&nulls, path)
}

fn compile_variable_literal(
    value: &GraphqlVariableValue,
    path: impl Into<String>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    match value {
        GraphqlVariableValue::Literal(value) => Ok(value.clone()),
        GraphqlVariableValue::List(_)
        | GraphqlVariableValue::Object(_)
        | GraphqlVariableValue::ObjectList(_) => Err(unsupported(
            path,
            "GraphQL variable value must be a scalar literal",
        )),
    }
}

fn compile_variable_literal_list(
    value: &GraphqlVariableValue,
    path: impl Into<String>,
) -> Result<Vec<Literal>, CoreError> {
    let path = path.into();
    match value {
        GraphqlVariableValue::List(values) => Ok(values.clone()),
        GraphqlVariableValue::Literal(_)
        | GraphqlVariableValue::Object(_)
        | GraphqlVariableValue::ObjectList(_) => Err(unsupported(
            path,
            "GraphQL variable value must be a scalar-list literal",
        )),
    }
}

fn compile_variable_regex_literal(
    value: &GraphqlVariableValue,
    path: impl Into<String>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let literal = compile_variable_literal(value, path.clone())?;
    let Literal::String(pattern) = &literal else {
        return Err(unsupported(
            path,
            "GraphQL regex filters require a string literal",
        ));
    };
    Regex::new(pattern)
        .map_err(|error| unsupported(path, format!("invalid GraphQL regex literal: {error}")))?;
    Ok(literal)
}

fn compile_variable_boolean(
    value: &GraphqlVariableValue,
    path: impl Into<String>,
    name: &str,
) -> Result<bool, CoreError> {
    let path = path.into();
    let GraphqlVariableValue::Literal(Literal::Boolean(value)) = value else {
        return Err(unsupported(
            path,
            format!("GraphQL variable value for {name} must be a boolean"),
        ));
    };
    Ok(*value)
}

fn compile_variable_name_value(
    value: &GraphqlVariableValue,
    path: impl Into<String>,
) -> Result<String, CoreError> {
    let path = path.into();
    match value {
        GraphqlVariableValue::Literal(Literal::String(value)) => Ok(value.clone()),
        GraphqlVariableValue::Literal(_)
        | GraphqlVariableValue::List(_)
        | GraphqlVariableValue::Object(_)
        | GraphqlVariableValue::ObjectList(_) => Err(unsupported(
            path,
            "GraphQL variable value must be a string or enum name",
        )),
    }
}

fn compile_variable_order_direction(
    value: &GraphqlVariableValue,
    path: impl Into<String>,
) -> Result<OrderDirection, CoreError> {
    let path = path.into();
    let direction = compile_variable_name_value(value, path.clone())?;
    compile_order_direction_name(&direction, path)
}

fn compile_variable_null_order(
    value: &GraphqlVariableValue,
    path: impl Into<String>,
) -> Result<NullOrder, CoreError> {
    let path = path.into();
    let nulls = compile_variable_name_value(value, path.clone())?;
    compile_null_order_name(&nulls, path)
}

fn compile_order_direction_name(
    direction: &str,
    path: impl Into<String>,
) -> Result<OrderDirection, CoreError> {
    let path = path.into();
    if direction.eq_ignore_ascii_case("ASC") || direction.eq_ignore_ascii_case("ASCENDING") {
        return Ok(OrderDirection::Ascending);
    }
    if direction.eq_ignore_ascii_case("DESC") || direction.eq_ignore_ascii_case("DESCENDING") {
        return Ok(OrderDirection::Descending);
    }
    Err(unsupported(
        path,
        "GraphQL orderBy direction must be ASC, ASCENDING, DESC, or DESCENDING",
    ))
}

fn compile_null_order_name(nulls: &str, path: impl Into<String>) -> Result<NullOrder, CoreError> {
    let path = path.into();
    if nulls.eq_ignore_ascii_case("FIRST") || nulls.eq_ignore_ascii_case("NULLS_FIRST") {
        return Ok(NullOrder::First);
    }
    if nulls.eq_ignore_ascii_case("LAST") || nulls.eq_ignore_ascii_case("NULLS_LAST") {
        return Ok(NullOrder::Last);
    }
    Err(unsupported(
        path,
        "GraphQL orderBy nulls must be FIRST, LAST, NULLS_FIRST, or NULLS_LAST",
    ))
}

fn compile_literal(
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    match value {
        Value::Int(number) => number
            .as_i64()
            .map(Literal::Integer)
            .ok_or_else(|| unsupported(path, "GraphQL integer literal is out of range")),
        Value::Float(value) if value.is_finite() => Ok(Literal::Float(OrderedFloat(*value))),
        Value::Float(_) => Err(unsupported(
            path,
            "GraphQL float literals must be finite numbers",
        )),
        Value::String(value) => Ok(Literal::String(value.clone())),
        Value::Boolean(value) => Ok(Literal::Boolean(*value)),
        Value::Null => Ok(Literal::Null),
        Value::Variable(variable) => match context.parameter_value(variable, path.clone())? {
            GraphqlVariableValue::Literal(value) => Ok(value.clone()),
            GraphqlVariableValue::List(_)
            | GraphqlVariableValue::Object(_)
            | GraphqlVariableValue::ObjectList(_) => Err(unsupported(
                path,
                format!("GraphQL variable '${variable}' must be a scalar literal"),
            )),
        },
        Value::Enum(_) | Value::List(_) | Value::Object(_) => {
            Err(unsupported(path, "GraphQL value must be a scalar literal"))
        }
    }
}

fn compile_variable_default_value(
    value: &Value<'_, String>,
    path: impl Into<String>,
) -> Result<GraphqlVariableValue, CoreError> {
    let path = path.into();
    match value {
        Value::List(items) => compile_variable_default_list(items, path),
        Value::Object(object) => Ok(GraphqlVariableValue::Object(
            compile_variable_default_object(object, path)?,
        )),
        _ => Ok(GraphqlVariableValue::Literal(
            compile_variable_default_literal(value, path)?,
        )),
    }
}

fn compile_variable_default_object(
    object: &BTreeMap<String, Value<'_, String>>,
    path: impl Into<String>,
) -> Result<BTreeMap<String, GraphqlVariableValue>, CoreError> {
    let path = path.into();
    object
        .iter()
        .map(|(key, value)| {
            Ok((
                key.clone(),
                compile_variable_default_value(value, format!("{path}.{key}"))?,
            ))
        })
        .collect()
}

fn compile_variable_default_list(
    items: &[Value<'_, String>],
    path: impl Into<String>,
) -> Result<GraphqlVariableValue, CoreError> {
    let path = path.into();
    if items.iter().any(|item| matches!(item, Value::Object(_))) {
        let mut objects = Vec::with_capacity(items.len());
        for (index, item) in items.iter().enumerate() {
            let Value::Object(object) = item else {
                return Err(unsupported(
                    format!("{path}[{index}]"),
                    "GraphQL default lists cannot mix object and scalar values",
                ));
            };
            objects.push(compile_variable_default_object(
                object,
                format!("{path}[{index}]"),
            )?);
        }
        return Ok(GraphqlVariableValue::ObjectList(objects));
    }

    let mut literals = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        literals.push(compile_variable_default_literal(
            item,
            format!("{path}[{index}]"),
        )?);
    }
    Ok(GraphqlVariableValue::List(literals))
}

fn compile_variable_default_literal(
    value: &Value<'_, String>,
    path: impl Into<String>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    match value {
        Value::Int(number) => number
            .as_i64()
            .map(Literal::Integer)
            .ok_or_else(|| unsupported(path, "GraphQL default integer is out of range")),
        Value::Float(value) if value.is_finite() => Ok(Literal::Float(OrderedFloat(*value))),
        Value::Float(_) => Err(unsupported(
            path,
            "GraphQL default float values must be finite numbers",
        )),
        Value::String(value) | Value::Enum(value) => Ok(Literal::String(value.clone())),
        Value::Boolean(value) => Ok(Literal::Boolean(*value)),
        Value::Null => Ok(Literal::Null),
        Value::Variable(_) => Err(unsupported(
            path,
            "GraphQL variable defaults cannot reference other variables",
        )),
        Value::List(_) => Err(unsupported(
            path,
            "nested GraphQL variable default lists are not supported yet",
        )),
        Value::Object(_) => Err(unsupported(path, "GraphQL default value must be scalar")),
    }
}

fn compile_regex_literal(
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Literal, CoreError> {
    let path = path.into();
    let literal = compile_literal(value, path.clone(), context)?;
    let Literal::String(pattern) = &literal else {
        return Err(unsupported(
            path,
            "GraphQL regex filters require a string literal",
        ));
    };
    Regex::new(pattern)
        .map_err(|error| unsupported(path, format!("invalid GraphQL regex literal: {error}")))?;
    Ok(literal)
}

fn compile_literal_list(
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Vec<Literal>, CoreError> {
    let path = path.into();
    if let Value::Variable(variable) = value {
        return match context.parameter_value(variable, path.clone())? {
            GraphqlVariableValue::List(values) => Ok(values.clone()),
            GraphqlVariableValue::Literal(_)
            | GraphqlVariableValue::Object(_)
            | GraphqlVariableValue::ObjectList(_) => Err(unsupported(
                path,
                format!("GraphQL variable '${variable}' must be a scalar-list literal"),
            )),
        };
    }
    let Value::List(items) = value else {
        return Err(unsupported(
            path,
            "GraphQL IN values must be a literal list",
        ));
    };
    items
        .iter()
        .enumerate()
        .map(|(index, value)| compile_literal(value, format!("{path}[{index}]"), context))
        .collect()
}

fn compile_non_negative_u64(
    value: &Value<'_, String>,
    path: impl Into<String>,
    name: &str,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<u64, CoreError> {
    let path = path.into();
    if let Value::Variable(variable) = value {
        let GraphqlVariableValue::Literal(Literal::Integer(value)) =
            context.parameter_value(variable, path.clone())?
        else {
            return Err(unsupported(
                path,
                format!("GraphQL variable '${variable}' must be a non-negative integer"),
            ));
        };
        return u64::try_from(*value).map_err(|error| {
            unsupported(
                path,
                format!("GraphQL {name} must be a non-negative integer: {error}"),
            )
        });
    }
    let Value::Int(number) = value else {
        return Err(unsupported(
            path,
            format!("GraphQL {name} must be a non-negative integer"),
        ));
    };
    let value = number
        .as_i64()
        .ok_or_else(|| unsupported(path.clone(), format!("GraphQL {name} is out of range")))?;
    u64::try_from(value).map_err(|error| {
        unsupported(
            path,
            format!("GraphQL {name} must be a non-negative integer: {error}"),
        )
    })
}

fn compile_boolean(
    value: &Value<'_, String>,
    path: impl Into<String>,
    name: &str,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<bool, CoreError> {
    let path = path.into();
    if let Value::Variable(variable) = value {
        let GraphqlVariableValue::Literal(Literal::Boolean(value)) =
            context.parameter_value(variable, path.clone())?
        else {
            return Err(unsupported(
                path,
                format!("GraphQL variable '${variable}' must be a boolean"),
            ));
        };
        return Ok(*value);
    }
    let Value::Boolean(value) = value else {
        return Err(unsupported(
            path,
            format!("GraphQL {name} must be a boolean"),
        ));
    };
    Ok(*value)
}

fn compile_name_value(
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<String, CoreError> {
    let path = path.into();
    match value {
        Value::Enum(value) | Value::String(value) => Ok(value.clone()),
        Value::Variable(variable) => match context.parameter_value(variable, path.clone())? {
            GraphqlVariableValue::Literal(Literal::String(value)) => Ok(value.clone()),
            GraphqlVariableValue::Literal(_)
            | GraphqlVariableValue::List(_)
            | GraphqlVariableValue::Object(_)
            | GraphqlVariableValue::ObjectList(_) => Err(unsupported(
                path,
                format!("GraphQL variable '${variable}' must be a string or enum name"),
            )),
        },
        _ => Err(unsupported(
            path,
            "GraphQL value must be an enum or string name",
        )),
    }
}

fn variable_for_label(label: &str) -> String {
    let mut chars = label.chars();
    match chars.next() {
        Some(first) => first.to_lowercase().chain(chars).collect(),
        None => "node".to_string(),
    }
}

fn nested_variable_for_label(label: &str, index: usize) -> String {
    format!("{}{}", variable_for_label(label), index)
}

fn nested_variable_for_field(field: &Field<'_, String>, label: &str, index: usize) -> String {
    field
        .alias
        .clone()
        .unwrap_or_else(|| nested_variable_for_label(label, index))
}

fn unsupported(path: impl Into<String>, message: impl Into<String>) -> CoreError {
    Diagnostic::new("UNSUPPORTED_GRAPHQL", path, message).into_core_error()
}

#[cfg(test)]
mod tests {
    use super::super::graphql_schema_sdl_for_graph;
    use super::*;

    fn variable_object(
        entries: impl IntoIterator<Item = (&'static str, GraphqlVariableValue)>,
    ) -> GraphqlVariableValue {
        GraphqlVariableValue::Object(variable_object_map(entries))
    }

    fn variable_object_map(
        entries: impl IntoIterator<Item = (&'static str, GraphqlVariableValue)>,
    ) -> BTreeMap<String, GraphqlVariableValue> {
        entries
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect()
    }

    fn predicate_expression_contains_not(expression: &PredicateExpression) -> bool {
        match expression {
            PredicateExpression::Not { .. } => true,
            PredicateExpression::And { left, right }
            | PredicateExpression::Or { left, right }
            | PredicateExpression::Xor { left, right } => {
                predicate_expression_contains_not(left) || predicate_expression_contains_not(right)
            }
            PredicateExpression::Boolean(_)
            | PredicateExpression::Comparison(_)
            | PredicateExpression::KeyComparison(_)
            | PredicateExpression::ElementIdComparison(_)
            | PredicateExpression::Presence(_)
            | PredicateExpression::PropertyKeyMembership(_)
            | PredicateExpression::ExistsPattern(_)
            | PredicateExpression::ScalarComparison(_) => false,
        }
    }

    #[test]
    fn compiles_root_node_query() {
        let plan = compile_graphql(
            r#"
            query {
              Service(
                where: { tier: { eq: "prod" }, risk: { gte: 0.5 } }
                orderBy: [{ field: name, direction: ASCENDING }]
                limit: 10
                offset: 2
              ) {
                serviceName: name
                tier
              }
            }
            "#,
        )
        .expect("GraphQL query should compile");

        assert_eq!(
            plan.nodes,
            vec![NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            }]
        );
        assert_eq!(
            plan.projections,
            vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("serviceName".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    },
                    alias: Some("tier".to_string()),
                },
            ]
        );
        assert_eq!(plan.predicates.len(), 2);
        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: None,
            }]
        );
        assert_eq!(plan.limit, Some(10));
        assert_eq!(plan.skip, Some(2));
    }

    #[test]
    fn compiles_root_first_argument_as_limit() {
        let plan = compile_graphql(
            r"
            query {
              Service(first: 3, skip: 1) {
                name
              }
            }
            ",
        )
        .expect("GraphQL first argument should compile as a limit");

        assert_eq!(plan.limit, Some(3));
        assert_eq!(plan.skip, Some(1));
    }

    #[test]
    fn rejects_ambiguous_graphql_root_arguments() {
        let cases = [
            (
                r"
                query {
                  Service(limit: 1, first: 2) {
                    name
                  }
                }
                ",
                "GraphQL root argument 'first' conflicts with earlier 'limit' argument",
            ),
            (
                r"
                query {
                  Service(offset: 1, skip: 2) {
                    name
                  }
                }
                ",
                "GraphQL root argument 'skip' conflicts with earlier 'offset' argument",
            ),
            (
                r#"
                query {
                  Service(
                    where: { tier: { eq: "prod" } }
                    where: { name: { eq: "billing-api" } }
                  ) {
                    name
                  }
                }
                "#,
                "GraphQL root argument 'where' is specified more than once",
            ),
            (
                r"
                query {
                  Service(
                    orderBy: [{ field: name }]
                    orderBy: [{ field: tier }]
                  ) {
                    name
                  }
                }
                ",
                "GraphQL root argument 'orderBy' is specified more than once",
            ),
            (
                r"
                query {
                  Service(distinct: true, distinct: false) {
                    name
                  }
                }
                ",
                "GraphQL root argument 'distinct' is specified more than once",
            ),
        ];

        for (query, expected) in cases {
            let error =
                compile_graphql(query).expect_err("ambiguous GraphQL root arguments should fail");
            assert!(
                error.to_string().contains(expected),
                "expected {expected:?}, got {error}"
            );
        }
    }

    #[test]
    fn compiles_root_alias_typename_and_directives() {
        let variables = BTreeMap::from([
            (
                "withRisk".to_string(),
                GraphqlVariableValue::Literal(Literal::Boolean(true)),
            ),
            (
                "skipTier".to_string(),
                GraphqlVariableValue::Literal(Literal::Boolean(true)),
            ),
        ]);
        let plan = compile_graphql_with_variables(
            r"
            query Services($withRisk: Boolean!, $skipTier: Boolean!) {
              services: Service {
                __typename
                name
                risk @include(if: $withRisk)
                tier @skip(if: $skipTier)
              }
            }
            ",
            &variables,
        )
        .expect("GraphQL root aliases, typename, and directives should compile");

        assert_eq!(
            plan.nodes,
            vec![NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            }]
        );
        assert_eq!(
            plan.projections,
            vec![
                Projection::Literal {
                    literal: Literal::String("Service".to_string()),
                    alias: "__typename".to_string(),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("name".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    },
                    alias: Some("risk".to_string()),
                },
            ]
        );
    }

    #[test]
    fn compiles_graphql_node_identity_fields() {
        let plan = compile_graphql(
            r"
            query {
              Service {
                nodeId: _id
                element: _elementId
                name
              }
            }
            ",
        )
        .expect("GraphQL node identity fields should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::Key {
                    variable: "service".to_string(),
                    alias: "nodeId".to_string(),
                },
                Projection::ElementId {
                    variable: "service".to_string(),
                    alias: "element".to_string(),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("name".to_string()),
                },
            ]
        );
    }

    #[test]
    fn compiles_graphql_identity_filters_and_ordering() {
        let plan = compile_graphql(
            r#"
            query {
              Service(
                where: {
                  _id: { in: [10, 20, 40] }
                  _elementId: { notIn: ["40"] }
                }
                orderBy: [
                  { field: _id, direction: DESC }
                  { field: _elementId, direction: ASC }
                ]
              ) {
                name
              }
            }
            "#,
        )
        .expect("GraphQL identity filters and ordering should compile");

        assert!(plan.predicates.is_empty());
        assert!(matches!(
            plan.predicate,
            Some(PredicateExpression::And { .. })
        ));
        assert_eq!(
            plan.order_by,
            vec![
                OrderKey {
                    expression: OrderExpression::Key {
                        variable: "service".to_string(),
                    },
                    direction: OrderDirection::Descending,
                    nulls: None,
                },
                OrderKey {
                    expression: OrderExpression::ElementId {
                        variable: "service".to_string(),
                    },
                    direction: OrderDirection::Ascending,
                    nulls: None,
                },
            ]
        );
    }

    #[test]
    fn compiles_graphql_shorthand_order_by_fields() {
        let plan = compile_graphql(
            r"
            query {
              Service(
                orderBy: [
                  { risk: DESC }
                  { name: ASCENDING }
                ]
              ) {
                name
              }
            }
            ",
        )
        .expect("GraphQL shorthand orderBy fields should compile");

        assert_eq!(
            plan.order_by,
            vec![
                OrderKey {
                    expression: OrderExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    direction: OrderDirection::Descending,
                    nulls: None,
                },
                OrderKey {
                    expression: OrderExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    }),
                    direction: OrderDirection::Ascending,
                    nulls: None,
                },
            ]
        );
    }

    #[test]
    fn compiles_graphql_order_by_null_placement() {
        let plan = compile_graphql(
            r"
            query {
              Service(
                orderBy: [
                  { field: tier, direction: ASC, nulls: LAST }
                  { field: name, direction: DESC, nulls: FIRST }
                ]
              ) {
                name
              }
            }
            ",
        )
        .expect("GraphQL orderBy null placement should compile");

        assert_eq!(
            plan.order_by,
            vec![
                OrderKey {
                    expression: OrderExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    }),
                    direction: OrderDirection::Ascending,
                    nulls: Some(NullOrder::Last),
                },
                OrderKey {
                    expression: OrderExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    }),
                    direction: OrderDirection::Descending,
                    nulls: Some(NullOrder::First),
                },
            ]
        );
    }

    #[test]
    fn compiles_graphql_flat_aggregate_fields() {
        let plan = compile_graphql(
            r"
            query {
              Service {
                tier
                services: _count
                namedServices: _count(field: name)
                tiers: _countDistinct(field: tier)
                totalRisk: _sum(field: risk)
                averageRisk: _avg(field: risk)
                minRisk: _min(field: risk)
                maxRisk: _max(field: risk)
              }
            }
            ",
        )
        .expect("GraphQL flat aggregate fields should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    },
                    alias: Some("tier".to_string()),
                },
                Projection::CountAll {
                    alias: "services".to_string(),
                },
                Projection::Aggregate {
                    function: AggregateFunction::Count,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    }),
                    distinct: false,
                    alias: "namedServices".to_string(),
                },
                Projection::Aggregate {
                    function: AggregateFunction::Count,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    }),
                    distinct: true,
                    alias: "tiers".to_string(),
                },
                Projection::Aggregate {
                    function: AggregateFunction::Sum,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    distinct: false,
                    alias: "totalRisk".to_string(),
                },
                Projection::Aggregate {
                    function: AggregateFunction::Avg,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    distinct: false,
                    alias: "averageRisk".to_string(),
                },
                Projection::Aggregate {
                    function: AggregateFunction::Min,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    distinct: false,
                    alias: "minRisk".to_string(),
                },
                Projection::Aggregate {
                    function: AggregateFunction::Max,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    distinct: false,
                    alias: "maxRisk".to_string(),
                },
            ]
        );
    }

    #[test]
    fn compiles_graphql_statistical_aggregate_fields() {
        let plan = compile_graphql(
            r"
            query {
              Service {
                sampleRisk: _stDev(field: risk)
                populationRisk: _stDevP(field: risk)
                distinctTotalRisk: _sumDistinct(field: risk)
                distinctAverageRisk: _avgDistinct(field: risk)
                medianRisk: _median(field: risk)
                distinctMedianRisk: _medianDistinct(field: risk)
                distinctMinRisk: _minDistinct(field: risk)
                distinctMaxRisk: _maxDistinct(field: risk)
              }
            }
            ",
        )
        .expect("GraphQL statistical aggregate fields should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::Aggregate {
                    function: AggregateFunction::StdDev,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    distinct: false,
                    alias: "sampleRisk".to_string(),
                },
                Projection::Aggregate {
                    function: AggregateFunction::StdDevP,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    distinct: false,
                    alias: "populationRisk".to_string(),
                },
                Projection::Aggregate {
                    function: AggregateFunction::Sum,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    distinct: true,
                    alias: "distinctTotalRisk".to_string(),
                },
                Projection::Aggregate {
                    function: AggregateFunction::Avg,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    distinct: true,
                    alias: "distinctAverageRisk".to_string(),
                },
                Projection::Aggregate {
                    function: AggregateFunction::Median,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    distinct: false,
                    alias: "medianRisk".to_string(),
                },
                Projection::Aggregate {
                    function: AggregateFunction::Median,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    distinct: true,
                    alias: "distinctMedianRisk".to_string(),
                },
                Projection::Aggregate {
                    function: AggregateFunction::Min,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    distinct: true,
                    alias: "distinctMinRisk".to_string(),
                },
                Projection::Aggregate {
                    function: AggregateFunction::Max,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "risk".to_string(),
                    }),
                    distinct: true,
                    alias: "distinctMaxRisk".to_string(),
                },
            ]
        );
    }

    #[test]
    fn compiles_graphql_collect_aggregate_fields() {
        let plan = compile_graphql(
            r"
            query {
              Service {
                serviceNames: _collect(field: name)
                uniqueTiers: _collectDistinct(field: tier)
              }
            }
            ",
        )
        .expect("GraphQL collect aggregate fields should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::Aggregate {
                    function: AggregateFunction::Collect,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    }),
                    distinct: false,
                    alias: "serviceNames".to_string(),
                },
                Projection::Aggregate {
                    function: AggregateFunction::Collect,
                    target: AggregateTarget::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    }),
                    distinct: true,
                    alias: "uniqueTiers".to_string(),
                },
            ]
        );
    }

    #[test]
    fn rejects_graphql_string_filters_on_raw_identity_fields() {
        let error = compile_graphql(
            r#"
            query {
              Service(where: { _id: { contains: "1" } }) {
                name
              }
            }
            "#,
        )
        .expect_err("GraphQL string filters on _id should fail");

        assert!(
            error
                .to_string()
                .contains("GraphQL _id filters do not support string predicates"),
            "{error}"
        );
    }

    #[test]
    fn compiles_root_boolean_where_filters() {
        let plan = compile_graphql(
            r#"
            query {
              Service(
                where: {
                  or: [
                    { tier: { eq: "prod" } }
                    { risk: { gte: 0.9 } }
                  ]
                  not: { name: { contains: "legacy" } }
                }
              ) {
                name
              }
            }
            "#,
        )
        .expect("GraphQL boolean where filters should compile");

        assert!(plan.predicates.is_empty());
        assert!(matches!(
            plan.predicate,
            Some(PredicateExpression::And { .. })
        ));
    }

    #[test]
    fn compiles_graphql_regex_and_xor_filters() {
        let plan = compile_graphql(
            r#"
            query {
              Service(
                where: {
                  xor: [
                    { name: { matches: "^billing.*" } }
                    { tier: { regex: "^dev$" } }
                  ]
                }
              ) {
                name
              }
            }
            "#,
        )
        .expect("GraphQL regex and xor filters should compile");

        assert!(plan.predicates.is_empty());
        let Some(PredicateExpression::Xor { left, right }) = plan.predicate.as_ref() else {
            panic!("expected GraphQL xor to compile as a non-conjunctive predicate");
        };
        assert!(matches!(
            left.as_ref(),
            PredicateExpression::Comparison(PropertyPredicate {
                operator: ComparisonOperator::RegexMatch,
                ..
            })
        ));
        assert!(matches!(
            right.as_ref(),
            PredicateExpression::Comparison(PropertyPredicate {
                operator: ComparisonOperator::RegexMatch,
                ..
            })
        ));
    }

    #[test]
    fn compiles_graphql_filter_operator_aliases() {
        let plan = compile_graphql(
            r#"
            query {
              Service(
                where: {
                  tier: { equals: "prod" }
                  name: { notEquals: "legacy-sync", starts_with: "billing" }
                  risk: { greaterThanOrEqual: 0.5, lessThanOrEqual: 0.95 }
                }
              ) {
                name
              }
            }
            "#,
        )
        .expect("GraphQL filter operator aliases should compile");

        assert_eq!(plan.predicates.len(), 5);
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.property == "tier"
                && predicate.operator == ComparisonOperator::Equal
                && predicate.rhs == PredicateRhs::Literal(Literal::String("prod".to_string()))
        }));
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.property == "name"
                && predicate.operator == ComparisonOperator::NotEqual
                && predicate.rhs
                    == PredicateRhs::Literal(Literal::String("legacy-sync".to_string()))
        }));
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.property == "name"
                && predicate.operator == ComparisonOperator::StartsWith
                && predicate.rhs == PredicateRhs::Literal(Literal::String("billing".to_string()))
        }));
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.property == "risk"
                && predicate.operator == ComparisonOperator::GreaterThanOrEqual
        }));
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.property == "risk"
                && predicate.operator == ComparisonOperator::LessThanOrEqual
        }));
    }

    #[test]
    fn compiles_graphql_shorthand_where_filters() {
        let plan = compile_graphql(
            r#"
            query {
              Service(
                where: {
                  tier: "prod"
                  risk: 0.5
                }
              ) {
                name
              }
            }
            "#,
        )
        .expect("GraphQL shorthand where filters should compile");

        assert_eq!(plan.predicates.len(), 2);
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.property == "tier"
                && predicate.operator == ComparisonOperator::Equal
                && predicate.rhs == PredicateRhs::Literal(Literal::String("prod".to_string()))
        }));
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.property == "risk"
                && predicate.operator == ComparisonOperator::Equal
                && predicate.rhs == PredicateRhs::Literal(Literal::Float(OrderedFloat(0.5)))
        }));
    }

    #[test]
    fn compiles_graphql_negated_filter_operators() {
        let plan = compile_graphql(
            r#"
            query {
              Service(
                where: {
                  tier: { isNotNull: true }
                  name: {
                    notIn: ["legacy-sync", "experiments"]
                    notContains: "legacy"
                    notRegex: "^internal"
                  }
                }
              ) {
                name
              }
            }
            "#,
        )
        .expect("GraphQL negated filter operators should compile");

        assert!(plan.predicates.is_empty());
        let expression = plan
            .predicate
            .as_ref()
            .expect("negated GraphQL filters should compile into the predicate tree");
        assert!(predicate_expression_contains_not(expression));
    }

    #[test]
    fn rejects_invalid_graphql_flat_aggregate_arguments() {
        for query in [
            r"
            query {
              Service {
                _sum
              }
            }
            ",
            r"
            query {
              Service {
                _avg(property: risk)
              }
            }
            ",
            r"
            query {
              Service {
                _countDistinct {
                  value
                }
              }
            }
            ",
        ] {
            let error = compile_graphql(query)
                .expect_err("invalid GraphQL flat aggregate field should fail");

            assert!(
                error.to_string().contains("GraphQL aggregate")
                    || error.to_string().contains("unsupported GraphQL aggregate"),
                "{error}"
            );
        }
    }

    #[test]
    fn compiles_root_query_with_variables() {
        let variables = BTreeMap::from([
            (
                "tier".to_string(),
                GraphqlVariableValue::Literal(Literal::String("prod".to_string())),
            ),
            (
                "minRisk".to_string(),
                GraphqlVariableValue::Literal(Literal::Float(OrderedFloat(0.5))),
            ),
            (
                "names".to_string(),
                GraphqlVariableValue::List(vec![
                    Literal::String("billing-api".to_string()),
                    Literal::String("deployments".to_string()),
                ]),
            ),
            (
                "sortField".to_string(),
                GraphqlVariableValue::Literal(Literal::String("name".to_string())),
            ),
            (
                "sortDirection".to_string(),
                GraphqlVariableValue::Literal(Literal::String("DESCENDING".to_string())),
            ),
            (
                "rowLimit".to_string(),
                GraphqlVariableValue::Literal(Literal::Integer(10)),
            ),
            (
                "dedupe".to_string(),
                GraphqlVariableValue::Literal(Literal::Boolean(true)),
            ),
        ]);
        let plan = compile_graphql_with_variables(
            r"
            query Services(
              $tier: String!
              $minRisk: Float!
              $names: [String!]
              $sortField: ServiceOrderField!
              $sortDirection: SortDirection!
              $rowLimit: Int!
              $dedupe: Boolean!
            ) {
              Service(
                where: {
                  tier: { eq: $tier }
                  risk: { gte: $minRisk }
                  name: { in: $names }
                }
                orderBy: [{ field: $sortField, direction: $sortDirection }]
                limit: $rowLimit
                distinct: $dedupe
              ) {
                name
              }
            }
            ",
            &variables,
        )
        .expect("GraphQL variables should compile");

        assert_eq!(plan.predicates.len(), 3);
        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Descending,
                nulls: None,
            }]
        );
        assert_eq!(plan.limit, Some(10));
        assert!(plan.distinct);
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.property == "name"
                && matches!(
                        &predicate.rhs,
                PredicateRhs::List(values) if values.len() == 2
                    )
        }));
    }

    #[test]
    fn compiles_named_operation_from_multi_operation_document() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let plan = compile_graphql_for_graph_with_operation_name(
            &graph,
            r"
            query Services {
              Service { name }
            }

            query People {
              Person { name }
            }
            ",
            "People",
        )
        .expect("named operation should compile from multi-operation document");

        assert_eq!(
            plan.nodes,
            vec![NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            }]
        );
        assert!(plan.projections.iter().any(|projection| {
            matches!(
                projection,
                Projection::Property {
                    property: PropertyRef { variable, property },
                    alias: Some(alias),
                } if variable == "person" && property == "name" && alias == "name"
            )
        }));
    }

    #[test]
    fn named_operation_selection_ignores_unselected_operation_variables() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let plan = compile_graphql_for_graph_with_variables_and_operation_name(
            &graph,
            r"
            query Services {
              Service { name }
            }

            query RequiresVariable($missing: String!) {
              Service(where: { tier: { eq: $missing } }) { name }
            }
            ",
            &BTreeMap::new(),
            "Services",
        )
        .expect("unselected operation variables should not be required");

        assert_eq!(
            plan.nodes
                .first()
                .expect("selected operation should bind a node")
                .label,
            "Service"
        );
        assert!(plan.predicates.is_empty());
    }

    #[test]
    fn rejects_multi_operation_graphql_without_operation_name() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let error = compile_graphql_for_graph(
            &graph,
            r"
            query Services { Service { name } }
            query People { Person { name } }
            ",
        )
        .expect_err("multi-operation document should require operationName");

        assert!(
            error.to_string().contains("require an operationName"),
            "{error}"
        );
    }

    #[test]
    fn rejects_missing_or_duplicate_graphql_operation_names() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let missing = compile_graphql_for_graph_with_operation_name(
            &graph,
            "query Services { Service { name } }",
            "People",
        )
        .expect_err("missing operation name should fail");
        assert!(
            missing
                .to_string()
                .contains("GraphQL operation 'People' was not found"),
            "{missing}"
        );

        let duplicate = compile_graphql_for_graph_with_operation_name(
            &graph,
            r"
            query Services { Service { name } }
            query Services { Service { tier } }
            ",
            "Services",
        )
        .expect_err("duplicate operation names should fail");
        assert!(
            duplicate.to_string().contains("defined more than once"),
            "{duplicate}"
        );
    }

    #[test]
    fn rejects_selected_non_query_graphql_operation() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let error = compile_graphql_for_graph_with_operation_name(
            &graph,
            r"
            query Services { Service { name } }
            mutation MutateService { updateService { id } }
            ",
            "MutateService",
        )
        .expect_err("selected mutation should remain unsupported");

        assert!(
            error
                .to_string()
                .contains("GraphQL mutations and subscriptions are not supported"),
            "{error}"
        );
    }

    #[test]
    fn compiles_root_query_with_object_where_variable() {
        let variables = BTreeMap::from([(
            "filter".to_string(),
            variable_object([
                (
                    "tier",
                    variable_object([(
                        "eq",
                        GraphqlVariableValue::Literal(Literal::String("prod".to_string())),
                    )]),
                ),
                (
                    "risk",
                    variable_object([(
                        "gte",
                        GraphqlVariableValue::Literal(Literal::Float(OrderedFloat(0.5))),
                    )]),
                ),
                (
                    "name",
                    variable_object([(
                        "in",
                        GraphqlVariableValue::List(vec![
                            Literal::String("billing-api".to_string()),
                            Literal::String("deployments".to_string()),
                        ]),
                    )]),
                ),
            ]),
        )]);
        let plan = compile_graphql_with_variables(
            r"
            query Services($filter: ServiceWhere!) {
              Service(where: $filter) { name }
            }
            ",
            &variables,
        )
        .expect("GraphQL object where variable should compile");

        assert_eq!(plan.predicates.len(), 3);
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.property == "tier"
                && predicate.rhs == PredicateRhs::Literal(Literal::String("prod".to_string()))
        }));
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.property == "risk"
                && predicate.operator == ComparisonOperator::GreaterThanOrEqual
        }));
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.property == "name"
                && matches!(
                    &predicate.rhs,
                    PredicateRhs::List(values) if values.len() == 2
                )
        }));
    }

    #[test]
    fn compiles_root_query_with_object_where_variable_operator_aliases() {
        let variables = BTreeMap::from([(
            "filter".to_string(),
            variable_object([
                (
                    "tier",
                    variable_object([(
                        "equals",
                        GraphqlVariableValue::Literal(Literal::String("prod".to_string())),
                    )]),
                ),
                (
                    "name",
                    variable_object([(
                        "neq",
                        GraphqlVariableValue::Literal(Literal::String("legacy-sync".to_string())),
                    )]),
                ),
                (
                    "risk",
                    variable_object([
                        (
                            "greaterThan",
                            GraphqlVariableValue::Literal(Literal::Float(OrderedFloat(0.25))),
                        ),
                        (
                            "lessThanOrEqual",
                            GraphqlVariableValue::Literal(Literal::Float(OrderedFloat(0.95))),
                        ),
                    ]),
                ),
            ]),
        )]);
        let plan = compile_graphql_with_variables(
            r"
            query Services($filter: ServiceWhere!) {
              Service(where: $filter) { name }
            }
            ",
            &variables,
        )
        .expect("GraphQL object where variable operator aliases should compile");

        assert_eq!(plan.predicates.len(), 4);
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.property == "tier" && predicate.operator == ComparisonOperator::Equal
        }));
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.property == "name"
                && predicate.operator == ComparisonOperator::NotEqual
        }));
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.property == "risk"
                && predicate.operator == ComparisonOperator::GreaterThan
        }));
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.property == "risk"
                && predicate.operator == ComparisonOperator::LessThanOrEqual
        }));
    }

    #[test]
    fn compiles_root_query_with_object_where_variable_negated_operators() {
        let variables = BTreeMap::from([(
            "filter".to_string(),
            variable_object([
                (
                    "tier",
                    variable_object([(
                        "isNotNull",
                        GraphqlVariableValue::Literal(Literal::Boolean(true)),
                    )]),
                ),
                (
                    "name",
                    variable_object([
                        (
                            "notIn",
                            GraphqlVariableValue::List(vec![
                                Literal::String("legacy-sync".to_string()),
                                Literal::String("experiments".to_string()),
                            ]),
                        ),
                        (
                            "notStartsWith",
                            GraphqlVariableValue::Literal(Literal::String("internal".to_string())),
                        ),
                    ]),
                ),
            ]),
        )]);
        let plan = compile_graphql_with_variables(
            r"
            query Services($filter: ServiceWhere!) {
              Service(where: $filter) { name }
            }
            ",
            &variables,
        )
        .expect("GraphQL object where variable negated operators should compile");

        assert!(plan.predicates.is_empty());
        let expression = plan
            .predicate
            .as_ref()
            .expect("negated GraphQL variable filters should compile into the predicate tree");
        assert!(predicate_expression_contains_not(expression));
    }

    #[test]
    fn compiles_root_query_with_property_condition_variable() {
        let variables = BTreeMap::from([(
            "tierCondition".to_string(),
            variable_object([(
                "eq",
                GraphqlVariableValue::Literal(Literal::String("prod".to_string())),
            )]),
        )]);
        let plan = compile_graphql_with_variables(
            r"
            query Services($tierCondition: StringCondition!) {
              Service(where: { tier: $tierCondition }) { name }
            }
            ",
            &variables,
        )
        .expect("GraphQL property condition variable should compile");

        assert_eq!(
            plan.predicates,
            vec![PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
            }]
        );
    }

    #[test]
    fn compiles_root_query_with_scalar_shorthand_where_variable() {
        let variables = BTreeMap::from([(
            "tier".to_string(),
            GraphqlVariableValue::Literal(Literal::String("prod".to_string())),
        )]);
        let plan = compile_graphql_with_variables(
            r"
            query Services($tier: String!) {
              Service(where: { tier: $tier }) { name }
            }
            ",
            &variables,
        )
        .expect("GraphQL scalar shorthand where variable should compile");

        assert_eq!(
            plan.predicates,
            vec![PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
            }]
        );
    }

    #[test]
    fn compiles_root_query_with_object_where_variable_shorthand_values() {
        let variables = BTreeMap::from([(
            "filter".to_string(),
            variable_object([
                (
                    "tier",
                    GraphqlVariableValue::Literal(Literal::String("prod".to_string())),
                ),
                (
                    "risk",
                    GraphqlVariableValue::Literal(Literal::Float(OrderedFloat(0.5))),
                ),
            ]),
        )]);
        let plan = compile_graphql_with_variables(
            r"
            query Services($filter: ServiceWhere!) {
              Service(where: $filter) { name }
            }
            ",
            &variables,
        )
        .expect("GraphQL object where variable shorthand values should compile");

        assert_eq!(plan.predicates.len(), 2);
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.property == "tier"
                && predicate.operator == ComparisonOperator::Equal
                && predicate.rhs == PredicateRhs::Literal(Literal::String("prod".to_string()))
        }));
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.property == "risk"
                && predicate.operator == ComparisonOperator::Equal
                && predicate.rhs == PredicateRhs::Literal(Literal::Float(OrderedFloat(0.5)))
        }));
    }

    #[test]
    fn compiles_root_query_with_object_list_boolean_variable() {
        let variables = BTreeMap::from([(
            "filters".to_string(),
            GraphqlVariableValue::ObjectList(vec![
                variable_object_map([(
                    "tier",
                    variable_object([(
                        "eq",
                        GraphqlVariableValue::Literal(Literal::String("prod".to_string())),
                    )]),
                )]),
                variable_object_map([(
                    "name",
                    variable_object([(
                        "contains",
                        GraphqlVariableValue::Literal(Literal::String("experiments".to_string())),
                    )]),
                )]),
            ]),
        )]);
        let plan = compile_graphql_with_variables(
            r"
            query Services($filters: [ServiceWhere!]!) {
              Service(where: { or: $filters }) { name }
            }
            ",
            &variables,
        )
        .expect("GraphQL object-list boolean variable should compile");

        assert!(plan.predicates.is_empty());
        assert!(matches!(
            plan.predicate,
            Some(PredicateExpression::Or { .. })
        ));
    }

    #[test]
    fn compiles_root_query_with_order_by_object_variable() {
        let variables = BTreeMap::from([(
            "order".to_string(),
            variable_object([
                (
                    "field",
                    GraphqlVariableValue::Literal(Literal::String("name".to_string())),
                ),
                (
                    "direction",
                    GraphqlVariableValue::Literal(Literal::String("DESC".to_string())),
                ),
            ]),
        )]);
        let plan = compile_graphql_with_variables(
            r"
            query Services($order: ServiceOrder!) {
              Service(orderBy: $order) { name }
            }
            ",
            &variables,
        )
        .expect("GraphQL orderBy object variable should compile");

        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Descending,
                nulls: None,
            }]
        );
    }

    #[test]
    fn compiles_root_query_with_order_by_null_placement_variable() {
        let variables = BTreeMap::from([(
            "order".to_string(),
            variable_object([
                (
                    "field",
                    GraphqlVariableValue::Literal(Literal::String("tier".to_string())),
                ),
                (
                    "direction",
                    GraphqlVariableValue::Literal(Literal::String("ASC".to_string())),
                ),
                (
                    "nulls",
                    GraphqlVariableValue::Literal(Literal::String("NULLS_LAST".to_string())),
                ),
            ]),
        )]);
        let plan = compile_graphql_with_variables(
            r"
            query Services($order: ServiceOrder!) {
              Service(orderBy: $order) { name }
            }
            ",
            &variables,
        )
        .expect("GraphQL orderBy null placement variable should compile");

        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                }),
                direction: OrderDirection::Ascending,
                nulls: Some(NullOrder::Last),
            }]
        );
    }

    #[test]
    fn compiles_root_query_with_shorthand_order_by_object_variable() {
        let variables = BTreeMap::from([(
            "order".to_string(),
            variable_object([(
                "name",
                GraphqlVariableValue::Literal(Literal::String("DESC".to_string())),
            )]),
        )]);
        let plan = compile_graphql_with_variables(
            r"
            query Services($order: ServiceOrder!) {
              Service(orderBy: $order) { name }
            }
            ",
            &variables,
        )
        .expect("GraphQL shorthand orderBy object variable should compile");

        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Descending,
                nulls: None,
            }]
        );
    }

    #[test]
    fn compiles_root_query_with_order_by_object_list_variable() {
        let variables = BTreeMap::from([(
            "orders".to_string(),
            GraphqlVariableValue::ObjectList(vec![
                variable_object_map([
                    (
                        "field",
                        GraphqlVariableValue::Literal(Literal::String("tier".to_string())),
                    ),
                    (
                        "direction",
                        GraphqlVariableValue::Literal(Literal::String("ASC".to_string())),
                    ),
                ]),
                variable_object_map([
                    (
                        "field",
                        GraphqlVariableValue::Literal(Literal::String("name".to_string())),
                    ),
                    (
                        "direction",
                        GraphqlVariableValue::Literal(Literal::String("DESC".to_string())),
                    ),
                ]),
            ]),
        )]);
        let plan = compile_graphql_with_variables(
            r"
            query Services($orders: [ServiceOrder!]!) {
              Service(orderBy: $orders) { name }
            }
            ",
            &variables,
        )
        .expect("GraphQL orderBy object-list variable should compile");

        assert_eq!(
            plan.order_by,
            vec![
                OrderKey {
                    expression: OrderExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    }),
                    direction: OrderDirection::Ascending,
                    nulls: None,
                },
                OrderKey {
                    expression: OrderExpression::Property(PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    }),
                    direction: OrderDirection::Descending,
                    nulls: None,
                },
            ]
        );
    }

    #[test]
    fn compiles_root_query_with_variable_defaults() {
        let plan = compile_graphql_with_variables(
            r#"
            query Services(
              $tier: String = "prod"
              $names: [String!] = ["billing-api", "deployments"]
              $sortField: ServiceOrderField = name
              $sortDirection: SortDirection = DESC
              $rowLimit: Int = 10
              $dedupe: Boolean = true
            ) {
              Service(
                where: {
                  tier: { eq: $tier }
                  name: { in: $names }
                }
                orderBy: [{ field: $sortField, direction: $sortDirection }]
                limit: $rowLimit
                distinct: $dedupe
              ) {
                name
              }
            }
            "#,
            &BTreeMap::new(),
        )
        .expect("GraphQL variable defaults should compile");

        assert_eq!(plan.predicates.len(), 2);
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.property == "tier"
                && predicate.rhs == PredicateRhs::Literal(Literal::String("prod".to_string()))
        }));
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.property == "name"
                && matches!(
                    &predicate.rhs,
                    PredicateRhs::List(values)
                        if values
                            == &vec![
                                Literal::String("billing-api".to_string()),
                                Literal::String("deployments".to_string()),
                            ]
                )
        }));
        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Descending,
                nulls: None,
            }]
        );
        assert_eq!(plan.limit, Some(10));
        assert!(plan.distinct);
    }

    #[test]
    fn runtime_graphql_variables_override_defaults() {
        let variables = BTreeMap::from([(
            "tier".to_string(),
            GraphqlVariableValue::Literal(Literal::String("dev".to_string())),
        )]);
        let plan = compile_graphql_with_variables(
            r#"
            query Services($tier: String = "prod") {
              Service(where: { tier: { eq: $tier } }) { name }
            }
            "#,
            &variables,
        )
        .expect("runtime variables should override defaults");

        assert_eq!(
            plan.predicates,
            vec![PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("dev".to_string())),
            }]
        );
    }

    #[test]
    fn rejects_missing_graphql_variables() {
        let error = compile_graphql_with_variables(
            r"
            query Services($tier: String!) {
              Service(where: { tier: { eq: $tier } }) { name }
            }
            ",
            &BTreeMap::new(),
        )
        .expect_err("missing GraphQL variable should fail");

        assert!(
            error
                .to_string()
                .contains("GraphQL variable '$tier' was not provided"),
            "{error}"
        );
    }

    #[test]
    fn rejects_undeclared_graphql_variables() {
        let variables = BTreeMap::from([(
            "tier".to_string(),
            GraphqlVariableValue::Literal(Literal::String("prod".to_string())),
        )]);
        let error = compile_graphql_with_variables(
            r"
            query Services {
              Service(where: { tier: { eq: $tier } }) { name }
            }
            ",
            &variables,
        )
        .expect_err("undeclared GraphQL variable should fail");

        assert!(
            error
                .to_string()
                .contains("GraphQL variable '$tier' is not declared"),
            "{error}"
        );
    }

    #[test]
    fn rejects_graphql_variable_list_in_scalar_position() {
        let variables = BTreeMap::from([(
            "tier".to_string(),
            GraphqlVariableValue::List(vec![Literal::String("prod".to_string())]),
        )]);
        let error = compile_graphql_with_variables(
            r"
            query Services($tier: [String!]) {
              Service(where: { tier: { eq: $tier } }) { name }
            }
            ",
            &variables,
        )
        .expect_err("list variable in scalar position should fail");

        assert!(
            error
                .to_string()
                .contains("GraphQL variable '$tier' must be a scalar literal"),
            "{error}"
        );
    }

    #[test]
    fn rejects_graphql_object_variable_in_scalar_position() {
        let variables = BTreeMap::from([(
            "tier".to_string(),
            variable_object([(
                "eq",
                GraphqlVariableValue::Literal(Literal::String("prod".to_string())),
            )]),
        )]);
        let error = compile_graphql_with_variables(
            r"
            query Services($tier: String!) {
              Service(where: { tier: { eq: $tier } }) { name }
            }
            ",
            &variables,
        )
        .expect_err("object variable in scalar position should fail");

        assert!(
            error
                .to_string()
                .contains("GraphQL variable '$tier' must be a scalar literal"),
            "{error}"
        );
    }

    #[test]
    fn rejects_graphql_scalar_variable_in_object_position() {
        let variables = BTreeMap::from([(
            "filter".to_string(),
            GraphqlVariableValue::Literal(Literal::String("prod".to_string())),
        )]);
        let error = compile_graphql_with_variables(
            r"
            query Services($filter: ServiceWhere!) {
              Service(where: $filter) { name }
            }
            ",
            &variables,
        )
        .expect_err("scalar variable in object position should fail");

        assert!(
            error
                .to_string()
                .contains("GraphQL variable '$filter' must be an object"),
            "{error}"
        );
    }

    #[test]
    fn rejects_graphql_list_variable_in_property_shorthand_position() {
        let variables = BTreeMap::from([(
            "condition".to_string(),
            GraphqlVariableValue::List(vec![Literal::String("prod".to_string())]),
        )]);
        let error = compile_graphql_with_variables(
            r"
            query Services($condition: [String!]!) {
              Service(where: { tier: $condition }) { name }
            }
            ",
            &variables,
        )
        .expect_err("list variable in property shorthand position should fail");

        assert!(
            error
                .to_string()
                .contains("must be a scalar literal or property condition object"),
            "{error}"
        );
    }

    #[test]
    fn rejects_graphql_scalar_variable_in_order_by_position() {
        let variables = BTreeMap::from([(
            "order".to_string(),
            GraphqlVariableValue::Literal(Literal::String("name".to_string())),
        )]);
        let error = compile_graphql_with_variables(
            r"
            query Services($order: ServiceOrder!) {
              Service(orderBy: $order) { name }
            }
            ",
            &variables,
        )
        .expect_err("scalar variable in orderBy position should fail");

        assert!(
            error
                .to_string()
                .contains("must be an orderBy object or list of objects"),
            "{error}"
        );
    }

    #[test]
    fn compiles_root_query_with_object_variable_defaults() {
        let plan = compile_graphql_with_variables(
            r#"
            query Services(
              $where: ServiceWhere = { tier: { eq: "prod" } }
              $order: ServiceOrder = { field: name, direction: DESC }
            ) {
              Service(where: $where, orderBy: $order) { name }
            }
            "#,
            &BTreeMap::new(),
        )
        .expect("GraphQL object variable defaults should compile");

        assert_eq!(
            plan.predicates,
            vec![PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String("prod".to_string())),
            }]
        );
        assert_eq!(
            plan.order_by,
            vec![OrderKey {
                expression: OrderExpression::Property(PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                }),
                direction: OrderDirection::Descending,
                nulls: None,
            }]
        );
    }

    #[test]
    fn compiles_root_query_with_object_list_variable_defaults() {
        let plan = compile_graphql_with_variables(
            r#"
            query Services(
              $filters: [ServiceWhere!] = [
                { tier: { eq: "prod" } }
                { name: { contains: "experiments" } }
              ]
            ) {
              Service(where: { or: $filters }) { name }
            }
            "#,
            &BTreeMap::new(),
        )
        .expect("GraphQL object-list variable defaults should compile");

        assert!(plan.predicates.is_empty());
        assert!(matches!(
            plan.predicate,
            Some(PredicateExpression::Or { .. })
        ));
    }

    #[test]
    fn compiles_empty_order_by_list_variable_default_as_no_order_keys() {
        let plan = compile_graphql_with_variables(
            r"
            query Services($orders: [ServiceOrder!] = []) {
              Service(orderBy: $orders) { name }
            }
            ",
            &BTreeMap::new(),
        )
        .expect("empty GraphQL orderBy defaults should compile as no-op ordering");

        assert!(plan.order_by.is_empty());
    }

    #[test]
    fn compiles_empty_order_by_list_variable_as_no_order_keys() {
        let variables =
            BTreeMap::from([("orders".to_string(), GraphqlVariableValue::List(Vec::new()))]);
        let plan = compile_graphql_with_variables(
            r"
            query Services($orders: [ServiceOrder!]!) {
              Service(orderBy: $orders) { name }
            }
            ",
            &variables,
        )
        .expect("empty GraphQL orderBy variables should compile as no-op ordering");

        assert!(plan.order_by.is_empty());
    }

    #[test]
    fn rejects_graphql_mixed_object_scalar_default_lists() {
        let error = compile_graphql_with_variables(
            r#"
            query Services(
              $filters: [ServiceWhere!] = [
                { tier: { eq: "prod" } }
                "bad"
              ]
            ) {
              Service(where: { or: $filters }) { name }
            }
            "#,
            &BTreeMap::new(),
        )
        .expect_err("mixed object/scalar defaults should fail");

        assert!(
            error
                .to_string()
                .contains("cannot mix object and scalar values"),
            "{error}"
        );
    }

    #[test]
    fn compiles_graphql_named_and_inline_fragments() {
        let plan = compile_graphql(
            r"
            query {
              Service {
                __typename
                ...ServiceFields
                ... on Service {
                  __typename
                  serviceTier: tier
                }
              }
            }

            fragment ServiceFields on Service {
              __typename
              serviceName: name
            }
            ",
        )
        .expect("GraphQL named and inline fragments should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::Literal {
                    literal: Literal::String("Service".to_string()),
                    alias: "__typename".to_string(),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("serviceName".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    },
                    alias: Some("serviceTier".to_string()),
                },
            ]
        );
    }

    #[test]
    fn merges_duplicate_graphql_relationship_fields_across_fragments() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let plan = compile_graphql_for_graph(
            &graph,
            r"
            query {
              Person {
                owner: name
                out_OWNS(to: Service) {
                  service: name
                  ...OwnedServiceFields
                }
                ...PersonRelationshipFields
              }
            }

            fragment PersonRelationshipFields on Person {
              out_OWNS(to: Service) {
                tier
              }
            }

            fragment OwnedServiceFields on Service {
              service: name
              risk
            }
            ",
        )
        .expect("duplicate GraphQL relationship fields should merge");

        assert_eq!(plan.nodes.len(), 2, "{plan:?}");
        assert_eq!(plan.relationships.len(), 1, "{plan:?}");
        let service_variable = plan
            .relationships
            .first()
            .expect("relationship should exist")
            .right
            .clone();
        assert_eq!(
            plan.projections
                .iter()
                .filter(|projection| {
                    matches!(
                        projection,
                        Projection::Property {
                            property: PropertyRef { variable, property },
                            alias: Some(alias),
                        } if variable == &service_variable
                            && property == "name"
                            && alias == "service"
                    )
                })
                .count(),
            1,
            "{:?}",
            plan.projections
        );
        for property in ["tier", "risk"] {
            assert!(
                plan.projections.iter().any(|projection| {
                    matches!(
                        projection,
                        Projection::Property {
                            property: PropertyRef { variable, property: projected_property },
                            ..
                        } if variable == &service_variable && projected_property == property
                    )
                }),
                "missing merged service property {property}: {:?}",
                plan.projections
            );
        }
    }

    #[test]
    fn rejects_conflicting_duplicate_graphql_relationship_fields() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let error = compile_graphql_for_graph(
            &graph,
            r#"
            query {
              Person {
                out_OWNS(to: Service, where: { tier: { eq: "prod" } }) {
                  name
                }
                out_OWNS(to: Service, where: { tier: { eq: "dev" } }) {
                  name
                }
              }
            }
            "#,
        )
        .expect_err("conflicting duplicate GraphQL relationship fields should fail");

        assert!(
            error
                .to_string()
                .contains("relationship response field 'out_OWNS' selects conflicting traversals"),
            "{error}"
        );
    }

    #[test]
    fn compiles_graphql_root_fragments() {
        let variables = BTreeMap::from([(
            "includeService".to_string(),
            GraphqlVariableValue::Literal(Literal::Boolean(true)),
        )]);
        let plan = compile_graphql_with_variables(
            r"
            query Services($includeService: Boolean!) {
              ...RootServices
              ... on Query {
                skipped: Team @skip(if: true) {
                  name
                }
              }
            }

            fragment RootServices on Query {
              services: Service(
                orderBy: [{ field: name, direction: ASC }]
                limit: 2
              ) @include(if: $includeService) {
                service: name
                tier
              }
            }
            ",
            &variables,
        )
        .expect("GraphQL root fragments should compile");

        assert_eq!(
            plan.nodes,
            vec![NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            }]
        );
        assert_eq!(
            plan.projections,
            vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("service".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    },
                    alias: Some("tier".to_string()),
                },
            ]
        );
        assert_eq!(plan.limit, Some(2));
    }

    #[test]
    fn merges_duplicate_graphql_root_fields_across_fragments() {
        let plan = compile_graphql(
            r#"
            query {
              Service(where: { tier: { eq: "prod" } }) {
                service: name
              }
              ...ServiceRootDetails
            }

            fragment ServiceRootDetails on Query {
              Service(where: { tier: { eq: "prod" } }) {
                tier
              }
            }
            "#,
        )
        .expect("duplicate GraphQL root fields should merge");

        assert_eq!(
            plan.nodes,
            vec![NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            }]
        );
        assert_eq!(
            plan.projections,
            vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("service".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    },
                    alias: Some("tier".to_string()),
                },
            ]
        );
    }

    #[test]
    fn rejects_conflicting_duplicate_graphql_root_fields() {
        let error = compile_graphql(
            r#"
            query {
              Service(where: { tier: { eq: "prod" } }) {
                name
              }
              Service(where: { tier: { eq: "dev" } }) {
                name
              }
            }
            "#,
        )
        .expect_err("conflicting duplicate GraphQL root fields should fail");

        assert!(
            error
                .to_string()
                .contains("root response field 'Service' selects conflicting root fields"),
            "{error}"
        );
    }

    #[test]
    fn compiles_declaration_aware_root_field_aliases() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let plan = compile_graphql_for_graph(
            &graph,
            r"
            query {
              services {
                __typename
                service: name
              }
            }
            ",
        )
        .expect("GraphQL declaration-aware root field alias should compile");

        assert_eq!(
            plan.nodes,
            vec![NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            }]
        );
        assert_eq!(
            plan.projections,
            vec![
                Projection::Literal {
                    literal: Literal::String("Service".to_string()),
                    alias: "__typename".to_string(),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("service".to_string()),
                },
            ]
        );
    }

    #[test]
    fn rejects_ambiguous_declaration_aware_root_field_aliases() {
        let graph = Declaration::from_yaml(
            r"
version: 1
name: ambiguous_roots
nodes:
  - label: User
    table: { schema: ops, name: users }
    key: id
    properties:
      name: name
  - label: user
    table: { schema: ops, name: lower_users }
    key: id
    properties:
      name: name
",
        )
        .expect("graph should parse");

        let error = compile_graphql_for_graph(
            &graph,
            r"
            query {
              users {
                name
              }
            }
            ",
        )
        .expect_err("ambiguous declaration-aware root field alias should fail");

        assert!(
            error
                .to_string()
                .contains("GraphQL root field 'users' is ambiguous"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_unknown_declaration_aware_root_fields() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let error = compile_graphql_for_graph(
            &graph,
            r"
            query {
              Incident {
                name
              }
            }
            ",
        )
        .expect_err("unknown graph-backed root field should fail");

        assert!(
            error
                .to_string()
                .contains("unknown GraphQL root node field 'Incident'"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn graph_aware_graphql_rejects_unknown_selected_properties() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let error = compile_graphql_for_graph(
            &graph,
            r"
            query {
              Service {
                missingProperty
              }
            }
            ",
        )
        .expect_err("unknown graph-backed selected property should fail");

        assert!(
            error.to_string().contains("UNKNOWN_PROPERTY"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn declaration_free_graphql_keeps_open_property_selection() {
        let plan = compile_graphql(
            r"
            query {
              Service {
                missingProperty
              }
            }
            ",
        )
        .expect("declaration-free GraphQL should keep open property names");

        assert_eq!(
            plan.projections,
            vec![Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "missingProperty".to_string(),
                },
                alias: Some("missingProperty".to_string()),
            }]
        );
    }

    #[test]
    fn graph_aware_graphql_rejects_unknown_filter_and_order_properties() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        for (query, reason) in [
            (
                r#"
                query {
                  Service(where: { missingProperty: { eq: "x" } }) {
                    name
                  }
                }
                "#,
                "unknown filter property",
            ),
            (
                r"
                query {
                  Service(orderBy: [{ field: missingProperty, direction: ASC }]) {
                    name
                  }
                }
                ",
                "unknown orderBy property",
            ),
        ] {
            let error = compile_graphql_for_graph(&graph, query).expect_err(reason);
            assert!(
                error.to_string().contains("UNKNOWN_PROPERTY"),
                "unexpected error for {reason}: {error}"
            );
        }
    }

    #[test]
    fn graph_aware_graphql_rejects_unknown_variable_filter_properties() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let variables = BTreeMap::from([(
            "filter".to_string(),
            variable_object([(
                "missingProperty",
                variable_object([(
                    "eq",
                    GraphqlVariableValue::Literal(Literal::String("x".to_string())),
                )]),
            )]),
        )]);
        let error = compile_graphql_for_graph_with_variables(
            &graph,
            r"
            query Services($filter: ServiceWhere!) {
              Service(where: $filter) {
                name
              }
            }
            ",
            &variables,
        )
        .expect_err("unknown object-variable filter property should fail");

        assert!(
            error.to_string().contains("UNKNOWN_PROPERTY"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn graph_aware_graphql_validates_only_selected_operation_properties() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        compile_graphql_for_graph_with_operation_name(
            &graph,
            r"
            query Good {
              Service {
                name
              }
            }

            query Bad {
              Service {
                missingProperty
              }
            }
            ",
            "Good",
        )
        .expect("unselected invalid operation should not be validated");

        let error = compile_graphql_for_graph_with_operation_name(
            &graph,
            r"
            query Good {
              Service {
                name
              }
            }

            query Bad {
              Service {
                missingProperty
              }
            }
            ",
            "Bad",
        )
        .expect_err("selected invalid operation should fail validation");

        assert!(
            error.to_string().contains("UNKNOWN_PROPERTY"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn declaration_free_graphql_keeps_unknown_root_labels() {
        let plan = compile_graphql(
            r"
            query {
              Incident {
                title
              }
            }
            ",
        )
        .expect("declaration-free GraphQL should keep root labels open");

        assert_eq!(
            plan.nodes,
            vec![NodePattern {
                variable: "incident".to_string(),
                label: "Incident".to_string(),
            }]
        );
    }

    #[test]
    fn compiles_graphql_edge_fields_inside_fragments() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let plan = compile_graphql_for_graph(
            &graph,
            r"
            {
              Person {
                owner: name
                out_OWNS(to: Service) {
                  service: name
                  ...OwnershipEdge
                }
              }
            }

            fragment OwnershipEdge on Service {
              _edge { source }
            }
            ",
        )
        .expect("GraphQL _edge inside fragments should compile");

        assert!(matches!(
            plan.relationships.as_slice(),
            [RelationshipPattern {
                variable: Some(variable),
                ..
            }] if variable == "relationship0"
        ));
        assert!(plan.projections.iter().any(|projection| {
            matches!(
                projection,
                Projection::Property {
                    property: PropertyRef { variable, property },
                    alias: Some(alias),
                } if variable == "relationship0"
                    && property == "source"
                    && alias == "relationship0_source"
            )
        }));
    }

    #[test]
    fn compiles_graphql_edge_identity_fields() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let plan = compile_graphql_for_graph(
            &graph,
            r"
            {
              Person {
                out_OWNS(to: Service) {
                  name
                  _edge {
                    edgeId: _id
                    edgeElement: _elementId
                  }
                }
              }
            }
            ",
        )
        .expect("GraphQL edge identity fields should compile");

        assert_eq!(
            plan.projections,
            vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "service1".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("service1_name".to_string()),
                },
                Projection::Key {
                    variable: "relationship0".to_string(),
                    alias: "edgeId".to_string(),
                },
                Projection::ElementId {
                    variable: "relationship0".to_string(),
                    alias: "edgeElement".to_string(),
                },
            ]
        );
    }

    #[test]
    fn compiles_graphql_fragments_inside_edge_selections() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let plan = compile_graphql_for_graph(
            &graph,
            r"
            {
              Person {
                owner: name
                out_OWNS(to: Service) {
                  service: name
                  _edge {
                    ...OwnershipEdge
                    ... on OWNS {
                      ownershipSourceInline: source
                    }
                  }
                }
              }
            }

            fragment OwnershipEdge on OWNS {
              edgeKind: __typename
              ownershipSource: source
            }
            ",
        )
        .expect("GraphQL fragments inside _edge should compile");

        assert!(matches!(
            plan.relationships.as_slice(),
            [RelationshipPattern {
                variable: Some(variable),
                ..
            }] if variable == "relationship0"
        ));
        assert!(plan.projections.iter().any(|projection| {
            matches!(
                projection,
                Projection::Literal {
                    literal: Literal::String(kind),
                    alias,
                } if kind == "OWNS" && alias == "edgeKind"
            )
        }));
        for alias in ["ownershipSource", "ownershipSourceInline"] {
            assert!(
                plan.projections.iter().any(|projection| {
                    matches!(
                        projection,
                        Projection::Property {
                            property: PropertyRef { variable, .. },
                            alias: Some(projected_alias),
                        } if variable == "relationship0" && projected_alias == alias
                    )
                }),
                "missing edge projection alias {alias}: {:?}",
                plan.projections
            );
        }
    }

    #[test]
    fn rejects_graphql_xor_with_wrong_arity() {
        for query in [
            r#"
            {
              Service(where: { xor: [{ name: { matches: "^billing" } }] }) {
                name
              }
            }
            "#,
            r#"
            {
              Service(
                where: {
                  xor: [
                    { name: { matches: "^billing" } }
                    { tier: { eq: "prod" } }
                    { risk: { gt: 0.5 } }
                  ]
                }
              ) {
                name
              }
            }
            "#,
        ] {
            let error = compile_graphql(query).expect_err("bad GraphQL xor arity should fail");

            assert!(
                error.to_string().contains("requires exactly two objects"),
                "{error}"
            );
        }
    }

    #[test]
    fn rejects_invalid_graphql_regex_filters() {
        let error = compile_graphql(
            r#"
            {
              Service(where: { name: { matches: "[" } }) {
                name
              }
            }
            "#,
        )
        .expect_err("invalid GraphQL regex should fail");

        assert!(
            error.to_string().contains("invalid GraphQL regex literal"),
            "{error}"
        );
    }

    #[test]
    fn rejects_non_string_graphql_regex_filters() {
        let error = compile_graphql(
            r"
            {
              Service(where: { name: { regex: 1 } }) {
                name
              }
            }
            ",
        )
        .expect_err("non-string GraphQL regex should fail");

        assert!(
            error
                .to_string()
                .contains("GraphQL regex filters require a string literal"),
            "{error}"
        );
    }

    #[test]
    fn rejects_conflicting_graphql_response_aliases() {
        let error = compile_graphql(
            r"
            {
              Service {
                value: name
                value: tier
              }
            }
            ",
        )
        .expect_err("conflicting GraphQL response aliases should fail");

        assert!(
            error
                .to_string()
                .contains("response alias 'value' selects conflicting fields"),
            "{error}"
        );
    }

    #[test]
    fn rejects_graphql_fragment_cycles() {
        let error = compile_graphql(
            r"
            query {
              Service { ...A }
            }

            fragment A on Service { ...B }
            fragment B on Service { ...A }
            ",
        )
        .expect_err("fragment cycles should fail");

        assert!(error.to_string().contains("forms a cycle"), "{error}");
    }

    #[test]
    fn rejects_graphql_fragment_type_mismatches() {
        let error = compile_graphql(
            r"
            query {
              Service { ...PersonFields }
            }

            fragment PersonFields on Person { name }
            ",
        )
        .expect_err("fragment type mismatches should fail");

        assert!(
            error
                .to_string()
                .contains("must match graph label 'Service'"),
            "{error}"
        );
    }

    #[test]
    fn rejects_graphql_root_fragment_type_mismatches() {
        let error = compile_graphql(
            r"
            query {
              ...NotQuery
            }

            fragment NotQuery on Service {
              Service { name }
            }
            ",
        )
        .expect_err("root fragment type mismatches should fail");

        assert!(error.to_string().contains("must be Query"), "{error}");
    }

    #[test]
    fn rejects_unknown_graphql_directives() {
        let error = compile_graphql(
            r"
            {
              Service { name @defer }
            }
            ",
        )
        .expect_err("unknown GraphQL directives should fail");

        assert!(
            error
                .to_string()
                .contains("unsupported GraphQL directive '@defer'"),
            "{error}"
        );
    }

    #[test]
    fn rejects_invalid_graphql_directives() {
        for (query, message) in [
            (
                r"
                {
                  Service { name @include(unless: true) }
                }
                ",
                "requires an 'if' argument",
            ),
            (
                r"
                {
                  Service { name @include(if: true) @include(if: false) }
                }
                ",
                "directive '@include' is repeated",
            ),
            (
                r"
                query Services($includeName: String!) {
                  Service { name @include(if: $includeName) }
                }
                ",
                "must be a boolean",
            ),
        ] {
            let variables = BTreeMap::from([(
                "includeName".to_string(),
                GraphqlVariableValue::Literal(Literal::String("yes".to_string())),
            )]);
            let error = compile_graphql_with_variables(query, &variables)
                .expect_err("invalid GraphQL directive should fail");

            assert!(error.to_string().contains(message), "{error}");
        }
    }

    #[test]
    fn compiles_nested_outgoing_relationship_query_with_declaration() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let plan = compile_graphql_for_graph(
            &graph,
            r#"
            {
              Person(where: { team: { eq: "infra" } }) {
                owner: name
                out_OWNS(
                  to: Service
                  relationshipWhere: { source: { eq: "pagerduty" } }
                  where: { tier: { eq: "prod" } }
                ) {
                  service: name
                  risk
                  _edge {
                    ownershipKind: __typename
                    ownershipSource: source
                  }
                }
              }
            }
            "#,
        )
        .expect("nested GraphQL query should compile");

        assert_eq!(
            plan.nodes,
            vec![
                NodePattern {
                    variable: "person".to_string(),
                    label: "Person".to_string(),
                },
                NodePattern {
                    variable: "service1".to_string(),
                    label: "Service".to_string(),
                },
            ]
        );
        assert_eq!(
            plan.relationships,
            vec![RelationshipPattern {
                variable: Some("relationship0".to_string()),
                relationship_type: "OWNS".to_string(),
                left: "person".to_string(),
                direction: Direction::Outgoing,
                right: "service1".to_string(),
            }]
        );
        assert_eq!(
            plan.projections,
            vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "person".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("owner".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "service1".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("service".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "service1".to_string(),
                        property: "risk".to_string(),
                    },
                    alias: Some("service1_risk".to_string()),
                },
                Projection::Literal {
                    literal: Literal::String("OWNS".to_string()),
                    alias: "ownershipKind".to_string(),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "relationship0".to_string(),
                        property: "source".to_string(),
                    },
                    alias: Some("ownershipSource".to_string()),
                },
            ]
        );
        assert_eq!(plan.predicates.len(), 3);
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.variable == "person" && predicate.property.property == "team"
        }));
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.variable == "service1" && predicate.property.property == "tier"
        }));
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.variable == "relationship0"
                && predicate.property.property == "source"
        }));
    }

    #[test]
    fn compiles_nested_relationship_query_with_inferred_endpoint_label() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let plan = compile_graphql_for_graph(
            &graph,
            r#"
            {
              Person {
                owner: name
                out_OWNS(where: { tier: { eq: "prod" } }) {
                  service: name
                }
              }
            }
            "#,
        )
        .expect("unambiguous GraphQL relationship endpoint labels should infer from declaration");

        assert_eq!(
            plan.nodes,
            vec![
                NodePattern {
                    variable: "person".to_string(),
                    label: "Person".to_string(),
                },
                NodePattern {
                    variable: "service1".to_string(),
                    label: "Service".to_string(),
                },
            ]
        );
        assert_eq!(
            plan.relationships,
            vec![RelationshipPattern {
                variable: None,
                relationship_type: "OWNS".to_string(),
                left: "person".to_string(),
                direction: Direction::Outgoing,
                right: "service1".to_string(),
            }]
        );
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.variable == "service1" && predicate.property.property == "tier"
        }));
    }

    #[test]
    fn compiles_graphql_relationship_existence_filters() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let plan = compile_graphql_for_graph(
            &graph,
            r#"
            {
              Person(
                where: {
                  out_OWNS: {
                    where: { tier: { eq: "prod" } }
                    relationshipWhere: { source: { eq: "pagerduty" } }
                  }
                }
              ) {
                name
              }
            }
            "#,
        )
        .expect("GraphQL relationship existence filters should compile");

        assert_eq!(plan.nodes.len(), 1);
        assert!(plan.relationships.is_empty());
        let Some(PredicateExpression::ExistsPattern(pattern)) = plan.predicate else {
            panic!("expected relationship filter to compile as an EXISTS pattern");
        };
        assert_eq!(
            pattern.nodes,
            vec![NodePattern {
                variable: "graphql_exists_service".to_string(),
                label: "Service".to_string(),
            }]
        );
        assert_eq!(
            pattern.relationships,
            vec![RelationshipPattern {
                variable: Some("graphql_exists_relationship".to_string()),
                relationship_type: "OWNS".to_string(),
                left: "person".to_string(),
                direction: Direction::Outgoing,
                right: "graphql_exists_service".to_string(),
            }]
        );
        assert!(matches!(
            pattern.predicate.as_deref(),
            Some(PredicateExpression::And { .. })
        ));
    }

    #[test]
    fn compiles_graphql_relationship_existence_filter_variables() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let variables = BTreeMap::from([(
            "ownsFilter".to_string(),
            variable_object([
                (
                    "where",
                    variable_object([(
                        "tier",
                        variable_object([(
                            "eq",
                            GraphqlVariableValue::Literal(Literal::String("prod".to_string())),
                        )]),
                    )]),
                ),
                (
                    "relationshipWhere",
                    variable_object([(
                        "source",
                        variable_object([(
                            "eq",
                            GraphqlVariableValue::Literal(Literal::String("pagerduty".to_string())),
                        )]),
                    )]),
                ),
            ]),
        )]);
        let plan = compile_graphql_for_graph_with_variables(
            &graph,
            r"
            query People($ownsFilter: PersonOutOWNSFilter!) {
              Person(where: { out_OWNS: $ownsFilter }) {
                name
              }
            }
            ",
            &variables,
        )
        .expect("GraphQL relationship existence filter variables should compile");

        assert!(plan.relationships.is_empty());
        assert!(matches!(
            plan.predicate,
            Some(PredicateExpression::ExistsPattern(_))
        ));
    }

    #[test]
    fn compiles_prefix_named_graphql_where_properties_when_no_relationship_matches() {
        let graph = Declaration::from_yaml(
            r"
version: 1
name: prefix_property_test
nodes:
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      name: service_name
      out_status: out_status
",
        )
        .expect("graph should parse");
        let plan = compile_graphql_for_graph(
            &graph,
            r#"
            {
              Service(where: { out_status: { eq: "green" } }) {
                name
              }
            }
            "#,
        )
        .expect("prefix-named properties should compile as property filters");

        assert!(plan.relationships.is_empty());
        assert!(matches!(
            plan.predicates.as_slice(),
            [PropertyPredicate {
                property: PropertyRef { variable, property },
                operator: ComparisonOperator::Equal,
                rhs: PredicateRhs::Literal(Literal::String(value)),
            }] if variable == "service" && property == "out_status" && value == "green"
        ));
    }

    #[test]
    fn compiles_nested_relationship_query_with_object_variables() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let variables = BTreeMap::from([
            (
                "personFilter".to_string(),
                variable_object([(
                    "team",
                    variable_object([(
                        "eq",
                        GraphqlVariableValue::Literal(Literal::String("infra".to_string())),
                    )]),
                )]),
            ),
            (
                "serviceFilter".to_string(),
                variable_object([(
                    "tier",
                    variable_object([(
                        "eq",
                        GraphqlVariableValue::Literal(Literal::String("prod".to_string())),
                    )]),
                )]),
            ),
            (
                "ownershipFilter".to_string(),
                variable_object([(
                    "source",
                    variable_object([(
                        "eq",
                        GraphqlVariableValue::Literal(Literal::String("pagerduty".to_string())),
                    )]),
                )]),
            ),
        ]);
        let plan = compile_graphql_for_graph_with_variables(
            &graph,
            r"
            query OwnedServices(
              $personFilter: PersonWhere!
              $serviceFilter: ServiceWhere!
              $ownershipFilter: OwnershipWhere!
            ) {
              Person(where: $personFilter) {
                owner: name
                out_OWNS(
                  to: Service
                  where: $serviceFilter
                  relationshipWhere: $ownershipFilter
                ) {
                  service: name
                  _edge { source }
                }
              }
            }
            ",
            &variables,
        )
        .expect("nested GraphQL object variables should compile");

        assert_eq!(plan.predicates.len(), 3);
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.variable == "person" && predicate.property.property == "team"
        }));
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.variable == "service1" && predicate.property.property == "tier"
        }));
        assert!(plan.predicates.iter().any(|predicate| {
            predicate.property.variable == "relationship0"
                && predicate.property.property == "source"
        }));
    }

    #[test]
    fn compiles_nested_relationship_identity_filters() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let plan = compile_graphql_for_graph(
            &graph,
            r#"
            query {
              Person {
                out_OWNS(
                  to: Service
                  relationshipWhere: {
                    _id: { eq: 200 }
                    _elementId: { eq: "200" }
                  }
                ) {
                  name
                }
              }
            }
            "#,
        )
        .expect("GraphQL relationship identity filters should compile");

        assert!(plan.predicates.is_empty());
        assert!(matches!(
            plan.predicate,
            Some(PredicateExpression::And { .. })
        ));
        assert!(matches!(
            plan.relationships.as_slice(),
            [RelationshipPattern {
                variable: Some(variable),
                ..
            }] if variable == "relationship0"
        ));
    }

    #[test]
    fn compiles_nested_boolean_where_filters_with_declaration() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let plan = compile_graphql_for_graph(
            &graph,
            r#"
            {
              Person(where: { or: [{ team: { eq: "infra" } }, { team: { eq: "analytics" } }] }) {
                owner: name
                out_OWNS(
                  to: Service
                  where: { or: [{ tier: { eq: "prod" } }, { name: { contains: "experiments" } }] }
                  relationshipWhere: { not: { source: { isNull: true } } }
                ) {
                  service: name
                  _edge { source }
                }
              }
            }
            "#,
        )
        .expect("nested GraphQL boolean where filters should compile");

        assert!(plan.predicates.is_empty());
        assert!(matches!(
            plan.predicate,
            Some(PredicateExpression::And { .. })
        ));
        assert!(matches!(
            plan.relationships.as_slice(),
            [RelationshipPattern {
                variable: Some(variable),
                ..
            }] if variable == "relationship0"
        ));
    }

    #[test]
    fn compiles_nested_graphql_regex_and_xor_filters_with_declaration() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let plan = compile_graphql_for_graph(
            &graph,
            r#"
            {
              Person(where: { name: { matches: "^Grace" } }) {
                owner: name
                out_OWNS(
                  to: Service
                  where: { name: { regex: "^(billing|deploy)" } }
                  relationshipWhere: {
                    xor: [
                      { source: { regex: "^pager" } }
                      { source: { isNull: true } }
                    ]
                  }
                ) {
                  service: name
                  _edge { source }
                }
              }
            }
            "#,
        )
        .expect("nested GraphQL regex and xor filters should compile");

        assert_eq!(plan.predicates.len(), 2);
        assert!(matches!(
            plan.predicate,
            Some(PredicateExpression::Xor { .. })
        ));
        assert!(matches!(
            plan.relationships.as_slice(),
            [RelationshipPattern {
                variable: Some(variable),
                ..
            }] if variable == "relationship0"
        ));
    }

    #[test]
    fn compiles_nested_incoming_relationship_query_with_declaration() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let plan = compile_graphql_for_graph(
            &graph,
            r"
            {
              Service {
                service: name
                owners: in_OWNS(from: Person) {
                  owner: name
                  team
                  _edge {
                    source
                  }
                }
              }
            }
            ",
        )
        .expect("incoming nested GraphQL query should compile");

        assert_eq!(
            plan.relationships,
            vec![RelationshipPattern {
                variable: Some("owners_edge".to_string()),
                relationship_type: "OWNS".to_string(),
                left: "service".to_string(),
                direction: Direction::Incoming,
                right: "owners".to_string(),
            }]
        );
        assert_eq!(
            plan.projections,
            vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("service".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "owners".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("owner".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "owners".to_string(),
                        property: "team".to_string(),
                    },
                    alias: Some("owners_team".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "owners_edge".to_string(),
                        property: "source".to_string(),
                    },
                    alias: Some("owners_edge_source".to_string()),
                },
            ]
        );
    }

    #[test]
    fn rejects_nested_graphql_selection() {
        let error = compile_graphql(
            r"
            {
              Service {
                name
                out_DEPENDS_ON { name }
              }
            }
            ",
        )
        .expect_err("nested selections should be rejected for first GraphQL slice");

        assert!(
            error.to_string().contains("relationship nesting"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_unknown_graphql_order_by_keys() {
        let error = compile_graphql(
            r"
            {
              Service(orderBy: { field: name, direction: ASC, collation: CASE_INSENSITIVE }) {
                name
              }
            }
            ",
        )
        .expect_err("unknown orderBy keys should be rejected");

        assert!(
            error
                .to_string()
                .contains("unsupported GraphQL orderBy key"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_unknown_graphql_order_by_null_placement() {
        let error = compile_graphql(
            r"
            {
              Service(orderBy: { field: name, direction: ASC, nulls: MIDDLE }) {
                name
              }
            }
            ",
        )
        .expect_err("unknown orderBy null placement should be rejected");

        assert!(
            error
                .to_string()
                .contains("GraphQL orderBy nulls must be FIRST, LAST"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_multi_field_graphql_shorthand_order_by_objects() {
        let error = compile_graphql(
            r"
            {
              Service(orderBy: { risk: DESC, name: ASC }) {
                name
              }
            }
            ",
        )
        .expect_err("multi-field shorthand orderBy object should fail");

        assert!(
            error
                .to_string()
                .contains("shorthand orderBy entries must contain exactly one field"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_empty_graphql_boolean_where_lists() {
        let error = compile_graphql(
            r"
            {
              Service(where: { or: [] }) { name }
            }
            ",
        )
        .expect_err("empty boolean filter list should fail");

        assert!(
            error.to_string().contains("require at least one object"),
            "{error:?}"
        );
    }

    #[test]
    fn rejects_duplicate_nested_graphql_relationship_arguments() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let cases = [
            (
                r"
                {
                  Person {
                    out_OWNS(
                      to: Service
                      to: Person
                    ) {
                      name
                    }
                  }
                }
                ",
                "GraphQL relationship argument 'to' is specified more than once",
            ),
            (
                r#"
                {
                  Person {
                    out_OWNS(
                      where: { tier: { eq: "prod" } }
                      where: { name: { eq: "billing-api" } }
                    ) {
                      name
                    }
                  }
                }
                "#,
                "GraphQL relationship argument 'where' is specified more than once",
            ),
            (
                r#"
                {
                  Person {
                    out_OWNS(
                      relationshipWhere: { source: { eq: "pagerduty" } }
                      relationshipWhere: { source: { eq: "catalog" } }
                    ) {
                      name
                    }
                  }
                }
                "#,
                "GraphQL relationship argument 'relationshipWhere' is specified more than once",
            ),
        ];

        for (query, expected) in cases {
            let error = compile_graphql_for_graph(&graph, query)
                .expect_err("duplicate GraphQL relationship arguments should fail");
            assert!(
                error.to_string().contains(expected),
                "expected {expected:?}, got {error}"
            );
        }
    }

    #[test]
    fn rejects_wrong_nested_graphql_relationship_endpoint_argument_before_inference() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let error = compile_graphql_for_graph(
            &graph,
            r"
            {
              Person {
                out_OWNS(from: Person) {
                  name
                }
              }
            }
            ",
        )
        .expect_err("wrong GraphQL relationship endpoint argument should fail");

        assert!(
            error
                .to_string()
                .contains("GraphQL relationship field 'out_OWNS' requires 'to' instead of 'from'"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_nested_graphql_relationship_endpoint_mismatches() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let error = compile_graphql_for_graph(
            &graph,
            r"
            {
              Person {
                out_OWNS(to: Person) { name }
              }
            }
            ",
        )
        .expect_err("endpoint mismatch should be rejected");

        assert!(
            error.to_string().contains("has no mapping"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_ambiguous_inferred_nested_graphql_relationship_endpoints() {
        let graph = Declaration::from_yaml(
            r"
version: 1
name: ambiguous_relationship_endpoint
nodes:
  - label: Person
    table: { schema: ops, name: people }
    key: id
  - label: Service
    table: { schema: ops, name: services }
    key: id
  - label: Team
    table: { schema: ops, name: teams }
    key: id
relationships:
  - type: OWNS
    table: { schema: ops, name: person_service_ownerships }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
  - type: OWNS
    table: { schema: ops, name: person_team_ownerships }
    from: { label: Person, key: person_id }
    to: { label: Team, key: team_id }
",
        )
        .expect("graph should parse");
        let error = compile_graphql_for_graph(
            &graph,
            r"
            {
              Person {
                out_OWNS { _id }
              }
            }
            ",
        )
        .expect_err("ambiguous inferred endpoint should be rejected");

        assert!(
            error
                .to_string()
                .contains("maps graph label 'Person' to multiple endpoint labels"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_unknown_inferred_nested_graphql_relationship_types() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let error = compile_graphql_for_graph(
            &graph,
            r"
            {
              Person {
                out_MANAGES { _id }
              }
            }
            ",
        )
        .expect_err("unknown inferred relationship type should be rejected");

        assert!(
            error
                .to_string()
                .contains("unknown GraphQL relationship type 'MANAGES'"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_nested_graphql_relationship_row_modifiers() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let error = compile_graphql_for_graph(
            &graph,
            r"
            {
              Person {
                out_OWNS(to: Service, first: 2) { name }
              }
            }
            ",
        )
        .expect_err("nested GraphQL relationship first argument should be rejected");

        assert!(
            error
                .to_string()
                .contains("nested relationship fields do not support row modifiers"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_graphql_edge_fragment_type_mismatches() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");
        let error = compile_graphql_for_graph(
            &graph,
            r"
            {
              Person {
                out_OWNS(to: Service) {
                  service: name
                  _edge { ...DependencyEdge }
                }
              }
            }

            fragment DependencyEdge on DEPENDS_ON {
              source
            }
            ",
        )
        .expect_err("edge fragment type mismatch should be rejected");

        assert!(
            error.to_string().contains("edge fragment type condition"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn generates_graphql_schema_sdl_for_declaration() {
        let graph = Declaration::from_yaml(TEST_GRAPH).expect("graph should parse");

        let sdl = graphql_schema_sdl_for_graph(&graph).expect("schema SDL should generate");

        graphql_parser::schema::parse_schema::<String>(&sdl)
            .expect("generated SDL should parse as GraphQL schema");
        assert!(sdl.contains("scalar CoralGraphValue"));
        assert!(sdl.contains("  ASCENDING"));
        assert!(sdl.contains("  DESCENDING"));
        assert!(sdl.contains("enum CoralGraphNullOrder {\n  FIRST\n  LAST\n}"));
        assert!(sdl.contains(
            "Person(where: PersonWhere, orderBy: [PersonOrderBy!], limit: Int, first: Int, offset: Int, skip: Int, distinct: Boolean): [Person!]!"
        ));
        assert!(sdl.contains(
            "service(where: ServiceWhere, orderBy: [ServiceOrderBy!], limit: Int, first: Int, offset: Int, skip: Int, distinct: Boolean): [Service!]!"
        ));
        assert!(sdl.contains(
            "Services(where: ServiceWhere, orderBy: [ServiceOrderBy!], limit: Int, first: Int, offset: Int, skip: Int, distinct: Boolean): [Service!]!"
        ));
        assert!(sdl.contains(
            "services(where: ServiceWhere, orderBy: [ServiceOrderBy!], limit: Int, first: Int, offset: Int, skip: Int, distinct: Boolean): [Service!]!"
        ));
        assert!(sdl.contains(
            "input PersonOrderBy {\n  field: PersonOrderField!\n  direction: CoralGraphOrderDirection = ASC\n  nulls: CoralGraphNullOrder\n}"
        ));
        assert!(sdl.contains("input PersonWhere {"));
        assert!(sdl.contains("  _id: CoralGraphIdentityFilter"));
        assert!(sdl.contains("  _elementId: CoralGraphElementIdFilter"));
        assert!(sdl.contains("  out_OWNS: PersonOutOWNSFilter"));
        assert!(sdl.contains("  _and: [PersonWhere!]"));
        assert!(sdl.contains("  _not: PersonWhere"));
        assert!(sdl.contains("  AND: [PersonWhere!]"));
        assert!(sdl.contains("  NOT: PersonWhere"));
        assert!(sdl.contains("enum PersonOrderField {"));
        assert!(sdl.contains("  team"));
        assert!(sdl.contains("enum PersonAggregateField {"));
        assert!(sdl.contains("  _count(field: PersonAggregateField): Int"));
        assert!(sdl.contains("  _countDistinct(field: PersonAggregateField!): Int"));
        assert!(sdl.contains("  _collect(field: PersonAggregateField!): [CoralGraphValue!]"));
        assert!(
            sdl.contains("  _collectDistinct(field: PersonAggregateField!): [CoralGraphValue!]")
        );
        assert!(sdl.contains("  _avg(field: PersonAggregateField!): CoralGraphValue"));
        assert!(sdl.contains("  _sumDistinct(field: PersonAggregateField!): CoralGraphValue"));
        assert!(sdl.contains("  _avgDistinct(field: PersonAggregateField!): CoralGraphValue"));
        assert!(sdl.contains("  _median(field: PersonAggregateField!): CoralGraphValue"));
        assert!(sdl.contains("  _medianDistinct(field: PersonAggregateField!): CoralGraphValue"));
        assert!(sdl.contains("  _stDev(field: PersonAggregateField!): CoralGraphValue"));
        assert!(sdl.contains("  _stDevP(field: PersonAggregateField!): CoralGraphValue"));
        assert!(sdl.contains("  _minDistinct(field: PersonAggregateField!): CoralGraphValue"));
        assert!(sdl.contains("  _maxDistinct(field: PersonAggregateField!): CoralGraphValue"));
        assert!(sdl.contains(
            "out_OWNS(to: PersonOutOWNSToLabel = Service, where: ServiceWhere, relationshipWhere: OWNSRelationshipWhere): [Service!]!"
        ));
        assert!(sdl.contains(
            "input PersonOutOWNSFilter {\n  to: PersonOutOWNSToLabel = Service\n  where: ServiceWhere\n  relationshipWhere: OWNSRelationshipWhere\n}"
        ));
        assert!(sdl.contains("enum PersonOutOWNSToLabel {\n  Service\n}"));
        assert!(sdl.contains("type OWNS {"));
        assert!(sdl.contains("  source: CoralGraphValue"));
    }

    #[test]
    fn rejects_graphql_schema_sdl_for_invalid_graphql_names() {
        let graph = Declaration::from_yaml(
            r"
version: 1
name: invalid_graphql
nodes:
  - label: Service-Account
    table: { schema: ops, name: services }
    key: id
",
        )
        .expect("graph should parse");

        let error = graphql_schema_sdl_for_graph(&graph)
            .expect_err("invalid GraphQL type names should be rejected");

        assert!(
            error.to_string().contains("not a valid GraphQL name"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_graphql_schema_sdl_for_reserved_property_names() {
        let graph = Declaration::from_yaml(
            r"
version: 1
name: reserved_property
nodes:
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      _id: source_id
",
        )
        .expect("graph should parse");

        let error = graphql_schema_sdl_for_graph(&graph)
            .expect_err("reserved GraphQL property names should be rejected");

        assert!(
            error.to_string().contains("reserved GraphQL virtual field"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_graphql_schema_sdl_for_reserved_aggregate_property_names() {
        let graph = Declaration::from_yaml(
            r"
version: 1
name: reserved_aggregate_property
nodes:
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      _median: risk_score
",
        )
        .expect("graph should parse");

        let error = graphql_schema_sdl_for_graph(&graph)
            .expect_err("reserved GraphQL aggregate property names should be rejected");

        assert!(
            error.to_string().contains("reserved GraphQL virtual field"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_graphql_schema_sdl_for_ambiguous_relationship_fields() {
        let graph = Declaration::from_yaml(
            r"
version: 1
name: ambiguous_relationship_field
nodes:
  - label: Person
    table: { schema: ops, name: people }
    key: id
  - label: Service
    table: { schema: ops, name: services }
    key: id
  - label: Team
    table: { schema: ops, name: teams }
    key: id
relationships:
  - type: OWNS
    table: { schema: ops, name: person_service_ownerships }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
  - type: OWNS
    table: { schema: ops, name: person_team_ownerships }
    from: { label: Person, key: person_id }
    to: { label: Team, key: team_id }
",
        )
        .expect("graph should parse");

        let error = graphql_schema_sdl_for_graph(&graph)
            .expect_err("duplicate GraphQL relationship fields should be rejected");

        assert!(
            error
                .to_string()
                .contains("GraphQL field 'out_OWNS' would be generated more than once"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_graphql_schema_sdl_for_ambiguous_root_aliases() {
        let graph = Declaration::from_yaml(
            r"
version: 1
name: ambiguous_roots
nodes:
  - label: User
    table: { schema: ops, name: users }
    key: id
    properties:
      name: name
  - label: user
    table: { schema: ops, name: lower_users }
    key: id
    properties:
      name: name
",
        )
        .expect("graph should parse");

        let error = graphql_schema_sdl_for_graph(&graph)
            .expect_err("ambiguous root aliases should fail SDL generation");

        assert!(
            error
                .to_string()
                .contains("GraphQL query root field 'user' would be generated more than once"),
            "unexpected error: {error}"
        );
    }

    const TEST_GRAPH: &str = r"
version: 1
name: test
nodes:
  - label: Person
    table: { schema: ops, name: people }
    key: id
    properties:
      name: full_name
      team: team
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      name: service_name
      tier: tier
      risk: risk_score
relationships:
  - type: OWNS
    table: { schema: ops, name: ownerships }
    key: ownership_id
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
    properties:
      source: source
";
}
