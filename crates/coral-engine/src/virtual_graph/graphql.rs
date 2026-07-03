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
use super::diagnostic_codes;
use super::graphql_aggregate::{
    GRAPHQL_PROPERTY_AGGREGATE_FIELDS, GraphqlAggregateArgumentSpec, GraphqlAggregateFieldSpec,
    GraphqlAggregateFunctionSpec, graphql_property_aggregate_field,
};
use super::ir::{
    AggregateFunction, AggregateTarget, ComparisonOperator, Direction, ElementIdPredicate,
    ExistsPatternPredicate, GraphPlan, KeyPredicate, Literal, NodePattern, NullOrder,
    OrderDirection, OrderExpression, OrderKey, PredicateExpression, PredicateRhs, Projection,
    PropertyPredicate, PropertyRef, RelationshipPattern,
};
use crate::CoreError;

mod response_signatures;

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "GraphQL response-signature helpers are split into a child module while preserving parent call sites."
)]
use self::response_signatures::*;

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
                    diagnostic_codes::MISSING_GRAPHQL_VARIABLE,
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
        Diagnostic::new(
            diagnostic_codes::GRAPHQL_PARSE_ERROR,
            "query",
            error.to_string(),
        )
        .into_core_error()
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
                validate_graphql_directive_syntax(
                    &fragment.directives,
                    format!("query.definitions[{index}].directives"),
                )?;
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
            let included = selection_is_included(&query.directives, "query.directives", &context)?;
            let mut plan = compile_root_selection_set(
                &query.selection_set,
                graph,
                "query.selectionSet",
                &context,
            )?;
            if !included {
                append_where_predicate(&mut plan, Some(PredicateExpression::Boolean(false)));
            }
            Ok(plan)
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
    let mut root_projections = Vec::new();
    collect_root_fields(
        selection_set,
        &path,
        context,
        &mut Vec::new(),
        &mut root_fields,
        &mut root_projections,
    )?;
    let [root] = root_fields.as_slice() else {
        return Err(unsupported(
            path,
            "GraphQL virtual graph queries must select exactly one included root node field",
        ));
    };
    let mut plan = compile_root_field(&root.field, graph, &root.path, context)?;
    for (projection, projection_path) in root_projections {
        push_graphql_projection(&mut plan, projection, &projection_path)?;
    }
    Ok(plan)
}

fn collect_root_fields<'query>(
    selection_set: &SelectionSet<'query, String>,
    path: &str,
    context: &GraphqlCompileContext<'_, 'query>,
    fragment_stack: &mut Vec<String>,
    root_fields: &mut Vec<GraphqlRootFieldSelection<'query>>,
    root_projections: &mut Vec<(Projection, String)>,
) -> Result<(), CoreError> {
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
                if field.name == "__typename" {
                    root_projections
                        .push((compile_root_typename_field(field, &item_path)?, item_path));
                } else {
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
                if !fragment_definition_is_included(fragment, context)? {
                    continue;
                }
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
                    root_projections,
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
                    root_projections,
                )?;
            }
        }
    }
    Ok(())
}

fn compile_root_typename_field(
    field: &Field<'_, String>,
    path: &str,
) -> Result<Projection, CoreError> {
    if !field.arguments.is_empty() {
        return Err(unsupported(
            format!("{path}.arguments"),
            "GraphQL root __typename arguments are not supported",
        ));
    }
    if !field.selection_set.items.is_empty() {
        return Err(unsupported(
            format!("{path}.selectionSet"),
            "GraphQL root __typename must be a scalar field",
        ));
    }
    Ok(Projection::Literal {
        literal: Literal::String("Query".to_string()),
        alias: graphql_response_name(field),
    })
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
        aggregate,
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
        &GraphqlAggregateFieldSpec {
            field_name: "_count",
            function: GraphqlAggregateFunctionSpec::Fixed(AggregateFunction::Count),
            distinct: false,
            arguments: GraphqlAggregateArgumentSpec::Field,
            return_type: super::graphql_aggregate::GraphqlAggregateReturnType::Int,
        },
        alias,
    )
}

fn compile_property_aggregate_field(
    field: &Field<'_, String>,
    context: &NodeContext,
    path: &str,
    compile_context: &GraphqlCompileContext<'_, '_>,
    aggregate: &GraphqlAggregateFieldSpec,
    alias: String,
) -> Result<Projection, CoreError> {
    let (property, function) =
        compile_property_aggregate_arguments(field, path, compile_context, aggregate)?;
    Ok(Projection::Aggregate {
        function,
        target: AggregateTarget::Property(PropertyRef {
            variable: context.variable.clone(),
            property,
        }),
        distinct: aggregate.distinct,
        alias,
    })
}

fn compile_property_aggregate_arguments(
    field: &Field<'_, String>,
    path: &str,
    compile_context: &GraphqlCompileContext<'_, '_>,
    aggregate: &GraphqlAggregateFieldSpec,
) -> Result<(String, AggregateFunction), CoreError> {
    let property = match aggregate.arguments {
        GraphqlAggregateArgumentSpec::Field => {
            compile_single_aggregate_field_argument(field, path, compile_context)?
        }
        GraphqlAggregateArgumentSpec::FieldAndPercentile => {
            let (property, percentile) =
                compile_field_and_percentile_aggregate_arguments(field, path, compile_context)?;
            return Ok((property, AggregateFunction::PercentileCont { percentile }));
        }
    };
    let GraphqlAggregateFunctionSpec::Fixed(function) = aggregate.function else {
        return Err(CoreError::internal(
            "GraphQL aggregate argument shape did not match function",
        ));
    };
    Ok((property, function))
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

fn compile_field_and_percentile_aggregate_arguments(
    field: &Field<'_, String>,
    path: &str,
    compile_context: &GraphqlCompileContext<'_, '_>,
) -> Result<(String, OrderedFloat<f64>), CoreError> {
    if field.arguments.len() != 2 {
        return Err(unsupported(
            format!("{path}.arguments"),
            format!(
                "GraphQL aggregate field '{}' requires exactly 'field' and 'percentile' arguments",
                field.name
            ),
        ));
    }
    let mut property = None;
    let mut percentile = None;
    for (index, (name, value)) in field.arguments.iter().enumerate() {
        match name.as_str() {
            "field" => {
                if property.is_some() {
                    return Err(unsupported(
                        format!("{path}.arguments[{index}].field"),
                        "GraphQL aggregate field argument 'field' was provided more than once",
                    ));
                }
                property = Some(compile_name_value(
                    value,
                    format!("{path}.arguments[{index}].field"),
                    compile_context,
                )?);
            }
            "percentile" => {
                if percentile.is_some() {
                    return Err(unsupported(
                        format!("{path}.arguments[{index}].percentile"),
                        "GraphQL aggregate field argument 'percentile' was provided more than once",
                    ));
                }
                percentile = Some(compile_percentile_aggregate_argument(
                    value,
                    format!("{path}.arguments[{index}].percentile"),
                    compile_context,
                )?);
            }
            _ => {
                return Err(unsupported(
                    format!("{path}.arguments[{index}].{name}"),
                    format!("unsupported GraphQL aggregate argument '{name}'"),
                ));
            }
        }
    }
    let property = property.ok_or_else(|| {
        unsupported(
            format!("{path}.arguments"),
            format!(
                "GraphQL aggregate field '{}' requires a 'field' argument",
                field.name
            ),
        )
    })?;
    let percentile = percentile.ok_or_else(|| {
        unsupported(
            format!("{path}.arguments"),
            format!(
                "GraphQL aggregate field '{}' requires a 'percentile' argument",
                field.name
            ),
        )
    })?;
    Ok((property, percentile))
}

fn compile_percentile_aggregate_argument(
    value: &Value<'_, String>,
    path: impl Into<String>,
    compile_context: &GraphqlCompileContext<'_, '_>,
) -> Result<OrderedFloat<f64>, CoreError> {
    let path = path.into();
    let literal = compile_literal(value, path.clone(), compile_context)?;
    let value = match literal {
        Literal::Integer(0) => 0.0,
        Literal::Integer(1) => 1.0,
        Literal::Integer(_) => {
            return Err(unsupported(
                path,
                "GraphQL percentile aggregate argument must be between 0.0 and 1.0 inclusive",
            ));
        }
        Literal::Float(value) => value.into_inner(),
        _ => {
            return Err(unsupported(
                path,
                "GraphQL percentile aggregate argument must be a numeric literal",
            ));
        }
    };
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        return Err(unsupported(
            path,
            "GraphQL percentile aggregate argument must be between 0.0 and 1.0 inclusive",
        ));
    }
    Ok(OrderedFloat(value))
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
    if !fragment_definition_is_included(fragment, selection_context.compile_context)? {
        return Ok(());
    }
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
    if !fragment_definition_is_included(fragment, compile_context)? {
        return Ok(());
    }
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

fn validate_graphql_directive_syntax(
    directives: &[Directive<'_, String>],
    path: impl Into<String>,
) -> Result<(), CoreError> {
    let path = path.into();
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
            "include" | "skip" => {
                validate_directive_if_argument_syntax(directive, &directive_path)?;
            }
            _ => {
                return Err(unsupported(
                    format!("{directive_path}.name"),
                    format!("unsupported GraphQL directive '@{}'", directive.name),
                ));
            }
        }
    }
    Ok(())
}

fn validate_directive_if_argument_syntax(
    directive: &Directive<'_, String>,
    path: &str,
) -> Result<(), CoreError> {
    let [(name, _)] = directive.arguments.as_slice() else {
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
    Ok(())
}

fn fragment_definition_is_included(
    fragment: &FragmentDefinition<'_, String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<bool, CoreError> {
    selection_is_included(
        &fragment.directives,
        format!("fragment.{}.directives", fragment.name),
        context,
    )
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
                if !fragment_definition_is_included(fragment, context)? {
                    continue;
                }
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
    Diagnostic::new(diagnostic_codes::UNSUPPORTED_GRAPHQL, path, message).into_core_error()
}

/// GraphQL read capability denominator grouped by the schema coverage report.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphqlCapabilitySurface {
    /// Canonical scalar property filter operators with accepted spellings.
    pub scalar_operators: Vec<GraphqlCapability>,
    /// Accepted scalar operator spellings mapped to canonical operators.
    pub scalar_operator_aliases: BTreeMap<&'static str, &'static str>,
    /// Canonical `_id` filter operators with accepted spellings.
    pub identity_operators: Vec<GraphqlCapability>,
    /// Accepted `_id` operator spellings mapped to canonical operators.
    pub identity_operator_aliases: BTreeMap<&'static str, &'static str>,
    /// Canonical `_elementId` filter operators with accepted spellings.
    pub element_id_operators: Vec<GraphqlCapability>,
    /// Accepted `_elementId` operator spellings mapped to canonical operators.
    pub element_id_operator_aliases: BTreeMap<&'static str, &'static str>,
    /// Flat aggregate fields accepted by the GraphQL frontend.
    pub aggregates: Vec<&'static str>,
    /// Boolean filter combinators with accepted spellings.
    pub boolean_combinators: Vec<GraphqlCapability>,
    /// Accepted boolean combinator spellings mapped to canonical operators.
    pub boolean_combinator_aliases: BTreeMap<&'static str, &'static str>,
    /// Supported GraphQL directives.
    pub directives: Vec<&'static str>,
    /// Canonical order directions with accepted spellings.
    pub order_directions: Vec<GraphqlCapability>,
    /// Accepted order direction spellings mapped to canonical directions.
    pub order_direction_aliases: BTreeMap<&'static str, &'static str>,
    /// Canonical null ordering modes with accepted spellings.
    pub null_orders: Vec<GraphqlCapability>,
    /// Accepted null ordering spellings mapped to canonical modes.
    pub null_order_aliases: BTreeMap<&'static str, &'static str>,
    /// Canonical root row modifiers with accepted spellings.
    pub row_modifiers: Vec<GraphqlCapability>,
    /// Accepted row modifier spellings mapped to canonical modifiers.
    pub row_modifier_aliases: BTreeMap<&'static str, &'static str>,
    /// Identity field sub-capabilities for selection, filtering, and ordering.
    pub identity_fields: Vec<&'static str>,
    /// Traversal sub-capabilities exposed by nested fields and existence filters.
    pub traversal: Vec<&'static str>,
    /// Metadata field sub-capabilities.
    pub meta_fields: Vec<&'static str>,
    /// Root node field forms generated by the GraphQL schema.
    pub root_field_forms: Vec<&'static str>,
    /// Distinct validation paths tracked separately from accepted capabilities.
    pub rejection_paths: Vec<GraphqlRejectionPath>,
}

/// A canonical GraphQL read capability and the spellings accepted for it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphqlCapability {
    /// Canonical capability identifier used by coverage reporting.
    pub canonical: &'static str,
    /// Accepted GraphQL spellings for the same canonical capability.
    pub aliases: Vec<&'static str>,
}

/// A stable GraphQL rejection path included in coverage accounting.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GraphqlRejectionPath {
    /// Stable rejection identifier used by coverage reporting.
    pub id: &'static str,
    /// Engine source file that owns the rejection string.
    pub source_file: &'static str,
    /// Source line for the rejection string in the current engine module.
    pub source_line: u32,
    /// Stable substring expected in structured GraphQL errors.
    pub stable_substring: &'static str,
}

/// Returns the GraphQL virtual-graph read capability surface used by coverage tooling.
#[must_use]
pub fn graphql_read_capability_surface() -> GraphqlCapabilitySurface {
    let (scalar_operators, scalar_operator_aliases) =
        graphql_where_capabilities(GRAPHQL_SCALAR_FILTER_OPERATOR_SPELLINGS);
    let (identity_operators, identity_operator_aliases) =
        graphql_where_capabilities(GRAPHQL_IDENTITY_FILTER_OPERATOR_SPELLINGS);
    let (element_id_operators, element_id_operator_aliases) =
        graphql_where_capabilities(GRAPHQL_ELEMENT_ID_FILTER_OPERATOR_SPELLINGS);
    let (boolean_combinators, boolean_combinator_aliases) =
        graphql_boolean_capabilities(GRAPHQL_BOOLEAN_OPERATOR_SPELLINGS);
    let (order_directions, order_direction_aliases) =
        canonical_capabilities(GRAPHQL_ORDER_DIRECTION_SPELLINGS);
    let (null_orders, null_order_aliases) = canonical_capabilities(GRAPHQL_NULL_ORDER_SPELLINGS);
    let (row_modifiers, row_modifier_aliases) = canonical_capabilities(GRAPHQL_ROW_MODIFIERS);

    let mut aggregates = vec!["_count"];
    aggregates.extend(
        GRAPHQL_PROPERTY_AGGREGATE_FIELDS
            .iter()
            .map(|field| field.field_name),
    );

    GraphqlCapabilitySurface {
        scalar_operators,
        scalar_operator_aliases,
        identity_operators,
        identity_operator_aliases,
        element_id_operators,
        element_id_operator_aliases,
        aggregates,
        boolean_combinators,
        boolean_combinator_aliases,
        directives: vec!["skip", "include"],
        order_directions,
        order_direction_aliases,
        null_orders,
        null_order_aliases,
        row_modifiers,
        row_modifier_aliases,
        identity_fields: vec![
            "_id.select",
            "_id.filter",
            "_id.order",
            "_elementId.select",
            "_elementId.filter",
            "_elementId.order",
        ],
        traversal: vec![
            "out",
            "in",
            "any",
            "_edge",
            "relationshipWhere",
            "existence",
        ],
        meta_fields: vec!["node.__typename", "edge.__typename"],
        root_field_forms: vec!["exact-label", "singular-alias", "plural-alias"],
        rejection_paths: graphql_rejection_paths(),
    }
}

const GRAPHQL_SCALAR_FILTER_OPERATOR_SPELLINGS: &[&str] = &[
    "eq",
    "equals",
    "ne",
    "neq",
    "notEq",
    "notEqual",
    "notEquals",
    "gt",
    "greaterThan",
    "gte",
    "ge",
    "greaterThanEqual",
    "greaterThanOrEqual",
    "lt",
    "lessThan",
    "lte",
    "le",
    "lessThanEqual",
    "lessThanOrEqual",
    "startsWith",
    "starts_with",
    "endsWith",
    "ends_with",
    "contains",
    "notStartsWith",
    "not_starts_with",
    "notEndsWith",
    "not_ends_with",
    "notContains",
    "not_contains",
    "matches",
    "regex",
    "notMatches",
    "notRegex",
    "not_regex",
    "in",
    "notIn",
    "not_in",
    "isNull",
    "is_null",
    "isNotNull",
    "is_not_null",
];

const GRAPHQL_IDENTITY_FILTER_OPERATOR_SPELLINGS: &[&str] = &[
    "eq",
    "equals",
    "ne",
    "neq",
    "notEq",
    "notEqual",
    "notEquals",
    "gt",
    "greaterThan",
    "gte",
    "ge",
    "greaterThanEqual",
    "greaterThanOrEqual",
    "lt",
    "lessThan",
    "lte",
    "le",
    "lessThanEqual",
    "lessThanOrEqual",
    "in",
    "notIn",
    "not_in",
    "isNull",
    "is_null",
    "isNotNull",
    "is_not_null",
];

const GRAPHQL_ELEMENT_ID_FILTER_OPERATOR_SPELLINGS: &[&str] =
    GRAPHQL_SCALAR_FILTER_OPERATOR_SPELLINGS;

const GRAPHQL_BOOLEAN_OPERATOR_SPELLINGS: &[&str] = &[
    "and", "AND", "_and", "or", "OR", "_or", "xor", "XOR", "_xor", "not", "NOT", "_not",
];

const GRAPHQL_ORDER_DIRECTION_SPELLINGS: &[(&str, &str)] = &[
    ("ASC", "ASC"),
    ("ASCENDING", "ASC"),
    ("DESC", "DESC"),
    ("DESCENDING", "DESC"),
];

const GRAPHQL_NULL_ORDER_SPELLINGS: &[(&str, &str)] = &[
    ("FIRST", "FIRST"),
    ("NULLS_FIRST", "FIRST"),
    ("LAST", "LAST"),
    ("NULLS_LAST", "LAST"),
];

const GRAPHQL_ROW_MODIFIERS: &[(&str, &str)] = &[
    ("where", "where"),
    ("orderBy", "orderBy"),
    ("limit", "limit"),
    ("first", "limit"),
    ("offset", "offset"),
    ("skip", "offset"),
    ("distinct", "distinct"),
];

fn graphql_where_capabilities(
    spellings: &'static [&'static str],
) -> (Vec<GraphqlCapability>, BTreeMap<&'static str, &'static str>) {
    let mut grouped = BTreeMap::<&'static str, Vec<&'static str>>::new();
    let mut aliases = BTreeMap::new();
    for spelling in spellings {
        if let Some(operator) = classify_graphql_where_operator(spelling) {
            let canonical = canonical_graphql_where_operator_name(operator);
            grouped.entry(canonical).or_default().push(*spelling);
            aliases.insert(*spelling, canonical);
        }
    }
    (capabilities_from_grouped(grouped), aliases)
}

fn graphql_boolean_capabilities(
    spellings: &'static [&'static str],
) -> (Vec<GraphqlCapability>, BTreeMap<&'static str, &'static str>) {
    let mut grouped = BTreeMap::<&'static str, Vec<&'static str>>::new();
    let mut aliases = BTreeMap::new();
    for spelling in spellings {
        if let Some(operator) = graphql_boolean_operator(spelling) {
            let canonical = canonical_graphql_boolean_operator_name(operator);
            grouped.entry(canonical).or_default().push(*spelling);
            aliases.insert(*spelling, canonical);
        }
    }
    (capabilities_from_grouped(grouped), aliases)
}

fn canonical_capabilities(
    spellings: &'static [(&'static str, &'static str)],
) -> (Vec<GraphqlCapability>, BTreeMap<&'static str, &'static str>) {
    let mut grouped = BTreeMap::<&'static str, Vec<&'static str>>::new();
    let mut aliases = BTreeMap::new();
    for (alias, canonical) in spellings {
        grouped.entry(*canonical).or_default().push(*alias);
        aliases.insert(*alias, *canonical);
    }
    (capabilities_from_grouped(grouped), aliases)
}

fn capabilities_from_grouped(
    grouped: BTreeMap<&'static str, Vec<&'static str>>,
) -> Vec<GraphqlCapability> {
    grouped
        .into_iter()
        .map(|(canonical, aliases)| GraphqlCapability { canonical, aliases })
        .collect()
}

fn canonical_graphql_boolean_operator_name(operator: GraphqlBooleanOperator) -> &'static str {
    match operator {
        GraphqlBooleanOperator::And => "and",
        GraphqlBooleanOperator::Or => "or",
        GraphqlBooleanOperator::Xor => "xor",
        GraphqlBooleanOperator::Not => "not",
    }
}

fn canonical_graphql_where_operator_name(operator: GraphqlWhereOperator) -> &'static str {
    match operator {
        GraphqlWhereOperator::Comparison(ComparisonOperator::Equal) => "eq",
        GraphqlWhereOperator::Comparison(ComparisonOperator::NotEqual) => "ne",
        GraphqlWhereOperator::Comparison(ComparisonOperator::GreaterThan) => "gt",
        GraphqlWhereOperator::Comparison(ComparisonOperator::GreaterThanOrEqual) => "gte",
        GraphqlWhereOperator::Comparison(ComparisonOperator::LessThan) => "lt",
        GraphqlWhereOperator::Comparison(ComparisonOperator::LessThanOrEqual) => "lte",
        GraphqlWhereOperator::Comparison(ComparisonOperator::StartsWith) => "startsWith",
        GraphqlWhereOperator::Comparison(ComparisonOperator::EndsWith) => "endsWith",
        GraphqlWhereOperator::Comparison(ComparisonOperator::Contains) => "contains",
        GraphqlWhereOperator::Comparison(ComparisonOperator::RegexMatch)
        | GraphqlWhereOperator::RegexMatch => "matches",
        GraphqlWhereOperator::Comparison(ComparisonOperator::In) | GraphqlWhereOperator::In => "in",
        GraphqlWhereOperator::IsNull => "isNull",
        GraphqlWhereOperator::IsNotNull => "isNotNull",
        GraphqlWhereOperator::NegatedComparison(ComparisonOperator::StartsWith) => "notStartsWith",
        GraphqlWhereOperator::NegatedComparison(ComparisonOperator::EndsWith) => "notEndsWith",
        GraphqlWhereOperator::NegatedComparison(ComparisonOperator::Contains) => "notContains",
        GraphqlWhereOperator::NegatedComparison(ComparisonOperator::RegexMatch)
        | GraphqlWhereOperator::NegatedRegexMatch => "notMatches",
        GraphqlWhereOperator::NegatedComparison(ComparisonOperator::In)
        | GraphqlWhereOperator::NotIn => "notIn",
        GraphqlWhereOperator::NegatedComparison(
            ComparisonOperator::Equal
            | ComparisonOperator::NotEqual
            | ComparisonOperator::GreaterThan
            | ComparisonOperator::GreaterThanOrEqual
            | ComparisonOperator::LessThan
            | ComparisonOperator::LessThanOrEqual,
        ) => "negatedComparison",
    }
}

fn graphql_rejection_paths() -> Vec<GraphqlRejectionPath> {
    vec![
        GraphqlRejectionPath {
            id: "unknown-property",
            source_file: "validation.rs",
            source_line: 3569,
            stable_substring: "UNKNOWN_PROPERTY",
        },
        GraphqlRejectionPath {
            id: "id-string-predicate",
            source_file: "graphql.rs",
            source_line: 3547,
            stable_substring: "GraphQL _id filters do not support string predicates",
        },
        GraphqlRejectionPath {
            id: "xor-arity",
            source_file: "graphql.rs",
            source_line: 3167,
            stable_substring: "requires exactly two objects",
        },
        GraphqlRejectionPath {
            id: "percentile-range",
            source_file: "graphql.rs",
            source_line: 1247,
            stable_substring: "percentile aggregate argument must be between",
        },
        GraphqlRejectionPath {
            id: "percentile-arg-count",
            source_file: "graphql.rs",
            source_line: 1170,
            stable_substring: "requires exactly 'field' and 'percentile' arguments",
        },
        GraphqlRejectionPath {
            id: "aggregate-missing-field",
            source_file: "graphql.rs",
            source_line: 1147,
            stable_substring: "requires exactly one 'field' argument",
        },
        GraphqlRejectionPath {
            id: "invalid-orderBy-direction",
            source_file: "graphql.rs",
            source_line: 4093,
            stable_substring: "orderBy direction must be",
        },
        GraphqlRejectionPath {
            id: "invalid-orderBy-nulls",
            source_file: "graphql.rs",
            source_line: 4107,
            stable_substring: "orderBy nulls must be",
        },
        GraphqlRejectionPath {
            id: "unsupported-directive",
            source_file: "graphql.rs",
            source_line: 2283,
            stable_substring: "unsupported GraphQL directive",
        },
        GraphqlRejectionPath {
            id: "repeated-directive",
            source_file: "graphql.rs",
            source_line: 2266,
            stable_substring: "directive '@skip' is repeated",
        },
        GraphqlRejectionPath {
            id: "nested-row-modifier",
            source_file: "graphql.rs",
            source_line: 1673,
            stable_substring: "do not support row modifiers",
        },
        GraphqlRejectionPath {
            id: "unknown-root-field",
            source_file: "graphql.rs",
            source_line: 733,
            stable_substring: "unknown GraphQL root node field",
        },
        GraphqlRejectionPath {
            id: "empty-boolean-array",
            source_file: "graphql.rs",
            source_line: 3139,
            stable_substring: "require at least one object",
        },
        GraphqlRejectionPath {
            id: "mutation-subscription-unsupported",
            source_file: "graphql.rs",
            source_line: 366,
            stable_substring: "mutations and subscriptions are not supported",
        },
        GraphqlRejectionPath {
            id: "multiple-root-fields",
            source_file: "graphql.rs",
            source_line: 417,
            stable_substring: "GraphQL virtual graph queries must select exactly one included root node field",
        },
    ]
}

#[path = "graphql_tests.rs"]
#[cfg(test)]
mod tests;
