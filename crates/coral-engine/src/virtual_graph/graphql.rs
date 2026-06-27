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
use super::ir::{
    ComparisonOperator, Direction, GraphPlan, Literal, NodePattern, OrderDirection,
    OrderExpression, OrderKey, PredicateExpression, PredicateRhs, Projection, PropertyPredicate,
    PropertyRef, RelationshipPattern,
};
use crate::CoreError;

/// Runtime value that can be bound to a GraphQL variable in the supported subset.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GraphqlVariableValue {
    /// Scalar literal variable, usable where a literal value is accepted.
    Literal(Literal),
    /// Scalar-list variable, usable where a literal list is accepted.
    List(Vec<Literal>),
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
/// operation with one root field whose name is the graph node label. Selected
/// scalar fields become property projections, and supported root arguments
/// compile into predicates, ordering, offsets, limits, and distinct row
/// selection. Relationship nesting is intentionally rejected until a
/// declaration-aware traversal convention is added.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when the query cannot be parsed or uses
/// GraphQL features outside Coral's current read-only virtual graph subset.
pub fn compile_graphql(graphql: &str) -> Result<GraphPlan, CoreError> {
    compile_graphql_with_variables(graphql, &BTreeMap::new())
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
    let document = parse_query::<String>(graphql).map_err(|error| {
        Diagnostic::new("GRAPHQL_PARSE_ERROR", "query", error.to_string()).into_core_error()
    })?;
    compile_document(&document, None, variables)
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
    let document = parse_query::<String>(graphql).map_err(|error| {
        Diagnostic::new("GRAPHQL_PARSE_ERROR", "query", error.to_string()).into_core_error()
    })?;
    compile_document(&document, Some(graph), variables)
}

fn compile_document(
    document: &graphql_parser::query::Document<'_, String>,
    graph: Option<&Declaration>,
    variables: &BTreeMap<String, GraphqlVariableValue>,
) -> Result<GraphPlan, CoreError> {
    let mut operation = None;
    let mut fragments = BTreeMap::new();
    for (index, definition) in document.definitions.iter().enumerate() {
        match definition {
            Definition::Operation(next_operation) => {
                if operation.replace(next_operation).is_some() {
                    return Err(unsupported(
                        format!("query.definitions[{index}]"),
                        "GraphQL virtual graph queries must contain exactly one operation",
                    ));
                }
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
    let operation = operation.ok_or_else(|| {
        unsupported(
            "query",
            "GraphQL virtual graph queries must contain exactly one operation",
        )
    })?;
    compile_operation(operation, graph, variables, fragments)
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

fn compile_root_selection_set(
    selection_set: &SelectionSet<'_, String>,
    graph: Option<&Declaration>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<GraphPlan, CoreError> {
    let path = path.into();
    let mut root_fields = Vec::new();
    for (index, selection) in selection_set.items.iter().enumerate() {
        let Selection::Field(field) = selection else {
            return Err(unsupported(
                format!("{path}.items[{index}]"),
                "GraphQL fragments are not supported at the root selection yet",
            ));
        };
        if selection_is_included(
            &field.directives,
            format!("{path}.items[{index}].directives"),
            context,
        )? {
            root_fields.push((index, field));
        }
    }
    let [(index, root)] = root_fields.as_slice() else {
        return Err(unsupported(
            path,
            "GraphQL virtual graph queries must select exactly one included root node field",
        ));
    };
    compile_root_field(root, graph, format!("{path}.items[{index}]"), context)
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

    let variable = variable_for_label(&root.name);
    let mut plan = GraphPlan {
        nodes: vec![NodePattern {
            variable: variable.clone(),
            label: root.name.clone(),
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

    compile_selection_set_into_plan(
        &mut plan,
        graph,
        &root.selection_set,
        &NodeContext {
            variable: variable.clone(),
            label: root.name.clone(),
            is_root: true,
            edge_variable: None,
        },
        format!("{path}.selectionSet"),
        context,
        &mut Vec::new(),
    )?;

    for (index, (name, value)) in root.arguments.iter().enumerate() {
        compile_root_argument(
            &mut plan,
            &variable,
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

#[derive(Debug, Clone)]
struct NodeContext {
    variable: String,
    label: String,
    is_root: bool,
    edge_variable: Option<String>,
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
    let path = path.into();
    for (index, selection) in selection_set.items.iter().enumerate() {
        let item_path = format!("{path}.items[{index}]");
        match selection {
            Selection::Field(field) => {
                if !selection_is_included(
                    &field.directives,
                    format!("{item_path}.directives"),
                    compile_context,
                )? {
                    continue;
                }
                if field.name == "_edge" {
                    compile_edge_field(plan, field, context, &item_path, compile_context)?;
                } else if field.selection_set.items.is_empty() {
                    compile_property_field(plan, field, context, &item_path)?;
                } else {
                    let graph = graph.ok_or_else(|| {
                        unsupported(
                            format!("{item_path}.selectionSet"),
                            "GraphQL relationship nesting requires a graph declaration",
                        )
                    })?;
                    compile_relationship_field(
                        plan,
                        graph,
                        context,
                        field,
                        &item_path,
                        compile_context,
                        fragment_stack,
                    )?;
                }
            }
            Selection::FragmentSpread(spread) => compile_fragment_spread(
                plan,
                graph,
                context,
                spread,
                &item_path,
                compile_context,
                fragment_stack,
            )?,
            Selection::InlineFragment(fragment) => compile_inline_fragment(
                plan,
                graph,
                context,
                fragment,
                &item_path,
                compile_context,
                fragment_stack,
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
) -> Result<(), CoreError> {
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

fn compile_fragment_spread(
    plan: &mut GraphPlan,
    graph: Option<&Declaration>,
    context: &NodeContext,
    spread: &FragmentSpread<'_, String>,
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
    ensure_fragment_type_condition(
        Some(&fragment.type_condition),
        context,
        format!("{path}.typeCondition"),
    )?;
    if fragment_stack.contains(&spread.fragment_name) {
        return Err(unsupported(
            format!("{path}.name"),
            format!("GraphQL fragment '{}' forms a cycle", spread.fragment_name),
        ));
    }
    fragment_stack.push(spread.fragment_name.clone());
    let result = compile_selection_set_into_plan(
        plan,
        graph,
        &fragment.selection_set,
        context,
        format!("fragment.{}.selectionSet", fragment.name),
        compile_context,
        fragment_stack,
    );
    fragment_stack.pop();
    result
}

fn compile_inline_fragment(
    plan: &mut GraphPlan,
    graph: Option<&Declaration>,
    context: &NodeContext,
    fragment: &InlineFragment<'_, String>,
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
    ensure_fragment_type_condition(
        fragment.type_condition.as_ref(),
        context,
        format!("{path}.typeCondition"),
    )?;
    compile_selection_set_into_plan(
        plan,
        graph,
        &fragment.selection_set,
        context,
        format!("{path}.selectionSet"),
        compile_context,
        fragment_stack,
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
    graph: &Declaration,
    source: &NodeContext,
    field: &Field<'_, String>,
    path: impl Into<String>,
    compile_context: &GraphqlCompileContext<'_, '_>,
    fragment_stack: &mut Vec<String>,
) -> Result<(), CoreError> {
    let path = path.into();
    let (direction, relationship_type, endpoint_argument) =
        compile_relationship_field_name(&field.name, format!("{path}.name"))?;
    let target_label = compile_relationship_target_label(
        field,
        endpoint_argument,
        format!("{path}.arguments"),
        compile_context,
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

    let relationship_index = plan.relationships.len();
    let needs_relationship_variable = field
        .arguments
        .iter()
        .any(|(name, _)| name == "relationshipWhere")
        || relationship_selection_needs_edge_variable(
            &field.selection_set,
            &target_label,
            compile_context,
            format!("{path}.selectionSet"),
            &mut Vec::new(),
        )?;
    let relationship_variable = needs_relationship_variable
        .then(|| relationship_variable_for_field(field, relationship_index));
    let target_variable = nested_variable_for_field(field, &target_label, plan.nodes.len());

    compile_relationship_field_arguments(
        plan,
        field,
        endpoint_argument,
        &target_variable,
        relationship_variable.as_deref(),
        &path,
        compile_context,
    )?;

    plan.nodes.push(NodePattern {
        variable: target_variable.clone(),
        label: target_label.clone(),
    });
    plan.relationships.push(RelationshipPattern {
        variable: relationship_variable.clone(),
        relationship_type,
        left: source.variable.clone(),
        direction,
        right: target_variable.clone(),
    });

    compile_selection_set_into_plan(
        plan,
        Some(graph),
        &field.selection_set,
        &NodeContext {
            variable: target_variable,
            label: target_label,
            is_root: false,
            edge_variable: relationship_variable,
        },
        format!("{path}.selectionSet"),
        compile_context,
        fragment_stack,
    )
}

fn compile_relationship_field_arguments(
    plan: &mut GraphPlan,
    field: &Field<'_, String>,
    endpoint_argument: &str,
    target_variable: &str,
    relationship_variable: Option<&str>,
    path: &str,
    compile_context: &GraphqlCompileContext<'_, '_>,
) -> Result<(), CoreError> {
    for (index, (name, value)) in field.arguments.iter().enumerate() {
        let argument_path = format!("{path}.arguments[{index}]");
        match name.as_str() {
            "to" | "from" | "label" => {
                if name != endpoint_argument {
                    return Err(unsupported(
                        argument_path,
                        format!(
                            "GraphQL relationship field '{}' requires '{}' instead of '{}'",
                            field.name, endpoint_argument, name
                        ),
                    ));
                }
            }
            "where" => append_where_predicate(
                plan,
                compile_where_argument(target_variable, value, argument_path, compile_context)?,
            ),
            "relationshipWhere" => {
                let relationship_variable = relationship_variable
                    .ok_or_else(|| CoreError::internal("relationshipWhere variable missing"))?;
                append_where_predicate(
                    plan,
                    compile_where_argument(
                        relationship_variable,
                        value,
                        argument_path,
                        compile_context,
                    )?,
                );
            }
            "orderBy" | "limit" | "offset" | "skip" | "distinct" => {
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

fn compile_edge_field(
    plan: &mut GraphPlan,
    field: &Field<'_, String>,
    context: &NodeContext,
    path: &str,
    compile_context: &GraphqlCompileContext<'_, '_>,
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
    for (index, selection) in field.selection_set.items.iter().enumerate() {
        let Selection::Field(property) = selection else {
            return Err(unsupported(
                format!("{path}.selectionSet.items[{index}]"),
                "GraphQL fragments are not supported inside _edge selections",
            ));
        };
        if !selection_is_included(
            &property.directives,
            format!("{path}.selectionSet.items[{index}].directives"),
            compile_context,
        )? {
            continue;
        }
        if !property.arguments.is_empty() {
            return Err(unsupported(
                format!("{path}.selectionSet.items[{index}].arguments"),
                "GraphQL _edge property arguments are not supported",
            ));
        }
        if !property.selection_set.items.is_empty() {
            return Err(unsupported(
                format!("{path}.selectionSet.items[{index}].selectionSet"),
                "GraphQL _edge properties must be scalar fields",
            ));
        }
        if property.name == "__typename" {
            return Err(unsupported(
                format!("{path}.selectionSet.items[{index}].name"),
                "GraphQL __typename is only supported on node selections",
            ));
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
            &format!("{path}.selectionSet.items[{index}]"),
        )?;
    }
    Ok(())
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

fn compile_relationship_target_label(
    field: &Field<'_, String>,
    endpoint_argument: &str,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<String, CoreError> {
    let path = path.into();
    let mut target_label = None;
    for (index, (name, value)) in field.arguments.iter().enumerate() {
        if name == endpoint_argument {
            if target_label.is_some() {
                return Err(unsupported(
                    format!("{path}[{index}]"),
                    format!("GraphQL relationship argument '{endpoint_argument}' is duplicated"),
                ));
            }
            target_label = Some(compile_name_value(
                value,
                format!("{path}.{endpoint_argument}"),
                context,
            )?);
        }
    }
    target_label.ok_or_else(|| {
        unsupported(
            path,
            format!("GraphQL relationship field requires '{endpoint_argument}'"),
        )
    })
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
    variable: &str,
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
                compile_where_argument(variable, value, path, context)?,
            );
            Ok(())
        }
        "orderBy" => {
            plan.order_by
                .extend(compile_order_by_argument(variable, value, path, context)?);
            Ok(())
        }
        "limit" => {
            plan.limit = Some(compile_non_negative_u64(value, path, "limit", context)?);
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
    variable: &str,
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    if matches!(value, Value::Variable(_)) {
        return Err(unsupported(
            path,
            "GraphQL object variables are not supported yet; bind variables inside scalar fields",
        ));
    }
    let Value::Object(properties) = value else {
        return Err(unsupported(path, "GraphQL where must be an object"));
    };
    let mut expression = None;
    for (property, condition) in properties {
        let next = if let Some(operator) = graphql_boolean_operator(property) {
            compile_where_boolean_operator(
                variable,
                operator,
                condition,
                format!("{path}.{property}"),
                context,
            )?
        } else {
            compile_where_property_conditions(
                variable,
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
    variable: &str,
    operator: GraphqlBooleanOperator,
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
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
                    compile_where_argument(variable, item, format!("{path}[{index}]"), context)?;
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
            let left = compile_where_argument(variable, left_item, format!("{path}[0]"), context)?
                .ok_or_else(|| {
                    unsupported(
                        format!("{path}[0]"),
                        "GraphQL where xor operands must not be empty",
                    )
                })?;
            let right =
                compile_where_argument(variable, right_item, format!("{path}[1]"), context)?
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
            let expression = compile_where_argument(variable, value, path.clone(), context)?
                .ok_or_else(|| unsupported(path, "GraphQL where not requires an object"))?;
            Ok(Some(PredicateExpression::Not {
                expression: Box::new(expression),
            }))
        }
    }
}

fn compile_where_property_conditions(
    variable: &str,
    property: &str,
    condition: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<Option<PredicateExpression>, CoreError> {
    let path = path.into();
    if matches!(condition, Value::Variable(_)) {
        return Err(unsupported(
            path,
            "GraphQL property condition variables are not supported yet; bind variables inside operators",
        ));
    }
    let Value::Object(operators) = condition else {
        return Err(unsupported(
            path,
            "GraphQL where property conditions must be objects",
        ));
    };
    let mut expression = None;
    for (operator, value) in operators {
        let predicate = compile_where_operator(
            variable,
            property,
            operator,
            value,
            format!("{path}.{operator}"),
            context,
        )?;
        expression =
            append_optional_and(expression, Some(PredicateExpression::Comparison(predicate)));
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
        | PredicateExpression::ScalarComparison(_)
        | PredicateExpression::Or { .. }
        | PredicateExpression::Xor { .. }
        | PredicateExpression::Not { .. } => {
            unreachable!("non-conjunctive GraphQL predicate reached conjunctive appender")
        }
    }
}

fn compile_where_operator(
    variable: &str,
    property: &str,
    operator: &str,
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<PropertyPredicate, CoreError> {
    let path = path.into();
    let property = PropertyRef {
        variable: variable.to_string(),
        property: property.to_string(),
    };
    match operator {
        "eq" => Ok(PropertyPredicate {
            property,
            operator: ComparisonOperator::Equal,
            rhs: PredicateRhs::Literal(compile_literal(value, path, context)?),
        }),
        "ne" => Ok(PropertyPredicate {
            property,
            operator: ComparisonOperator::NotEqual,
            rhs: PredicateRhs::Literal(compile_literal(value, path, context)?),
        }),
        "gt" => Ok(PropertyPredicate {
            property,
            operator: ComparisonOperator::GreaterThan,
            rhs: PredicateRhs::Literal(compile_literal(value, path, context)?),
        }),
        "gte" => Ok(PropertyPredicate {
            property,
            operator: ComparisonOperator::GreaterThanOrEqual,
            rhs: PredicateRhs::Literal(compile_literal(value, path, context)?),
        }),
        "lt" => Ok(PropertyPredicate {
            property,
            operator: ComparisonOperator::LessThan,
            rhs: PredicateRhs::Literal(compile_literal(value, path, context)?),
        }),
        "lte" => Ok(PropertyPredicate {
            property,
            operator: ComparisonOperator::LessThanOrEqual,
            rhs: PredicateRhs::Literal(compile_literal(value, path, context)?),
        }),
        "startsWith" => Ok(PropertyPredicate {
            property,
            operator: ComparisonOperator::StartsWith,
            rhs: PredicateRhs::Literal(compile_literal(value, path, context)?),
        }),
        "endsWith" => Ok(PropertyPredicate {
            property,
            operator: ComparisonOperator::EndsWith,
            rhs: PredicateRhs::Literal(compile_literal(value, path, context)?),
        }),
        "contains" => Ok(PropertyPredicate {
            property,
            operator: ComparisonOperator::Contains,
            rhs: PredicateRhs::Literal(compile_literal(value, path, context)?),
        }),
        "matches" | "regex" => Ok(PropertyPredicate {
            property,
            operator: ComparisonOperator::RegexMatch,
            rhs: PredicateRhs::Literal(compile_regex_literal(value, path, context)?),
        }),
        "in" => Ok(PropertyPredicate {
            property,
            operator: ComparisonOperator::In,
            rhs: PredicateRhs::List(compile_literal_list(value, path, context)?),
        }),
        "isNull" => {
            let is_null = compile_boolean(value, path, "isNull", context)?;
            Ok(PropertyPredicate {
                property,
                operator: if is_null {
                    ComparisonOperator::Equal
                } else {
                    ComparisonOperator::NotEqual
                },
                rhs: PredicateRhs::Literal(Literal::Null),
            })
        }
        _ => Err(unsupported(
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
        Value::Object(_) => Ok(vec![compile_order_by_object(
            variable, value, path, context,
        )?]),
        Value::List(items) => items
            .iter()
            .enumerate()
            .map(|(index, value)| {
                compile_order_by_object(variable, value, format!("{path}[{index}]"), context)
            })
            .collect(),
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
) -> Result<OrderKey, CoreError> {
    let path = path.into();
    let Value::Object(object) = value else {
        return Err(unsupported(path, "GraphQL orderBy entries must be objects"));
    };
    for name in object.keys() {
        if name != "field" && name != "direction" {
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
    Ok(OrderKey {
        expression: OrderExpression::Property(PropertyRef {
            variable: variable.to_string(),
            property: field,
        }),
        direction,
    })
}

fn compile_order_direction(
    value: &Value<'_, String>,
    path: impl Into<String>,
    context: &GraphqlCompileContext<'_, '_>,
) -> Result<OrderDirection, CoreError> {
    let path = path.into();
    let direction = compile_name_value(value, path.clone(), context)?;
    match direction.as_str() {
        "ASC" | "asc" => Ok(OrderDirection::Ascending),
        "DESC" | "desc" => Ok(OrderDirection::Descending),
        _ => Err(unsupported(
            path,
            "GraphQL orderBy direction must be ASC or DESC",
        )),
    }
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
            GraphqlVariableValue::List(_) => Err(unsupported(
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
    if let Value::List(items) = value {
        let mut literals = Vec::with_capacity(items.len());
        for (index, item) in items.iter().enumerate() {
            literals.push(compile_variable_default_literal(
                item,
                format!("{path}[{index}]"),
            )?);
        }
        return Ok(GraphqlVariableValue::List(literals));
    }
    Ok(GraphqlVariableValue::Literal(
        compile_variable_default_literal(value, path)?,
    ))
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
        Value::Object(_) => Err(unsupported(
            path,
            "GraphQL object variable default values are not supported yet",
        )),
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
            GraphqlVariableValue::Literal(_) => Err(unsupported(
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
            GraphqlVariableValue::Literal(_) | GraphqlVariableValue::List(_) => Err(unsupported(
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
    use super::*;

    #[test]
    fn compiles_root_node_query() {
        let plan = compile_graphql(
            r#"
            query {
              Service(
                where: { tier: { eq: "prod" }, risk: { gte: 0.5 } }
                orderBy: [{ field: name, direction: ASC }]
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
            }]
        );
        assert_eq!(plan.limit, Some(10));
        assert_eq!(plan.skip, Some(2));
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
                GraphqlVariableValue::Literal(Literal::String("DESC".to_string())),
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
    fn rejects_graphql_object_variable_defaults() {
        let error = compile_graphql_with_variables(
            r#"
            query Services($where: ServiceWhere = { tier: { eq: "prod" } }) {
              Service(where: $where) { name }
            }
            "#,
            &BTreeMap::new(),
        )
        .expect_err("GraphQL object variable defaults should fail");

        assert!(
            error
                .to_string()
                .contains("GraphQL object variable default values are not supported yet"),
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
              Service(orderBy: { field: name, direction: ASC, nulls: LAST }) {
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
