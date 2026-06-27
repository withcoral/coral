//! GraphQL SDL generation for the Coral-supported virtual graph query surface.
//!
//! This module renders a graph declaration into the GraphQL schema describing
//! the executable subset accepted by the GraphQL frontend.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use super::declaration::{Declaration, Node, Relationship};
use super::diagnostic::Diagnostic;
use crate::CoreError;

#[derive(Debug, Clone, PartialEq, Eq)]
struct RelationshipSchemaShape {
    relationship_type: String,
    has_key: bool,
    properties: BTreeSet<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RelationshipSchemaField {
    name: String,
    endpoint_argument: &'static str,
    endpoint_enum: String,
    target_label: String,
    target_where: String,
    relationship_where: String,
}

const GRAPHQL_SCHEMA_HEADER_SDL: &str = r"schema {
  query: Query
}

scalar CoralGraphValue

enum CoralGraphOrderDirection {
  ASC
  DESC
}

";

const GRAPHQL_VALUE_FILTER_SDL: &str = r"input CoralGraphValueFilter {
  eq: CoralGraphValue
  equals: CoralGraphValue
  ne: CoralGraphValue
  neq: CoralGraphValue
  notEq: CoralGraphValue
  notEqual: CoralGraphValue
  notEquals: CoralGraphValue
  gt: CoralGraphValue
  greaterThan: CoralGraphValue
  gte: CoralGraphValue
  ge: CoralGraphValue
  greaterThanEqual: CoralGraphValue
  greaterThanOrEqual: CoralGraphValue
  lt: CoralGraphValue
  lessThan: CoralGraphValue
  lte: CoralGraphValue
  le: CoralGraphValue
  lessThanEqual: CoralGraphValue
  lessThanOrEqual: CoralGraphValue
  startsWith: CoralGraphValue
  starts_with: CoralGraphValue
  endsWith: CoralGraphValue
  ends_with: CoralGraphValue
  contains: CoralGraphValue
  notStartsWith: CoralGraphValue
  not_starts_with: CoralGraphValue
  notEndsWith: CoralGraphValue
  not_ends_with: CoralGraphValue
  notContains: CoralGraphValue
  not_contains: CoralGraphValue
  matches: CoralGraphValue
  regex: CoralGraphValue
  notMatches: CoralGraphValue
  notRegex: CoralGraphValue
  not_regex: CoralGraphValue
  in: [CoralGraphValue]
  notIn: [CoralGraphValue]
  not_in: [CoralGraphValue]
  isNull: Boolean
  is_null: Boolean
  isNotNull: Boolean
  is_not_null: Boolean
}

";

const GRAPHQL_IDENTITY_FILTER_SDL: &str = r"input CoralGraphIdentityFilter {
  eq: CoralGraphValue
  equals: CoralGraphValue
  ne: CoralGraphValue
  neq: CoralGraphValue
  notEq: CoralGraphValue
  notEqual: CoralGraphValue
  notEquals: CoralGraphValue
  gt: CoralGraphValue
  greaterThan: CoralGraphValue
  gte: CoralGraphValue
  ge: CoralGraphValue
  greaterThanEqual: CoralGraphValue
  greaterThanOrEqual: CoralGraphValue
  lt: CoralGraphValue
  lessThan: CoralGraphValue
  lte: CoralGraphValue
  le: CoralGraphValue
  lessThanEqual: CoralGraphValue
  lessThanOrEqual: CoralGraphValue
  in: [CoralGraphValue]
  notIn: [CoralGraphValue]
  not_in: [CoralGraphValue]
  isNull: Boolean
  is_null: Boolean
  isNotNull: Boolean
  is_not_null: Boolean
}

";

const GRAPHQL_ELEMENT_ID_FILTER_SDL: &str = r"input CoralGraphElementIdFilter {
  eq: String
  equals: String
  ne: String
  neq: String
  notEq: String
  notEqual: String
  notEquals: String
  gt: String
  greaterThan: String
  gte: String
  ge: String
  greaterThanEqual: String
  greaterThanOrEqual: String
  lt: String
  lessThan: String
  lte: String
  le: String
  lessThanEqual: String
  lessThanOrEqual: String
  startsWith: String
  starts_with: String
  endsWith: String
  ends_with: String
  contains: String
  notStartsWith: String
  not_starts_with: String
  notEndsWith: String
  not_ends_with: String
  notContains: String
  not_contains: String
  matches: String
  regex: String
  notMatches: String
  notRegex: String
  not_regex: String
  in: [String]
  notIn: [String]
  not_in: [String]
  isNull: Boolean
  is_null: Boolean
  isNotNull: Boolean
  is_not_null: Boolean
}

";

/// Generates GraphQL SDL for the Coral-supported virtual graph query surface.
///
/// The generated schema describes the executable subset accepted by
/// `compile_graphql_for_graph`: root node fields, scalar property selections,
/// reserved identity fields, relationship traversal fields, relationship object
/// types, filters, ordering, and row modifiers. Source column types are
/// intentionally exposed as the custom `CoralGraphValue` scalar because v1 graph
/// declarations do not carry property type metadata.
///
/// # Errors
///
/// Returns [`CoreError::InvalidInput`] when declaration names cannot be exposed
/// as valid GraphQL names, when graph property names collide with reserved
/// GraphQL virtual fields, or when overloaded relationship mappings cannot be
/// represented as one unambiguous GraphQL field/type shape.
pub fn graphql_schema_sdl_for_graph(graph: &Declaration) -> Result<String, CoreError> {
    graph.validate()?;
    validate_graphql_schema_names(graph)?;
    validate_relationship_field_shapes(graph)?;
    validate_generated_type_names_are_unique(graph)?;
    let relationship_shapes = collect_relationship_schema_shapes(graph)?;

    let mut sdl = String::new();
    push_schema_prelude(&mut sdl);
    push_query_type(&mut sdl, graph);

    for node in &graph.nodes {
        push_node_order_field_enum(&mut sdl, node);
        push_node_order_input(&mut sdl, node);
        push_where_input(
            &mut sdl,
            &node_where_type(&node.label),
            &node_property_names(node),
            true,
        );
    }

    for relationship in relationship_shapes.values() {
        push_where_input(
            &mut sdl,
            &relationship_where_type(&relationship.relationship_type),
            &relationship.properties,
            relationship.has_key,
        );
    }

    for node in &graph.nodes {
        push_node_type(&mut sdl, graph, node);
    }

    for relationship in relationship_shapes.values() {
        push_relationship_type(&mut sdl, relationship);
    }

    Ok(sdl)
}

fn push_schema_prelude(sdl: &mut String) {
    sdl.push_str(GRAPHQL_SCHEMA_HEADER_SDL);
    sdl.push_str(GRAPHQL_VALUE_FILTER_SDL);
    sdl.push_str(GRAPHQL_IDENTITY_FILTER_SDL);
    sdl.push_str(GRAPHQL_ELEMENT_ID_FILTER_SDL);
}

fn push_query_type(sdl: &mut String, graph: &Declaration) {
    sdl.push_str("type Query {\n");
    for node in &graph.nodes {
        writeln!(
            sdl,
            "  {}(where: {}, orderBy: [{}!], limit: Int, first: Int, offset: Int, skip: Int, distinct: Boolean): [{}!]!",
            node.label,
            node_where_type(&node.label),
            node_order_by_type(&node.label),
            node.label
        )
        .expect("writing GraphQL SDL to string should not fail");
    }
    sdl.push_str("}\n\n");
}

fn push_node_order_field_enum(sdl: &mut String, node: &Node) {
    writeln!(sdl, "enum {} {{", node_order_field_type(&node.label))
        .expect("writing GraphQL SDL to string should not fail");
    sdl.push_str("  _id\n");
    sdl.push_str("  _elementId\n");
    for property in node_property_names(node) {
        writeln!(sdl, "  {property}").expect("writing GraphQL SDL to string should not fail");
    }
    sdl.push_str("}\n\n");
}

fn push_node_order_input(sdl: &mut String, node: &Node) {
    write!(
        sdl,
        "input {} {{\n  field: {}!\n  direction: CoralGraphOrderDirection = ASC\n}}\n\n",
        node_order_by_type(&node.label),
        node_order_field_type(&node.label)
    )
    .expect("writing GraphQL SDL to string should not fail");
}

fn push_where_input(
    sdl: &mut String,
    input_name: &str,
    properties: &BTreeSet<String>,
    has_identity: bool,
) {
    writeln!(sdl, "input {input_name} {{").expect("writing GraphQL SDL to string should not fail");
    if has_identity {
        sdl.push_str("  _id: CoralGraphIdentityFilter\n");
        sdl.push_str("  _elementId: CoralGraphElementIdFilter\n");
    }
    for property in properties {
        writeln!(sdl, "  {property}: CoralGraphValueFilter")
            .expect("writing GraphQL SDL to string should not fail");
    }
    writeln!(sdl, "  and: [{input_name}!]").expect("writing GraphQL SDL to string should not fail");
    writeln!(sdl, "  or: [{input_name}!]").expect("writing GraphQL SDL to string should not fail");
    writeln!(sdl, "  xor: [{input_name}!]").expect("writing GraphQL SDL to string should not fail");
    writeln!(sdl, "  not: {input_name}").expect("writing GraphQL SDL to string should not fail");
    writeln!(sdl, "  _and: [{input_name}!]")
        .expect("writing GraphQL SDL to string should not fail");
    writeln!(sdl, "  _or: [{input_name}!]").expect("writing GraphQL SDL to string should not fail");
    writeln!(sdl, "  _xor: [{input_name}!]")
        .expect("writing GraphQL SDL to string should not fail");
    writeln!(sdl, "  _not: {input_name}").expect("writing GraphQL SDL to string should not fail");
    writeln!(sdl, "  AND: [{input_name}!]").expect("writing GraphQL SDL to string should not fail");
    writeln!(sdl, "  OR: [{input_name}!]").expect("writing GraphQL SDL to string should not fail");
    writeln!(sdl, "  XOR: [{input_name}!]").expect("writing GraphQL SDL to string should not fail");
    writeln!(sdl, "  NOT: {input_name}").expect("writing GraphQL SDL to string should not fail");
    sdl.push_str("}\n\n");
}

fn push_node_type(sdl: &mut String, graph: &Declaration, node: &Node) {
    writeln!(sdl, "type {} {{", node.label).expect("writing GraphQL SDL to string should not fail");
    sdl.push_str("  _id: CoralGraphValue\n");
    sdl.push_str("  _elementId: String\n");
    for property in node_property_names(node) {
        writeln!(sdl, "  {property}: CoralGraphValue")
            .expect("writing GraphQL SDL to string should not fail");
    }
    for field in relationship_schema_fields_for_node(graph, &node.label) {
        writeln!(
            sdl,
            "  {}({}: {}!, where: {}, relationshipWhere: {}): [{}!]!",
            field.name,
            field.endpoint_argument,
            field.endpoint_enum,
            field.target_where,
            field.relationship_where,
            field.target_label
        )
        .expect("writing GraphQL SDL to string should not fail");
    }
    sdl.push_str("}\n\n");

    for field in relationship_schema_fields_for_node(graph, &node.label) {
        writeln!(sdl, "enum {} {{", field.endpoint_enum)
            .expect("writing GraphQL SDL to string should not fail");
        writeln!(sdl, "  {}", field.target_label)
            .expect("writing GraphQL SDL to string should not fail");
        sdl.push_str("}\n\n");
    }
}

fn push_relationship_type(sdl: &mut String, relationship: &RelationshipSchemaShape) {
    writeln!(sdl, "type {} {{", relationship.relationship_type)
        .expect("writing GraphQL SDL to string should not fail");
    if relationship.has_key {
        sdl.push_str("  _id: CoralGraphValue\n");
        sdl.push_str("  _elementId: String\n");
    }
    for property in &relationship.properties {
        writeln!(sdl, "  {property}: CoralGraphValue")
            .expect("writing GraphQL SDL to string should not fail");
    }
    sdl.push_str("}\n\n");
}

fn validate_graphql_schema_names(graph: &Declaration) -> Result<(), CoreError> {
    validate_graphql_name("Query", "Query")?;
    for (index, node) in graph.nodes.iter().enumerate() {
        let path = format!("nodes[{index}]");
        validate_graphql_name(&node.label, format!("{path}.label"))?;
        validate_graphql_name(&node_where_type(&node.label), format!("{path}.label"))?;
        validate_graphql_name(&node_order_by_type(&node.label), format!("{path}.label"))?;
        validate_graphql_name(&node_order_field_type(&node.label), format!("{path}.label"))?;
        for property in node_property_names(node) {
            validate_graphql_property_name(&property, format!("{path}.properties.{property}"))?;
        }
    }

    for (index, relationship) in graph.relationships.iter().enumerate() {
        let path = format!("relationships[{index}]");
        validate_graphql_name(&relationship.relationship_type, format!("{path}.type"))?;
        validate_graphql_name(
            &relationship_where_type(&relationship.relationship_type),
            format!("{path}.type"),
        )?;
        for field in relationship_schema_fields(relationship) {
            validate_graphql_name(&field.name, format!("{path}.type"))?;
            validate_graphql_name(&field.endpoint_enum, format!("{path}.type"))?;
        }
        for property in relationship_property_names(relationship) {
            validate_graphql_property_name(&property, format!("{path}.properties.{property}"))?;
        }
    }

    Ok(())
}

fn validate_generated_type_names_are_unique(graph: &Declaration) -> Result<(), CoreError> {
    let mut names = BTreeSet::from([
        "Query".to_string(),
        "CoralGraphValue".to_string(),
        "CoralGraphOrderDirection".to_string(),
        "CoralGraphValueFilter".to_string(),
        "CoralGraphIdentityFilter".to_string(),
        "CoralGraphElementIdFilter".to_string(),
    ]);
    for node in &graph.nodes {
        insert_generated_type_name(&mut names, &node.label, format!("node '{}'", node.label))?;
        insert_generated_type_name(
            &mut names,
            &node_where_type(&node.label),
            format!("node '{}'", node.label),
        )?;
        insert_generated_type_name(
            &mut names,
            &node_order_by_type(&node.label),
            format!("node '{}'", node.label),
        )?;
        insert_generated_type_name(
            &mut names,
            &node_order_field_type(&node.label),
            format!("node '{}'", node.label),
        )?;
    }
    let mut relationship_types = BTreeSet::new();
    for relationship in &graph.relationships {
        if relationship_types.insert(relationship.relationship_type.as_str()) {
            insert_generated_type_name(
                &mut names,
                &relationship.relationship_type,
                format!("relationship '{}'", relationship.relationship_type),
            )?;
            insert_generated_type_name(
                &mut names,
                &relationship_where_type(&relationship.relationship_type),
                format!("relationship '{}'", relationship.relationship_type),
            )?;
        }
        for field in relationship_schema_fields(relationship) {
            insert_generated_type_name(
                &mut names,
                &field.endpoint_enum,
                format!("relationship field '{}'", field.name),
            )?;
        }
    }
    Ok(())
}

fn insert_generated_type_name(
    names: &mut BTreeSet<String>,
    name: &str,
    owner: String,
) -> Result<(), CoreError> {
    if names.insert(name.to_string()) {
        return Ok(());
    }
    Err(Diagnostic::new(
        "UNSUPPORTED_GRAPHQL_SCHEMA",
        owner,
        format!("generated GraphQL type name '{name}' is not unique"),
    )
    .into_core_error())
}

fn validate_relationship_field_shapes(graph: &Declaration) -> Result<(), CoreError> {
    for node in &graph.nodes {
        let mut fields = BTreeSet::from(["_id".to_string(), "_elementId".to_string()]);
        for property in node_property_names(node) {
            insert_graphql_field_name(&mut fields, &property, format!("node '{}'", node.label))?;
        }
        for field in relationship_schema_fields_for_node(graph, &node.label) {
            insert_graphql_field_name(&mut fields, &field.name, format!("node '{}'", node.label))?;
        }
    }
    Ok(())
}

fn insert_graphql_field_name(
    fields: &mut BTreeSet<String>,
    name: &str,
    owner: String,
) -> Result<(), CoreError> {
    if fields.insert(name.to_string()) {
        return Ok(());
    }
    Err(Diagnostic::new(
        "UNSUPPORTED_GRAPHQL_SCHEMA",
        owner,
        format!("GraphQL field '{name}' would be generated more than once"),
    )
    .into_core_error())
}

fn collect_relationship_schema_shapes(
    graph: &Declaration,
) -> Result<BTreeMap<String, RelationshipSchemaShape>, CoreError> {
    let mut shapes = BTreeMap::new();
    for relationship in &graph.relationships {
        let shape = RelationshipSchemaShape {
            relationship_type: relationship.relationship_type.clone(),
            has_key: relationship.key.is_some(),
            properties: relationship_property_names(relationship),
        };
        match shapes.get(&relationship.relationship_type) {
            Some(existing) if existing == &shape => {}
            Some(_) => {
                return Err(Diagnostic::new(
                    "UNSUPPORTED_GRAPHQL_SCHEMA",
                    format!("relationships.{}", relationship.relationship_type),
                    format!(
                        "relationship type '{}' has multiple mappings with different GraphQL _edge shapes",
                        relationship.relationship_type
                    ),
                )
                .into_core_error());
            }
            None => {
                shapes.insert(relationship.relationship_type.clone(), shape);
            }
        }
    }
    Ok(shapes)
}

fn relationship_schema_fields_for_node(
    graph: &Declaration,
    label: &str,
) -> Vec<RelationshipSchemaField> {
    graph
        .relationships
        .iter()
        .flat_map(|relationship| {
            let mut fields = Vec::new();
            if relationship.from.label == label {
                fields.push(relationship_schema_field(
                    relationship,
                    label,
                    "out",
                    "to",
                    &relationship.to.label,
                ));
            }
            if relationship.to.label == label {
                fields.push(relationship_schema_field(
                    relationship,
                    label,
                    "in",
                    "from",
                    &relationship.from.label,
                ));
            }
            if relationship.from.label == label || relationship.to.label == label {
                let target_label = if relationship.from.label == label {
                    &relationship.to.label
                } else {
                    &relationship.from.label
                };
                fields.push(relationship_schema_field(
                    relationship,
                    label,
                    "any",
                    "label",
                    target_label,
                ));
            }
            fields
        })
        .collect()
}

fn relationship_schema_fields(relationship: &Relationship) -> Vec<RelationshipSchemaField> {
    let mut fields = vec![
        relationship_schema_field(
            relationship,
            &relationship.from.label,
            "out",
            "to",
            &relationship.to.label,
        ),
        relationship_schema_field(
            relationship,
            &relationship.to.label,
            "in",
            "from",
            &relationship.from.label,
        ),
        relationship_schema_field(
            relationship,
            &relationship.from.label,
            "any",
            "label",
            &relationship.to.label,
        ),
    ];
    if relationship.from.label != relationship.to.label {
        fields.push(relationship_schema_field(
            relationship,
            &relationship.to.label,
            "any",
            "label",
            &relationship.from.label,
        ));
    }
    fields
}

fn relationship_schema_field(
    relationship: &Relationship,
    source_label: &str,
    direction: &'static str,
    endpoint_argument: &'static str,
    target_label: &str,
) -> RelationshipSchemaField {
    RelationshipSchemaField {
        name: format!("{direction}_{}", relationship.relationship_type),
        endpoint_argument,
        endpoint_enum: relationship_endpoint_enum_type(
            source_label,
            direction,
            &relationship.relationship_type,
            endpoint_argument,
        ),
        target_label: target_label.to_string(),
        target_where: node_where_type(target_label),
        relationship_where: relationship_where_type(&relationship.relationship_type),
    }
}

fn node_property_names(node: &Node) -> BTreeSet<String> {
    let mut properties = node.properties.keys().cloned().collect::<BTreeSet<_>>();
    properties.insert(node.key.clone());
    properties
}

fn relationship_property_names(relationship: &Relationship) -> BTreeSet<String> {
    let mut properties = relationship
        .properties
        .keys()
        .cloned()
        .collect::<BTreeSet<_>>();
    if let Some(key) = &relationship.key {
        properties.insert(key.clone());
    }
    properties
}

fn node_where_type(label: &str) -> String {
    format!("{label}Where")
}

fn node_order_by_type(label: &str) -> String {
    format!("{label}OrderBy")
}

fn node_order_field_type(label: &str) -> String {
    format!("{label}OrderField")
}

fn relationship_where_type(relationship_type: &str) -> String {
    format!("{relationship_type}RelationshipWhere")
}

fn relationship_endpoint_enum_type(
    source_label: &str,
    direction: &str,
    relationship_type: &str,
    endpoint_argument: &str,
) -> String {
    format!(
        "{source_label}{}{relationship_type}{}Label",
        capitalize_graphql_suffix(direction),
        capitalize_graphql_suffix(endpoint_argument)
    )
}

fn capitalize_graphql_suffix(value: &str) -> String {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
        None => String::new(),
    }
}

fn validate_graphql_property_name(name: &str, path: impl Into<String>) -> Result<(), CoreError> {
    let path = path.into();
    match name {
        "_id" | "_elementId" | "__typename" => {
            return Err(Diagnostic::new(
                "UNSUPPORTED_GRAPHQL_SCHEMA",
                path,
                format!("graph property '{name}' collides with a reserved GraphQL virtual field"),
            )
            .into_core_error());
        }
        _ => {}
    }
    validate_graphql_name(name, path)
}

fn validate_graphql_name(name: &str, path: impl Into<String>) -> Result<(), CoreError> {
    if is_graphql_name(name) {
        return Ok(());
    }
    Err(Diagnostic::new(
        "UNSUPPORTED_GRAPHQL_SCHEMA",
        path,
        format!("'{name}' is not a valid GraphQL name"),
    )
    .into_core_error())
}

fn is_graphql_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if name.starts_with("__") || !(first == '_' || first.is_ascii_alphabetic()) {
        return false;
    }
    chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}
