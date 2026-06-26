use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;

use super::declaration::{Declaration, Node, Relationship, TableRef};
use super::diagnostic::Diagnostic;
use super::ir::{
    ComparisonOperator, Direction, GraphPlan, Literal, OrderDirection, Projection, PropertyRef,
};
use crate::CoreError;

/// Result of lowering a graph query plan to `DataFusion` SQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlTranslation {
    sql: String,
    diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone)]
enum BindingKind<'a> {
    Node(&'a Node),
    Relationship(&'a Relationship),
}

#[derive(Debug, Clone)]
struct Binding<'a> {
    alias: String,
    kind: BindingKind<'a>,
}

impl SqlTranslation {
    /// Builds a SQL translation result.
    #[must_use]
    pub fn new(sql: String, diagnostics: Vec<Diagnostic>) -> Self {
        Self { sql, diagnostics }
    }

    /// Returns the translated `DataFusion` SQL.
    #[must_use]
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Returns non-fatal translation diagnostics.
    #[must_use]
    pub fn diagnostics(&self) -> &[Diagnostic] {
        &self.diagnostics
    }
}

impl Declaration {
    /// Lowers a shared graph query plan into `DataFusion` SQL.
    ///
    /// # Errors
    ///
    /// Returns [`CoreError::InvalidInput`] when the graph plan references
    /// unknown labels, relationship types, variables, or properties, or when
    /// the plan uses a relationship shape not yet supported by the lowerer.
    pub fn lower_graph_plan(&self, plan: &GraphPlan) -> Result<SqlTranslation, CoreError> {
        Lowerer::new(self, plan).lower()
    }
}

struct Lowerer<'a> {
    graph: &'a Declaration,
    plan: &'a GraphPlan,
    bindings: BTreeMap<&'a str, Binding<'a>>,
    joined_nodes: BTreeSet<&'a str>,
    joined_relationships: BTreeSet<&'a str>,
    from_clause: String,
}

impl<'a> Lowerer<'a> {
    fn new(graph: &'a Declaration, plan: &'a GraphPlan) -> Self {
        Self {
            graph,
            plan,
            bindings: BTreeMap::new(),
            joined_nodes: BTreeSet::new(),
            joined_relationships: BTreeSet::new(),
            from_clause: String::new(),
        }
    }

    fn lower(mut self) -> Result<SqlTranslation, CoreError> {
        self.bind_nodes()?;
        self.bind_relationships()?;
        self.build_from_clause()?;

        let select = self.render_select()?;
        let where_clause = self.render_where()?;
        let order_by = self.render_order_by()?;
        let limit = self
            .plan
            .limit
            .map(|limit| format!(" LIMIT {limit}"))
            .unwrap_or_default();

        Ok(SqlTranslation::new(
            format!(
                "{select} {}{where_clause}{order_by}{limit}",
                self.from_clause
            ),
            Vec::new(),
        ))
    }

    fn bind_nodes(&mut self) -> Result<(), CoreError> {
        if self.plan.nodes.is_empty() {
            return Err(Diagnostic::new(
                "EMPTY_PLAN",
                "nodes",
                "at least one node pattern is required",
            )
            .into_core_error());
        }

        for (index, pattern) in self.plan.nodes.iter().enumerate() {
            if self.bindings.contains_key(pattern.variable.as_str()) {
                return Err(Diagnostic::new(
                    "DUPLICATE_VARIABLE",
                    format!("nodes[{index}].variable"),
                    format!("variable '{}' is bound more than once", pattern.variable),
                )
                .into_core_error());
            }
            let node = self.graph.node(&pattern.label).ok_or_else(|| {
                Diagnostic::new(
                    "UNKNOWN_NODE_LABEL",
                    format!("nodes[{index}].label"),
                    format!("unknown node label '{}'", pattern.label),
                )
                .into_core_error()
            })?;
            self.bindings.insert(
                pattern.variable.as_str(),
                Binding {
                    alias: format!("n{index}"),
                    kind: BindingKind::Node(node),
                },
            );
        }
        Ok(())
    }

    fn bind_relationships(&mut self) -> Result<(), CoreError> {
        for (index, pattern) in self.plan.relationships.iter().enumerate() {
            let relationship = self
                .graph
                .relationship(&pattern.relationship_type)
                .ok_or_else(|| {
                    Diagnostic::new(
                        "UNKNOWN_RELATIONSHIP_TYPE",
                        format!("relationships[{index}].type"),
                        format!("unknown relationship type '{}'", pattern.relationship_type),
                    )
                    .into_core_error()
                })?;
            self.relationship_endpoint_nodes(
                index,
                relationship,
                pattern.direction,
                &pattern.left,
                &pattern.right,
            )?;
            if let Some(variable) = &pattern.variable {
                if self.bindings.contains_key(variable.as_str()) {
                    return Err(Diagnostic::new(
                        "DUPLICATE_VARIABLE",
                        format!("relationships[{index}].variable"),
                        format!("variable '{variable}' is bound more than once"),
                    )
                    .into_core_error());
                }
                self.bindings.insert(
                    variable.as_str(),
                    Binding {
                        alias: format!("r{index}"),
                        kind: BindingKind::Relationship(relationship),
                    },
                );
            }
        }
        Ok(())
    }

    fn relationship_endpoint_nodes(
        &self,
        index: usize,
        relationship: &Relationship,
        direction: Direction,
        left: &str,
        right: &str,
    ) -> Result<(&Node, &Node), CoreError> {
        let left_node = self.node_binding(left).ok_or_else(|| {
            Diagnostic::new(
                "UNKNOWN_VARIABLE",
                format!("relationships[{index}].left"),
                format!("relationship references unknown left node variable '{left}'"),
            )
            .into_core_error()
        })?;
        let right_node = self.node_binding(right).ok_or_else(|| {
            Diagnostic::new(
                "UNKNOWN_VARIABLE",
                format!("relationships[{index}].right"),
                format!("relationship references unknown right node variable '{right}'"),
            )
            .into_core_error()
        })?;

        let (expected_left, expected_right) = match direction {
            Direction::Outgoing => (&relationship.from.label, &relationship.to.label),
            Direction::Incoming => (&relationship.to.label, &relationship.from.label),
        };
        if left_node.label != *expected_left || right_node.label != *expected_right {
            return Err(Diagnostic::new(
                "RELATIONSHIP_ENDPOINT_MISMATCH",
                format!("relationships[{index}]"),
                format!(
                    "relationship type '{}' expects {} -> {}, got {} -> {}",
                    relationship.relationship_type,
                    relationship.from.label,
                    relationship.to.label,
                    left_node.label,
                    right_node.label
                ),
            )
            .into_core_error());
        }

        Ok((left_node, right_node))
    }

    fn build_from_clause(&mut self) -> Result<(), CoreError> {
        let first_node = self.plan.nodes.first().ok_or_else(|| {
            Diagnostic::new(
                "EMPTY_PLAN",
                "nodes",
                "at least one node pattern is required",
            )
            .into_core_error()
        })?;
        let first_binding = self.binding(first_node.variable.as_str())?;
        let BindingKind::Node(first_node_mapping) = first_binding.kind else {
            return Err(CoreError::internal("first graph binding was not a node"));
        };
        self.from_clause = format!(
            "FROM {} AS {}",
            render_table_ref(&first_node_mapping.table),
            quote_ident(&first_binding.alias)
        );
        self.joined_nodes.insert(first_node.variable.as_str());

        let mut remaining_relationships =
            (0..self.plan.relationships.len()).collect::<BTreeSet<_>>();
        while !remaining_relationships.is_empty() {
            let mut progressed = false;
            for index in remaining_relationships.iter().copied().collect::<Vec<_>>() {
                let pattern = self.plan.relationships.get(index).ok_or_else(|| {
                    CoreError::internal("validated relationship index was out of bounds")
                })?;
                let left_joined = self.joined_nodes.contains(pattern.left.as_str());
                let right_joined = self.joined_nodes.contains(pattern.right.as_str());
                if left_joined || right_joined {
                    let relationship = self
                        .graph
                        .relationship(&pattern.relationship_type)
                        .ok_or_else(|| {
                            CoreError::internal("relationship binding missing during lowering")
                        })?;
                    self.join_relationship(index, pattern, relationship)?;
                    remaining_relationships.remove(&index);
                    progressed = true;
                }
            }
            if !progressed {
                return Err(Diagnostic::new(
                    "DISCONNECTED_PATTERN",
                    "relationships",
                    "remaining relationships do not connect to an already joined node",
                )
                .into_core_error());
            }
        }

        for node in &self.plan.nodes {
            if !self.joined_nodes.contains(node.variable.as_str()) {
                return Err(Diagnostic::new(
                    "DISCONNECTED_PATTERN",
                    "nodes",
                    format!(
                        "node variable '{}' is not connected to the first node pattern",
                        node.variable
                    ),
                )
                .into_core_error());
            }
        }
        Ok(())
    }

    fn join_relationship(
        &mut self,
        index: usize,
        pattern: &'a super::ir::RelationshipPattern,
        relationship: &Relationship,
    ) -> Result<(), CoreError> {
        let left_joined = self.joined_nodes.contains(pattern.left.as_str());
        let right_joined = self.joined_nodes.contains(pattern.right.as_str());
        if !left_joined && !right_joined {
            return Err(Diagnostic::new(
                "DISCONNECTED_PATTERN",
                format!("relationships[{index}]"),
                "relationship does not connect to an already joined node",
            )
            .into_core_error());
        }

        let relationship_alias = pattern
            .variable
            .as_deref()
            .and_then(|variable| {
                self.bindings
                    .get(variable)
                    .map(|binding| binding.alias.clone())
            })
            .unwrap_or_else(|| format!("r{index}"));
        let quoted_relationship_alias = quote_ident(&relationship_alias);

        if left_joined && right_joined {
            let left_condition = self.relationship_join_condition(
                relationship,
                pattern.direction,
                &relationship_alias,
                &pattern.left,
                true,
            )?;
            let right_condition = self.relationship_join_condition(
                relationship,
                pattern.direction,
                &relationship_alias,
                &pattern.right,
                false,
            )?;
            write!(
                &mut self.from_clause,
                " JOIN {} AS {} ON {} AND {}",
                render_table_ref(&relationship.table),
                quoted_relationship_alias,
                left_condition,
                right_condition
            )
            .map_err(|_| CoreError::internal("failed to render graph SQL"))?;
        } else if left_joined {
            self.join_from_known_node(relationship, pattern, &relationship_alias, true)?;
        } else {
            self.join_from_known_node(relationship, pattern, &relationship_alias, false)?;
        }

        if let Some(variable) = &pattern.variable {
            self.joined_relationships.insert(variable.as_str());
        }
        Ok(())
    }

    fn join_from_known_node(
        &mut self,
        relationship: &Relationship,
        pattern: &'a super::ir::RelationshipPattern,
        relationship_alias: &str,
        left_is_known: bool,
    ) -> Result<(), CoreError> {
        let (known_variable, unknown_variable, known_is_left) = if left_is_known {
            (pattern.left.as_str(), pattern.right.as_str(), true)
        } else {
            (pattern.right.as_str(), pattern.left.as_str(), false)
        };
        let unknown_node = self.node_binding(unknown_variable).ok_or_else(|| {
            CoreError::internal("unknown node binding missing after relationship validation")
        })?;
        let relationship_join = self.relationship_join_condition(
            relationship,
            pattern.direction,
            relationship_alias,
            known_variable,
            known_is_left,
        )?;
        let unknown_table_ref = render_table_ref(&unknown_node.table);
        let unknown_alias = self.binding(unknown_variable)?.alias.clone();
        let unknown_join = self.relationship_join_condition(
            relationship,
            pattern.direction,
            relationship_alias,
            unknown_variable,
            !known_is_left,
        )?;

        write!(
            &mut self.from_clause,
            " JOIN {} AS {} ON {}",
            render_table_ref(&relationship.table),
            quote_ident(relationship_alias),
            relationship_join
        )
        .map_err(|_| CoreError::internal("failed to render graph SQL"))?;
        write!(
            &mut self.from_clause,
            " JOIN {} AS {} ON {}",
            unknown_table_ref,
            quote_ident(&unknown_alias),
            unknown_join
        )
        .map_err(|_| CoreError::internal("failed to render graph SQL"))?;
        self.joined_nodes.insert(unknown_variable);
        Ok(())
    }

    fn relationship_join_condition(
        &self,
        relationship: &Relationship,
        direction: Direction,
        relationship_alias: &str,
        node_variable: &str,
        node_is_left: bool,
    ) -> Result<String, CoreError> {
        let node_binding = self.binding(node_variable)?;
        let BindingKind::Node(node) = node_binding.kind else {
            return Err(CoreError::internal(
                "relationship endpoint was not a node binding",
            ));
        };
        let endpoint_column = match (direction, node_is_left) {
            (Direction::Outgoing, true) | (Direction::Incoming, false) => &relationship.from.key,
            (Direction::Outgoing, false) | (Direction::Incoming, true) => &relationship.to.key,
        };

        Ok(format!(
            "{}.{} = {}.{}",
            quote_ident(relationship_alias),
            quote_ident(endpoint_column),
            quote_ident(&node_binding.alias),
            quote_ident(&node.key)
        ))
    }

    fn render_select(&self) -> Result<String, CoreError> {
        if self.plan.projections.is_empty() {
            return Err(Diagnostic::new(
                "EMPTY_PROJECTION",
                "projections",
                "at least one projection is required",
            )
            .into_core_error());
        }

        let mut rendered = Vec::with_capacity(self.plan.projections.len());
        for projection in &self.plan.projections {
            match projection {
                Projection::Property { property, alias } => {
                    let expression = self.render_property_ref(property)?;
                    let alias = alias
                        .clone()
                        .unwrap_or_else(|| format!("{}_{}", property.variable, property.property));
                    rendered.push(format!("{expression} AS {}", quote_ident(&alias)));
                }
                Projection::CountAll { alias } => {
                    rendered.push(format!("COUNT(*) AS {}", quote_ident(alias)));
                }
            }
        }
        Ok(format!("SELECT {}", rendered.join(", ")))
    }

    fn render_where(&self) -> Result<String, CoreError> {
        if self.plan.predicates.is_empty() {
            return Ok(String::new());
        }

        let mut predicates = Vec::with_capacity(self.plan.predicates.len());
        for predicate in &self.plan.predicates {
            let property = self.render_property_ref(&predicate.property)?;
            predicates.push(match (predicate.operator, &predicate.literal) {
                (ComparisonOperator::Equal, Literal::Null) => format!("{property} IS NULL"),
                (ComparisonOperator::NotEqual, Literal::Null) => {
                    format!("{property} IS NOT NULL")
                }
                _ => format!(
                    "{} {} {}",
                    property,
                    render_operator(predicate.operator),
                    render_literal(&predicate.literal)
                ),
            });
        }
        Ok(format!(" WHERE {}", predicates.join(" AND ")))
    }

    fn render_order_by(&self) -> Result<String, CoreError> {
        if self.plan.order_by.is_empty() {
            return Ok(String::new());
        }

        let mut keys = Vec::with_capacity(self.plan.order_by.len());
        for key in &self.plan.order_by {
            keys.push(format!(
                "{} {}",
                self.render_property_ref(&key.property)?,
                match key.direction {
                    OrderDirection::Ascending => "ASC",
                    OrderDirection::Descending => "DESC",
                }
            ));
        }
        Ok(format!(" ORDER BY {}", keys.join(", ")))
    }

    fn render_property_ref(&self, property: &PropertyRef) -> Result<String, CoreError> {
        let binding = self.binding(&property.variable)?;
        let column = match binding.kind {
            BindingKind::Node(node) => node.column_for_property(&property.property),
            BindingKind::Relationship(relationship) => {
                relationship.column_for_property(&property.property)
            }
        }
        .ok_or_else(|| {
            Diagnostic::new(
                "UNKNOWN_PROPERTY",
                "property",
                format!(
                    "variable '{}' does not expose property '{}'",
                    property.variable, property.property
                ),
            )
            .into_core_error()
        })?;

        Ok(format!(
            "{}.{}",
            quote_ident(&binding.alias),
            quote_ident(column)
        ))
    }

    fn node_binding(&self, variable: &str) -> Option<&Node> {
        match self.bindings.get(variable).map(|binding| &binding.kind) {
            Some(BindingKind::Node(node)) => Some(node),
            _ => None,
        }
    }

    fn binding(&self, variable: &str) -> Result<&Binding<'a>, CoreError> {
        self.bindings.get(variable).ok_or_else(|| {
            Diagnostic::new(
                "UNKNOWN_VARIABLE",
                "variable",
                format!("unknown graph variable '{variable}'"),
            )
            .into_core_error()
        })
    }
}

fn render_table_ref(table: &TableRef) -> String {
    format!(
        "{}.{}",
        quote_ident(&table.schema),
        quote_ident(&table.name)
    )
}

fn render_operator(operator: ComparisonOperator) -> &'static str {
    match operator {
        ComparisonOperator::Equal => "=",
        ComparisonOperator::NotEqual => "<>",
        ComparisonOperator::GreaterThan => ">",
        ComparisonOperator::GreaterThanOrEqual => ">=",
        ComparisonOperator::LessThan => "<",
        ComparisonOperator::LessThanOrEqual => "<=",
    }
}

fn render_literal(literal: &Literal) -> String {
    match literal {
        Literal::String(value) => quote_string_literal(value),
        Literal::Integer(value) => value.to_string(),
        Literal::Boolean(value) => value.to_string(),
        Literal::Null => "NULL".to_string(),
    }
}

fn quote_ident(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn quote_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::virtual_graph::ir::{
        ComparisonOperator, Direction, GraphPlan, Literal, NodePattern, OrderDirection, OrderKey,
        Projection, PropertyPredicate, PropertyRef, RelationshipPattern,
    };

    const GRAPH: &str = r"
version: 1
name: ownership
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
relationships:
  - type: OWNS
    table: { schema: ops, name: ownerships }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
    properties:
      since: since
  - type: DEPENDS_ON
    table: { schema: ops, name: service_dependencies }
    from: { label: Service, key: from_service_id }
    to: { label: Service, key: to_service_id }
    properties:
      criticality: criticality
";

    #[test]
    fn lower_graph_plan_renders_forward_relationship_sql() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let plan = ownership_plan(Direction::Outgoing);

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("plan should lower to SQL");

        assert_eq!(
            translation.sql(),
            "SELECT \"n0\".\"full_name\" AS \"owner\", \"n1\".\"service_name\" AS \"service\" \
             FROM \"ops\".\"people\" AS \"n0\" \
             JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"person_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"service_id\" = \"n1\".\"id\" \
             WHERE \"n1\".\"tier\" = 'prod' ORDER BY \"n0\".\"full_name\" ASC LIMIT 25"
        );
    }

    #[test]
    fn lower_graph_plan_renders_reverse_relationship_sql() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let plan = GraphPlan {
            nodes: vec![
                NodePattern {
                    variable: "service".to_string(),
                    label: "Service".to_string(),
                },
                NodePattern {
                    variable: "person".to_string(),
                    label: "Person".to_string(),
                },
            ],
            relationships: vec![RelationshipPattern {
                variable: None,
                relationship_type: "OWNS".to_string(),
                left: "service".to_string(),
                direction: Direction::Incoming,
                right: "person".to_string(),
            }],
            projections: vec![Projection::Property {
                property: PropertyRef {
                    variable: "person".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("owner".to_string()),
            }],
            predicates: Vec::new(),
            order_by: Vec::new(),
            limit: None,
        };

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("reverse relationship should lower");

        assert_eq!(
            translation.sql(),
            "SELECT \"n1\".\"full_name\" AS \"owner\" FROM \"ops\".\"services\" AS \"n0\" \
             JOIN \"ops\".\"ownerships\" AS \"r0\" ON \"r0\".\"service_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"people\" AS \"n1\" ON \"r0\".\"person_id\" = \"n1\".\"id\""
        );
    }

    #[test]
    fn lower_graph_plan_reorders_connected_relationships() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let plan = GraphPlan {
            nodes: vec![
                NodePattern {
                    variable: "source".to_string(),
                    label: "Service".to_string(),
                },
                NodePattern {
                    variable: "middle".to_string(),
                    label: "Service".to_string(),
                },
                NodePattern {
                    variable: "target".to_string(),
                    label: "Service".to_string(),
                },
            ],
            relationships: vec![
                RelationshipPattern {
                    variable: None,
                    relationship_type: "DEPENDS_ON".to_string(),
                    left: "middle".to_string(),
                    direction: Direction::Outgoing,
                    right: "target".to_string(),
                },
                RelationshipPattern {
                    variable: None,
                    relationship_type: "DEPENDS_ON".to_string(),
                    left: "source".to_string(),
                    direction: Direction::Outgoing,
                    right: "middle".to_string(),
                },
            ],
            projections: vec![Projection::Property {
                property: PropertyRef {
                    variable: "target".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("target".to_string()),
            }],
            predicates: Vec::new(),
            order_by: Vec::new(),
            limit: None,
        };

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("connected relationships should lower independent of order");

        assert_eq!(
            translation.sql(),
            "SELECT \"n2\".\"service_name\" AS \"target\" FROM \"ops\".\"services\" AS \"n0\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r1\" ON \"r1\".\"from_service_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r1\".\"to_service_id\" = \"n1\".\"id\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r0\" ON \"r0\".\"from_service_id\" = \"n1\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n2\" ON \"r0\".\"to_service_id\" = \"n2\".\"id\""
        );
    }

    #[test]
    fn lower_graph_plan_quotes_identifiers_and_literals() {
        let graph = Declaration::from_yaml(
            r#"
version: 1
name: quoting
nodes:
  - label: Weird
    table: { schema: weird-schema, name: table"name }
    key: id"key
    properties:
      display: display"name
relationships: []
"#,
        )
        .expect("graph should parse");
        let plan = GraphPlan {
            nodes: vec![NodePattern {
                variable: "weird".to_string(),
                label: "Weird".to_string(),
            }],
            relationships: Vec::new(),
            projections: vec![Projection::Property {
                property: PropertyRef {
                    variable: "weird".to_string(),
                    property: "display".to_string(),
                },
                alias: Some("value".to_string()),
            }],
            predicates: vec![PropertyPredicate {
                property: PropertyRef {
                    variable: "weird".to_string(),
                    property: "display".to_string(),
                },
                operator: ComparisonOperator::Equal,
                literal: Literal::String("Ada's laptop".to_string()),
            }],
            order_by: Vec::new(),
            limit: None,
        };

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("quoted plan should lower");

        assert_eq!(
            translation.sql(),
            "SELECT \"n0\".\"display\"\"name\" AS \"value\" \
             FROM \"weird-schema\".\"table\"\"name\" AS \"n0\" \
             WHERE \"n0\".\"display\"\"name\" = 'Ada''s laptop'"
        );
    }

    #[test]
    fn lower_graph_plan_renders_null_predicates() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.predicates = vec![PropertyPredicate {
            property: PropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            literal: Literal::Null,
        }];
        plan.order_by = Vec::new();
        plan.limit = None;

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("null predicate should lower");

        assert!(
            translation.sql().contains("\"n1\".\"tier\" IS NULL"),
            "{}",
            translation.sql()
        );
    }

    #[test]
    fn lower_graph_plan_rejects_endpoint_mismatch() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        let service_node = plan
            .nodes
            .get_mut(1)
            .expect("ownership fixture should include a service node");
        service_node.label = "Person".to_string();

        let error = graph
            .lower_graph_plan(&plan)
            .expect_err("endpoint mismatch should fail");

        assert!(
            error.to_string().contains("RELATIONSHIP_ENDPOINT_MISMATCH"),
            "{error:?}"
        );
    }

    fn ownership_plan(direction: Direction) -> GraphPlan {
        GraphPlan {
            nodes: vec![
                NodePattern {
                    variable: "person".to_string(),
                    label: "Person".to_string(),
                },
                NodePattern {
                    variable: "service".to_string(),
                    label: "Service".to_string(),
                },
            ],
            relationships: vec![RelationshipPattern {
                variable: None,
                relationship_type: "OWNS".to_string(),
                left: "person".to_string(),
                direction,
                right: "service".to_string(),
            }],
            projections: vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "person".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("owner".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("service".to_string()),
                },
            ],
            predicates: vec![PropertyPredicate {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "tier".to_string(),
                },
                operator: ComparisonOperator::Equal,
                literal: Literal::String("prod".to_string()),
            }],
            order_by: vec![OrderKey {
                property: PropertyRef {
                    variable: "person".to_string(),
                    property: "name".to_string(),
                },
                direction: OrderDirection::Ascending,
            }],
            limit: Some(25),
        }
    }
}
