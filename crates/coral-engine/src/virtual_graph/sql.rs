use std::collections::BTreeSet;
use std::fmt::Write as _;

use super::declaration::{Declaration, Relationship, TableRef};
use super::diagnostic::Diagnostic;
use super::ir::{
    ComparisonOperator, Direction, GraphPlan, Literal, OrderDirection, Projection, PropertyRef,
};
use super::validation::{ValidatedBindingKind, ValidatedGraphPlan};
use crate::CoreError;

/// Result of lowering a graph query plan to `DataFusion` SQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlTranslation {
    sql: String,
    diagnostics: Vec<Diagnostic>,
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
        let validated = self.validate_graph_plan(plan)?;
        Lowerer::new(validated).lower()
    }
}

struct Lowerer<'a> {
    validated: ValidatedGraphPlan<'a>,
    joined_nodes: BTreeSet<&'a str>,
    from_clause: String,
}

impl<'a> Lowerer<'a> {
    fn new(validated: ValidatedGraphPlan<'a>) -> Self {
        Self {
            validated,
            joined_nodes: BTreeSet::new(),
            from_clause: String::new(),
        }
    }

    fn lower(mut self) -> Result<SqlTranslation, CoreError> {
        self.build_from_clause()?;

        let select = self.render_select()?;
        let where_clause = self.render_where()?;
        let order_by = self.render_order_by()?;
        let limit = self
            .validated
            .plan()
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

    fn build_from_clause(&mut self) -> Result<(), CoreError> {
        let plan = self.validated.plan();
        let validated = &self.validated;
        let first_node = plan
            .nodes
            .first()
            .ok_or_else(|| CoreError::internal("validated graph plan had no nodes"))?;
        let first_binding = validated.binding(first_node.variable.as_str())?;
        let ValidatedBindingKind::Node(first_node_mapping) = first_binding.kind() else {
            return Err(CoreError::internal("first graph binding was not a node"));
        };
        self.from_clause = format!(
            "FROM {} AS {}",
            render_table_ref(&first_node_mapping.table),
            quote_ident(first_binding.alias())
        );
        self.joined_nodes.insert(first_node.variable.as_str());

        let mut remaining_relationships = (0..plan.relationships.len()).collect::<BTreeSet<_>>();
        while !remaining_relationships.is_empty() {
            let mut progressed = false;
            for index in remaining_relationships.iter().copied().collect::<Vec<_>>() {
                let pattern = plan.relationships.get(index).ok_or_else(|| {
                    CoreError::internal("validated relationship index was out of bounds")
                })?;
                let left_joined = self.joined_nodes.contains(pattern.left.as_str());
                let right_joined = self.joined_nodes.contains(pattern.right.as_str());
                if left_joined || right_joined {
                    let relationship = validated.relationship_mapping(index)?;
                    Self::join_relationship(
                        validated,
                        &mut self.joined_nodes,
                        &mut self.from_clause,
                        index,
                        pattern,
                        relationship,
                    )?;
                    remaining_relationships.remove(&index);
                    progressed = true;
                }
            }
            if !progressed {
                return Err(CoreError::internal(
                    "validated graph plan contained an unjoinable relationship",
                ));
            }
        }

        for node in &plan.nodes {
            if !self.joined_nodes.contains(node.variable.as_str()) {
                return Err(CoreError::internal(
                    "validated graph plan contained a disconnected node",
                ));
            }
        }
        Ok(())
    }

    fn join_relationship(
        validated: &ValidatedGraphPlan<'a>,
        joined_nodes: &mut BTreeSet<&'a str>,
        from_clause: &mut String,
        index: usize,
        pattern: &'a super::ir::RelationshipPattern,
        relationship: &Relationship,
    ) -> Result<(), CoreError> {
        let left_joined = joined_nodes.contains(pattern.left.as_str());
        let right_joined = joined_nodes.contains(pattern.right.as_str());
        if !left_joined && !right_joined {
            return Err(CoreError::internal(
                "validated graph relationship was not joinable",
            ));
        }

        let relationship_alias = validated.relationship_alias(index, pattern);
        let quoted_relationship_alias = quote_ident(&relationship_alias);

        if left_joined && right_joined {
            let left_condition = Self::relationship_join_condition(
                validated,
                relationship,
                pattern.direction,
                &relationship_alias,
                &pattern.left,
                true,
            )?;
            let right_condition = Self::relationship_join_condition(
                validated,
                relationship,
                pattern.direction,
                &relationship_alias,
                &pattern.right,
                false,
            )?;
            write!(
                from_clause,
                " JOIN {} AS {} ON {} AND {}",
                render_table_ref(&relationship.table),
                quoted_relationship_alias,
                left_condition,
                right_condition
            )
            .map_err(|_| CoreError::internal("failed to render graph SQL"))?;
        } else if left_joined {
            Self::join_from_known_node(
                validated,
                joined_nodes,
                from_clause,
                relationship,
                pattern,
                &relationship_alias,
                true,
            )?;
        } else {
            Self::join_from_known_node(
                validated,
                joined_nodes,
                from_clause,
                relationship,
                pattern,
                &relationship_alias,
                false,
            )?;
        }

        Ok(())
    }

    fn join_from_known_node(
        validated: &ValidatedGraphPlan<'a>,
        joined_nodes: &mut BTreeSet<&'a str>,
        from_clause: &mut String,
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
        let unknown_node = validated.node_binding(unknown_variable)?;
        let relationship_join = Self::relationship_join_condition(
            validated,
            relationship,
            pattern.direction,
            relationship_alias,
            known_variable,
            known_is_left,
        )?;
        let unknown_table_ref = render_table_ref(&unknown_node.table);
        let unknown_alias = validated.binding(unknown_variable)?.alias().to_string();
        let unknown_join = Self::relationship_join_condition(
            validated,
            relationship,
            pattern.direction,
            relationship_alias,
            unknown_variable,
            !known_is_left,
        )?;

        write!(
            from_clause,
            " JOIN {} AS {} ON {}",
            render_table_ref(&relationship.table),
            quote_ident(relationship_alias),
            relationship_join
        )
        .map_err(|_| CoreError::internal("failed to render graph SQL"))?;
        write!(
            from_clause,
            " JOIN {} AS {} ON {}",
            unknown_table_ref,
            quote_ident(&unknown_alias),
            unknown_join
        )
        .map_err(|_| CoreError::internal("failed to render graph SQL"))?;
        joined_nodes.insert(unknown_variable);
        Ok(())
    }

    fn relationship_join_condition(
        validated: &ValidatedGraphPlan<'a>,
        relationship: &Relationship,
        direction: Direction,
        relationship_alias: &str,
        node_variable: &str,
        node_is_left: bool,
    ) -> Result<String, CoreError> {
        let node_binding = validated.binding(node_variable)?;
        let ValidatedBindingKind::Node(node) = node_binding.kind() else {
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
            quote_ident(node_binding.alias()),
            quote_ident(&node.key)
        ))
    }

    fn render_select(&self) -> Result<String, CoreError> {
        let mut rendered = Vec::with_capacity(self.validated.plan().projections.len());
        for projection in &self.validated.plan().projections {
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
        if self.validated.plan().predicates.is_empty() {
            return Ok(String::new());
        }

        let mut predicates = Vec::with_capacity(self.validated.plan().predicates.len());
        for predicate in &self.validated.plan().predicates {
            predicates.push(self.render_predicate(predicate)?);
        }
        Ok(format!(" WHERE {}", predicates.join(" AND ")))
    }

    fn render_predicate(
        &self,
        predicate: &super::ir::PropertyPredicate,
    ) -> Result<String, CoreError> {
        let property = self.render_property_ref(&predicate.property)?;
        match (&predicate.operator, &predicate.literal) {
            (ComparisonOperator::Equal, Literal::Null) => Ok(format!("{property} IS NULL")),
            (ComparisonOperator::NotEqual, Literal::Null) => Ok(format!("{property} IS NOT NULL")),
            (
                ComparisonOperator::GreaterThan
                | ComparisonOperator::GreaterThanOrEqual
                | ComparisonOperator::LessThan
                | ComparisonOperator::LessThanOrEqual,
                Literal::Null,
            ) => Err(CoreError::internal(
                "validated graph predicate contained an invalid null comparison",
            )),
            _ => Ok(format!(
                "{property} {} {}",
                render_operator(predicate.operator),
                render_literal(&predicate.literal)
            )),
        }
    }

    fn render_order_by(&self) -> Result<String, CoreError> {
        if self.validated.plan().order_by.is_empty() {
            return Ok(String::new());
        }

        let mut keys = Vec::with_capacity(self.validated.plan().order_by.len());
        for key in &self.validated.plan().order_by {
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
        let binding = self.validated.binding(&property.variable)?;
        let column = match binding.kind() {
            ValidatedBindingKind::Node(node) => node.column_for_property(&property.property),
            ValidatedBindingKind::Relationship(relationship) => {
                relationship.column_for_property(&property.property)
            }
        }
        .ok_or_else(|| {
            CoreError::internal("validated graph property reference was not resolvable")
        })?;

        Ok(format!(
            "{}.{}",
            quote_ident(binding.alias()),
            quote_ident(column)
        ))
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
    fn lower_graph_plan_renders_relationship_between_joined_nodes() {
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
                    left: "source".to_string(),
                    direction: Direction::Outgoing,
                    right: "middle".to_string(),
                },
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
                    right: "target".to_string(),
                },
            ],
            projections: vec![
                Projection::Property {
                    property: PropertyRef {
                        variable: "source".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("source".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "middle".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("middle".to_string()),
                },
                Projection::Property {
                    property: PropertyRef {
                        variable: "target".to_string(),
                        property: "name".to_string(),
                    },
                    alias: Some("target".to_string()),
                },
            ],
            predicates: Vec::new(),
            order_by: Vec::new(),
            limit: None,
        };

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("closed service dependency path should lower");

        assert_eq!(
            translation.sql(),
            "SELECT \"n0\".\"service_name\" AS \"source\", \"n1\".\"service_name\" AS \"middle\", \"n2\".\"service_name\" AS \"target\" \
             FROM \"ops\".\"services\" AS \"n0\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r0\" ON \"r0\".\"from_service_id\" = \"n0\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n1\" ON \"r0\".\"to_service_id\" = \"n1\".\"id\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r1\" ON \"r1\".\"from_service_id\" = \"n1\".\"id\" \
             JOIN \"ops\".\"services\" AS \"n2\" ON \"r1\".\"to_service_id\" = \"n2\".\"id\" \
             JOIN \"ops\".\"service_dependencies\" AS \"r2\" ON \"r2\".\"from_service_id\" = \"n0\".\"id\" AND \"r2\".\"to_service_id\" = \"n2\".\"id\""
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
        let plan = GraphPlan {
            nodes: vec![NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            }],
            relationships: Vec::new(),
            projections: vec![Projection::Property {
                property: PropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service".to_string()),
            }],
            predicates: vec![
                PropertyPredicate {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "tier".to_string(),
                    },
                    operator: ComparisonOperator::Equal,
                    literal: Literal::Null,
                },
                PropertyPredicate {
                    property: PropertyRef {
                        variable: "service".to_string(),
                        property: "name".to_string(),
                    },
                    operator: ComparisonOperator::NotEqual,
                    literal: Literal::Null,
                },
            ],
            order_by: Vec::new(),
            limit: None,
        };

        let translation = graph
            .lower_graph_plan(&plan)
            .expect("null predicate plan should lower");

        assert_eq!(
            translation.sql(),
            "SELECT \"n0\".\"service_name\" AS \"service\" FROM \"ops\".\"services\" AS \"n0\" \
             WHERE \"n0\".\"tier\" IS NULL AND \"n0\".\"service_name\" IS NOT NULL"
        );
    }

    #[test]
    fn lower_graph_plan_rejects_mixed_count_and_property_projection() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.projections.push(Projection::CountAll {
            alias: "ownership_count".to_string(),
        });

        let error = graph
            .lower_graph_plan(&plan)
            .expect_err("mixed aggregate and property projection should fail");

        assert!(
            error.to_string().contains("UNSUPPORTED_AGGREGATION"),
            "{error:?}"
        );
    }

    #[test]
    fn lower_graph_plan_rejects_count_with_property_ordering() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        plan.projections = vec![Projection::CountAll {
            alias: "ownership_count".to_string(),
        }];

        let error = graph
            .lower_graph_plan(&plan)
            .expect_err("count with property ordering should fail");

        assert!(
            error.to_string().contains("UNSUPPORTED_AGGREGATION"),
            "{error:?}"
        );
    }

    #[test]
    fn lower_graph_plan_rejects_ordered_null_comparisons() {
        let graph = Declaration::from_yaml(GRAPH).expect("graph should parse");
        let mut plan = ownership_plan(Direction::Outgoing);
        let predicate = plan
            .predicates
            .get_mut(0)
            .expect("ownership fixture should include a predicate");
        predicate.operator = ComparisonOperator::GreaterThan;
        predicate.literal = Literal::Null;

        let error = graph
            .lower_graph_plan(&plan)
            .expect_err("ordered null comparison should fail");

        assert!(
            error.to_string().contains("INVALID_NULL_COMPARISON"),
            "{error:?}"
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
