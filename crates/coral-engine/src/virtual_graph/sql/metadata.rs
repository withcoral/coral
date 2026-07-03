//! Binding and graph-metadata reference rendering for the SQL Lowerer: resolves graph
//! bindings and projection aliases into SQL column expressions for properties, keys,
//! element ids, labels, relationship types, property keys, and presence/identity
//! (including undirected-endpoint variants), plus precomputed EXISTS/COUNT/COLLECT
//! scalar-subquery result references and their EXISTS local-node/relationship mappings.

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "SQL metadata helpers are split into a child module while preserving parent-private access."
)]
use super::*;

impl<'a> Lowerer<'a> {
    pub(super) fn next_scalar_subquery_alias(&self, prefix: &str) -> String {
        let index = self.next_scalar_subquery_alias.get();
        self.next_scalar_subquery_alias.set(index + 1);
        quote_ident(&format!("{prefix}_{index}"))
    }

    pub(super) fn render_precomputed_count_subquery_ref(
        &self,
        pattern: &CountSubqueryPattern,
        distinct_target: Option<&ScalarExpression>,
    ) -> Option<String> {
        self.precomputed_scalar_subqueries
            .iter()
            .find(|precomputed| {
                precomputed.candidate
                    == ScalarSubqueryCandidate::Count {
                        pattern: pattern.clone(),
                        distinct_target: distinct_target.cloned(),
                    }
            })
            .map(Self::render_precomputed_count_ref)
    }

    pub(super) fn render_precomputed_exists_pattern_ref(
        &self,
        predicate: &ExistsPatternPredicate,
    ) -> Option<String> {
        self.precomputed_scalar_subqueries
            .iter()
            .find(|precomputed| {
                precomputed.candidate == ScalarSubqueryCandidate::Exists(predicate.clone())
            })
            .map(Self::render_precomputed_exists_ref)
    }

    pub(super) fn render_precomputed_collect_subquery_ref(
        &self,
        pattern: &ExistsPatternPredicate,
        target: &ScalarExpression,
        distinct: bool,
    ) -> Option<String> {
        self.precomputed_scalar_subqueries
            .iter()
            .find(|precomputed| {
                precomputed.candidate
                    == ScalarSubqueryCandidate::Collect {
                        pattern: pattern.clone(),
                        target: target.clone(),
                        distinct,
                    }
            })
            .map(Self::render_precomputed_collect_ref)
    }

    fn render_precomputed_count_ref(precomputed: &PrecomputedScalarSubquery) -> String {
        format!(
            "COALESCE({}.{}, 0)",
            quote_ident(&precomputed.table_alias),
            quote_ident(&precomputed.value_alias)
        )
    }

    pub(super) fn render_precomputed_exists_ref(precomputed: &PrecomputedScalarSubquery) -> String {
        format!(
            "COALESCE({}.{}, FALSE)",
            quote_ident(&precomputed.table_alias),
            quote_ident(&precomputed.value_alias)
        )
    }

    fn render_precomputed_collect_ref(precomputed: &PrecomputedScalarSubquery) -> String {
        format!(
            "COALESCE({}.{}, make_array())",
            quote_ident(&precomputed.table_alias),
            quote_ident(&precomputed.value_alias)
        )
    }

    pub(super) fn exists_local_node_map<'b>(
        &self,
        predicate: &'b ExistsPatternPredicate,
    ) -> Result<BTreeMap<&'b str, &'a Node>, CoreError> {
        self.scoped_local_node_map(&predicate.nodes)
    }

    pub(super) fn exists_local_node_aliases(
        predicate: &ExistsPatternPredicate,
    ) -> BTreeMap<&str, String> {
        predicate
            .nodes
            .iter()
            .enumerate()
            .map(|(index, node)| (node.variable.as_str(), format!("__coral_exists_n{index}")))
            .collect()
    }

    pub(super) fn count_local_node_aliases(nodes: &[NodePattern]) -> BTreeMap<&str, String> {
        nodes
            .iter()
            .enumerate()
            .map(|(index, node)| (node.variable.as_str(), format!("__coral_count_n{index}")))
            .collect()
    }

    pub(super) fn exists_relationship_bindings<'b>(
        &self,
        predicate: &'b ExistsPatternPredicate,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
    ) -> Result<Vec<ExistsRelationshipSqlBinding<'a, 'b>>, CoreError> {
        predicate
            .relationships
            .iter()
            .enumerate()
            .map(|(index, pattern)| {
                self.exists_relationship_mapping(pattern, local_nodes)
                    .map(|relationship| ExistsRelationshipSqlBinding {
                        pattern,
                        relationship,
                        alias: Self::exists_relationship_alias(index),
                    })
            })
            .collect()
    }

    fn exists_relationship_alias(index: usize) -> String {
        format!("__coral_exists_r{index}")
    }

    fn exists_relationship_mapping<'b>(
        &self,
        pattern: &'b RelationshipPattern,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
    ) -> Result<&'a Relationship, CoreError> {
        let left_node = self.exists_node_mapping(local_nodes, &pattern.left)?;
        let right_node = self.exists_node_mapping(local_nodes, &pattern.right)?;
        let matches = self
            .validated
            .graph()
            .relationships_for_type(&pattern.relationship_type)
            .filter(|relationship| {
                Self::relationship_matches_labels(
                    relationship,
                    pattern.direction,
                    &left_node.label,
                    &right_node.label,
                )
            })
            .collect::<Vec<_>>();
        match matches.as_slice() {
            [relationship] => Ok(*relationship),
            [] => Err(CoreError::internal(
                "validated EXISTS relationship mapping was not resolvable",
            )),
            _ => Err(CoreError::internal(
                "validated EXISTS relationship mapping was ambiguous",
            )),
        }
    }

    pub(super) fn exists_node_mapping<'b>(
        &self,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        variable: &str,
    ) -> Result<&'a Node, CoreError> {
        if let Some(node) = local_nodes.get(variable).copied() {
            return Ok(node);
        }
        let binding = self.validated.binding(variable)?;
        let ValidatedBindingKind::Node(node) = binding.kind() else {
            return Err(CoreError::internal(
                "validated EXISTS endpoint was not a node binding",
            ));
        };
        Ok(*node)
    }

    pub(super) fn exists_relationship_condition<'b>(
        &self,
        pattern: &'b RelationshipPattern,
        relationship: &Relationship,
        relationship_alias: &str,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        let left_node = self.exists_node_mapping(local_nodes, &pattern.left)?;
        let right_node = self.exists_node_mapping(local_nodes, &pattern.right)?;
        let orientations = Self::relationship_orientations_for_labels(
            relationship,
            pattern.direction,
            &left_node.label,
            &right_node.label,
        )?;
        let has_multiple_orientations = orientations.len() > 1;
        let conditions = orientations
            .iter()
            .map(|orientation| {
                let left_ref =
                    self.exists_node_key_ref(&pattern.left, left_node, local_nodes, local_aliases)?;
                let right_ref = self.exists_node_key_ref(
                    &pattern.right,
                    right_node,
                    local_nodes,
                    local_aliases,
                )?;
                let condition = format!(
                    "{}.{} = {} AND {}.{} = {}",
                    quote_ident(relationship_alias),
                    quote_ident(&orientation.left_relationship_key),
                    left_ref,
                    quote_ident(relationship_alias),
                    quote_ident(&orientation.right_relationship_key),
                    right_ref
                );
                if has_multiple_orientations {
                    Ok(format!("({condition})"))
                } else {
                    Ok(condition)
                }
            })
            .collect::<Result<Vec<_>, CoreError>>()?;
        Self::render_condition_disjunction(&conditions)
    }

    fn exists_node_key_ref<'b>(
        &self,
        variable: &str,
        node: &Node,
        local_nodes: &BTreeMap<&'b str, &'a Node>,
        local_aliases: &BTreeMap<&'b str, String>,
    ) -> Result<String, CoreError> {
        if local_nodes.contains_key(variable) {
            let alias = local_aliases
                .get(variable)
                .ok_or_else(|| CoreError::internal("validated EXISTS node alias was missing"))?;
            return Ok(format!("{}.{}", quote_ident(alias), quote_ident(&node.key)));
        }
        self.render_binding_key_ref(variable)
    }

    pub(super) fn render_binding_presence_ref(&self, variable: &str) -> Result<String, CoreError> {
        let binding = self.validated.binding(variable)?;
        let column = match binding.kind() {
            ValidatedBindingKind::Node(node) => node.key.as_str(),
            ValidatedBindingKind::Relationship(relationship) => relationship
                .key
                .as_deref()
                .unwrap_or(&relationship.from.key),
        };
        Ok(format!(
            "{}.{}",
            quote_ident(binding.alias()),
            quote_ident(column)
        ))
    }

    pub(super) fn render_relationship_type_ref(
        &self,
        variable: &str,
        relationship_type: &str,
    ) -> Result<String, CoreError> {
        let presence = self.render_relationship_presence_ref(variable)?;
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE {} END",
            quote_string_literal(relationship_type)
        ))
    }

    pub(super) fn render_node_labels_ref(
        &self,
        variable: &str,
        label: &str,
    ) -> Result<String, CoreError> {
        let binding = self.validated.binding(variable)?;
        let ValidatedBindingKind::Node(node) = binding.kind() else {
            return Err(CoreError::internal(
                "validated labels expression did not reference a node",
            ));
        };
        if node.label != label {
            return Err(CoreError::internal(
                "validated labels expression did not match the node label",
            ));
        }
        let presence = self.render_binding_presence_ref(variable)?;
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE make_array({}) END",
            quote_string_literal(label)
        ))
    }

    pub(super) fn render_property_keys_ref(&self, variable: &str) -> Result<String, CoreError> {
        let binding = self.validated.binding(variable)?;
        let property_names = match binding.kind() {
            ValidatedBindingKind::Node(node) => node.properties.keys(),
            ValidatedBindingKind::Relationship(relationship) => relationship.properties.keys(),
        }
        .map(|property| quote_string_literal(property))
        .collect::<Vec<_>>()
        .join(", ");
        let presence = self.render_binding_presence_ref(variable)?;
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE make_array({property_names}) END"
        ))
    }

    fn render_relationship_presence_ref(&self, variable: &str) -> Result<String, CoreError> {
        let binding = self.validated.binding(variable)?;
        let ValidatedBindingKind::Relationship(relationship) = binding.kind() else {
            return Err(CoreError::internal(
                "validated relationship type expression did not reference a relationship",
            ));
        };
        let column = relationship
            .key
            .as_deref()
            .unwrap_or(&relationship.from.key);
        Ok(format!(
            "{}.{}",
            quote_ident(binding.alias()),
            quote_ident(column)
        ))
    }

    pub(super) fn render_projection_alias_ref(&self, alias: &str) -> Result<String, CoreError> {
        let projection = self
            .validated
            .plan()
            .projections
            .iter()
            .find(|projection| projection_output_alias(projection) == Some(alias))
            .ok_or_else(|| {
                CoreError::internal(format!(
                    "validated projected predicate referenced unknown alias '{alias}'"
                ))
            })?;
        match projection {
            Projection::Property { property, .. } => self.render_property_ref(property),
            Projection::Key { variable, .. } => self.render_binding_key_ref(variable),
            Projection::ElementId { variable, .. } => self.render_binding_element_id_ref(variable),
            Projection::NodeLabels {
                variable, label, ..
            } => self.render_node_labels_ref(variable, label),
            Projection::PropertyKeys { variable, .. } => self.render_property_keys_ref(variable),
            Projection::RelationshipType {
                variable,
                relationship_type,
                ..
            } => self.render_relationship_type_ref(variable, relationship_type),
            Projection::Literal { literal, .. } => Ok(render_literal(literal)),
            Projection::LiteralList { literals, .. } => Ok(render_literal_list(literals)),
            Projection::Expression { expression, .. } => self.render_scalar_expression(expression),
            Projection::CountAll { .. } => Ok("COUNT(*)".to_string()),
            Projection::Aggregate {
                function,
                target,
                distinct,
                ..
            } => self.render_aggregate_invocation(*function, target, *distinct),
        }
    }

    pub(super) fn render_binding_key_ref(&self, variable: &str) -> Result<String, CoreError> {
        let binding = self.validated.binding(variable)?;
        let key = match binding.kind() {
            ValidatedBindingKind::Node(node) => node.key.as_str(),
            ValidatedBindingKind::Relationship(relationship) => {
                relationship.key.as_deref().ok_or_else(|| {
                    CoreError::internal(
                        "validated aggregate relationship target did not have a key",
                    )
                })?
            }
        };
        Ok(format!(
            "{}.{}",
            quote_ident(binding.alias()),
            quote_ident(key)
        ))
    }

    pub(super) fn render_graph_key_list_ref(
        &self,
        variables: &[String],
    ) -> Result<String, CoreError> {
        let values = variables
            .iter()
            .map(|variable| self.render_binding_key_ref(variable))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(render_sql_array(&values))
    }

    pub(super) fn render_binding_element_id_ref(
        &self,
        variable: &str,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "CAST({} AS VARCHAR)",
            self.render_binding_key_ref(variable)?
        ))
    }

    pub(super) fn render_binding_graph_identity_ref(
        &self,
        variable: &str,
    ) -> Result<String, CoreError> {
        let binding = self.validated.binding(variable)?;
        let prefix = match binding.kind() {
            ValidatedBindingKind::Node(node) => format!("node:{}:", node.label),
            ValidatedBindingKind::Relationship(relationship) => {
                format!("relationship:{}:", relationship.relationship_type)
            }
        };
        let key = self.render_binding_key_ref(variable)?;
        Ok(format!(
            "CASE WHEN {key} IS NULL THEN NULL ELSE concat({}, CAST({key} AS VARCHAR)) END",
            render_literal(&Literal::String(prefix))
        ))
    }

    pub(super) fn render_binding_graph_presence_ref(
        &self,
        variable: &str,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "CAST({} AS VARCHAR)",
            self.render_binding_presence_ref(variable)?
        ))
    }

    pub(super) fn render_property_ref(&self, property: &PropertyRef) -> Result<String, CoreError> {
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

    pub(super) fn render_undirected_endpoint_property_ref(
        &self,
        relationship_variable: &str,
        endpoint: UndirectedRelationshipEndpoint,
        property: &str,
    ) -> Result<String, CoreError> {
        let selection =
            self.render_undirected_endpoint_selection(relationship_variable, endpoint)?;
        let left_property = self.render_property_ref(&PropertyRef {
            variable: selection.left_variable,
            property: property.to_string(),
        })?;
        let right_property = self.render_property_ref(&PropertyRef {
            variable: selection.right_variable,
            property: property.to_string(),
        })?;
        let presence = selection.presence;
        let left_matches_endpoint = selection.left_matches_endpoint;
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE CASE WHEN {left_matches_endpoint} THEN {left_property} ELSE {right_property} END END"
        ))
    }

    pub(super) fn render_undirected_endpoint_key_ref(
        &self,
        relationship_variable: &str,
        endpoint: UndirectedRelationshipEndpoint,
    ) -> Result<String, CoreError> {
        let selection =
            self.render_undirected_endpoint_selection(relationship_variable, endpoint)?;
        let presence = selection.presence;
        let left_matches_endpoint = selection.left_matches_endpoint;
        let left_key = selection.left_key;
        let right_key = selection.right_key;
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE CASE WHEN {left_matches_endpoint} THEN {left_key} ELSE {right_key} END END"
        ))
    }

    pub(super) fn render_undirected_endpoint_element_id_ref(
        &self,
        relationship_variable: &str,
        endpoint: UndirectedRelationshipEndpoint,
    ) -> Result<String, CoreError> {
        Ok(format!(
            "CAST({} AS VARCHAR)",
            self.render_undirected_endpoint_key_ref(relationship_variable, endpoint)?
        ))
    }

    pub(super) fn render_undirected_endpoint_labels_ref(
        &self,
        relationship_variable: &str,
        label: &str,
    ) -> Result<String, CoreError> {
        let presence = self.render_relationship_presence_ref(relationship_variable)?;
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE make_array({}) END",
            quote_string_literal(label)
        ))
    }

    pub(super) fn render_undirected_endpoint_property_keys_ref(
        &self,
        relationship_variable: &str,
    ) -> Result<String, CoreError> {
        let (_, relationship_pattern) =
            self.relationship_pattern_for_variable(relationship_variable)?;
        let binding = self.validated.binding(&relationship_pattern.left)?;
        let ValidatedBindingKind::Node(node) = binding.kind() else {
            return Err(CoreError::internal(
                "validated undirected endpoint keys did not reference a node",
            ));
        };
        let property_names = node
            .properties
            .keys()
            .map(|property| quote_string_literal(property))
            .collect::<Vec<_>>()
            .join(", ");
        let presence = self.render_relationship_presence_ref(relationship_variable)?;
        Ok(format!(
            "CASE WHEN {presence} IS NULL THEN NULL ELSE make_array({property_names}) END"
        ))
    }

    fn relationship_pattern_for_variable(
        &self,
        relationship_variable: &str,
    ) -> Result<(usize, &RelationshipPattern), CoreError> {
        self.validated
            .plan()
            .relationships
            .iter()
            .enumerate()
            .find(|(_, relationship)| {
                relationship.variable.as_deref() == Some(relationship_variable)
            })
            .ok_or_else(|| {
                CoreError::internal("validated undirected endpoint referenced unknown relationship")
            })
    }

    fn render_undirected_endpoint_selection(
        &self,
        relationship_variable: &str,
        endpoint: UndirectedRelationshipEndpoint,
    ) -> Result<UndirectedEndpointSelection, CoreError> {
        let (relationship_index, relationship_pattern) =
            self.relationship_pattern_for_variable(relationship_variable)?;
        let relationship = self.validated.relationship_mapping(relationship_index)?;
        let relationship_alias = self
            .validated
            .relationship_alias(relationship_index, relationship_pattern);
        let endpoint_column = match endpoint {
            UndirectedRelationshipEndpoint::Start => &relationship.from.key,
            UndirectedRelationshipEndpoint::End => &relationship.to.key,
        };
        let selector = format!(
            "{}.{}",
            quote_ident(&relationship_alias),
            quote_ident(endpoint_column)
        );
        let presence = self.render_relationship_presence_ref(relationship_variable)?;
        let left_key = self.render_binding_key_ref(&relationship_pattern.left)?;
        let right_key = self.render_binding_key_ref(&relationship_pattern.right)?;
        Ok(UndirectedEndpointSelection {
            presence,
            left_matches_endpoint: format!("{left_key} = {selector}"),
            left_key,
            right_key,
            left_variable: relationship_pattern.left.clone(),
            right_variable: relationship_pattern.right.clone(),
        })
    }
}
