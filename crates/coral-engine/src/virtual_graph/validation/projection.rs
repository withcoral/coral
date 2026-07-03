//! Projection and ordering validation: RETURN-column shape and scalar-type inference,
//! per-column projectability checks (labels, id/elementId, type, property-keys, graph
//! identity/presence, literal lists), projection-alias resolution, and ORDER BY / DISTINCT
//! ordering rules.

#[allow(
    clippy::allow_attributes,
    clippy::wildcard_imports,
    reason = "Projection validation methods are split into a child module while preserving parent-private access."
)]
use super::*;

#[allow(
    clippy::allow_attributes,
    clippy::elidable_lifetime_names,
    reason = "Keep the split validation impl shape aligned with the parent GraphPlanValidator impl."
)]
impl<'a> GraphPlanValidator<'a> {
    pub(super) fn validate_projection_shape(&self) -> Result<(), CoreError> {
        if self.plan.projections.is_empty() {
            return Err(Diagnostic::new(
                diagnostic_codes::EMPTY_PROJECTION,
                "projections",
                "at least one projection is required",
            )
            .into_core_error());
        }
        Ok(())
    }

    pub(super) fn projection_scalar_types(&self) -> Result<Vec<ScalarType>, CoreError> {
        self.plan
            .projections
            .iter()
            .enumerate()
            .map(|(index, projection)| {
                self.infer_projection_scalar_type(projection, format!("projections[{index}]"))
            })
            .collect()
    }

    fn infer_projection_scalar_type(
        &self,
        projection: &Projection,
        path: impl Into<String>,
    ) -> Result<ScalarType, CoreError> {
        let path = path.into();
        match projection {
            Projection::Property { property, .. } => {
                self.validate_property_ref(property, &path)?;
                self.property_ref_scalar_type(property)
            }
            Projection::Key { variable, .. } => {
                self.validate_key_projection(variable, &path)?;
                self.key_scalar_type(variable)
            }
            Projection::ElementId { variable, .. } => {
                self.validate_element_id_projection(variable, &path)?;
                Ok(ScalarType::String)
            }
            Projection::NodeLabels {
                variable, label, ..
            } => {
                self.validate_node_labels_projection(variable, label, &path)?;
                Ok(ScalarType::Other)
            }
            Projection::PropertyKeys { variable, .. } => {
                self.validate_property_keys_projection(variable, &path)?;
                Ok(ScalarType::Other)
            }
            Projection::RelationshipType {
                variable,
                relationship_type,
                ..
            } => {
                self.validate_relationship_type_projection(variable, relationship_type, &path)?;
                Ok(ScalarType::String)
            }
            Projection::Literal { literal, .. } => Ok(literal_scalar_type(literal)),
            Projection::LiteralList { literals, .. } => {
                Self::validate_literal_list_projection(literals, &path)?;
                Ok(ScalarType::Other)
            }
            Projection::Expression { expression, .. } => {
                self.infer_scalar_expression_type(expression, &path)
            }
            Projection::CountAll { .. } => Ok(ScalarType::Integer),
            Projection::Aggregate {
                function, target, ..
            } => self.infer_aggregate_projection_type(*function, target, &path),
        }
    }

    pub(super) fn validate_distinct_ordering(&self) -> Result<(), CoreError> {
        if !self.plan.distinct || self.plan.order_by.is_empty() {
            return Ok(());
        }

        let projected_properties = self.projected_properties();
        for (index, order_key) in self.plan.order_by.iter().enumerate() {
            if !self.order_expression_is_projected_property_or_alias(
                &order_key.expression,
                &projected_properties,
            ) {
                return Err(Diagnostic::new(
                    diagnostic_codes::UNSUPPORTED_DISTINCT_ORDERING,
                    format!("order_by[{index}]"),
                    "ORDER BY with DISTINCT must use a projected property or projection alias",
                )
                .into_core_error());
            }
        }
        Ok(())
    }

    pub(super) fn projected_properties(&self) -> Vec<&PropertyRef> {
        self.plan
            .projections
            .iter()
            .filter_map(|projection| match projection {
                Projection::Property { property, .. } => Some(property),
                Projection::Key { .. }
                | Projection::ElementId { .. }
                | Projection::NodeLabels { .. }
                | Projection::PropertyKeys { .. }
                | Projection::RelationshipType { .. }
                | Projection::Literal { .. }
                | Projection::LiteralList { .. }
                | Projection::Expression { .. }
                | Projection::CountAll { .. }
                | Projection::Aggregate { .. } => None,
            })
            .collect()
    }

    pub(super) fn validate_order_expression(
        &self,
        expression: &OrderExpression,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        match expression {
            OrderExpression::Property(property) => {
                self.validate_property_ref(property, format!("{path}.property"))
            }
            OrderExpression::Key { variable } => {
                self.validate_key_projection(variable, format!("{path}.variable"))
            }
            OrderExpression::ElementId { variable } => {
                self.validate_element_id_projection(variable, format!("{path}.variable"))
            }
            OrderExpression::NodeLabels { variable, label } => {
                self.validate_node_labels_projection(variable, label, format!("{path}.variable"))
            }
            OrderExpression::PropertyKeys { variable } => {
                self.validate_property_keys_projection(variable, format!("{path}.variable"))
            }
            OrderExpression::RelationshipType {
                variable,
                relationship_type,
            } => self.validate_relationship_type_projection(
                variable,
                relationship_type,
                format!("{path}.variable"),
            ),
            OrderExpression::CountAll | OrderExpression::Literal(_) => Ok(()),
            OrderExpression::Aggregate {
                function, target, ..
            } => self
                .infer_aggregate_projection_type(*function, target, &format!("{path}.aggregate"))
                .map(|_| ()),
            OrderExpression::Scalar(expression) => {
                self.validate_scalar_expression(expression, format!("{path}.expression"))
            }
            OrderExpression::ProjectionAlias(alias) => {
                if self.projection_alias_exists(alias) {
                    Ok(())
                } else {
                    Err(Diagnostic::new(
                        diagnostic_codes::UNKNOWN_PROJECTION_ALIAS,
                        path,
                        format!("unknown projection alias '{alias}'"),
                    )
                    .into_core_error())
                }
            }
        }
    }

    fn order_expression_is_projected_property_or_alias(
        &self,
        expression: &OrderExpression,
        projected_properties: &[&PropertyRef],
    ) -> bool {
        match expression {
            OrderExpression::Property(property) => projected_properties.contains(&property),
            OrderExpression::Key { variable } => {
                self.plan.projections.iter().any(|projection| {
                    matches!(projection, Projection::Key { variable: projected, .. } if projected == variable)
                })
            }
            OrderExpression::ElementId { variable } => self.plan.projections.iter().any(|projection| {
                matches!(projection, Projection::ElementId { variable: projected, .. } if projected == variable)
            }),
            OrderExpression::RelationshipType {
                variable,
                relationship_type,
            } => self.plan.projections.iter().any(|projection| {
                matches!(
                    projection,
                    Projection::RelationshipType {
                        variable: projected,
                        relationship_type: projected_type,
                        ..
                    } if projected == variable && projected_type == relationship_type
                )
            }),
            OrderExpression::NodeLabels { variable, label } => {
                self.plan.projections.iter().any(|projection| {
                    matches!(
                        projection,
                        Projection::NodeLabels {
                            variable: projected,
                            label: projected_label,
                            ..
                        } if projected == variable && projected_label == label
                    )
                })
            }
            OrderExpression::PropertyKeys { variable } => {
                self.plan.projections.iter().any(|projection| {
                    matches!(
                        projection,
                        Projection::PropertyKeys {
                            variable: projected,
                            ..
                        } if projected == variable
                    )
                })
            }
            OrderExpression::Literal(literal) => {
                self.plan.projections.iter().any(|projection| {
                    matches!(projection, Projection::Literal { literal: projected, .. } if projected == literal)
                })
            }
            OrderExpression::Scalar(expression) => {
                self.plan.projections.iter().any(|projection| {
                    matches!(
                        projection,
                        Projection::Expression {
                            expression: projected,
                            ..
                        } if projected == expression
                    )
                })
            }
            OrderExpression::ProjectionAlias(alias) => self.projection_alias_exists(alias),
            OrderExpression::CountAll | OrderExpression::Aggregate { .. } => false,
        }
    }

    pub(super) fn order_expression_is_projected_property_alias_or_aggregate(
        &self,
        expression: &OrderExpression,
        projected_properties: &[&PropertyRef],
    ) -> bool {
        matches!(
            expression,
            OrderExpression::CountAll | OrderExpression::Aggregate { .. }
        ) || self.order_expression_is_projected_property_or_alias(expression, projected_properties)
    }

    fn projection_alias_exists(&self, alias: &str) -> bool {
        self.find_projection_alias(alias).is_some()
    }

    fn find_projection_alias(&self, alias: &str) -> Option<(usize, &Projection)> {
        self.plan
            .projections
            .iter()
            .enumerate()
            .find(|(_, projection)| {
                projection_alias_name(projection).is_some_and(|name| name == alias)
            })
    }

    pub(super) fn projection_alias_scalar_type(
        &self,
        alias: &str,
        path: impl Into<String>,
    ) -> Result<ScalarType, CoreError> {
        let path = path.into();
        let Some((index, projection)) = self.find_projection_alias(alias) else {
            return Err(Diagnostic::new(
                diagnostic_codes::UNKNOWN_PROJECTION_ALIAS,
                path,
                format!("unknown projection alias '{alias}'"),
            )
            .into_core_error());
        };
        self.infer_projection_scalar_type(projection, format!("projections[{index}]"))
    }

    pub(super) fn validate_node_labels_projection(
        &self,
        variable: &str,
        label: &str,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        let binding = self.bindings.get(variable).ok_or_else(|| {
            Diagnostic::new(
                diagnostic_codes::UNKNOWN_VARIABLE,
                path.clone(),
                format!("unknown graph variable '{variable}'"),
            )
            .into_core_error()
        })?;
        let ValidatedBindingKind::Node(node) = binding.kind() else {
            return Err(Diagnostic::new(
                diagnostic_codes::INVALID_LABELS_PROJECTION,
                path,
                format!("labels({variable}) requires a node variable"),
            )
            .into_core_error());
        };
        if node.label != label {
            return Err(Diagnostic::new(
                diagnostic_codes::INVALID_LABELS_PROJECTION,
                path,
                format!(
                    "labels({variable}) expected node label '{}', got '{label}'",
                    node.label
                ),
            )
            .into_core_error());
        }
        Ok(())
    }

    pub(super) fn validate_property_keys_projection(
        &self,
        variable: &str,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        validate_variable(path.clone(), variable)?;
        self.bindings.get(variable).map(|_| ()).ok_or_else(|| {
            Diagnostic::new(
                diagnostic_codes::UNKNOWN_VARIABLE,
                path,
                format!("unknown graph variable '{variable}'"),
            )
            .into_core_error()
        })
    }

    pub(super) fn validate_key_projection(
        &self,
        variable: &str,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        let binding = self.bindings.get(variable).ok_or_else(|| {
            Diagnostic::new(
                diagnostic_codes::UNKNOWN_VARIABLE,
                path.clone(),
                format!("unknown graph variable '{variable}'"),
            )
            .into_core_error()
        })?;
        match binding.kind() {
            ValidatedBindingKind::Node(_) => Ok(()),
            ValidatedBindingKind::Relationship(relationship) => {
                if relationship.key.is_some() {
                    Ok(())
                } else {
                    Err(Diagnostic::new(
                        diagnostic_codes::INVALID_KEY_PROJECTION,
                        path,
                        format!(
                            "id({variable}) requires relationship type '{}' to declare a key column",
                            relationship.relationship_type
                        ),
                    )
                    .into_core_error())
                }
            }
        }
    }

    pub(super) fn validate_element_id_projection(
        &self,
        variable: &str,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        let binding = self.bindings.get(variable).ok_or_else(|| {
            Diagnostic::new(
                diagnostic_codes::UNKNOWN_VARIABLE,
                path.clone(),
                format!("unknown graph variable '{variable}'"),
            )
            .into_core_error()
        })?;
        match binding.kind() {
            ValidatedBindingKind::Node(_) => Ok(()),
            ValidatedBindingKind::Relationship(relationship) => {
                if relationship.key.is_some() {
                    Ok(())
                } else {
                    Err(Diagnostic::new(
                        diagnostic_codes::INVALID_ELEMENT_ID_PROJECTION,
                        path,
                        format!(
                            "elementId({variable}) requires relationship type '{}' to declare a key column",
                            relationship.relationship_type
                        ),
                    )
                    .into_core_error())
                }
            }
        }
    }

    pub(super) fn validate_graph_identity_projection(
        &self,
        variable: &str,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        let binding = self.bindings.get(variable).ok_or_else(|| {
            Diagnostic::new(
                diagnostic_codes::UNKNOWN_VARIABLE,
                path.clone(),
                format!("unknown graph variable '{variable}'"),
            )
            .into_core_error()
        })?;
        match binding.kind() {
            ValidatedBindingKind::Node(_) => Ok(()),
            ValidatedBindingKind::Relationship(relationship) => {
                if relationship.key.is_some() {
                    Ok(())
                } else {
                    Err(Diagnostic::new(
                        diagnostic_codes::INVALID_GRAPH_IDENTITY_PROJECTION,
                        path,
                        format!(
                            "graph identity for relationship variable '{variable}' requires relationship type '{}' to declare a key column",
                            relationship.relationship_type
                        ),
                    )
                    .into_core_error())
                }
            }
        }
    }

    pub(super) fn validate_graph_presence_projection(
        &self,
        variable: &str,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        validate_variable(path.clone(), variable)?;
        self.bindings.get(variable).map(|_| ()).ok_or_else(|| {
            Diagnostic::new(
                diagnostic_codes::UNKNOWN_VARIABLE,
                path,
                format!("unknown graph variable '{variable}'"),
            )
            .into_core_error()
        })
    }

    pub(super) fn validate_relationship_type_projection(
        &self,
        variable: &str,
        relationship_type: &str,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        let binding = self.bindings.get(variable).ok_or_else(|| {
            Diagnostic::new(
                diagnostic_codes::UNKNOWN_VARIABLE,
                path.clone(),
                format!("unknown graph variable '{variable}'"),
            )
            .into_core_error()
        })?;
        let ValidatedBindingKind::Relationship(relationship) = binding.kind() else {
            return Err(Diagnostic::new(
                diagnostic_codes::INVALID_TYPE_PROJECTION,
                path,
                format!("type({variable}) requires a relationship variable"),
            )
            .into_core_error());
        };
        if relationship.relationship_type != relationship_type {
            return Err(Diagnostic::new(
                diagnostic_codes::INVALID_TYPE_PROJECTION,
                path,
                format!(
                    "type({variable}) expected relationship type '{}', got '{relationship_type}'",
                    relationship.relationship_type
                ),
            )
            .into_core_error());
        }
        Ok(())
    }

    pub(super) fn validate_literal_list_projection(
        literals: &[Literal],
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        if literals.is_empty() {
            return Err(Diagnostic::new(
                diagnostic_codes::INVALID_LITERAL_LIST_PROJECTION,
                path,
                "literal list projections require at least one element",
            )
            .into_core_error());
        }

        let mut expected = None;
        for literal in literals {
            let Some(kind) = literal_list_element_kind(literal) else {
                continue;
            };
            match expected {
                Some(expected) if expected != kind => {
                    return Err(Diagnostic::new(
                        diagnostic_codes::INVALID_LITERAL_LIST_PROJECTION,
                        path,
                        "literal list projections require all non-null elements to have the same type",
                    )
                    .into_core_error());
                }
                Some(_) => {}
                None => expected = Some(kind),
            }
        }

        if expected.is_none() {
            return Err(Diagnostic::new(
                diagnostic_codes::INVALID_LITERAL_LIST_PROJECTION,
                path,
                "literal list projections require at least one non-null element",
            )
            .into_core_error());
        }

        Ok(())
    }

    pub(super) fn validate_typed_literal_list(
        literals: &[Literal],
        element_type: LiteralListElementType,
        path: impl Into<String>,
    ) -> Result<(), CoreError> {
        let path = path.into();
        for literal in literals {
            let Some(kind) = literal_list_element_kind(literal) else {
                continue;
            };
            if kind != element_type {
                return Err(Diagnostic::new(
                    diagnostic_codes::INVALID_TYPED_LITERAL_LIST,
                    path,
                    "typed literal lists require all non-null elements to match the declared element type",
                )
                .into_core_error());
            }
        }
        Ok(())
    }
}
